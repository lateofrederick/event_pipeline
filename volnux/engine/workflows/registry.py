import logging
import typing
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from volnux.base import RetryPolicy, get_event_registry
from volnux.exceptions import ImproperlyConfigured
from volnux.parser.options import Options
from volnux.registry import RegistryNotReady
from volnux.result import EventResult
from volnux.utils import get_function_call_args

logger = logging.getLogger(__name__)


if typing.TYPE_CHECKING:
    from volnux import Event

    from .workflow import WorkflowConfig


def get_system_events() -> List[typing.Type["Event"]]:
    from volnux.base import EventType

    system_events = []
    event_registry = get_event_registry()
    for event in event_registry.list_all_classes():
        event = typing.cast(typing.Type["Event"], event)
        if event.event_type == EventType.SYSTEM:
            system_events.append(event)

    return system_events


SYSTEM_EVENTS = get_system_events()


class RegistrySource(Enum):
    """Source types for workflow registries."""

    LOCAL = "local"
    PYPI = "pypi"
    GIT = "git"

    def loader(self) -> typing.Optional[typing.Type["Event"]]:
        if not SYSTEM_EVENTS:
            return None
        for event in SYSTEM_EVENTS:
            name = getattr(event, "name", None)
            if name and name == self.value:
                return event
        return None


@dataclass
class SourceCredentials:
    """Credentials for registry authentication."""

    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    email: Optional[str] = None

    def is_valid(self) -> bool:
        """Check if credentials are valid."""
        return bool(self.token or (self.username and self.password))


@dataclass
class WorkflowSource:
    """Configuration for remote workflow sources."""

    name: str
    source_type: RegistrySource
    location: str  # URL, package name, or path
    version: Optional[str] = None
    credentials: Optional[SourceCredentials] = None
    timeout: int = 30000
    retries: int = 3
    priority: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.source_type, str):
            self.source_type = RegistrySource(self.source_type)


class WorkflowRegistry:
    """
    Registry for managing workflow configurations.

    Supports both LOCAL and REMOTE workflow sources:
    - LOCAL: Workflows from local filesystem
    - PYPI: Workflows installed as Python packages
    - GIT: Workflows from any Git repository
    """

    def __init__(self, cache_dir: Optional[Path] = None):
        self._workflows: Dict[str, "WorkflowConfig"] = {}
        self._workflow_source: typing.Optional[WorkflowSource] = None
        self._ready = False

        # Cache directory for remote workflows
        self._cache_dir = cache_dir or Path.home() / ".workflow_cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def name(self) -> str:
        if not self._ready:
            raise RegistryNotReady("No workflow source configured")
        return self._workflow_source.name

    def register(self, workflow_config: "WorkflowConfig"):
        """Register a workflow configuration."""
        if workflow_config.name in self._workflows:
            raise ValueError(f"Workflow '{workflow_config.name}' already registered")
        self._workflows[workflow_config.name] = workflow_config
        if not self.is_ready():
            self.make_ready()

    def get_workflow_config(self, name: str) -> Optional["WorkflowConfig"]:
        """Get a workflow configuration by name."""
        return self._workflows.get(name)

    def get_workflow_configs(self) -> List["WorkflowConfig"]:
        """Get all registered workflow configurations."""
        return list(self._workflows.values())

    def add_workflow_source(self, source: WorkflowSource):
        """Add a remote workflow source."""
        self._workflow_source[source.name] = source
        logger.info(f"Added remote source: {source.name} ({source.source_type.value})")

    def check_all(self) -> Dict[str, List[str]]:
        """Run infrastructure checks on all workflows."""
        all_issues = {}
        for name, workflow in self._workflows.items():
            issues = workflow.check()
            if issues:
                all_issues[name] = issues
        return all_issues

    def is_ready(self) -> bool:
        """Check if registry is ready."""
        return self._ready

    def make_ready(self) -> None:
        self._ready = True

    def load_workflows_from_source(self) -> EventResult:
        """
        Load workflows from source.
        Return:
            EventResult. Result of loader execution.
        Raises:
            ImproperlyConfigured: If workflow source is not configured.
        """
        if not self.is_ready():
            loader_class = self._workflow_source.source_type.loader()
            if loader_class is None:
                raise ImproperlyConfigured(
                    f"No valid loader found for source type {self._workflow_source.name}"
                )

            # setup retry policy
            if self._workflow_source.retries > 0:
                retry_policy = RetryPolicy(
                    max_attempts=self._workflow_source.retries,
                    retry_on_exceptions=[Exception],
                )

                setattr(loader_class, "retry_policy", retry_policy)

            loader = loader_class(
                None,
                self._workflow_source.name,
                options=Options.from_dict({"cache_dir": self._cache_dir}),
            )
            kwargs = {
                "location": self._workflow_source.location,
                "credentials": self._workflow_source.credentials,
                "timeout": self._workflow_source.timeout,
                "retries": self._workflow_source.retries,
                "workflows_dir": self._workflow_source.location,
                "registry": self,
                "version": self._workflow_source.version,
                **self._workflow_source.metadata,
            }

            actual_kwargs = get_function_call_args(loader.process, kwargs)
            try:
                return loader(**actual_kwargs)
            except Exception as e:
                logger.debug(
                    f"Failed to load workflow source '{self._workflow_source.name}': %s",
                    e,
                )

        return EventResult(
            error=True,
            event_name=self.name,
            content=f"Failed to load workflow source '{self._workflow_source.name}'",
            task_id=self.name,
        )

    def get_events(self):
        if not self.is_ready():
            raise RegistryNotReady("No workflow not ready.")

        events = []

        event_registry = get_event_registry()
        for workflow in self._workflows.values():
            if workflow.module:
                events.extend(event_registry.get_classes_for_module(workflow.module))
        return events

    def get_pipeline(self):
        pass
