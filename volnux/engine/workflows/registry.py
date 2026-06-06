import logging
import typing
import os
import asyncio
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from collections import ChainMap
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

from volnux.event.base import get_event_registry
from volnux import __version__ as version
from volnux.exceptions import ImproperlyConfigured
from volnux.event.registry import RegistryNotReady
from volnux.result import EventResult
from .source import WorkflowSource, RegistrySource

__all__ = ["get_workflow_registry"]

logger = logging.getLogger(__name__)


if typing.TYPE_CHECKING:
    from .workflow import WorkflowConfig


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
        self._workflow_local_sources: Dict[str, WorkflowSource] = {}
        self._workflow_remote_sources: Dict[str, WorkflowSource] = {}

        self._ready = False
        self._loading = False

        # Cache directory for remote workflows
        self._cache_dir = cache_dir or Path.home() / ".volnux" / "workflow_cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def combined_workflow_sources(self) -> ChainMap[str, WorkflowSource]:
        return ChainMap(self._workflow_local_sources, self._workflow_remote_sources)

    def register(self, workflow_config: "WorkflowConfig"):
        """Register a workflow configuration."""
        if workflow_config.name in self._workflows:
            raise ValueError(f"Workflow '{workflow_config.name}' already registered")
        self._workflows[workflow_config.name] = workflow_config

    def get_workflow_config(self, name: str) -> Optional["WorkflowConfig"]:
        """Get a workflow configuration by name."""
        return self._workflows.get(name)

    def get_workflow_source(self, name: str) -> Optional["WorkflowSource"]:
        """Get a workflow source by name."""
        return self.combined_workflow_sources.get(name)

    def get_workflow_configs(self) -> List["WorkflowConfig"]:
        """Get all registered workflow configurations."""
        return list(self._workflows.values())

    def add_workflow_source(self, source: WorkflowSource):
        """Add a workflow source."""
        sources = self.combined_workflow_sources
        if source.name in sources:
            raise ValueError(f"Workflow source '{source.name}' already registered")

        if source.source_type == RegistrySource.LOCAL:
            self._workflow_local_sources[source.name] = source
        else:
            self._workflow_remote_sources[source.name] = source

        logger.info(
            "Added workflow source: %s (%s)",
            source.name,
            source.source_type.value,
        )

    def check_all(self) -> Dict[str, List[str]]:
        """Run infrastructure checks on all workflows."""
        all_issues: Dict[str, List[str]] = {}
        for name, workflow in self._workflows.items():
            issues = workflow.check()
            if issues:
                all_issues[name] = issues
        return all_issues

    def is_ready(self) -> bool:
        """Check if the registry is ready."""
        return not self._loading and self._ready

    def make_ready(self) -> None:
        self._ready = True

    async def populate_local_workflow_configs(
        self, project_dir: Path, workflow_name: typing.Optional[str] = None
    ) -> None:
        """
        Populate local workflow configurations.

        Args:
            project_dir: Project root directory.
            workflow_name: Name of the workflow to load. If None, load all workflows.
        """
        workflows_root = project_dir / "workflows"
        if not workflows_root.exists():
            logger.warning("Workflows directory does not exist: %s", workflows_root)
            return

        if not workflows_root.is_dir():
            raise ImproperlyConfigured(f"Not a directory: {workflows_root}")

        for workflow_dir in workflows_root.iterdir():
            if not workflow_dir.is_dir():
                continue

            dirname = workflow_dir.name
            if workflow_name is not None and workflow_name != dirname:
                continue

            local = WorkflowSource(
                name=dirname,
                location=workflow_dir,
                source_type=RegistrySource.LOCAL,
                version=version,
            )
            self._workflow_local_sources[dirname] = local

    async def load_workflow_configs(self) -> None:
        """
        Load workflows from all configured sources.
        """
        self._loading = True

        try:
            sources = list(self.combined_workflow_sources.values())
            if not sources:
                logger.warning("No workflow sources to load")
                return

            params = {"cache_dir": self._cache_dir}

            loop = asyncio.get_running_loop()
            with ThreadPoolExecutor(max_workers=min(4, len(sources))) as executor:
                futures = [
                    loop.run_in_executor(
                        executor, self.process_workflow_source, source, self, params
                    )
                    for source in sources
                ]

                results = await asyncio.gather(*futures, return_exceptions=True)

                for source, result in zip(sources, results):
                    if isinstance(result, BaseException):
                        logger.error(
                            "Failed to load config from %s: %s",
                            source.name,
                            result,
                            exc_info=True,
                        )
                    else:
                        logger.debug("Successfully loaded config from %s", source.name)

            if self._workflows:
                self.make_ready()

        finally:
            self._loading = False

    @staticmethod
    async def process_workflow_source(
        workflow_source: WorkflowSource, registry, params: Dict[str, Any]
    ) -> EventResult:
        """
        Process a workflow source.
        """
        return await workflow_source.load_workflow_config(registry, params)

    def get_events(self):
        if not self.is_ready():
            raise RegistryNotReady("Workflow registry is not ready yet.")

        events = []
        event_registry = get_event_registry()

        for workflow in self._workflows.values():
            if workflow.module:
                events.extend(event_registry.get_classes_for_module(workflow.module))

        return events

    def get_pipeline(self):
        raise NotImplementedError(
            "WorkflowRegistry.get_pipeline() is not implemented yet."
        )


_workflow_registry = WorkflowRegistry(cache_dir=os.environ.get("WORKFLOWS_CACHE_DIR"))


def get_workflow_registry() -> WorkflowRegistry:
    return _workflow_registry
