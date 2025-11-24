"""
Workflow Configuration System - Pure Registry Pattern

WorkflowConfig is ONLY for infrastructure/registry configuration (like Django's AppConfig).
User business logic (steps, pipelines, events) lives in workflow files.

Structure:
workflows/
├── docker_registry/
│   ├── __init__.py
│   ├── workflow.py      # WorkflowConfig - ONLY registries/infrastructure
│   ├── event.py         # USER CODE - event definitions
│   ├── pipeline.py      # USER CODE - pipeline logic
│   └── pointy.pty       # USER CODE - workflow structure
"""

import importlib
import logging
import typing
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .registry import RegistrySource, WorkflowRegistry, WorkflowSource

logger = logging.getLogger(__name__)


class WorkflowConfig(ABC):
    """
    Base class for workflow configuration.

    RESPONSIBILITY: Register infrastructure resources (registries, credentials, settings)
    NOT RESPONSIBLE FOR: Business logic, workflow steps, pipeline definitions

    Like Django's AppConfig:
    - Manages configuration and resources
    - Provides infrastructure to user code
    - Does NOT contain business logic

    Example:
        class SimpleConfig(WorkflowConfig):
            name = 'simple'
            verbose_name = 'Simple Configuration'

            def ready(self):
                # Register registries (infrastructure)
                self.register_registry(...)

                # Set defaults
                self.default_timeout = 60000
    """

    # Attributes to override in subclass
    name: str = None
    verbose_name: Optional[str] = None
    version: str = "1.0.0"
    mode: typing.Literal["DAG", "CFG"] = "CFG"

    # Paths (automatically set by registry)
    path: Optional[Path] = None
    module: Optional[str] = None

    # Default settings (can override)
    default_timeout: int = 300000
    default_retries: int = 3
    default_auto_cleanup: bool = False

    def __init__(self, workflow_path: Optional[Path] = None):
        """Initialize workflow configuration."""
        if self.name is None:
            raise ValueError("WorkflowConfig.name must be set")

        if self.verbose_name is None:
            self.verbose_name = self.name.replace("_", " ").title()

        self.path = workflow_path
        if self.path is None:
            pass

        # Registry storage
        self._registries: Dict[str, "WorkflowRegistry"] = {}
        self._registry_sources: Dict[str, "WorkflowSource"] = {}
        self._settings: Dict[str, Any] = {}

        # User code modules (loaded but not executed by config)
        self._event_module = None
        self._pipeline_module = None
        self._structure = None

        # Discover user code files
        self._discover_user_code()

        # Call ready hook for infrastructure setup
        self.load_local()
        self.ready()

    def load_local(self):
        self.register_registry_source(
            WorkflowSource(
                name=self.name,
                location=self.path,  # type: ignore
                source_type=RegistrySource.LOCAL,
                version=self.version,
            )
        )

    @abstractmethod
    def ready(self):
        """
        Override this to register infrastructure resources.

        SHOULD DO:
        - Register registries
        - Set default configurations
        - Initialize connections
        - Load environment variables
        """
        pass

    def _init_registries(self):
        """Initialize registries."""
        for source in self._registry_sources.values():
            registry = WorkflowRegistry()
            registry._workflow_source = source
            self._registries[source.name] = registry

    def _get_unready_registries(self) -> Set[WorkflowRegistry]:
        """Get unready registries."""
        registries = set()
        for registry in self._registries.values():
            if not registry.is_ready():
                registries.add(registry)

        return registries

    def pull_workflows_from_registry_source(self) -> bool:
        """
        Pull workflows from all registry sources concurrently.

        Returns:
            bool: True if all workflows were loaded successfully, False otherwise.

        Raises:
            ValueError: If no registries were initialized.
        """
        self._init_registries()

        if not self._registries:
            raise ValueError("No registries were initialized.")

        unready_registries = self._get_unready_registries()
        if not unready_registries:
            logger.info("All registries are already ready. No workflows to pull.")
            return True

        logger.info(f"Loading workflows from {len(unready_registries)} registries...")

        success_count = 0
        failed_registries: List[tuple] = []

        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_registry = {
                executor.submit(self._load_registry_workflows, reg): reg
                for reg in unready_registries
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_registry):
                registry = future_to_registry[future]
                try:
                    future.result(timeout=self.default_timeout)
                    success_count += 1
                    logger.debug(
                        f"Successfully loaded workflows from registry: {registry}"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to load workflows from registry {registry}: {e}",
                        exc_info=True,
                    )
                    failed_registries.append((registry, str(e)))

        # Log summary
        total = len(unready_registries)
        logger.info(
            f"Workflow loading complete: {success_count}/{total} succeeded, "
            f"{len(failed_registries)} failed"
        )

        if failed_registries:
            logger.warning(
                f"Failed registries: {[str(reg) for reg, _ in failed_registries]}"
            )

        return len(failed_registries) == 0

    def _load_registry_workflows(self, registry: "WorkflowRegistry") -> None:
        """
        Load workflows from a single registry.

        Args:
            registry: The workflow registry to load from.

        Raises:
            Exception: If loading fails for any reason.
        """
        registry.load_workflows_from_source()

    def register_registry_source(self, source: "WorkflowSource"):
        """Register a registry (infrastructure resource)."""
        if source.name in self._registry_sources:
            raise ValueError(f"Registry '{source.name}' already registered")
        self._registry_sources[source.name] = source

    def get_registry(self, name: str) -> Optional["WorkflowRegistry"]:
        """Get a registry by name."""
        return self._registries.get(name)

    def get_registries(
        self, registry_source: Optional[RegistrySource] = None
    ) -> List["WorkflowRegistry"]:
        """Get all registries, optionally filtered by type."""
        registries = list(self._registries.values())
        if registry_source:
            registries = [
                r
                for r in registries
                if r._workflow_source.source_type == registry_source
            ]
        return sorted(registries, key=lambda r: r.priority)

    def set_setting(self, key: str, value: Any):
        """Set a configuration setting."""
        self._settings[key] = value

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a configuration setting."""
        return self._settings.get(key, default)

    def _discover_user_code(self):
        """
        Discover user code modules (event.py, pipeline.py, pointy.pty).
        Config only LOADS these, it doesn't define their content.
        """
        if not self.path or not self.module:
            return

        # Load event module (user code)
        try:
            self._event_module = importlib.import_module(f"{self.module}.event")
        except ImportError:
            pass

        # Load pipeline module (user code)
        try:
            self._pipeline_module = importlib.import_module(f"{self.module}.pipeline")
        except ImportError:
            pass

        # Load structure file (user code)
        pty_file = self.path / "pointy.pty"
        if pty_file.exists():
            with open(pty_file, "r") as f:
                self._structure = f.read()

    def get_event_module(self):
        """Get the event module (user code)."""
        return self._event_module

    def get_pipeline_module(self):
        """Get the pipeline module (user code)."""
        return self._pipeline_module

    def get_structure(self) -> Optional[str]:
        """Get the workflow structure definition (user code)."""
        return self._structure

    def check(self) -> List[str]:
        """
        Check configuration for issues.
        Only validates infrastructure, not user business logic.
        """
        issues = []

        # Check registries
        if not self._registries:
            issues.append(f"Workflow '{self.name}' has no registries registered")

        for name, registry in self._registry_sources.items():
            if not registry.location:
                issues.append(f"Registry '{name}' has no location configured")

            if registry.credentials and not registry.credentials.is_valid():
                issues.append(f"Registry '{name}' has invalid credentials")

        # Check user code exists (but don't validate its logic)
        if not self._event_module:
            issues.append(f"Workflow '{self.name}' has no event.py file")

        if not self._pipeline_module:
            issues.append(f"Workflow '{self.name}' has no pipeline.py file")

        if not self._structure:
            issues.append(f"Workflow '{self.name}' has no pointy.pty file")

        return issues
