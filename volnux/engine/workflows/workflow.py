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
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .registry import (
    RegistrySource,
    WorkflowRegistry,
    WorkflowSource,
    get_workflow_registry,
)

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
        self._registry = None
        # self._registries: Dict[str, "WorkflowRegistry"] = {}
        # self._registry_sources: Dict[str, "WorkflowSource"] = {}
        self._settings: Dict[str, Any] = {}

        # User code modules (loaded but not executed by config)
        self._event_module = None
        self._pipeline_module = None
        self._structure = None

        # Discover user code files
        self._discover_user_code()

        # Call ready hook for infrastructure setup
        self.ready()

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

    def get_registry(self):
        if self._registry is None:
            self._registry = get_workflow_registry()
        return self._registry

    def register_registry_source(self, source: "WorkflowSource") -> None:
        """Register a registry source (infrastructure resource)."""
        self.get_registry().add_workflow_source(source)

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
        # if not self.get_registry().get_workflow_config(self.name):
        #     issues.append(f"Workflow '{self.name}' has no registries registered")
        #
        # for name, registry in self.get_registry().get_workflow_source(self.name):
        #     if not registry.location:
        #         issues.append(f"Registry '{name}' has no location configured")
        #
        #     if registry.credentials and not registry.credentials.is_valid():
        #         issues.append(f"Registry '{name}' has invalid credentials")

        # Check user code exists (but don't validate its logic)
        if not self._event_module:
            issues.append(f"Workflow '{self.name}' has no event.py file")

        if not self._pipeline_module:
            issues.append(f"Workflow '{self.name}' has no pipeline.py file")

        if not self._structure:
            issues.append(f"Workflow '{self.name}' has no pointy.pty file")

        return issues
