import asyncio
import types
from typing import Optional, Tuple, cast
from pathlib import Path

from volnux.setup import initialise_workflows
from volnux.import_utils import load_module_from_path
from volnux.exceptions import CommandError
from volnux.engine.workflows.trigger.engine import TriggerEngine, WorkflowExecutionError


class ProjectMixin:
    """
    Provides functionality for workflows initialization, configuration loading, and
    project context handling.
    """

    async def _initialise_workflows(
        self, project_dir: Path, workflow_name: Optional[str] = None
    ) -> TriggerEngine:
        """
        Initialise the workflow registry.
        :param project_dir: Project directory
        :param workflow_name: workflow name
        :return: Registry
        """
        if workflow_name is None:
            workflows_initialiser = load_module_from_path(
                "initialiser", project_dir / "init.py"
            )
            if not workflows_initialiser:
                raise CommandError(
                    f"Failed to load workflow initializer module from path: {project_dir / 'init.py'}"
                )

            engine = cast(TriggerEngine, await workflows_initialiser.engine)
        else:
            engine = await initialise_workflows(project_dir, workflow_name)

        try:
            workflows_registry = engine.get_workflow_registry()
        except WorkflowExecutionError:
            raise CommandError(
                "Workflow executor was not provided. The framework was not initialized."
            )

        if not workflows_registry.is_ready():
            raise CommandError("Workflow registry is not ready yet, try again later.")
        return engine

    def initialise_workflows(
        self, project_dir: Path, workflow_name: Optional[str] = None
    ) -> TriggerEngine:
        """Initialise the workflows engine. Handles async execution for CLI context."""
        try:
            return asyncio.run(self._initialise_workflows(project_dir, workflow_name))
        except Exception as e:
            raise CommandError(f"Failed to initialize workflows: {e}")

    def load_project_config(self) -> Optional[types.ModuleType]:
        """
        Load the config.py file from the current directory.

        Returns:
            Dictionary containing the configuration attributes, or None if not found.

        Raises:
            CommandError: If a config file exists but cannot be loaded or is invalid.
        """
        config_path = Path.cwd() / "config.py"

        if not config_path.exists():
            self.warning("No config.py found in current directory")
            return None

        if not config_path.is_file():
            raise CommandError(f"config.py exists but is not a file: {config_path}")

        try:
            config = load_module_from_path("project_config", config_path)

            self.success(f"Loaded project configuration from {config_path}\n")
            return config

        except SyntaxError as e:
            raise CommandError(f"Syntax error in config.py at line {e.lineno}: {e.msg}")
        except Exception as e:
            raise CommandError(f"Failed to load config.py: {str(e)}")

    def get_project_root_and_config_module(
        self,
    ) -> Tuple[Path, types.ModuleType]:
        """
        Get the project root directory and module path.
        Returns:
             Project root directory and module path.
        Raises:
            CommandError: If a config file exists but cannot be loaded or is invalid.
        """
        config_module = self.load_project_config()
        if not config_module:
            raise CommandError(
                "You are not in any active project. Run 'volnux startproject' first."
            )

        project_dir: Path = getattr(config_module, "PROJECT_DIR", None)
        if not project_dir:
            raise CommandError(
                "You are not in any project. Run 'volnux startproject' first."
            )
        return project_dir, config_module
