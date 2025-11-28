import typing
import logging
import importlib
from pathlib import Path
from typing import Optional

from volnux.import_utils import load_module_from_path
from volnux.engine.workflows import WorkflowConfig, WorkflowRegistry
from ..base import BaseCommand, CommandCategory, CommandError


class ListWorkflowsCommand(BaseCommand):
    help = "List all available workflows"
    name = "list"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def handle(self, *args, **options) -> Optional[str]:
        config_module = self.load_project_config()
        if not config_module:
            raise CommandError(
                "You are not in any active project. Run 'volnux startproject' first."
            )

        self.stdout.write(self.style.BOLD("\nAvailable Workflows:\n"))

        project_name = getattr(config_module, "PROJECT_NAME", None)
        if not project_name:
            raise CommandError(
                "You are not in any project. Run 'volnux startproject' first."
            )

        project_dir: Path = getattr(config_module, "PROJECT_DIR", None)
        if not project_dir:
            raise CommandError(
                "You are not in any project. Run 'volnux startproject' first."
            )

        workflows_initialiser = load_module_from_path(
            "initialiser", project_dir / "init.py"
        )
        if not workflows_initialiser:
            raise CommandError(
                f"Failed to load workflow initialiser module from path: {project_dir / 'init.py'}"
            )

        workflows_registry = typing.cast(
            WorkflowRegistry, workflows_initialiser.workflows
        )
        # if not workflows_registry.is_ready():
        #     raise CommandError("Workflow registry is not ready yet, try again later.")
        num_of_workflows = 0
        for workflow in workflows_registry.get_workflow_configs():
            num_of_workflows += 1
            self.stdout.write(f"  • {workflow.name}")

        self.stdout.write(f"\nTotal: {num_of_workflows} workflow(s)\n")

        return None
