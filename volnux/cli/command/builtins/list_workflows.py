import typing
import logging
import importlib
from pathlib import Path
from typing import Optional

from volnux.import_utils import load_module_from_path
from volnux.engine.workflows import WorkflowConfig
from ..base import BaseCommand, CommandCategory, CommandError


class ListWorkflowsCommand(BaseCommand):
    help = "List all available workflows"
    name = "list"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def handle(self, *args, **options) -> Optional[str]:
        config_module = self.load_project_config()
        if not config_module:
            raise CommandError("You are not in any active project. Run 'volnux startproject' first.")

        self.stdout.write(self.style.BOLD("\nAvailable Workflows:\n"))

        project_name = getattr(config_module, "PROJECT_NAME", None)
        if not project_name:
            raise CommandError("You are not in any project. Run 'volnux startproject' first.")

        project_dir: Path = getattr(config_module, "PROJECT_DIR", None)
        if not project_dir:
            raise CommandError("You are not in any project. Run 'volnux startproject' first.")

        registered_workflows = getattr(config_module, "WORKFLOWS", [])
        if not registered_workflows:
            self.warning("No registered workflows found.")
            return None

        num_workflows = 0

        package = importlib.import_module(project_dir.name)

        for workflow_dotted_path in registered_workflows:
            try:
                workflow = typing.cast(typing.Type[WorkflowConfig], importlib.import_module(workflow_dotted_path, package.__name__))
                num_workflows += 1
                self.stdout.write(f"  • {workflow.name}")
            except ModuleNotFoundError as e:
                logging.error(e, exc_info=True)
                self.warning(f"{workflow_dotted_path} is not a valid registered workflow.")

        self.stdout.write(f"\nTotal: {num_workflows} workflow(s)\n")

        return None
