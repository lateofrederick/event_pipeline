import typing
from pathlib import Path
from typing import Optional

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

        workflows_registry = self._initialise_workflows(project_dir)

        num_of_workflows = 0
        for workflow in workflows_registry.get_workflow_configs():
            num_of_workflows += 1
            self.stdout.write(f"  ✓ {workflow.name}\n")

        self.stdout.write(f"\nTotal: {num_of_workflows} workflow(s)\n")

        return None
