from pathlib import Path
from typing import Optional

from ..base import BaseCommand, CommandCategory, CommandError


class ListWorkflowsCommand(BaseCommand):
    help = "List all available workflows"
    name = "list"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def handle(self, *args, **options) -> Optional[str]:
        workflows_dir = Path.cwd() / "workflows"
        if not workflows_dir.exists():
            raise CommandError(
                "Not in a Volnux project. Run 'volnux startproject' first."
            )

        self.stdout.write(self.style.BOLD("\nAvailable Workflows:\n"))

        workflows = list(workflows_dir.glob("*.py"))
        workflows = [w for w in workflows if not w.name.startswith("_")]

        if not workflows:
            self.warning("No workflows found.")
            return None

        for workflow_file in sorted(workflows):
            workflow_name = workflow_file.stem
            self.stdout.write(f"  • {workflow_name}")

        self.stdout.write(f"\nTotal: {len(workflows)} workflow(s)\n")

        return None
