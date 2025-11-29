import argparse
from typing import Optional
from pathlib import Path

from ..base import BaseCommand, CommandCategory, CommandError


class ValidateWorkflowCommand(BaseCommand):
    help = "Validate workflow definitions"
    name = "validate"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "workflow", nargs="?", help="Specific workflow to validate (optional)"
        )

    def handle(self, *args, **options) -> Optional[str]:
        workflow_name = options.get("workflow")
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

        workflows = self._initialise_workflows(project_dir)

        if workflow_name:
            self.success(f"Validating workflow: {workflow_name}\n")
            workflow = workflows.get_workflow_config(workflow_name)
            if not workflow:
                raise CommandError(
                    f"Workflow {workflow_name} not found in project {project_dir}"
                )
            for index, issue in enumerate(workflow.check()):
                self.warning(f"Workflow {workflow_name}/{index}: {issue}\n")
        else:
            self.success("\nValidating all workflows...")
            for workflow in workflows.get_workflow_configs():
                self.success(f"\nValidating workflow {workflow.name}...\n")
                for index, issue in enumerate(workflow.check()):
                    self.warning(f"Workflow {workflow.name}/{index}: {issue}\n")

        return None
