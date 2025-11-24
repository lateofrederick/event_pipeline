import argparse
import typing
from pathlib import Path
from typing import Optional

from ..base import BaseCommand, CommandCategory, CommandError


class StartWorkflowCommand(BaseCommand):
    help = "Create a new workflow"
    name = "startworkflow"
    category = CommandCategory.WORKFLOW_MANAGEMENT

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("name", help="Name of the workflow")
        parser.add_argument(
            "--template",
            default="cfg",
            choices=["cfg", "dag"],
            help="Workflow template type",
        )

    def handle(self, *args, **options) -> Optional[str]:
        workflow_name = options["name"]
        template = options["template"]

        workflows_dir = Path.cwd() / "workflows"
        if not workflows_dir.exists():
            raise CommandError(
                "Not in a Volnux project. Run 'volnux startproject' first."
            )

        workflow_specific_dir = workflows_dir / workflow_name
        if not workflow_specific_dir.exists():
            workflow_specific_dir.mkdir(parents=True)

        file_structure = []

        workflow_file = workflow_specific_dir / f"{workflow_name}.py"
        if workflow_file.exists():
            raise CommandError(f"Workflow '{workflow_name}' already exists")

        template_content = self._get_template(workflow_name, template)

        with open(workflow_file, "w") as f:
            f.write(template_content)

        self.success(f"Workflow '{workflow_name}' created successfully!")
        self.stdout.write(f"Edit: workflows/{workflow_name}.py")

        return None

    def _get_template(
        self, name: str, template: str, params: typing.Dict[str, typing]
    ) -> None:
        """Get workflow template content."""
        class_name = "".join(word.capitalize() for word in name.split("_"))
