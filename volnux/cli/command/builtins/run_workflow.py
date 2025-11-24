import argparse
import json
from typing import Optional

from ..base import BaseCommand, CommandCategory


class RunWorkflowCommand(BaseCommand):
    help = "Run a workflow"
    name = "run"
    category = CommandCategory.EXECUTION

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("workflow", help="Name of the workflow to run")
        parser.add_argument(
            "--params", help="JSON string of parameters to pass to workflow"
        )
        parser.add_argument(
            "--dry-run", action="store_true", help="Show execution plan without running"
        )

    def handle(self, *args, **options) -> Optional[str]:
        workflow_name = options["workflow"]
        params = json.loads(options["params"]) if options.get("params") else {}
        dry_run = options.get("dry_run", False)

        if dry_run:
            self.warning(f"DRY RUN: Would execute workflow '{workflow_name}'")
            self.stdout.write(f"Parameters: {params}")
            return None

        self.success(f"Running workflow: {workflow_name}")
        self.stdout.write(f"Parameters: {params}")
        self.stdout.write("\n[Workflow execution would happen here]\n")

        return None
