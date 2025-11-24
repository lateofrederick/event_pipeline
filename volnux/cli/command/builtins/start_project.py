import argparse
import json
from pathlib import Path
from typing import Optional

from volnux import __version__ as version

from ..base import BaseCommand, CommandCategory, CommandError


class StartProjectCommand(BaseCommand):
    help = "Create a new Volnux project structure"
    name = "startproject"
    category = CommandCategory.PROJECT_MANAGEMENT

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("name", help="Name of the project")
        # parser.add_argument(
        #     "--template", default="default", help="Project template to use"
        # )

    def handle(self, *args, **options) -> Optional[str]:
        project_name = options["name"]
        # template = options["template"]

        project_path = Path.cwd() / project_name

        if project_path.exists():
            raise CommandError(f"Project '{project_name}' already exists")

        # Create project structure
        self.success(f"Creating Volnux project '{project_name}'...")

        dirs = [
            project_path,
            project_path / "workflows",
            project_path / "workflows",
            project_path / "commands",
            project_path / "config",
            project_path / "logs",
        ]

        for dir_path in dirs:
            dir_path.mkdir(parents=True)
            (dir_path / "__init__.py").touch()

        # Create config file
        config = {
            "project_name": project_name,
            "version": version,
            "executor": "local",
            "max_workers": 4,
            "log_level": "INFO",
        }

        with open(project_path / "config" / "volnux.json", "w") as f:
            json.dump(config, f, indent=2)

        # Create README
        readme = f"""# {project_name}

A Volnux workflow orchestration project.

## Getting Started

```bash
# List available workflows
volnux list

# Run a workflow
volnux run my_workflow

# Create a new workflow
volnux startworkflow my_workflow

# Validate workflows
volnux validate
```

## Project Structure

```
{project_name}/
├── workflows/          # Workflow definitions
├── config/            # Configuration files
├── logs/              # Execution logs
└── commands/          # Custom CLI commands
```
"""

        with open(project_path / "README.md", "w") as f:
            f.write(readme)

        self.success(f"\nProject '{project_name}' created successfully!")
        self.stdout.write(f"\nNext steps:")
        self.stdout.write(f"  cd {project_name}")
        self.stdout.write(f"  volnux list")
        self.stdout.write(f"  volnux run sample_workflow\n")

        return None
