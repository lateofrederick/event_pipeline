class StartProjectCommand(BaseCommand):
    help = "Create a new Volnux project structure"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("name", help="Name of the project")
        parser.add_argument(
            "--template", default="default", help="Project template to use"
        )

    def handle(self, *args, **options) -> Optional[str]:
        project_name = options["name"]
        template = options["template"]

        project_path = Path.cwd() / project_name

        if project_path.exists():
            raise CommandError(f"Project '{project_name}' already exists")

        # Create project structure
        self.success(f"Creating Volnux project '{project_name}'...")

        # Create project directory and workflows subdirectory
        project_path.mkdir(parents=True)
        workflows_dir = project_path / "workflows"
        workflows_dir.mkdir(parents=True)
        (workflows_dir / "__init__.py").touch()

        # Create config.py file at project root
        config_content = f'''"""
Configuration file for {project_name} Volnux project.
"""

# Project metadata
PROJECT_NAME = '{project_name}'
VERSION = '1.0.0'

# Execution settings
EXECUTOR = 'local'  # Options: 'local', 'celery', 'dask', 'ray'
MAX_WORKERS = 4

# Logging configuration
LOG_LEVEL = 'INFO'  # Options: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = 'volnux.log'

# Workflow settings
WORKFLOW_TIMEOUT = 3600  # seconds
RETRY_ATTEMPTS = 3
RETRY_DELAY = 60  # seconds

# Storage settings
STORAGE_BACKEND = 'local'  # Options: 'local', 's3', 'gcs'
STORAGE_PATH = './data'

# Database settings (if needed)
DATABASE_URL = None

# Custom settings
CUSTOM_SETTINGS = {{
    # Add your custom configuration here
}}
'''

        with open(project_path / "config.py", "w") as f:
            f.write(config_content)

        # Create sample workflow
        sample_workflow = '''"""
Sample workflow for demonstration.
"""
from volnux.workflow import Workflow, Task


class SampleWorkflow(Workflow):
    """A simple example workflow."""

    name = "sample_workflow"
    description = "Demonstrates basic workflow functionality"

    def define(self):
        """Define workflow tasks and dependencies."""
        task1 = Task(
            name="hello",
            func=self.say_hello,
            args=["World"]
        )

        task2 = Task(
            name="process",
            func=self.process_data,
            depends_on=[task1]
        )

        return [task1, task2]

    def say_hello(self, name):
        print(f"Hello, {name}!")
        return f"Greeted {name}"

    def process_data(self, *results):
        print(f"Processing: {results}")
        return "Data processed"
'''

        with open(project_path / "workflows" / "sample_workflow.py", "w") as f:
            f.write(sample_workflow)

        # Create README
        readme = f"""# {project_name}

A Volnux workflow orchestration project.

## Getting Started

```bash
# List available workflows
volnux list

# Run a workflow
volnux run sample_workflow

# Create a new workflow
volnux startworkflow my_workflow

# Validate workflows
volnux validate
```

## Project Structure

```
{project_name}/
├── config.py          # Project configuration
├── workflows/         # Workflow definitions
│   └── sample_workflow.py
└── README.md         # This file
```

## Configuration

Edit `config.py` to customize:
- Executor type (local, celery, dask, ray)
- Worker settings
- Logging configuration
- Storage backends
- Custom settings

## Creating Workflows

Workflows are Python classes that define tasks and their dependencies:

```python
from volnux.workflow import Workflow, Task

class MyWorkflow(Workflow):
    name = "my_workflow"
    description = "My workflow description"

    def define(self):
        task1 = Task(name="step1", func=self.step1)
        task2 = Task(name="step2", func=self.step2, depends_on=[task1])
        return [task1, task2]

    def step1(self):
        return "Step 1 complete"

    def step2(self, *results):
        return "Step 2 complete"
```

## Running Workflows

```bash
# Simple run
volnux run my_workflow

# With parameters
volnux run my_workflow --params '{{"key": "value"}}'

# Dry run (show execution plan)
volnux run my_workflow --dry-run
```
"""

        with open(project_path / "README.md", "w") as f:
            f.write(readme)

        self.success(f"\nProject '{project_name}' created successfully!")
        self.stdout.write(f"\n{self.style.BOLD('Project structure:')}")
        self.stdout.write(f"  {project_name}/")
        self.stdout.write(f"  ├── config.py")
        self.stdout.write(f"  ├── workflows/")
        self.stdout.write(f"  │   └── sample_workflow.py")
        self.stdout.write(f"  └── README.md")
        self.stdout.write(f"\n{self.style.BOLD('Next steps:')}")
        self.stdout.write(f"  cd {project_name}")
        self.stdout.write(f"  volnux list")
        self.stdout.write(f"  volnux run sample_workflow\n")

        return None
