import importlib
import logging
import typing
from pathlib import Path

from volnux import Event
from volnux.base import EventType

from .utils import get_workflow_config_name

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from ..registry import WorkflowRegistry


class LoadFromLocal(Event):
    name = "local"

    event_type = EventType.SYSTEM

    def _load_local_workflow(
        self, workflow_dir: Path, registry: "WorkflowRegistry"
    ) -> typing.Tuple[bool, typing.Any]:
        """Load a workflow configuration from local directory."""
        from ..workflow import WorkflowConfig

        workflow_name = workflow_dir.name
        module_path = f"workflows.{workflow_name}.workflow"

        loading_status = False

        try:
            module = importlib.import_module(module_path)

            # Find WorkflowConfig subclass
            for attr_name in dir(module):
                attr_name = get_workflow_config_name(attr_name)
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, WorkflowConfig)
                    and attr != WorkflowConfig
                ):
                    # Instantiate the workflow config
                    workflow_config = attr(workflow_path=workflow_dir)
                    workflow_config.module = f"workflows.{workflow_name}"
                    registry.register(workflow_config)
                    logger.info(f"  ✓ Loaded local workflow: {workflow_name}")
                    loading_status = True
                    break
        except ImportError as e:
            logger.error(f"  ✗ Error loading workflow '{workflow_name}': {e}")
            loading_status = False
        return loading_status, None

    def process(
        self, workflows_dir: Path, registry: "WorkflowRegistry"
    ) -> typing.Tuple[bool, typing.Any]:
        if not workflows_dir.exists():
            return False, f"Workflows directory not found: {workflows_dir}"

        logger.info(f"Discovering local workflows in: {workflows_dir}")

        for workflow_dir in workflows_dir.iterdir():
            if not workflow_dir.is_dir():
                continue

            # Check if workflow.py exists
            workflow_file = workflow_dir / "workflow.py"
            if not workflow_file.exists():
                continue

            # Load the workflow configuration
            status, _ = self._load_local_workflow(workflow_dir, registry)
            if not status:
                logger.error(f"Failed to load workflow from: {workflow_dir}")

        return True, None
