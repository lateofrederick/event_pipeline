import importlib
import logging
import typing
from pathlib import Path

from volnux import Event
from volnux.base import EventType
from volnux.import_utils import load_module_from_path
from .utils import get_workflow_config_name

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from ..registry import WorkflowRegistry


class LoadFromLocal(Event):
    name = "local"

    event_type = EventType.SYSTEM

    def _load_local_workflow(
        self, workflow_file: Path, registry: "WorkflowRegistry"
    ) -> typing.Tuple[bool, typing.Any]:
        """Load a workflow configuration from local directory."""
        from ..workflow import WorkflowConfig

        loading_status = False
        workflow_dir = workflow_file.parent

        try:
            module = load_module_from_path("workflow", workflow_file)
            workflow_config = None
            for attr_name in dir(module):
                # attr_name = get_workflow_config_name(attr_name)
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, WorkflowConfig)
                    and attr != WorkflowConfig
                ):
                    # Instantiate the workflow config
                    workflow_config = attr(workflow_path=workflow_dir)
                    workflow_config.module = f"workflows.{workflow_file.name}"
                    registry.register(workflow_config)
                    logger.info(f"  ✓ Loaded local workflow: {workflow_file.name}")
                    loading_status = True
                    break

            return loading_status, workflow_config
        except ImportError as e:
            logger.error(f"  ✗ Error loading workflow '{workflow_file}': {e}")
            loading_status = False

        return loading_status, None

    def process(
        self, workflow_dir: Path, registry: "WorkflowRegistry"
    ) -> typing.Tuple[bool, typing.Any]:
        if not workflow_dir.exists():
            return False, f"Workflows directory not found: {workflow_dir}"

        logger.info(f"Discovering local workflows in: {workflow_dir}")

        # Check if workflow.py exists
        workflow_file = workflow_dir / "workflow.py"
        if not workflow_file.exists():
            logger.debug(f"  <UNK> No workflow found at {workflow_file}")
            return False, f"Workflow file not found: {workflow_file}"

        # Load the workflow configuration
        status, _ = self._load_local_workflow(workflow_file, registry)
        if not status:
            logger.error(f"Failed to load workflow from: {workflow_dir}")

        return True, None
