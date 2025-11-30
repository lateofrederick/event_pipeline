import typing
import logging.config
from pathlib import Path

if typing.TYPE_CHECKING:
    from volnux.engine.workflows import WorkflowRegistry

__all__ = ["initialise_workflows"]


def initialise_workflows(
    project_path: Path, workflow_name: typing.Optional[str] = None
) -> "WorkflowRegistry":
    """
    Initialise workflow registry.
    :param project_path: Project root directory.
    :param workflow_name: The workflow name. If None, load all workflows.
    :return: Registry.
    """
    from volnux.engine.workflows import get_workflow_registry
    from volnux.conf import ConfigLoader

    system_configuration = ConfigLoader.get_lazily_loaded_config()

    logging.config.dictConfig(system_configuration.LOGGING_CONFIG)

    workflow_registry = get_workflow_registry()
    if not workflow_registry.is_ready():
        workflow_registry.populate_local_workflow_configs(project_path, workflow_name)
        workflow_registry.load_workflow_configs()
    return workflow_registry
