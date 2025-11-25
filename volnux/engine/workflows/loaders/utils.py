def get_workflow_class_name(workflow_name: str) -> str:
    """Convert workflow name to workflow class name"""
    return "".join(word.capitalize() for word in workflow_name.split("_"))


def get_workflow_config_name(workflow_name: str) -> str:
    """Convert a workflow name to its corresponding WorkflowConfig class name."""
    return get_workflow_class_name(workflow_name) + "Config"
