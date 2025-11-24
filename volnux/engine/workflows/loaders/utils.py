def get_workflow_config_name(workflow_name: str) -> str:
    """Convert a workflow name to its corresponding WorkflowConfig class name."""
    return "".join(word.capitalize() for word in workflow_name.split("_")) + "Config"
