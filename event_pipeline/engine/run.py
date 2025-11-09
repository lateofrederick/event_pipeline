from event_pipeline.pipeline import Pipeline
from .default_engine import DefaultWorkflowEngine
from .base import EngineExecutionResult
from event_pipeline.parser.protocols import TaskType

# Global default engine instance
_default_engine = DefaultWorkflowEngine(strict_mode=True)


def run_workflow(
    root_task: TaskType,
    pipeline: Pipeline,
) -> None:

    result = _default_engine.execute(root_task, pipeline)

    if result.status == EngineExecutionResult.FAILED and result.error:
        raise result.error
