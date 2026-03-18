import typing
import traceback
from enum import Enum

from .snapshot import TaskTemplate, QueueTaskTemplate

from volnux.result import EventResult
from volnux.pipeline import Pipeline

if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskType


class StateSerializer:
    """
    Handles serialization of complex Volnux objects to KeyValue-compatible formats.
    """

    @staticmethod
    def serialize_task(task: "TaskType") -> TaskTemplate:
        """Serialize PipelineTask to dict."""
        event_class = task.get_event_class()

        payload: TaskTemplate = {
            "task_id": task.get_id(),
            "task_type": "group" if getattr(task, "is_grouping", False) else "normal",
            "event_name": task.get_event_name(),
            "event_class_import_path": (
                f"{event_class.__module__}.{event_class.__name__}"
            ),
            "options": task.options.to_dict(),
            "sequence_number": task.sequence_number,
            "descriptor": getattr(task, "descriptor", None),
            "descriptor_pipe_type": (
                getattr(task.descriptor_pipe, "value", None)
                if getattr(task, "descriptor_pipe", None) is not None
                else None
            ),
        }

        condition_node = getattr(task, "condition_node", None)
        if condition_node is not None:
            payload["condition_node"] = condition_node.to_dict()

        chain = getattr(task, "chain", None)
        if chain:
            payload["chain"] = [StateSerializer.serialize_task(child) for child in chain]

        strategy = getattr(task, "strategy", None)
        if strategy is not None:
            payload["strategy"] = str(strategy)

        return payload

    @staticmethod
    def _task_ref(task: typing.Any) -> typing.Optional[dict]:
        """Serialize a lightweight reference to a task-like object."""
        if task is None:
            return None
        return {
            "task_id": getattr(task, "task_id", getattr(task, "_id", None)),
            "event_name": getattr(task, "event", getattr(task, "event_name", None)),
        }

    @staticmethod
    def serialize_result(result: "EventResult") -> typing.Dict[str, typing.Any]:
        """Serialize EventResult."""
        return result.as_dict()

    @staticmethod
    def serialize_exception(exc: Exception) -> typing.Dict[str, typing.Any]:
        """Serialize exception to structured data."""
        return {
            "type": exc.__class__.__name__,
            "message": str(exc),
            "traceback": "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            ),
        }

    @staticmethod
    def serialize_pipeline_ref(pipeline: "Pipeline") -> typing.Tuple[str, str]:
        """Extract pipeline identity and class path."""
        class_path = f"{pipeline.__class__.__module__}.{pipeline.__class__.__name__}"
        return pipeline.id, class_path