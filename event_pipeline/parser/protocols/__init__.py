import typing
from .task import TaskProtocol
from .task_group import TaskGroupingProtocol, GroupingStrategy

# The type for all tasks
TaskType = typing.Union[TaskProtocol, TaskGroupingProtocol]


__all__ = ["TaskType", "TaskProtocol", "TaskGroupingProtocol", "GroupingStrategy"]
