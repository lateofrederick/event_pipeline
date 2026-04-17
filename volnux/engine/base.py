import typing
import logging
from enum import Enum
from abc import ABC, abstractmethod
from dataclasses import dataclass

from .checkpoint_config import CheckPointConfig, CheckPointFrequency
from volnux.pipeline import Pipeline
from volnux.parser.protocols import TaskType
from volnux.execution.context import ExecutionContext

# from volnux.execution.rehydrator.checkpoint import AutoCheckPointer

logger = logging.getLogger(__name__)


class EngineExecutionResult(Enum):
    COMPLETED = "completed"
    TERMINATED_EARLY = "terminated_early"
    FAILED = "failed"


class TaskNode(typing.NamedTuple):
    task: TaskType
    previous_context: typing.Optional[ExecutionContext] = None


@dataclass
class EngineResult:
    status: EngineExecutionResult
    final_context: typing.Optional[ExecutionContext] = None
    error: typing.Optional[Exception] = None
    tasks_processed: int = 0


class WorkflowEngine(ABC):
    """
    Abstract interface for workflow execution engines.

    Engines are responsible for orchestrating the execution flow:
    - Task traversal strategy (iterative, recursive, async, etc.)
    - Work queue management
    - Task scheduling and ordering
    - Flow control (loops, branches, parallelism)

    Engines delegate actual task execution, metrics, and hooks to ExecutionContext and Coordinator.
    """

    def __init__(
        self,
        enable_checkpointing: bool = False,
        checkpoint_config: typing.Optional[CheckPointConfig] = None,
    ) -> None:
        self.tasks_processed: int = 0
        self.current_task_node: typing.Optional["TaskNode"] = None
        self.final_context: typing.Optional["ExecutionContext"] = None

        self._checkpointer: typing.Optional["AutoCheckPointer"] = None
        self.checkpoint_config: typing.Optional[CheckPointConfig] = None

        if enable_checkpointing:
            self.checkpoint_config = (
                checkpoint_config if checkpoint_config else CheckPointConfig()
            )

    @property
    @abstractmethod
    def task_queue(self) -> typing.Any:
        """
        Primary engine queue used for active task scheduling.

        Concrete engines decide the backing implementation.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def sink_queue(self) -> typing.Any:
        """
        Queue used for deferred/sink tasks.

        Concrete engines decide the backing implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    async def execute(
        self,
        root_task: TaskType,
        pipeline: Pipeline,
    ) -> EngineResult:
        """
        Execute a workflow starting from the root task.

        The engine orchestrates the flow but delegates execution to contexts.
        All metrics, hooks, and task execution are handled by ExecutionContext
        and the Coordinator.
        """
        pass

    def get_name(self) -> str:
        """
        Get the engine name/identifier.

        Returns:
            Human-readable engine name
        """
        return self.__class__.__name__

    def enable_checkpointing(
        self,
        checkpointer: "AutoCheckPointer",
        checkpoint_frequency: "CheckPointFrequency" = CheckPointFrequency.PER_TASK,
    ):
        """
        Enable automatic checkpointing for this engine.
        """
        self._checkpointer = checkpointer
        self._checkpoint_frequency = checkpoint_frequency

    async def _checkpoint_before_task(
        self, context: "ExecutionContext", task_node: "TaskNode"
    ):
        """
        Checkpoint before executing a task (idempotency support).
        """
        if not self._checkpointer:
            return

        self.current_task_node = task_node

        if self._checkpoint_frequency == CheckPointFrequency.PER_TASK:
            await context.persist()
            logger.debug(f"Checkpointed before task: {task_node.task.event}")

    async def _checkpoint_after_task(self, context: "ExecutionContext", success: bool):
        """
        Checkpoint after task completion.
        """
        if not self._checkpointer:
            return

        self.tasks_processed += 1
        self.current_task_node = None

        if self._checkpoint_frequency in [
            CheckPointFrequency.PER_TASK,
            CheckPointFrequency.ON_STATE_CHANGE,
        ]:
            await context.persist()
            logger.debug(
                f"Checkpointed after task: {self.tasks_processed} tasks processed"
            )
