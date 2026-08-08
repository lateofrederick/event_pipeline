import asyncio
import weakref
import logging
import time
import datetime
import typing
from collections import deque
from dataclasses import dataclass, field

from formax import (
    Attrib,
    BaseModel,
    MiniAnnotated,
    ValidationFlags,
    InitStrategy,
    ValidationError as FormaxValidationError,
)

from volnux.mixins import ObjectIdentityMixin
from volnux.parser.operator import PipeType
from volnux.parser.options import ResultEvaluationStrategy
from volnux.parser.protocols import TaskType
from volnux.execution.pipeline import Pipeline
from volnux.result import EventResult, ResultSet
from volnux.result_evaluators import EventEvaluator, ResultEvaluationStrategies
from volnux.signal.signals import (
    event_execution_aborted,
    event_execution_cancelled,
    event_execution_failed,
    event_execution_paused,
)
from volnux.task import PipelineTask, PipelineTaskGrouping
from volnux.concurrency.async_utils import to_thread

from .state_manager import ExecutionState, ExecutionStatus, StateManager

if typing.TYPE_CHECKING:
    from volnux.engine.base import WorkflowEngine
    from volnux.execution.rehydrator.engine.snapshot import ContextSnapshot

logger = logging.getLogger(__name__)


@dataclass
class ExecutionMetrics:
    """Execution timing and statistics"""

    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time if self.end_time else 0.0


def preformat_task_profile(
    task_profiles: typing.Union[
        TaskType, typing.List[TaskType], typing.Deque[TaskType]
    ],
) -> typing.Deque[TaskType]:
    if isinstance(task_profiles, (PipelineTask, PipelineTaskGrouping)):
        return deque([task_profiles])  # type: ignore
    elif isinstance(task_profiles, (list, tuple)):
        return deque(task_profiles)
    elif isinstance(task_profiles, deque):
        return task_profiles
    # TODO: descriptive error message
    raise FormaxValidationError("invalid task format")  # type: ignore


class ExecutionContext(ObjectIdentityMixin, BaseModel):
    """
    Represents the execution context for a particular event in the pipeline.

    This class encapsulates the necessary data and state associated with
    executing an event, such as the task being processed and the pipeline
    it belongs to. It ensures thread safety using a conditional variable for
    concurrent event execution.

    :ivar task_profiles: The specific tasks being executed within the pipeline.
    :ivar pipeline: The pipeline that orchestrates the execution of the task.
    :ivar metrics: Execution metrics for monitoring and evaluating performance.
    :ivar previous_context: The preceding context in a doubly-linked list structure.
    :ivar next_context: The succeeding context in a doubly-linked list structure.
    :ivar parent_context: The parent context in a tree structure, used for hierarchical task management.
    :ivar child_contexts: The child contexts in a tree structure, representing branches of execution.
    :type task_profiles: typing.Deque[TaskType]
    :type pipeline: Pipeline
    :type metrics: ExecutionMetrics
    :type previous_context: typing.Optional[ExecutionContext]
    :type next_context: typing.Optional[ExecutionContext]
    :type parent_context: typing.Optional[ExecutionContext]
    :type child_contexts: typing.List[ExecutionContext]

    Details:
        Represents the execution context of the pipeline as a bidirectional (doubly-linked) list.
        Each node corresponds to an event's execution context, allowing traversal both forward and backward
        through the pipeline's events.

        You can filter contexts by event name using the `filter_by_event` method.

        The context is iterable in the forward direction, so you can loop through it like this:
            for context in pipeline.start():
                pass

        To access specific ends of the context queue:
        - Use `get_head_context()` to retrieve the head (starting context).
        - Use `get_tail_context()` to retrieve the tail (ending context).

        Reverse traversal can be done by walking backward from the tail using the linked structure.
    """

    task_profiles: MiniAnnotated[
        typing.Deque[TaskType],
        Attrib(pre_formatter=preformat_task_profile),
    ]
    pipeline: Pipeline
    metrics: ExecutionMetrics = field(default_factory=lambda: ExecutionMetrics())

    # Horizontal Links (Linked list)
    previous_context: typing.Optional["ExecutionContext"] = None
    next_context: typing.Optional["ExecutionContext"] = None

    # Vertical Links (The Tree)
    parent_context: typing.Optional["ExecutionContext"] = None
    child_contexts: typing.List["ExecutionContext"] = field(default_factory=list)

    _context_lock: asyncio.Lock = field(
        default_factory=lambda: asyncio.Lock()
    )  # Protects concurrent append ops

    _state_manager: typing.ClassVar[typing.Optional["StateManager"]] = None

    # Weak reference to the engine (not persisted)
    _engine_ref: typing.Optional[weakref.ReferenceType] = None

    # Workflow identifier for grouping contexts
    workflow_id: str = None
    workflow_name: str = None

    # Checkpoint data for idempotency
    _task_checkpoint: typing.Optional[typing.Dict[str, typing.Any]] = None

    class Config:
        validation = ValidationFlags.NONE
        init_strategy = InitStrategy.DATACLASS

    def __post_init__(
        self, *args: typing.Tuple[typing.Any], **kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        from .state_manager import ExecutionState, ExecutionStatus, StateManager

        super().__init__(*args, **kwargs)  # type: ignore

        # Initialize shared state manager
        if self.__class__._state_manager is None:
            self.__class__._state_manager = StateManager()

        # Create a state in shared memory with its own lock
        initial_state = ExecutionState(ExecutionStatus.PENDING)
        self._state_manager.create_state(self.state_id, initial_state)

    @classmethod
    async def create_context(
        cls,
        workflow_id: str,
        workflow_name: str,
        task_profiles: typing.Deque[TaskType],
        pipeline: Pipeline,
        metrics: ExecutionMetrics = None,
        previous_context: typing.Optional["ExecutionContext"] = None,
        next_context: typing.Optional["ExecutionContext"] = None,
        parent_context: typing.Optional["ExecutionContext"] = None,
        child_contexts: typing.List["ExecutionContext"] = None,
    ) -> "ExecutionContext":
        """
        Creates an ExecutionContext instance in an asynchronous, thread-safe manner. This method wraps the
        initialization logic to allow for non-blocking execution while ensuring thread synchronization when
        creating the context. It enables efficient execution context creation, especially in concurrent
        environments.

        :param workflow_id: Unique identifier of the workflow.
        :type workflow_id: str
        :param workflow_name: Name of the workflow.
        :type workflow_name: str
        :param task_profiles: Queue of task profiles to be executed as part of this context.
        :type task_profiles: typing.Deque[TaskType]
        :param pipeline: Pipeline object managing the execution flow and dependencies.
        :type pipeline: Pipeline
        :param metrics: (Optional) Metrics object containing execution statistics and performance data.
        :type metrics: ExecutionMetrics, optional
        :param previous_context: (Optional) Reference to a previously executed ExecutionContext in the workflow chain.
        :type previous_context: typing.Optional[ExecutionContext], optional
        :param next_context: (Optional) Reference to the next ExecutionContext in the workflow chain.
        :type next_context: typing.Optional[ExecutionContext], optional
        :param parent_context: (Optional) Reference to the immediate parent ExecutionContext, if nested.
        :type parent_context: typing.Optional[ExecutionContext], optional
        :param child_contexts: (Optional) List of child ExecutionContexts derived from the current context.
        :type child_contexts: typing.List[ExecutionContext], optional

        :return: An instance of ExecutionContext constructed asynchronously.
        :rtype: ExecutionContext
        """

        if child_contexts is None:
            child_contexts = []

        if not isinstance(child_contexts, (list, tuple)):
            child_contexts = list(child_contexts)

        context = await to_thread(
            cls,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            task_profiles=task_profiles,
            pipeline=pipeline,
            metrics=metrics,
            previous_context=previous_context,
            next_context=next_context,
            parent_context=parent_context,
            child_contexts=child_contexts,
        )
        return context

    @property
    def state_id(self) -> str:
        return self.id

    @property
    def state(self) -> "ExecutionState":
        """
        Get the current state from shared memory.
        """
        return self.get_state_manager().get_state(self.state_id)

    @property
    async def state_async(self) -> "ExecutionState":
        """
        Async version of getting the current state from shared memory.
        """
        return await self.get_state_manager().get_state_async(self.state_id)

    async def spawn_child(
        self, task_profiles: typing.Deque[TaskType]
    ) -> "ExecutionContext":
        """
        Creates and returns a child ExecutionContext. The child inherits the same
        StateManager as the parent but is assigned a unique state_id. The parent-child
        relationship is established by linking the child context to the parent's list
        of child contexts.

        :param task_profiles: A deque containing TaskType instances to be executed in
            the context of the child ExecutionContext.
        :return: A newly created ExecutionContext configured as a child of the
            current context.
        :rtype: ExecutionContext
        """
        # Child inherit the same StateManager but get a unique state_id
        child = await ExecutionContext.create_context(
            task_profiles=task_profiles,
            pipeline=self.pipeline,
            parent_context=self,
            workflow_id=self.workflow_id,
            workflow_name=self.workflow_name,
        )

        async with self._context_lock:
            self.child_contexts.append(child)  # Link Down
        return child

    @property
    def is_root(self) -> bool:
        return self.parent_context is None

    @property
    def is_leaf(self) -> bool:
        return len(self.child_contexts) == 0

    def get_root_context(self) -> "ExecutionContext":
        """Climb the tree to find the absolute start of the orchestration."""
        current = self
        while current.parent_context:
            current = current.parent_context
        return current

    def get_depth(self) -> int:
        """Calculates nesting level for directive validation."""
        depth = 0
        current = self
        while current.parent_context:
            depth += 1
            current = current.parent_context
        return depth

    async def _evaluate_group_finality(self):
        """
        Internal check: Is every child context in this subtree finished?
        """
        all_done = True
        for child in self.child_contexts:
            child_state = await child.state_async
            if child_state.status not in [
                ExecutionStatus.COMPLETED,
                ExecutionStatus.FAILED,
            ]:
                all_done = False
                break

        if all_done:
            # The 'Super-Task' is now officially complete
            await self.update_status_async(ExecutionStatus.COMPLETED)

    def get_state_manager(self) -> StateManager:
        """
        Get context state manager

        Returns:
            The state manager for this context
        """
        if self.__class__._state_manager is None:
            state_manager = StateManager()
            initial_state = ExecutionState(ExecutionStatus.PENDING)
            state_manager.create_state(self.state_id, initial_state)
            self.__class__._state_manager = state_manager
            return state_manager
        return self._state_manager  # type: ignore

    def update_status(self, new_status: "ExecutionStatus") -> None:
        self.get_state_manager().update_status(self.state_id, new_status)

    async def update_status_async(self, new_status: "ExecutionStatus") -> None:
        await self.get_state_manager().update_status_async(self.state_id, new_status)

        # If this child is done, signal the parent to check its 'Group' status
        if new_status == ExecutionStatus.COMPLETED and self.parent_context:
            await self.parent_context._evaluate_group_finality()

    def add_error(self, error: Exception) -> None:
        self.get_state_manager().append_error(self.state_id, error)

    async def add_error_async(self, error: Exception) -> None:
        await self.get_state_manager().append_error_async(self.state_id, error)

    def add_result(self, result: EventResult) -> None:
        self.get_state_manager().append_result(self.state_id, result)

    async def add_result_async(self, result: EventResult) -> None:
        await self.get_state_manager().append_result_async(self.state_id, result)

    def cancel(self) -> None:
        """
        Cancel execution - only locks THIS context.
        Other contexts continue running unaffected.
        """
        self.get_state_manager().update_status(self.state_id, ExecutionStatus.CANCELLED)
        # Emit event
        event_execution_cancelled.emit(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.CANCELLED,
        )

    async def cancel_async(self) -> None:
        """
        Async version of cancel execution - only locks THIS context.
        Other contexts continue running unaffectedly.
        """
        await self.get_state_manager().update_status_async(
            self.state_id, ExecutionStatus.CANCELLED
        )
        # Emit event
        await event_execution_cancelled.emit_async(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.CANCELLED,
        )

    def abort(self) -> None:
        """
        Abort execution - only locks THIS context.
        Other contexts continue running unaffected.
        """
        self.get_state_manager().update_status(self.state_id, ExecutionStatus.ABORTED)
        # Emit event
        event_execution_aborted.emit(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.ABORTED,
        )

    async def abort_async(self) -> None:
        """
        Async version of abort execution - only locks THIS context.
        Other contexts continue running unaffected.
        """
        await self.get_state_manager().update_status_async(
            self.state_id, ExecutionStatus.ABORTED
        )
        # Emit event
        await event_execution_aborted.emit_async(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.ABORTED,
        )

    def failed(self) -> None:
        """
        Mark the execution context as failed.
        """
        self.get_state_manager().update_status(self.state_id, ExecutionStatus.FAILED)
        # Emit event
        event_execution_failed.emit(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.FAILED,
        )

    async def failed_async(self) -> None:
        """
        Async version of marking the execution context as failed.
        """
        await self.get_state_manager().update_status_async(
            self.state_id, ExecutionStatus.FAILED
        )
        # Emit event
        await event_execution_failed.emit_async(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.FAILED,
        )

    async def paused_async(self) -> None:
        await self.get_state_manager().update_status_async(
            self.state_id, ExecutionStatus.PAUSED
        )

        # Emit event
        await event_execution_paused.emit_async(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.PAUSED,
        )

    def get_state_snapshot(self) -> "ExecutionState":
        """Get a thread-safe copy of the current state."""
        return self.state

    def bulk_update(
        self,
        status: typing.Optional["ExecutionStatus"] = None,
        errors: typing.Optional[typing.Sequence[Exception]] = None,
        results: typing.Optional[typing.Sequence[EventResult]] = None,
    ) -> None:
        """Efficient bulk update"""
        state = self.state
        if status is not None:
            state.status = status
        if errors is not None:
            state.errors.extend(errors)  # type: ignore
        if results is not None:
            state.results.extend(results)
        self.get_state_manager().update_state(self.state_id, state)  # type: ignore

    async def bulk_update_async(
        self,
        status: typing.Optional["ExecutionStatus"] = None,
        errors: typing.Optional[typing.Sequence[Exception]] = None,
        results: typing.Optional[typing.Sequence[EventResult]] = None,
    ) -> None:
        """Async version of efficient bulk update"""
        state = await self.state_async
        if status is not None:
            state.status = status
        if errors is not None:
            state.errors.extend(errors)  # type: ignore
        if results is not None:
            state.results.extend(results)
        await self.get_state_manager().update_state_async(self.state_id, state)

    async def update_aggregated_result(self, result: "EventResult") -> None:
        state = await self.state_async
        state.aggregated_result = result
        self.get_state_manager().update_state(self.state_id, state)

    def __iter__(self) -> typing.Generator["ExecutionContext", typing.Any, None]:
        current: typing.Optional["ExecutionContext"] = self
        while current is not None:
            yield current
            current = current.next_context

    def __hash__(self) -> int:
        return hash(self.id)

    async def dispatch(
        self, timeout: typing.Optional[float] = None
    ) -> typing.Tuple[typing.Any, typing.Any]:
        """
        Dispatch the task associated with this execution context.
        Args:
            timeout: Optional dispatch timeout
        Returns:
            SwitchRequest if task switching is requested, else None.
        Raises:
            RuntimeError: If called from within an existing event loop
            Exception: If execution fails
        """
        from .coordinator import ExecutionCoordinator

        coordinator = ExecutionCoordinator(execution_context=self, timeout=timeout)

        try:
            return await coordinator.execute_async()
        except Exception as e:
            logger.error(
                f"{self.pipeline.__class__.__name__} : {str(self.task_profiles)} : {str(e)}"
            )
            raise

    def get_task_profiles(self) -> typing.Deque["TaskType"]:
        task_profiles = self.task_profiles

        return typing.cast(typing.Deque["TaskType"], task_profiles)

    def is_multitask(self) -> bool:
        return len(self.get_task_profiles()) > 1

    def get_head_context(self) -> "ExecutionContext":
        """
        Returns the execution context head of the execution context.
        :return: ExecutionContext head of the execution context.
        """
        current = self
        while current.previous_context:
            current = current.previous_context
        return current

    def get_latest_context(self) -> "ExecutionContext":
        """
        Returns the latest execution context.
        :return: ExecutionContext
        """
        current = self.get_head_context()
        while current.next_context:
            current = current.next_context
        return current

    def get_tail_context(self) -> "ExecutionContext":
        """
        Returns the tail context of the execution context.
        :return: ExecutionContext
        """
        return self.get_latest_context()

    def filter_by_event(self, event_name: str) -> ResultSet:
        """
        Filters the execution context based on the event name.
        :param event_name: Case-insensitive event name.
        :return: ResultSet with the filtered execution context.
        """
        head = self.get_head_context()
        event = ""  # PipelineTask.resolve_event_name(event_name)
        result = ResultSet()

        def filter_condition(context: ExecutionContext, term: str) -> bool:
            task_profiles = context.task_profiles

        for context in head:
            if event in [task.event for task in context.task_profiles]:
                result.add(context)
        return result

    def get_decision_task_profile(
        self,
    ) -> typing.Optional[TaskType]:
        """
        Retrieves a task profile crucial for decision-making processes.

        This method identifies the final task in a pipeline or sequence of tasks.
        If a single task profile exists, it is directly returned. For multiple
        task profiles, the method analyzes each profile to determine its role in
        a parallel task pipeline scenario. Specifically, it identifies a task profile
        associated with parallelism (PipeType.PARALLELISM) while ensuring the
        subsequent condition does not point to parallelism, marking it as the
        concluding task of the pipeline.

        :return: A PipelineTask object representing the final task profile in the
                 pipeline or None if no such profile is found.
        :rtype: Optional[TaskType]
        """
        task_profiles = self.get_task_profiles()
        if len(task_profiles) == 1:
            return task_profiles[0]

        for task_profile in task_profiles:
            pointer_to_task = task_profile.get_pointer_to_task()
            if (
                pointer_to_task == PipeType.PARALLELISM
                and task_profile.condition_node.on_success_pipe != PipeType.PARALLELISM
            ):
                return task_profile
        return None

    def get_result_evaluator(self) -> typing.Optional["EventEvaluator"]:
        """
        Retrieves the result evaluation strategy from the task profile.

        This method first identifies the relevant task profile using the
        `get_decision_task_profile` method. If a task profile is found,
        it then accesses the associated condition node to retrieve the
        result evaluation strategy.

        Returns:
            EventEvaluator: The result evaluation strategy associated with the
            task profile, or None if no task profile is found.
        """
        task_profile = self.get_decision_task_profile()
        if task_profile:
            if task_profile.options and task_profile.options.is_configured(
                "result_evaluation_strategy"
            ):
                # resolve evaluation strategy from options
                try:
                    evaluator_strategy = typing.cast(
                        ResultEvaluationStrategy,
                        task_profile.options.result_evaluation_strategy,
                    )
                    strategy = getattr(
                        ResultEvaluationStrategies,
                        evaluator_strategy.name,
                    )
                except AttributeError as e:
                    logger.warning(
                        f"Error resolving result evaluation strategy from options: {e}"
                    )
                    strategy = None

                if strategy:
                    return EventEvaluator(strategy=strategy)

            return task_profile.get_event_class().evaluator()
        return None

    def cleanup(self) -> None:
        """Clean up shared memory resources"""
        self.get_state_manager().release_state(self.state_id)

    def set_engine(self, engine: "WorkflowEngine") -> None:
        """Associate this context with its execution engine"""
        self._engine_ref = weakref.ref(engine)

    def get_engine(self) -> typing.Optional["WorkflowEngine"]:
        """Get the associated engine if still alive"""
        if self._engine_ref:
            return self._engine_ref()
        return None

    async def create_snapshot(self) -> "ContextSnapshot":
        """
        Creates a snapshot of the current context.

        The method asynchronously generates a snapshot of the current
        context state using the `SnapshotBuilder`. This snapshot can be
        used for preserving the state or for other rehydration operations.

        :return: An instance of `ContextSnapshot` representing the captured snapshot.
        :rtype: ContextSnapshot
        """

        from .rehydrator.engine.builder import SnapshotBuilder

        snapshot = await SnapshotBuilder().build(self)
        return snapshot

    def _extract_current_task_from_engine(
        self, engine: "WorkflowEngine"
    ) -> typing.Tuple[
        typing.Optional[str], typing.Optional[str], typing.Optional[dict]
    ]:
        """
        Extract the current task being executed from the engine.

        Returns:
            Tuple of (task_id, event_name, checkpoint_data)
        """
        # The engine's queue structure is: deque[TaskNode]
        # We need to peek at what's currently being processed

        # If engine tracks the current task explicitly
        node = engine.current_task_node
        if node and node.task:
            return (
                getattr(node.task, "id", None),
                node.task.event,
                self._task_checkpoint,
            )

        # Peek at the front of the queue
        # if hasattr(engine, "queue") and engine.queue:
        #     node = engine.queue[0]  # Peek without removing
        #     if node and node.task:
        #         return (
        #             getattr(node.task, "id", None),
        #             node.task.event,
        #             self._task_checkpoint,
        #         )

        return None, None, None

    async def persist(self) -> None:
        """Persist current state"""
        snapshot = await self.create_snapshot()
        await snapshot.save_async()
        logger.debug(f"Persisted context {self.state_id}")

    def set_task_checkpoint(self, checkpoint_data: dict) -> None:
        """
        Set checkpoint data for the current task (idempotency support).

        Args:
            checkpoint_data: Arbitrary data marking progress within a task
        """
        self._task_checkpoint = checkpoint_data

    def __del__(self) -> None:
        """Ensure cleanup on garbage collection"""
        try:
            if hasattr(self, "state_id") and self.state_id:
                if self._state_manager:
                    self._state_manager.release_state(self.state_id)
        except:
            pass  # Ignore errors during cleanup
