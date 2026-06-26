import logging
import typing
from collections import deque

from volnux.exceptions import TaskSwitchingError
from volnux.execution.context import ExecutionContext
from volnux.execution.state_manager import ExecutionState, ExecutionStatus
from volnux.parser.operator import PipeType
from volnux.parser.protocols import TaskType
from volnux.pipeline import Pipeline

from ...execution.utils import evaluate_context_execution_results
from .base import (
    CheckPointConfig,
    CheckPointFrequency,
    EngineExecutionResult,
    EngineResult,
    TaskNode,
    WorkflowEngine,
)

logger = logging.getLogger(__name__)


class DefaultWorkflowEngine(WorkflowEngine):
    """
    Default iterative workflow execution engine.

    This engine uses a queue-based traversal strategy to execute
    workflows without recursion, preventing stack overflow issues.

    Orchestration Strategy:
    - Queue-based task scheduling (LIFO for depth-first behavior)
    - Parallel task detection and grouping
    - Conditional branching evaluation
    - Dynamic task switching support
    - Deferred sink node execution
    - Context chaining for execution history
    - Opt-in checkpointing via base class hooks

    The engine focuses solely on flow control and delegates:
    - Task execution -> ExecutionContext
    - Metrics collection -> ExecutionContext
    - Hook invocation -> ExecutionContext
    - Error handling -> ExecutionContext
    """

    def __init__(
        self,
        enable_checkpointing: bool = False,
        checkpoint_config: typing.Optional[CheckPointConfig] = None,
        enable_debug_logging: bool = False,
        strict_mode: bool = True,
    ):
        """
        Initialize the default workflow engine.

        Args:
            enable_checkpointing: If True, enables checkpoint persistence
            checkpoint_config: Optional checkpoint configuration; defaults
                to CheckPointConfig() when enable_checkpointing is True
            enable_debug_logging: If True, enables detailed flow logging
            strict_mode: If True, stops execution on first error; if False,
                attempts to continue
        """
        super().__init__(
            enable_checkpointing=enable_checkpointing,
            checkpoint_config=checkpoint_config,
        )
        self._task_queue: typing.Deque[TaskNode] = deque()
        self._sink_queue: typing.Deque[TaskType] = deque()
        self.enable_debug_logging = enable_debug_logging
        self.strict_mode = strict_mode

    @property
    def task_queue(self) -> typing.Deque[TaskNode]:
        """Primary engine queue used for active task scheduling."""
        return self._task_queue

    @property
    def sink_queue(self) -> typing.Deque[TaskType]:
        """Queue used for deferred/sink tasks."""
        return self._sink_queue

    def get_name(self) -> str:
        return "DefaultIterativeEngine"

    async def execute(
        self,
        root_task: TaskType,
        pipeline: Pipeline,
    ) -> EngineResult:
        """
        Execute a workflow using iterative queue-based traversal.

        Flow:
        1. Reset queues and counters for fresh execution
        2. Initialize a work queue with a root task
        3. Process tasks from queue (LIFO via appendleft/popleft)
        4. Detect parallelism and group parallel tasks
        5. Create execution context and chain to previous
        6. Checkpoint before a task (if checkpointing is enabled)
        7. Delegate execution to context (context handles hooks/metrics)
        8. Checkpoint after a task (if checkpointing is enabled)
        9. Evaluate execution state for early termination
        10. Handle dynamic task switching
        11. Evaluate conditionals and determine the next task
        12. Schedule the next task in the queue
        13. Process deferred sink nodes

        Args:
            root_task: The workflow entry point
            pipeline: The pipeline with configuration

        Returns:
            EngineResult with execution status and final context
        """
        if not root_task:
            if self.enable_debug_logging:
                logger.debug("[Engine] No root task provided")
            return EngineResult(
                status=EngineExecutionResult.COMPLETED, tasks_processed=0
            )

        # Reset state for fresh execution
        self.task_queue.clear()
        self.sink_queue.clear()
        self.tasks_processed = 0
        self.final_context = None
        self.current_task_node = None

        # Seed the work queue
        self.task_queue.append(TaskNode(root_task, None))

        try:
            while self.task_queue:
                executable_node = self.task_queue.popleft()
                self.tasks_processed += 1

                if self.enable_debug_logging:
                    logger.debug(f"[Engine] Processing task: {executable_node.task}")

                try:
                    # Detect parallelism
                    parallel_tasks = self._detect_parallel_tasks(executable_node.task)

                    # Build execution context
                    execution_context = self._build_context(
                        task=executable_node.task,
                        pipeline=pipeline,
                        previous_context=executable_node.previous_context,
                        parallel_tasks=parallel_tasks,
                    )

                    self.final_context = execution_context

                    # Checkpoint before task execution (idempotency support)
                    await self._checkpoint_before_task(
                        execution_context, executable_node
                    )

                    # Dispatch task profiles for execution
                    await execution_context.dispatch()

                    # Get execution state
                    execution_state = await execution_context.state_async

                    # Checkpoint after task completion
                    await self._checkpoint_after_task(execution_context, success=True)

                    if self._should_terminate(execution_state):
                        status = self._map_termination_status(execution_state.status)
                        return EngineResult(
                            status=status,
                            final_context=self.final_context,
                            tasks_processed=self.tasks_processed,
                        )

                    # Handle task switching
                    switched = self._handle_task_switch(
                        task=executable_node.task,
                        execution_state=execution_state,
                        previous_context=executable_node.previous_context,
                        queue=self.task_queue,
                    )
                    if switched:
                        continue

                    # Determine next task
                    next_task = self._resolve_next_task(
                        executable_node.task, execution_context
                    )

                    # Schedule the next task (prepend for LIFO depth-first)
                    if next_task:
                        self._task_queue.appendleft(
                            TaskNode(next_task, execution_context)
                        )

                except Exception as e:
                    logger.error(
                        f"[Engine] Error processing task "
                        f"{executable_node.task}: {e}",
                        exc_info=True,
                    )

                    if self.strict_mode:
                        return EngineResult(
                            status=EngineExecutionResult.FAILED,
                            final_context=self.final_context,
                            error=e,
                            tasks_processed=self.tasks_processed,
                        )
                    # In non-strict mode, continue to next task

            # Process deferred sink nodes
            await self._drain_sink_nodes(pipeline)

            if self.enable_debug_logging:
                logger.debug(
                    f"[Engine] Completed processing {self.tasks_processed} tasks"
                )

        except Exception as e:
            logger.exception("[Engine] Fatal error during workflow execution")
            return EngineResult(
                status=EngineExecutionResult.FAILED,
                final_context=self.final_context,
                error=e,
                tasks_processed=self.tasks_processed,
            )

        return EngineResult(
            status=EngineExecutionResult.COMPLETED,
            final_context=self.final_context,
            tasks_processed=self.tasks_processed,
        )

    async def _checkpoint_after_task(self, context: "ExecutionContext", success: bool):
        """
        Override base to persist after a task without double-counting.

        The execute() loop manages ``self.tasks_processed`` directly,
        so this override only handles persistence and state cleanup.
        """
        if not self._checkpointer:
            return

        self.current_task_node = None

        if self._checkpoint_frequency in [
            CheckPointFrequency.PER_TASK,
            CheckPointFrequency.ON_STATE_CHANGE,
        ]:
            await context.persist()
            logger.debug(
                f"[Engine] Checkpointed after task: "
                f"{self.tasks_processed} tasks processed"
            )

    def _detect_parallel_tasks(
        self, task: TaskType
    ) -> typing.Optional[typing.Set[TaskType]]:
        """
        Detect parallel task chains by following PARALLELISM pipes.

        Walks the success path collecting tasks marked for parallel execution
        until a non-parallel pipe or end is reached.

        Args:
            task: Task to check for parallel execution

        Returns:
            Set of tasks to execute in parallel, or None if sequential
        """
        if not task.is_parallel_execution_node:
            return None

        parallel_tasks: typing.Set[TaskType] = set()
        current = task

        while (
            current and current.condition_node.on_success_pipe == PipeType.PARALLELISM
        ):
            parallel_tasks.add(current)
            current = current.condition_node.on_success_event

        # Include final task in parallel chain
        if parallel_tasks and current:
            parallel_tasks.add(current)

        if self.enable_debug_logging and parallel_tasks:
            logger.debug(f"[Engine] Detected {len(parallel_tasks)} parallel tasks")

        return parallel_tasks if parallel_tasks else None

    def _build_context(
        self,
        task: TaskType,
        pipeline: Pipeline,
        previous_context: typing.Optional[ExecutionContext] = None,
        parallel_tasks: typing.Optional[typing.Set[TaskType]] = None,
    ) -> ExecutionContext:
        """
        Create and chain execution context.

        Context creation is the engine's responsibility, but execution,
        metrics, and hooks are all handled by the context itself.

        Args:
            task: Primary task
            pipeline: Workflow pipeline
            previous_context: Previous context for chaining
            parallel_tasks: Parallel task group if applicable

        Returns:
            Configured ExecutionContext
        """
        context = ExecutionContext(
            pipeline=pipeline,  # type: ignore
            task_profiles=list(parallel_tasks) if parallel_tasks else task,  # type: ignore
        )
        context.set_engine(self)

        if previous_context is None:
            # First context becomes pipeline's root context
            pipeline.execution_context = context
        else:
            # Collect sink nodes for deferred execution
            if task.sink_node:
                self.sink_queue.append(task.sink_node)

            # Link context chain
            context.previous_context = previous_context
            previous_context.next_context = context

        return context

    def _should_terminate(self, execution_state: ExecutionState) -> bool:
        """
        Check if execution should stop due to cancellation/abortion.

        Args:
            execution_state: Current execution state

        Returns:
            True if execution should terminate early
        """
        should_stop = execution_state.status in {
            ExecutionStatus.CANCELLED,
            ExecutionStatus.ABORTED,
        }

        if should_stop and self.enable_debug_logging:
            logger.debug(f"[Engine] Early termination: {execution_state.status}")

        return should_stop

    def _map_termination_status(
        self, execution_status: ExecutionStatus
    ) -> EngineExecutionResult:
        """Map execution status to engine result status."""
        return EngineExecutionResult.TERMINATED_EARLY

    def _handle_task_switch(
        self,
        task: TaskType,
        execution_state: ExecutionState,
        queue: typing.Deque[TaskNode],
        previous_context: typing.Optional[ExecutionContext] = None,
    ) -> bool:
        """
        Handle dynamic task switching via descriptors.

        When a task requests a switch, validates the target descriptor
        and schedules the new task with the same previous context.

        Args:
            task: Current task
            execution_state: State with potential switch request
            previous_context: Context to reuse for switched task
            queue: Work queue to prepend a switched task

        Returns:
            True if a switch occurred, False otherwise

        Raises:
            TaskSwitchingError: If target descriptor doesn't exist
        """
        switch_request = execution_state.get_switch_request()

        if not switch_request or not switch_request.descriptor_configured:  # type: ignore
            return False

        next_task = task.get_descriptor(switch_request.next_task_descriptor)  # type: ignore

        if next_task is None:
            raise TaskSwitchingError(
                f"Cannot switch to descriptor "
                f"'{switch_request.next_task_descriptor}'",
                params=switch_request,
                code="task-switching-failed",
            )

        # Schedule the switched task
        queue.appendleft(TaskNode(next_task, previous_context))

        if self.enable_debug_logging:
            logger.debug(
                f"[Engine] Switched to descriptor: "
                f"{switch_request.next_task_descriptor}"  # type: ignore
            )

        return True

    def _resolve_next_task(
        self, task: TaskType, execution_context: ExecutionContext
    ) -> typing.Optional[TaskType]:
        """
        Determine the next task based on conditional or sequential flow.

        For conditional tasks, evaluate the condition and follow
         the success / failure branch. For normal tasks, follow a success path.

        Args:
            task: Current task
            execution_context: Context with execution results

        Returns:
            Next task to execute, or None if the workflow ends
        """
        if task.is_conditional:
            return self._evaluate_conditional_branch(task, execution_context)
        else:
            return self._follow_sequential_flow(task, execution_context)

    def _evaluate_conditional_branch(
        self, task: TaskType, execution_context: ExecutionContext
    ) -> typing.Optional[TaskType]:
        """
        Evaluate conditional and select branch.

        Args:
            task: Conditional task
            execution_context: Context with results

        Returns:
            Task on success or failure branch
        """
        result = evaluate_context_execution_results(execution_context)

        if result is None:
            logger.error(f"[Engine] Conditional task has no result: {task}")
            return None

        next_task = (
            task.condition_node.on_failure_event
            if not result.success
            else task.condition_node.on_success_event
        )

        if self.enable_debug_logging:
            branch = "success" if result.success else "failure"
            logger.debug(f"[Engine] Conditional branch: {branch}")

        return next_task  # type: ignore

    def _follow_sequential_flow(
        self, task: TaskType, execution_context: ExecutionContext
    ) -> typing.Optional[TaskType]:
        """
        Follow the normal sequential flow.

        For multitask contexts, uses decision task's success path.
        For single tasks, follows standard success path.

        Args:
            task: Current task
            execution_context: Context

        Returns:
            Next task in sequence
        """
        if execution_context.is_multitask():
            decision_task = execution_context.get_decision_task_profile()
            return (
                decision_task.condition_node.on_success_event if decision_task else None
            )
        else:
            return task.condition_node.on_success_event  # type: ignore

    async def _drain_sink_nodes(self, pipeline: Pipeline) -> None:
        """
        Execute deferred sink nodes.

        Sink nodes are executed after the main workflow completes,
        typically for cleanup or finalization tasks.

        Args:
            pipeline: Workflow pipeline
        """
        if not self._sink_queue:
            return

        if self.enable_debug_logging:
            logger.debug(f"[Engine] Processing {len(self._sink_queue)} sink nodes")

        while self._sink_queue:
            sink_task = self._sink_queue.popleft()

            try:
                # Create standalone context for sink node
                context = ExecutionContext(
                    pipeline=pipeline,  # type: ignore
                    task_profiles=sink_task,  # type: ignore
                )
                await context.dispatch()

            except Exception as e:
                logger.error(f"[Engine] Error in sink node {sink_task}: {e}")
                if self.strict_mode:
                    raise
