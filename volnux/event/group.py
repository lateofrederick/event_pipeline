import asyncio
import logging
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from volnux.exceptions import StopProcessingError, TaskSwitchingError
from volnux.execution.context import ExecutionContext
from volnux.execution.state_manager import ExecutionState, ExecutionStatus
from volnux.parser.operator import PipeType
from volnux.parser.protocols import TaskType
from volnux.pipeline import Pipeline

from .base import ControlFlowEvent, TaskDefinition
from .result import ResultProcessor, ResultSet, EventResult

logger = logging.getLogger(__name__)


class SubgraphEngineResult:
    """Result from a subgraph engine execution."""

    def __init__(
        self,
        success: bool,
        aggregated_output: Any = None,
        errors: Optional[List[Exception]] = None,
        tasks_processed: int = 0,
    ):
        self.success = success
        self.aggregated_output = aggregated_output
        self.errors = errors or []
        self.tasks_processed = tasks_processed


class SubgraphTaskNode:
    """A node in the subgraph engine's work queue."""

    def __init__(
        self,
        task: TaskType,
        previous_context: Optional[ExecutionContext] = None,
    ):
        self.task = task
        self.previous_context = previous_context


class _SubgraphEngine:
    """
    Lightweight engine for executing events within a {} subgraph.

    This is a simplified version of DefaultWorkflowEngine. It processes
    events in the subgraph using a queue-based BFS traversal. It handles
    concurrency within the subgraph and reports results to the parent
    SubgraphControlFlow.

    Key differences from DefaultWorkflowEngine:
    - No sink nodes (subgraphs don't have sinks)
    - Simplified parallelism (all branches in {} start together)
    - Runs as a coroutine, not a separate process
    - Reports to parent SubgraphControlFlow, not directly to executor
    """

    def __init__(
        self,
        root_task: TaskType,
        events: List[TaskType],
        parent_context: ExecutionContext,
        pipeline: Pipeline,
        task_id: str,
    ):
        self._root_task = root_task
        self._events = events
        self._parent_context = parent_context
        self._pipeline = pipeline
        self._task_id = task_id
        self._final_context: Optional[ExecutionContext] = None
        self._tasks_processed = 0

    async def run(self) -> SubgraphEngineResult:
        """
        Execute the subgraph using iterative queue-based traversal.

        Returns:
            SubgraphEngineResult with success status and output
        """
        if self._root_task is None:
            return SubgraphEngineResult(success=True, aggregated_output=None)

        queue: Deque[SubgraphTaskNode] = deque()
        queue.append(SubgraphTaskNode(self._root_task, None))

        errors: List[Exception] = []

        try:
            while queue:
                node = queue.popleft()
                self._tasks_processed += 1

                try:
                    # Detect concurrent branches — all branches in {}
                    # at the same nesting level start together
                    concurrent_branches = self._detect_concurrent_branches(node.task)

                    if concurrent_branches:
                        # Execute all branches concurrently
                        result = await self._execute_concurrent_branches(
                            branches=concurrent_branches,
                            previous_context=node.previous_context,
                        )
                        if not result.success:
                            errors.extend(result.errors)
                            if result.errors:
                                return SubgraphEngineResult(
                                    success=False,
                                    errors=errors,
                                    tasks_processed=self._tasks_processed,
                                )

                        self._final_context = result.final_context

                    else:
                        # Single task execution
                        context = self._build_context(
                            task=node.task,
                            previous_context=node.previous_context,
                        )
                        self._final_context = context

                        context.dispatch()

                        if self._should_terminate(context.state):
                            return SubgraphEngineResult(
                                success=False,
                                errors=[
                                    Exception(
                                        f"Execution terminated: {context.state.status}"
                                    )
                                ],
                                tasks_processed=self._tasks_processed,
                            )

                    # Determine next task
                    next_task = self._resolve_next_task(
                        node.task,
                        self._final_context if self._final_context else context,
                    )

                    if next_task:
                        queue.appendleft(
                            SubgraphTaskNode(
                                next_task,
                                self._final_context,
                            )
                        )

                except Exception as e:
                    logger.error(
                        f"[Subgraph:{self._task_id}] Error: {e}",
                        exc_info=True,
                    )
                    errors.append(e)
                    return SubgraphEngineResult(
                        success=False,
                        errors=errors,
                        tasks_processed=self._tasks_processed,
                    )

            # Aggregate results from final context
            aggregated = self._aggregate_subgraph_results()

            return SubgraphEngineResult(
                success=True,
                aggregated_output=aggregated,
                tasks_processed=self._tasks_processed,
            )

        except Exception as e:
            logger.exception(f"[Subgraph:{self._task_id}] Fatal error")
            return SubgraphEngineResult(
                success=False,
                errors=[e],
                tasks_processed=self._tasks_processed,
            )

    # ------------------------------------------------------------------
    # Concurrent branch handling
    # ------------------------------------------------------------------

    def _detect_concurrent_branches(self, task: TaskType) -> Optional[List[TaskType]]:
        """
        Detect if this task is a {} concurrency group.

        A task is a concurrency group if it has multiple outgoing
        edges labeled as concurrent branches. This is how the Pointy-Lang
        compiler marks {} groups in the compiled graph.

        Args:
            task: Task to check

        Returns:
            List of branch entry tasks, or None if sequential
        """
        if not hasattr(task, "concurrent_branches"):
            return None

        branches = task.concurrent_branches
        if not branches or len(branches) <= 1:
            return None

        return branches

    async def _execute_concurrent_branches(
        self,
        branches: List[TaskType],
        previous_context: Optional[ExecutionContext],
    ) -> "ConcurrentExecutionResult":
        """
        Execute all branches concurrently and wait for completion.

        Each branch is a chain of events. All branches start at the
        same time. The subgraph completes when all branches finish.

        Args:
            branches: Entry tasks for each concurrent branch
            previous_context: Context from before the {} group

        Returns:
            ConcurrentExecutionResult with aggregated results
        """
        # Create a sub-engine for each branch
        branch_tasks = []
        for branch_root in branches:
            branch_engine = _BranchExecutor(
                root_task=branch_root,
                parent_context=previous_context,
                pipeline=self._pipeline,
                parent_task_id=self._task_id,
            )
            branch_tasks.append(branch_engine.run())

        # Run all branches concurrently
        results = await asyncio.gather(*branch_tasks, return_exceptions=True)

        # Collect results
        errors = []
        final_contexts = []
        for result in results:
            if isinstance(result, Exception):
                errors.append(result)
            elif isinstance(result, _BranchResult):
                if not result.success:
                    errors.append(Exception(f"Branch failed: {result.errors}"))
                if result.final_context:
                    final_contexts.append(result.final_context)

        success = len(errors) == 0

        return ConcurrentExecutionResult(
            success=success,
            errors=errors,
            final_context=final_contexts[-1] if final_contexts else previous_context,
        )

    # ------------------------------------------------------------------
    # Context and flow control
    # ------------------------------------------------------------------

    def _build_context(
        self,
        task: TaskType,
        previous_context: Optional[ExecutionContext] = None,
    ) -> ExecutionContext:
        """Create an ExecutionContext for a single task."""
        context = ExecutionContext(
            pipeline=self._pipeline,
            task_profiles=task,
        )

        if previous_context:
            context.previous_context = previous_context
            previous_context.next_context = context
        elif self._parent_context:
            context.previous_context = self._parent_context

        return context

    def _should_terminate(self, state: ExecutionState) -> bool:
        """Check if execution should stop."""
        return state.status in {
            ExecutionStatus.CANCELLED,
            ExecutionStatus.ABORTED,
        }

    def _resolve_next_task(
        self,
        task: TaskType,
        context: ExecutionContext,
    ) -> Optional[TaskType]:
        """Determine the next task in the subgraph sequence."""
        if task.is_conditional:
            return self._evaluate_conditional(task, context)
        else:
            return task.condition_node.on_success_event

    def _evaluate_conditional(
        self,
        task: TaskType,
        context: ExecutionContext,
    ) -> Optional[TaskType]:
        """Evaluate a conditional branch."""
        result = context.get_last_result()
        if result is None:
            return None

        if result.success:
            return task.condition_node.on_success_event
        else:
            return task.condition_node.on_failure_event

    def _aggregate_subgraph_results(self) -> Any:
        """Collect results from the subgraph's execution context chain."""
        if self._final_context is None:
            return None

        results = []
        context = self._final_context
        while context is not None:
            if hasattr(context, "get_last_result"):
                result = context.get_last_result()
                if result is not None:
                    results.append(result)
            context = context.previous_context

        return results


# ------------------------------------------------------------------
# Support classes
# ------------------------------------------------------------------


class ConcurrentExecutionResult:
    """Result from executing concurrent branches."""

    def __init__(
        self,
        success: bool,
        errors: List[Exception],
        final_context: Optional[ExecutionContext],
    ):
        self.success = success
        self.errors = errors
        self.final_context = final_context


class _BranchResult:
    """Result from executing a single branch."""

    def __init__(
        self,
        success: bool,
        final_context: Optional[ExecutionContext] = None,
        errors: Optional[List[Exception]] = None,
    ):
        self.success = success
        self.final_context = final_context
        self.errors = errors or []


class _BranchExecutor:
    """
    Executes a single branch within a {} subgraph.

    Each branch is a chain of events (possibly with its own nested {}).
    The branch executor processes the chain sequentially and returns
    the final context and any errors.
    """

    def __init__(
        self,
        root_task: TaskType,
        parent_context: Optional[ExecutionContext],
        pipeline: Pipeline,
        parent_task_id: str,
    ):
        self._root_task = root_task
        self._parent_context = parent_context
        self._pipeline = pipeline
        self._parent_task_id = parent_task_id

    async def run(self) -> _BranchResult:
        """Execute the branch chain and return results."""
        current_task = self._root_task
        previous_context = self._parent_context
        final_context = None
        tasks_processed = 0

        try:
            while current_task is not None:
                tasks_processed += 1

                # If this task is itself a nested subgraph, execute it
                if hasattr(current_task, "is_subgraph") and current_task.is_subgraph:
                    nested = SubgraphControlFlow()
                    nested.set_subgraph(
                        current_task.subgraph_root,
                        current_task.subgraph_events,
                    )
                    nested._execution_context = previous_context
                    nested._pipeline = self._pipeline

                    success, result = await nested.process()

                    if not success:
                        return _BranchResult(
                            success=False,
                            errors=[Exception(f"Nested subgraph failed: {result}")],
                            final_context=previous_context,
                        )

                    final_context = previous_context
                else:
                    # Regular event execution
                    context = ExecutionContext(
                        pipeline=self._pipeline,
                        task_profiles=current_task,
                    )

                    if previous_context:
                        context.previous_context = previous_context

                    context.dispatch()
                    final_context = context

                previous_context = final_context
                current_task = (
                    current_task.condition_node.on_success_event
                    if current_task.condition_node
                    else None
                )

            return _BranchResult(
                success=True,
                final_context=final_context,
            )

        except Exception as e:
            return _BranchResult(
                success=False,
                errors=[e],
                final_context=final_context,
            )
