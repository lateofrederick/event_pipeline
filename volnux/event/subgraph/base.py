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

from ..meta import ControlFlowEvent, TaskDefinition
from volnux.result import ResultProcessor, ResultSet, EventResult

logger = logging.getLogger(__name__)


class SubgraphControlFlow(ControlFlowEvent):
    """
    Orchestrator for {} subgraphs in Pointy-Lang.

    Each {} in a Pointy-Lang workflow becomes a SubgraphControlFlow instance.
    It creates a lightweight sub-engine that coordinates the events inside
    the subgraph concurrently or sequentially depending on the graph structure.

    The sub-engine is a coroutine — never a process. It reports success/failure
    to the parent engine just like any other event.

    Key behaviors:
    - Concurrent branches: {A, B} — all branches start together, subgraph
      completes when all finish
    - Sequential chains: {A -> B} — B starts after A completes
    - Mixed: {A -> B, C -> D} — two chains run concurrently
    - Nested: {A -> {B, C}} — sub-engine spawns child sub-engine
    - Result aggregation: all branch results collected and returned
    - Error handling: first failure stops all branches, error propagated
    - Checkpointing: subgraph state is checkpointed as a unit
    - Command channel: pause/resume propagates to all child events
    """

    name = "SUBGRAPH"
    event_type = None  # Not a Meta Event — set at instance creation

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._subgraph_events: List[TaskType] = []
        self._subgraph_root: Optional[TaskType] = None
        self._result_processor = ResultProcessor()

    # ------------------------------------------------------------------
    # Public API — called by the parent engine
    # ------------------------------------------------------------------

    def set_subgraph(self, root: TaskType, events: List[TaskType]) -> None:
        """
        Configure the subgraph with its root task and all contained events.

        Called by the Pointy-Lang compiler when it encounters a {} node.

        Args:
            root: Entry point task for the subgraph
            events: All events contained in this subgraph
        """
        self._subgraph_root = root
        self._subgraph_events = events

    # ------------------------------------------------------------------
    # ControlFlowEvent interface
    # ------------------------------------------------------------------

    def get_template_class(self) -> None:
        # Subgraphs don't have a template class — they contain their own events
        return None

    async def process(self, *args, **kwargs) -> Tuple[bool, Any]:
        """
        Execute the subgraph using a lightweight sub-engine.

        Returns:
            Tuple of (success, aggregated_results)
        """
        if self._subgraph_root is None:
            raise StopProcessingError("Subgraph has no root task")

        logger.debug(
            f"[Subgraph:{self._task_id}] Starting execution with "
            f"{len(self._subgraph_events)} events"
        )

        sub_engine = _SubgraphEngine(
            root_task=self._subgraph_root,
            events=self._subgraph_events,
            parent_context=self._execution_context,
            pipeline=self._pipeline,
            task_id=self._task_id,
        )

        result = await sub_engine.run()

        if result.success:
            logger.debug(
                f"[Subgraph:{self._task_id}] Completed successfully. "
                f"Processed {result.tasks_processed} tasks."
            )
            return True, result.aggregated_output
        else:
            logger.error(
                f"[Subgraph:{self._task_id}] Failed with "
                f"{len(result.errors)} error(s)"
            )
            return False, result.errors

    def aggregate_results(self, results: ResultSet, original_input: Any) -> Any:
        """Aggregate is handled by the sub-engine — not called directly."""
        return results
