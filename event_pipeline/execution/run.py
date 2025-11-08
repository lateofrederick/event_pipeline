import typing
import logging
from collections import deque
from event_pipeline.typing import TaskType
from event_pipeline.pipeline import Pipeline
from .utils import evaluate_context_execution_results
from event_pipeline.exceptions import SwitchTask
from event_pipeline.parser.operator import PipeType
from event_pipeline.exceptions import TaskSwitchingError
from event_pipeline.execution.state_manager import ExecutionStatus
from event_pipeline.execution.context import ExecutionContext

logger = logging.getLogger(__name__)

# TODO: This code needs massive refactoring in favour of
#  iterative algorithm instead of recursive one. Also, a
#  clear execution mechanism must be designed to avoid edge cases(Runners scheme is better).
#  For now we are focus on getting the flow runner to work with our old code


def get_next_non_executable_task(context: ExecutionContext):
    pass


def handle_parallel_tasks(task: TaskType) -> typing.Set[TaskType]:
    parallel_tasks = set()

    # Loop through the chain of tasks, adding each task to the 'parallel_tasks' set,
    # until we encounter a task where the 'on_success_pipe' is no longer equal
    # to PipeType.PARALLELISM.
    # This indicates that the task is part of a parallel execution chain.
    while task and task.condition_node.on_success_pipe == PipeType.PARALLELISM:
        parallel_tasks.add(task)
        task = task.condition_node.on_success_event

    if parallel_tasks:
        parallel_tasks.add(task)

    return parallel_tasks


def run_workflow(
    task: TaskType,
    pipeline: "Pipeline",
    sink_queue: typing.Deque[TaskType],
    previous_context: typing.Optional[ExecutionContext] = None,
) -> None:
    """
    Executes a specific task in the pipeline and manages the flow of data.

    Args:
        task: The pipeline task to be executed.
        pipeline: The pipeline object that orchestrates the task execution.
        sink_queue: The queue used to store sink nodes for later processing.
        previous_context: An optional EventExecutionContext containing previous
                          execution context, if available.

    This method performs the necessary operations for executing a task, handles
    task-specific logic, and updates the sink queue with sink nodes for further processing.
    """
    # TODO: make this function iterative

    if task:
        parallel_tasks = None

        if task.is_parallel_execution_node:
            parallel_tasks = handle_parallel_tasks(task)

        execution_context = ExecutionContext(
            pipeline=pipeline,
            task_profiles=list(parallel_tasks) if parallel_tasks else task,
        )

        if previous_context is None:
            pipeline.execution_context = execution_context
        else:
            if task.sink_node:
                sink_queue.append(task.sink_node)

            execution_context.previous_context = previous_context
            previous_context.next_context = execution_context

        execution_context.dispatch()  # execute task

        execution_state = execution_context.state

        if execution_state.status in [
            ExecutionStatus.CANCELLED,
            ExecutionStatus.ABORTED,
        ]:
            logger.warning(
                f"Task execution terminated due to state '{execution_state.status}'."
                f"\n Skipping task execution..."
            )
            return

        switch_request = execution_state.get_switch_request()

        if typing.TYPE_CHECKING:
            switch_request = typing.cast(SwitchTask, switch_request)

        if switch_request and switch_request.descriptor_configured:
            task_profile = task.get_descriptor(switch_request.next_task_descriptor)
            if task_profile is None:
                logger.warning(
                    f"Task cannot switch to task with the descriptor {switch_request.next_task_descriptor}."
                )
                raise TaskSwitchingError(
                    f"Task cannot switch to task using the descriptor {switch_request.next_task_descriptor}.",
                    params=switch_request,
                    code="task-switching-failed",
                )

            run_workflow(
                task=task_profile,
                pipeline=pipeline,
                previous_context=previous_context,
                sink_queue=sink_queue,
            )
        else:
            if task.is_conditional:
                evaluation_result = evaluate_context_execution_results(
                    execution_context
                )

                if evaluation_result is not None:

                    if not evaluation_result.success:
                        run_workflow(
                            task=task.condition_node.on_failure_event,
                            previous_context=execution_context,
                            pipeline=pipeline,
                            sink_queue=sink_queue,
                        )
                    else:
                        run_workflow(
                            task=task.condition_node.on_success_event,
                            previous_context=execution_context,
                            pipeline=pipeline,
                            sink_queue=sink_queue,
                        )
                else:
                    logger.error(
                        f"Cannot evaluate conditional task '{task}' without execution results."
                    )
            else:
                if execution_context.is_multitask():
                    last_task = execution_context.get_decision_task_profile()
                    if last_task is None:
                        return

                    run_workflow(
                        task=last_task.condition_node.on_success_event,
                        previous_context=execution_context,
                        pipeline=pipeline,
                        sink_queue=sink_queue,
                    )
                else:
                    run_workflow(
                        task=task.condition_node.on_success_event,
                        previous_context=execution_context,
                        pipeline=pipeline,
                        sink_queue=sink_queue,
                    )

    else:
        # clear the sink nodes
        while sink_queue:
            task = sink_queue.pop()
            run_workflow(
                task=task,
                previous_context=previous_context,
                pipeline=pipeline,
                sink_queue=sink_queue,
            )
