import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, List, Tuple

from volnux.exceptions import SagaError, SagaCompensationError

logger = logging.getLogger(__name__)


async def _invoke(fn: Callable[[], Any]) -> Any:
    """Call fn(); if it returns a coroutine, await it."""
    result = fn()
    if asyncio.iscoroutine(result):
        return await result
    return result


@dataclass
class SagaStep:
    """A single unit of work with its corresponding undo operation."""

    action: Callable[[], Any]
    compensation: Callable[[], Any]


class Saga:
    """Execute a sequence of steps with compensation on failure.

    Each step's action is run in order. If any action raises, all previously
    completed steps are compensated in reverse order.

    Results from actions are not forwarded to subsequent steps. If your steps
    need to share state (e.g. a generated ID from step 1 used in step 2),
    close over a shared mutable container in your callables::

        >>> state = {}
        ... saga.add_step(
        ...    action=lambda: state.update(id=redis.insert(...)) or state["id"],
        ...    compensation=lambda: redis.delete(state["id"]),
        ... )

    Raises:
        SagaError: A step failed and all compensations succeeded.
        SagaCompensationError: A step failed *and* one or more compensations
            also failed. The system may be inconsistent; manual intervention
            is likely required.
        RuntimeError: If ``execute()`` is called more than once on the same
            instance.

    Usage:

        >>> saga = Saga()
        ... saga.add_step(
        ...    action=lambda: redis.insert(...),
        ...    compensation=lambda: redis.delete(...),
        ... )
        ... saga.add_step(
        ...    action=lambda: pg.insert(...),
        ...    compensation=lambda: pg.update(...),
        ... )
        ... await saga.execute()
    """

    def __init__(self):
        self._steps: List[SagaStep] = []
        self._executed = False

    def add_step(
        self, action: Callable[[], Any], compensation: Callable[[], Any]
    ) -> None:
        """Register a step.

        Args:
            action: Callable that performs the work. May be sync or async.
            compensation: Callable that undoes the work. May be sync or async.
                Must be provided; use ``lambda: None`` explicitly if a step is
                truly irreversible and you accept that risk.

        Raises:
            ValueError: If either argument is not callable.
            RuntimeError: If called after ``execute()`` has already been invoked.
        """
        if self._executed:
            raise RuntimeError("Cannot add steps after execute() has been called.")
        if not callable(action):
            raise ValueError(
                f"'action' must be callable, got {type(action).__name__!r}."
            )
        if not callable(compensation):
            raise ValueError(
                f"'compensation' must be callable, got {type(compensation).__name__!r}."
            )
        self._steps.append(SagaStep(action=action, compensation=compensation))

    async def execute(self) -> None:
        """Run all steps in order. On failure, compensate completed ones in reverse.

        Raises:
            RuntimeError: If called more than once on this instance.
            SagaError: If a step fails and all compensations succeed.
            SagaCompensationError: If a step fails and one or more compensations
                also fail.
        """
        if self._executed:
            raise RuntimeError(
                "This Saga instance has already been executed. "
                "Create a new Saga to retry."
            )
        self._executed = True

        completed_count = 0
        try:
            for i, step in enumerate(self._steps):
                await _invoke(step.action)
                completed_count = i + 1
        except Exception as original_error:
            logger.error("Saga step %d failed: %s", completed_count, original_error)
            compensation_errors = await self._compensate(completed_count - 1)

            if compensation_errors:
                failed_indices = [idx for idx, _ in compensation_errors]
                raise SagaCompensationError(
                    f"Saga failed at step {completed_count} and compensation also "
                    f"failed for step(s) {failed_indices}. "
                    f"Manual intervention may be required.",
                    original_error=original_error,
                    compensation_errors=compensation_errors,
                ) from original_error

            raise SagaError(
                f"Saga failed at step {completed_count}. "
                f"Compensated {completed_count - 1} step(s) successfully.",
                original_error=original_error,
            ) from original_error

    async def _compensate(self, up_to_index: int) -> List[Tuple[int, Exception]]:
        """Compensate steps from up_to_index down to 0, inclusive.

        If up_to_index is -1 (step 0 failed before completing), this is a
        no-op because there is nothing to undo.

        Returns:
            A list of (step_index, exception) for any compensation that raised.
            An empty list means all compensations succeeded.
        """
        failures: List[Tuple[int, Exception]] = []
        for i in range(up_to_index, -1, -1):
            try:
                await _invoke(self._steps[i].compensation)
            except Exception as e:
                logger.error(
                    "Saga compensation for step %d failed: %s. "
                    "Manual intervention may be required.",
                    i,
                    e,
                )
                failures.append((i, e))
        return failures
