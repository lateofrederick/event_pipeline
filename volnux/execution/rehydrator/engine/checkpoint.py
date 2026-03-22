import asyncio
import logging
import typing
import weakref

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext


class AutoCheckPointer:
    """
    Automatically persists execution state at strategic points.

    Integration points:
    - Before task execution
    - After task completion
    - On status changes
    - On errors
    - Periodic timer
    """

    def __init__(
        self,
        checkpoint_interval: float = 5.0,  # seconds
        max_concurrent_checkpoints: int = 5,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
    ):
        self.checkpoint_interval = checkpoint_interval
        self.max_concurrent_checkpoints = max_concurrent_checkpoints
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self._checkpoint_task: typing.Optional[asyncio.Task] = None
        self._contexts: "weakref.WeakSet[ExecutionContext]" = weakref.WeakSet()
        self._lock = asyncio.Lock()

    def register_context(self, context: "ExecutionContext") -> None:
        """Add a context to automatic checkpointing."""
        self._contexts.add(context)

    def unregister_context(self, context: "ExecutionContext") -> None:
        """Remove a context from automatic checkpointing."""
        self._contexts.discard(context)

    async def start(self) -> None:
        """Start the periodic checkpointing loop."""
        async with self._lock:
            if self._checkpoint_task and not self._checkpoint_task.done():
                return
            self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())

    async def stop(self) -> None:
        """Stop the checkpointing loop."""
        async with self._lock:
            task = self._checkpoint_task
            self._checkpoint_task = None

        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _checkpoint_loop(self) -> None:
        """Periodic checkpoint task."""
        try:
            while True:
                await asyncio.sleep(self.checkpoint_interval)
                await self.checkpoint_all()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Checkpoint loop crashed: {e}")

    async def _checkpoint_with_retry(self, context: "ExecutionContext") -> None:
        """Persist in a single context with retries."""
        last_error = None

        for attempt in range(1, self.retry_attempts + 1):
            try:
                await context.persist()
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Checkpoint failed for {context.state_id} "
                    f"(attempt {attempt}/{self.retry_attempts}): {e}"
                )
                if attempt < self.retry_attempts:
                    await asyncio.sleep(self.retry_delay * attempt)

        logger.error(
            f"Failed to checkpoint {context.state_id} after "
            f"{self.retry_attempts} attempts: {last_error}"
        )

    async def checkpoint_all(self) -> None:
        """Checkpoint all registered contexts."""
        contexts = [context for context in list(self._contexts) if context is not None]
        if not contexts:
            return

        semaphore = asyncio.Semaphore(self.max_concurrent_checkpoints)

        async def checkpoint_one(context: "ExecutionContext") -> None:
            async with semaphore:
                await self._checkpoint_with_retry(context)

        await asyncio.gather(
            *(checkpoint_one(context) for context in contexts),
            return_exceptions=True,
        )

    async def checkpoint_on_event(
        self, context: "ExecutionContext", event_name: str
    ) -> None:
        """
        Checkpoint triggered by specific events.

        Args:
            context: The context to checkpoint
            event_name: Event that triggered the checkpoint
        """
        logger.debug(f"Checkpointing {context.state_id} on event: {event_name}")
        await self._checkpoint_with_retry(context)
