import asyncio
import logging
import weakref
import typing
import time
from dataclasses import dataclass
from typing import Protocol

from .protocol import Monitorable, Snapshot

logger = logging.getLogger(__name__)


class VolnuxCheckPointManager:
    """
    Manages checkpoints for monitoring and persisting application state.

    The VolnuxCheckPointManager is designed to handle periodic state snapshotting,
    queued persistence operations, and long-lived monitoring of execution contexts.
    It supports concurrency, retry mechanisms, and snapshot expiration policies,
    while offloading persistence to an asynchronous worker.

    :ivar checkpoint_interval: Interval in seconds between periodic state snapshots.
    :type checkpoint_interval: float
    :ivar retry_attempts: Number of retry attempts for persistence operations.
    :type retry_attempts: int
    :ivar retry_delay: Delay in seconds between retry attempts for persistence operations.
    :type retry_delay: float
    :ivar snapshot_ttl: Time-to-live (in seconds) for captured snapshots in the state store.
    :type snapshot_ttl: int
    """

    def __init__(
        self,
        *,
        checkpoint_interval: float = 5.0,
        max_concurrent: int = 5,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
        snapshot_ttl: int = 3600,
    ):
        # self.state_store = state_store
        self.checkpoint_interval = checkpoint_interval
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.snapshot_ttl = snapshot_ttl

        # Push Queue
        self._queue: asyncio.Queue[Snapshot] = asyncio.Queue()

        # Monitored Set: For periodic snapshotting
        self._monitored_contexts: weakref.WeakSet[Monitorable] = weakref.WeakSet()

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._worker_task: typing.Optional[asyncio.Task] = None
        self._monitor_task: typing.Optional[asyncio.Task] = None
        self._running = False

    def enqueue(self, snapshot: Snapshot):
        """
        Enqueue a snapshot for persistence.
        :param snapshot: The snapshot data to be enqueued.
        :return: None
        """
        self._queue.put_nowait(snapshot)

    def monitor(self, context: Monitorable):
        """
        Standard 'Pull' interface for long-lived Monitorable contexts.

        Adds the specified context to the monitored contexts, enabling long-lived
        tracking and management of execution states related to the provided context.

        :param context: The monitorable context to be added to the monitored list.
          Must be an instance of Monitorable, such as ExecutionContext.
        :type context: Monitorable
        :return: None
        """
        self._monitored_contexts.add(context)

    def unmonitor(self, context: Monitorable):
        """
        Unmonitors the given monitorable context by discarding it from the set of
        monitored contexts.

        This method provides a standard 'Pull' interface for handling long-lived
        execution contexts. Once unmonitored, the specified context will no longer
        be tracked.

        :param context: The monitorable context to be unmonitored.
        :type context: Monitorable
        :return: None
        """
        self._monitored_contexts.discard(context)

    async def start(self):
        """Starts both the consumer and the periodic monitor."""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._persistence_loop())
        self._monitor_task = asyncio.create_task(self._periodic_monitor())
        logger.info("Unified CheckpointManager started.")

    async def _periodic_monitor(self):
        while self._running:
            await asyncio.sleep(self.checkpoint_interval)
            for context in list(self._monitored_contexts):
                try:
                    # Offload the work to the queue
                    snapshot = await context.create_snapshot()
                    self.enqueue(snapshot)
                except Exception as e:
                    logger.error(f"Failed to sample context {context.state_id}: {e}")

    async def _persistence_loop(self):
        """The single consumer for all persistence requests."""
        while self._running:
            snapshot = await self._queue.get()
            async with self._semaphore:
                await self._persist_with_retry(snapshot)
            self._queue.task_done()

    async def _persist_with_retry(self, snapshot: Snapshot):
        """Implements your original retry logic with backoff."""
        last_error = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                await snapshot.save_async(ttl=self.snapshot_ttl)
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_error = e
                delay = self.retry_delay * attempt
                logger.warning(
                    f"Persist failed for {snapshot.id} (attempt {attempt}): {e}"
                )
                if attempt < self.retry_attempts:
                    await asyncio.sleep(delay)

        logger.error(
            f"Persistence failed after {self.retry_attempts} tries: {last_error}"
        )

    async def flush(self):
        """The Preemption Barrier: Ensures the queue is empty before a swap."""
        await self._queue.join()

    async def stop(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
        await self.flush()
        if self._worker_task:
            self._worker_task.cancel()
