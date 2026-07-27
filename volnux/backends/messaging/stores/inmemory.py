import asyncio
import copy
import json
import logging
import threading
import typing
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Type, Union

from volnux.backends.messaging.base import (
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    QueueSide,
)

logger = logging.getLogger(__name__)


def _serialise(value: Any) -> str:
    """Serialise Python objects to JSON strings."""
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def _deserialise(raw: str) -> Any:
    """Deserialise JSON strings back to Python objects."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw


class InMemoryPubSubMixin(PubSubCapabilityMixin):
    """
    In-memory pub/sub implementation for InMemoryKeyValueStoreBackend.

    Uses thread-safe queues to broadcast messages to subscribers within the
    same process. This is suitable for testing, single-process applications,
    and scenarios where cross-process communication is not required.

    Contract
    --------
    - All operations are async (wrapped with asyncio.to_thread where needed)
    - Subscribers within the same process receive messages
    - Messages do NOT propagate across processes (in-memory only)
    - Pattern matching uses fnmatch (similar to Redis glob patterns)
    """

    def __post_init_pubsub__(self):
        """Initialize pub/sub data structures. Call from __init__."""
        if not hasattr(self, "_pubsub_channels"):
            # channel -> set of subscriber queues
            self._pubsub_channels: Dict[str, set] = defaultdict(set)
            # pattern -> set of subscriber queues
            self._pubsub_patterns: Dict[str, set] = defaultdict(set)
            self._pubsub_lock = threading.RLock()

    async def publish(self, channel: str, message: Any) -> int:
        """
        Publish a message to a channel.

        Broadcasts to all subscribers matching the exact channel name
        and to subscribers whose patterns match the channel.
        """
        if not hasattr(self, "_pubsub_lock"):
            self.__post_init_pubsub__()

        payload = _serialise(message)
        receivers = 0

        # Offload synchronous operations to thread pool
        await asyncio.to_thread(self._publish_sync, channel, payload, receivers)

        # Count receivers
        with self._pubsub_lock:
            # Exact channel subscribers
            receivers += len(self._pubsub_channels.get(channel, set()))

            # Pattern subscribers
            import fnmatch

            for pattern, queues in self._pubsub_patterns.items():
                if fnmatch.fnmatch(channel, pattern):
                    receivers += len(queues)

        logger.debug("Published to %s — %d receiver(s)", channel, receivers)
        return receivers

    def _publish_sync(self, channel: str, payload: str, receivers: int) -> None:
        """Synchronous publish logic executed in thread pool."""
        import fnmatch

        with self._pubsub_lock:
            # Send to exact channel subscribers
            for queue in self._pubsub_channels.get(channel, set()):
                try:
                    queue.put_nowait(
                        {
                            "type": "message",
                            "channel": channel,
                            "data": payload,
                        }
                    )
                except:
                    pass  # Subscriber queue full or closed

            # Send to pattern subscribers
            for pattern, queues in self._pubsub_patterns.items():
                if fnmatch.fnmatch(channel, pattern):
                    for queue in queues:
                        try:
                            queue.put_nowait(
                                {
                                    "type": "pmessage",
                                    "channel": channel,
                                    "pattern": pattern,
                                    "data": payload,
                                }
                            )
                        except:
                            pass

    def subscribe(self, *channels: str) -> "InMemorySubscriptionContext":
        """Subscribe to one or more exact channel names."""
        if not hasattr(self, "_pubsub_lock"):
            self.__post_init_pubsub__()
        return InMemorySubscriptionContext(
            backend=self,
            channels=list(channels),
            patterns=[],
        )

    def psubscribe(self, *patterns: str) -> "InMemorySubscriptionContext":
        """Subscribe using glob patterns (fnmatch-style)."""
        if not hasattr(self, "_pubsub_lock"):
            self.__post_init_pubsub__()
        return InMemorySubscriptionContext(
            backend=self,
            channels=[],
            patterns=list(patterns),
        )


class InMemorySubscriptionContext:
    """
    Async context manager for in-memory subscriptions.

    Creates a queue for receiving messages, registers it with the backend's
    channel/pattern maps, and yields an async iterator of messages.
    """

    def __init__(
        self,
        backend: "InMemoryKeyValueStoreBackend",
        channels: List[str],
        patterns: List[str],
    ):
        self.backend = backend
        self.channels = channels
        self.patterns = patterns
        self._queue: Optional[asyncio.Queue] = None
        self._sync_queue: Optional[typing.Any] = (
            None  # threading.Queue for sync operations
        )

    async def __aenter__(self) -> "InMemorySubscriptionContext":
        """Register subscription and create message queue."""
        import queue

        # Create both async and sync queues
        self._queue = asyncio.Queue(maxsize=1000)
        self._sync_queue = queue.Queue(maxsize=1000)

        # Register in thread pool
        await asyncio.to_thread(self._register_sync)

        logger.debug(
            "Subscribed to channels=%s patterns=%s", self.channels, self.patterns
        )
        return self

    def _register_sync(self):
        """Synchronous registration logic."""
        with self.backend._pubsub_lock:
            for channel in self.channels:
                self.backend._pubsub_channels[channel].add(self._sync_queue)
            for pattern in self.patterns:
                self.backend._pubsub_patterns[pattern].add(self._sync_queue)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Unsubscribe and cleanup."""
        await asyncio.to_thread(self._unregister_sync)

        # Clear queue
        if self._queue:
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

    def _unregister_sync(self):
        """Synchronous unregistration logic."""
        with self.backend._pubsub_lock:
            for channel in self.channels:
                self.backend._pubsub_channels[channel].discard(self._sync_queue)
                if not self.backend._pubsub_channels[channel]:
                    del self.backend._pubsub_channels[channel]

            for pattern in self.patterns:
                self.backend._pubsub_patterns[pattern].discard(self._sync_queue)
                if not self.backend._pubsub_patterns[pattern]:
                    del self.backend._pubsub_patterns[pattern]

    def __aiter__(self):
        """Return self as async iterator."""
        return self

    async def __anext__(self):
        """
        Yield the next message from the subscription queue.

        Bridges between sync queue (where publishers write) and async iterator.
        """
        import queue

        while True:
            try:
                # Check sync queue in thread pool
                msg = await asyncio.to_thread(self._sync_queue.get, timeout=0.1)

                if msg["type"] in ("message", "pmessage"):
                    return {
                        "channel": msg["channel"],
                        "pattern": msg.get("pattern"),
                        "data": _deserialise(msg["data"]),
                    }
            except queue.Empty:
                # Small sleep to prevent busy-wait
                await asyncio.sleep(0.01)
            except Exception as exc:
                logger.error("Error receiving message: %s", exc)
                raise StopAsyncIteration


class InMemoryPushPopMixin(PushPopCapabilityMixin):
    """
    In-memory queue implementation for InMemoryKeyValueStoreBackend.

    Uses Python's `collections.deque` for efficient FIFO/LIFO operations.
    All operations are thread-safe and async-compatible.

    Contract
    --------
    - Thread-safe operations via RLock
    - Async-compatible via asyncio.to_thread wrappers
    - Blocking pop uses asyncio.Event for signaling
    - All operations are in-process only (not distributed)
    """

    def __post_init_queues__(self):
        """Initialize queue data structures. Call from __init__."""
        if not hasattr(self, "_queues"):
            # key -> deque
            self._queues: Dict[str, deque] = {}
            # key -> asyncio.Event for blocking pop signaling
            self._queue_events: Dict[str, asyncio.Event] = {}
            self._queue_lock = threading.RLock()

    async def push(
        self,
        key: str,
        *values: Any,
        side: QueueSide = QueueSide.RIGHT,
    ) -> int:
        """Push one or more values onto a queue."""
        if not hasattr(self, "_queue_lock"):
            self.__post_init_queues__()

        payloads = [_serialise(v) for v in values]

        # Offload to thread pool
        length = await asyncio.to_thread(self._push_sync, key, payloads, side)

        # Signal waiting consumers
        if key in self._queue_events:
            self._queue_events[key].set()

        logger.debug(
            "%sPUSH %s — queue length now %d",
            "L" if side == QueueSide.LEFT else "R",
            key,
            length,
        )
        return length

    def _push_sync(self, key: str, payloads: List[str], side: QueueSide) -> int:
        """Synchronous push logic."""
        with self._queue_lock:
            if key not in self._queues:
                self._queues[key] = deque()

            q = self._queues[key]
            for payload in payloads:
                if side == QueueSide.LEFT:
                    q.appendleft(payload)
                else:
                    q.append(payload)

            return len(q)

    async def pop(
        self,
        key: str,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Any]:
        """Pop a single value from a queue."""
        if not hasattr(self, "_queue_lock"):
            self.__post_init_queues__()

        if timeout is None:
            # Non-blocking pop
            raw = await asyncio.to_thread(self._pop_sync, key, side)
            return _deserialise(raw) if raw is not None else None

        # Blocking pop with timeout
        start_time = asyncio.get_event_loop().time()
        timeout_remaining = float(timeout) if timeout > 0 else None

        while True:
            # Try non-blocking pop first
            raw = await asyncio.to_thread(self._pop_sync, key, side)
            if raw is not None:
                return _deserialise(raw)

            # Check timeout
            if timeout_remaining is not None:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    return None
                timeout_remaining = timeout - elapsed

            # Wait for signal or timeout
            if key not in self._queue_events:
                self._queue_events[key] = asyncio.Event()

            try:
                if timeout_remaining is not None:
                    await asyncio.wait_for(
                        self._queue_events[key].wait(),
                        timeout=min(0.1, timeout_remaining),
                    )
                else:
                    # Infinite timeout - wait indefinitely but check periodically
                    await asyncio.wait_for(self._queue_events[key].wait(), timeout=0.1)
            except asyncio.TimeoutError:
                if timeout_remaining is not None:
                    continue
                # For infinite timeout, just continue
                pass

            # Clear event for next round
            self._queue_events[key].clear()

    def _pop_sync(self, key: str, side: QueueSide) -> Optional[str]:
        """Synchronous pop logic."""
        with self._queue_lock:
            if key not in self._queues or len(self._queues[key]) == 0:
                return None

            q = self._queues[key]
            try:
                if side == QueueSide.LEFT:
                    return q.popleft()
                else:
                    return q.pop()
            except IndexError:
                return None

    async def pop_many(
        self,
        *keys: str,
        timeout: float = 0,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[tuple[str, Any]]:
        """Block until any of the given queues has a value, then pop it."""
        if not keys:
            raise ValueError("pop_many() requires at least one key")

        if not hasattr(self, "_queue_lock"):
            self.__post_init_queues__()

        start_time = asyncio.get_event_loop().time()

        while True:
            # Try popping from each key
            for key in keys:
                raw = await asyncio.to_thread(self._pop_sync, key, side)
                if raw is not None:
                    return key, _deserialise(raw)

            # Check timeout
            if timeout > 0:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    return None

            # Wait a bit before retrying
            await asyncio.sleep(0.01)

    async def queue_length(self, key: str) -> int:
        """Return the number of items in a queue."""
        if not hasattr(self, "_queue_lock"):
            self.__post_init_queues__()

        return await asyncio.to_thread(self._queue_length_sync, key)

    def _queue_length_sync(self, key: str) -> int:
        """Synchronous queue length logic."""
        with self._queue_lock:
            if key not in self._queues:
                return 0
            return len(self._queues[key])

    async def queue_range(
        self,
        key: str,
        start: int = 0,
        stop: int = -1,
    ) -> List[Any]:
        """Return a slice of the queue without removing items."""
        if not hasattr(self, "_queue_lock"):
            self.__post_init_queues__()

        raws = await asyncio.to_thread(self._queue_range_sync, key, start, stop)
        return [_deserialise(r) for r in raws]

    def _queue_range_sync(self, key: str, start: int, stop: int) -> List[str]:
        """Synchronous queue range logic."""
        with self._queue_lock:
            if key not in self._queues:
                return []

            q = self._queues[key]
            q_list = list(q)

            # Handle negative indices
            if stop == -1:
                stop = len(q_list)
            else:
                stop = stop + 1  # Make it inclusive

            return q_list[start:stop]
