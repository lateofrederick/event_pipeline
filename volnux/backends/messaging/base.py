import logging
from abc import ABC, abstractmethod
from typing import (
    Any,
    AsyncIterator,
    List,
    Optional,
    Protocol,
)
from enum import Enum
from dataclasses import dataclass, field

from volnux.backends.connection import BackendConnectorBase

logger = logging.getLogger(__name__)


class QueueSide(str, Enum):
    """Which end of the list to push to or pop from."""

    LEFT = "left"  # Head of the list (LPUSH / LPOP)
    RIGHT = "right"  # Tail of the list (RPUSH / RPOP)


@dataclass
class Message:
    """
    A message received from a pub/sub channel.

    channel : The channel on which the message arrived.
              For pattern subscriptions, this is the specific channel
              that matched, not the pattern itself.
    pattern : The glob pattern that matched, or None for exact subscriptions.
    data    : The deserialised message payload.
    raw     : The original bytes/string from the transport, before deserialisation.
    """

    channel: str
    data: Any
    pattern: Optional[str] = None
    raw: Optional[Any] = field(default=None, repr=False)


class PubSubCapabilityMixin(Protocol):
    """
    Abstract pub/sub interface.

    Backends that implement this mixin expose the messaging primitives
    required by:
        - RedisChannel (command channel pub/sub routing)
        - NodeHeartbeat (presence publication)
        - RehydrationManager (HITL response subscription)
        - EventTrigger (event bus subscription)
        - WindowedTrigger aggregators

    Contract
    --------
    publish() is fire-and-forget. It does not block waiting for subscribers.
    subscribe() returns an async context manager that yields an async iterator
                 of Message objects. The subscriber connection is created on
                 enter and closed on exit.
    psubscribe() is the pattern-matching variant of subscribe().
    """

    connector: BackendConnectorBase[Any]

    @abstractmethod
    async def publish(self, channel: str, message: Any) -> int:
        """
        Publish a message to the channel.

        Parameters
        ----------
        channel:
            Target channel name.
        message:
            Payload. Dicts and lists are JSON-serialised automatically.

        Returns
        -------
        int
            Number of subscribers that received the message.
            0 means no subscribers were listening — message was dropped.
        """

    @abstractmethod
    def subscribe(self, *channels: str) -> "AsyncSubscriptionContext":
        """
        Subscribe to one or more exact channel names.

        Usage:
            async with backend.subscribe("volnux:task:cmd:abc") as messages:
                async for message in messages:
                    handle(message)

        Returns
        -------
        AsyncSubscriptionContext
            Async context manager. Enter it to open a dedicated subscriber
            connection. Exit to cleanly unsubscribe and close the connection.
        """

    @abstractmethod
    def psubscribe(self, *patterns: str) -> "AsyncSubscriptionContext":
        """
        Subscribe using glob patterns.

        Patterns follow the Redis glob syntax:
            *        matches any sequence of characters
            ?        matches any single character
            [abc]    matches any character in the set

        Example patterns:
            "volnux:task:cmd:*" — all task command channels
            "volnux:hitl:*" — all HITL channels

        Usage:
            async with backend.psubscribe("volnux:task:cmd:*") as messages:
                async for message in messages:
                    handle(message)
        """

    async def get_subscribed_channels(self) -> List[str]:
        """
        Return the list of channels this backend instance is currently
        subscribed to (exact subscriptions only, not patterns).
        """
        return []


class PushPopCapabilityMixin(Protocol):
    """
    Abstract push/pop queue interface.

    Backends that implement this mixin expose list/queue primitives
    required by:
        - HITLQueue (enqueue/dequeue approval requests)
        - WorkflowDispatchQueue (CeleryWorkflowExecutor task queue)
        - CommandBuffer (batch command delivery to agents)

    The underlying data structure is an ordered list where:
        - push(side=RIGHT) + pop(side=LEFT) = FIFO queue
        - push(side=LEFT) + pop(side=LEFT) = LIFO stack
        - push(side=RIGHT) + pop(side=RIGHT) = LIFO stack (other end)

    All pop operations support a blocking timeout. When timeout=0, pop
    blocks indefinitely. When timeout=None, pop is non-blocking and
    returns None immediately if the queue is empty.
    """

    connector: BackendConnectorBase[Any]

    @abstractmethod
    async def push(
        self,
        key: str,
        *values: Any,
        side: QueueSide = QueueSide.RIGHT,
    ) -> int:
        """
        Push one or more values onto a queue.

        Parameters
        ----------
        key:
            Queue identifier.
        *values:
            One or more values to push. Pushed atomically in order.
            Dicts and lists are JSON-serialised.
        side:
            Which end of the list to push to.
            RIGHT (default) = tail = RPUSH = FIFO enqueue end.
            LEFT = head = LPUSH = stack push end.

        Returns
        -------
        int
            Length of the queue after the push.
        """

    @abstractmethod
    async def pop(
        self,
        key: str,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Any]:
        """
        Pop a single value from a queue.

        Parameters
        ----------
        key:
            Queue identifier.
        timeout:
            Seconds to wait for a value to become available.
            None  — non-blocking; returns None immediately if queue is empty.
            0 — block indefinitely until a value is available.
            > 0 — block for at most this many seconds.
        side:
            Which end to pop from.
            LEFT (default)  = head = LPOP = FIFO dequeue end.
            RIGHT = tail = RPOP.

        Returns
        -------
        Any | None
            The deserialised value, or None if the queue was empty
            (non-blocking) or the timeout expired.
        """

    @abstractmethod
    async def pop_many(
        self,
        *keys: str,
        timeout: float = 0,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[tuple[str, Any]]:
        """
        Block until any of the given queues has a value, then pop it.

        Parameters
        ----------
        *keys:
            One or more queue keys to watch. Checked in order.
        timeout:
            Seconds to wait. ``0`` = block indefinitely (default).
            Positive float = bounded wait. Sub-second precision is supported.
            Non-blocking pop is not available for multi-key operations.
        side:
            Which end to pop from.

        Returns
        -------
        (key, value) | None
            A tuple of (queue_key, value) identifying which queue yielded
            the item, or None if the timeout expired.
        """

    @abstractmethod
    async def queue_length(self, key: str) -> int:
        """Return the number of items currently in the queue."""

    @abstractmethod
    async def queue_range(
        self,
        key: str,
        start: int = 0,
        stop: int = -1,
    ) -> List[Any]:
        """
        Return a slice of the queue without removing items.

        Parameters
        ----------
        start:
            Zero-based start index. Negative values count from the tail.
        stop:
            Inclusive end index. -1 means the last element.

        Returns
        -------
        List[Any]
            Deserialised elements from start to stop, inclusive.
        """

    # Convenience methods built on the abstract primitives
    # These have default implementations. Backends may override them
    # for performance (e.g. Redis has atomic GETSET for some of these).

    async def enqueue(self, key: str, value: Any) -> int:
        """
        FIFO enqueue — push to the right (tail).
        Equivalent to push(key, value, side=RIGHT).
        """
        return await self.push(key, value, side=QueueSide.RIGHT)

    async def dequeue(
        self,
        key: str,
        timeout: Optional[float] = None,
    ) -> Optional[Any]:
        """
        FIFO dequeue — pop from the left (head).
        Equivalent to pop(key, timeout, side=LEFT).
        """
        return await self.pop(key, timeout=timeout, side=QueueSide.LEFT)

    async def queue_peek(self, key: str) -> Optional[Any]:
        """
        Return the head item without removing it.
        Returns None if the queue is empty.
        """
        items = await self.queue_range(key, start=0, stop=0)
        return items[0] if items else None


class AsyncSubscriptionContext(ABC):
    """
    Async context manager returned by subscribe() and psubscribe().

    Usage:
        async with backend.subscribe("channel") as messages:
            async for msg in messages:
                print(msg.data)

    The context manager owns a dedicated subscriber connection for the
    duration of the with-block. On __aexit__ it unsubscribes cleanly and
    closes the connection.
    """

    @abstractmethod
    async def __aenter__(self) -> AsyncIterator[Message]: ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None: ...
