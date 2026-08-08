import logging
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    TYPE_CHECKING,
    cast,
)

from .key_value_store_integration import KeyValueStoreIntegrationMixin
from volnux.concurrency.async_utils import as_coroutine
from volnux.backends.messaging.util import require_pubsub, require_pushpop

if TYPE_CHECKING:
    from volnux.backends.messaging.base import (
        PubSubCapabilityMixin,
        PushPopCapabilityMixin,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="MessagingBackendIntegrationMixin")


class QueueSide(str, Enum):
    """Directional sides for queue push/pop operations."""

    LEFT = "LEFT"
    RIGHT = "RIGHT"


class MessagingNotSupportedError(NotImplementedError):
    """Raised when the configured backend does not implement MessagingBackendMixin."""


class MessagingBackendIntegrationMixin(KeyValueStoreIntegrationMixin):
    """
    Mixin providing messaging and queue backend integration for implementations that support Pub/Sub
    and push/pop queue operations.

    Detailed description of the class, its purpose, and usage.

    This mixin provides a unified interface for interacting with backend systems that support
    messaging (Pub/Sub) and queuing functionality. It enables publishing messages, subscribing to
    message channels, as well as enqueuing and dequeuing tasks in a queue model. It ensures backends
    comply with the required capabilities before invoking operations. Typically, this mixin is
    intended for classes utilizing specialized storage backends.
    """

    @classmethod
    def _ensure_pub_sub_supported(cls) -> "PubSubCapabilityMixin":
        backend = cls.get_backend()

        require_pubsub(backend, cls.__name__)
        return backend  # type: ignore

    @classmethod
    def _ensure_push_pop_supported(cls) -> "PushPopCapabilityMixin":
        backend = cls.get_backend()

        require_pushpop(backend, cls.__name__)
        return backend  # type:ignore

    @classmethod
    async def publish(cls, record: T) -> int:
        """
        Publishes a given record to the appropriate channel using the backend system.

        :param record: The record to be published.
        :type record: T
        :return: The number of subscribers that received the published record.
        :rtype: int
        """
        backend = cls._ensure_pub_sub_supported()
        return await backend.publish(
            channel=cls.get_schema_name(),
            record=record,
        )

    @classmethod
    async def subscribe(
        cls: Type[T],
        callback: Callable[[T], Awaitable[None]],
    ) -> None:
        """
        Subscribes to a publish-subscribe channel for receiving updates about a specific schema.

        This asynchronous method allows listeners to subscribe to a specific schema channel. When
        a new record is published to the channel, the provided callback is triggered with the record
        as an argument.

        :param callback: A callable that accepts an instance of the class as input and returns
                         an awaitable object. It will be invoked for each record received on
                         the channel.
        :type callback: Callable[[T], Awaitable[None]]
        """
        backend = cls._ensure_pub_sub_supported()

        async with backend.subscribe(cls.get_schema_name(), record_class=cls) as pub:
            async for record in pub:
                await as_coroutine(callback, record)

    @classmethod
    async def psubscribe(
        cls, *patterns: str, callback: Callable[[T], Awaitable[None]]
    ) -> None:
        """
        Subscribes to a set of patterns on the backend and listens for matching messages in a
        publish-subscribe model. This method enables clients to receive notifications when
        messages are published to channels that match specified patterns. The callback function
        is invoked with each matching record.

        :param patterns: A variable number of string arguments representing the patterns to
            subscribe to. These patterns should adhere to the backend's supported pattern
            syntax.
        :param callback: An asynchronous callable that takes a single argument. The argument
            passed to the callback is an instance of the record matching the subscribed patterns.
        :return: This method does not return any value.
        """
        pattern_set = {cls.get_schema_name(), *patterns}
        backend = cls._ensure_pub_sub_supported()
        async with backend.psubscribe(*pattern_set, record_class=cls) as pub:
            async for record in pub:
                await as_coroutine(callback, record)

    @classmethod
    async def push(
        cls,
        *instances: Union[T, Any],
        side: QueueSide = QueueSide.RIGHT,
    ) -> int:
        """
        Pushes multiple instances to the queue on the specified side. This method is
        asynchronous and interacts with a backend to perform the operation.

        :param instances: One or more instances to be added to the queue.
        :type instances: Union[T, Any]
        :param side: The side of the queue where the instances should be pushed.
                     Possible values are defined in the QueueSide enumeration.
        :return: The total number of instances in the queue after the operation is
                 complete.
        :rtype: int
        """
        backend = cls._ensure_push_pop_supported()
        return await backend.push(
            cls.get_schema_name(),
            *instances,
            side=side,
        )

    @classmethod
    async def pop(
        cls: Type[T],
        *,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[T]:
        """
        Asynchronously removes and returns an item from the queue. The side from which
        the item is removed can be specified (left or right). If the timeout is
        provided and no item is available within the specified time, the operation
        will return `None`.

        :param timeout: Optional; the maximum time in seconds to wait for an item to
                        become available in the queue before returning `None`.
                        If not provided, it waits indefinitely.
        :type timeout: Optional[float]
        :param side: The side of the queue from which the item is to be removed.
                     Defaults to `QueueSide.LEFT` (left side of the queue).
        :type side: QueueSide
        :return: The item removed from the queue, or `None` if the timeout expires
                 without any available item.
        :rtype: Optional[T]
        """
        backend = cls._ensure_push_pop_supported()
        return await backend.pop(
            cls.get_schema_name(),
            record_class=cls,
            timeout=timeout,
            side=side,
        )

    @classmethod
    async def pop_many(
        cls: Type[T],
        *,
        limit: int,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> List[Optional[T]]:
        """
        Retrieve and remove multiple items from the queue with specified conditions.

        This method asynchronously fetches a specified number of items from the queue,
        removing them as a batch. The operation can optionally block until the desired
        criteria are met or the timeout expires, depending on the implementation.

        :param limit: The maximum number of items to retrieve and remove from the queue.
        :param timeout: The optional maximum time, in seconds, to wait before giving
            up the operation. If not provided, the method will default to implementation-specific
            behavior of immediate return or blocking indefinitely.
        :param side: Specifies the side of the queue from which items should be retrieved
            and removed. Defaults to ``QueueSide.LEFT`` if not provided.
        :return: A list containing the retrieved items from the queue. The element
            type corresponds to the queue's item type. If retrieval fails or no items
            are present during the operation, the resulting list may contain ``None`` values.
        """
        backend = cls._ensure_push_pop_supported()
        return await backend.pop_many(
            cls.get_schema_name(),
            record_class=cls,
            limit=limit,
            timeout=timeout,
            side=side,
        )

    @classmethod
    async def enqueue(cls, instance: Union[T, Any]) -> int:
        """
        Asynchronously enqueues an instance into the backend system. This method ensures that the backend
        supports the necessary push and pop operations before proceeding.

        :param instance: The instance to be enqueued. It is of type `Union[T, Any]`, where `T` represents
            a specific type of instance required by the implementation.
        :return: An integer representing the result of the enqueue operation.
        """
        backend = cls._ensure_push_pop_supported()

        return await backend.enqueue(cls.get_schema_name(), instance)

    @classmethod
    async def dequeue(
        cls: Type[T],
        timeout: Optional[float] = None,
    ) -> Optional[T]:
        """
        Dequeues an item from the queue with an optional timeout. This method will wait
        for an item to become available until the timeout duration has been reached. If
        the timeout is not provided or set to None, the method will not wait.

        :param timeout: Optional; The maximum time in seconds to wait for an item to
            become available in the queue. If None, waits indefinitely.
        :type timeout: Optional[float]
        :return: The dequeued item of type T if an item is available, otherwise None.
        :rtype: Optional[T]
        """
        backend = cls._ensure_push_pop_supported()

        return await backend.dequeue(
            cls.get_schema_name(),
            record_class=cls,
            timeout=timeout,
        )

    @classmethod
    async def queue_length(cls) -> int:
        """Return the number of items currently in the queue for this model identity."""
        backend = cls._ensure_push_pop_supported()
        return await backend.queue_length(cls.get_schema_name())

    @classmethod
    async def queue_range(
        cls: Type[T],
        start: int = 0,
        stop: int = -1,
    ) -> List[T]:
        """Return a slice of the queue with items deserialized into model instances."""
        backend = cls._ensure_push_pop_supported()
        return await backend.queue_range(
            cls.get_schema_name(),
            record_class=cls,
            start=start,
            stop=stop,
        )

    @classmethod
    async def queue_peek(cls: Type[T]) -> Optional[T]:
        """Return the head item as a model instance without removing it."""
        items = await cls.queue_range(start=0, stop=0)
        return items[0] if items else None
