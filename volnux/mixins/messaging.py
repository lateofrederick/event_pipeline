import logging
from enum import Enum
from typing import Any, Awaitable, Callable, List, Optional, Tuple, Type, TypeVar, Union

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="MessagingBackendIntegrationMixin")


class QueueSide(str, Enum):
    """Directional sides for queue push/pop operations."""

    LEFT = "LEFT"
    RIGHT = "RIGHT"


class MessagingNotSupportedError(NotImplementedError):
    """Raised when the configured backend does not implement MessagingBackendMixin."""


class MessagingBackendIntegrationMixin:
    """
    Model-level classmixin for pub/sub and queue messaging operations.

    The model's ``get_schema_name()`` serves as the single source of truth
    for the target channel and queue identity.
    """

    @classmethod
    def _get_messaging_backend(cls, method_name: str) -> Any:
        """Fetch the backend configured for this class and ensure it supports the method."""
        backend = cls.get_backend()
        if not hasattr(backend, method_name):
            raise MessagingNotSupportedError(
                f"Backend '{type(backend).__name__}' does not support messaging method '{method_name}'."
            )
        return backend

    @classmethod
    def _to_payload(cls, instance: Any) -> Any:
        """Convert a model instance or raw payload to a serializable dict/primitive."""
        if isinstance(instance, cls) and hasattr(instance, "model_dump"):
            return instance.model_dump()
        return instance

    @classmethod
    def _from_payload(cls: Type[T], payload: Any) -> Optional[T]:
        """Convert a raw payload back into an instance of this model class."""
        if payload is None:
            return None
        if isinstance(payload, cls):
            return payload
        if isinstance(payload, dict) and hasattr(cls, "model_validate"):
            return cls.model_validate(payload)
        return payload

    @classmethod
    async def publish(cls, message: Union[T, Any]) -> None:
        """
        Publish a model instance or message payload directly to
        the channel identified by ``get_schema_name()``.
        """
        backend = cls._get_messaging_backend("publish")
        payload = cls._to_payload(message)
        return await backend.publish(
            schema_name=cls.get_schema_name(),
            message=payload,
        )

    @classmethod
    async def subscribe(
        cls: Type[T],
        callback: Callable[[T], Awaitable[None]],
    ) -> None:
        """
        Register an async callback for messages on the channel identified by
        ``get_schema_name()``. Automatically deserializes messages into instances of this class.
        """
        backend = cls._get_messaging_backend("subscribe")

        async def _wrapper(raw_payload: Any) -> None:
            instance = cls._from_payload(raw_payload)
            await callback(instance)

        return await backend.subscribe(
            schema_name=cls.get_schema_name(),
            callback=_wrapper,
        )

    @classmethod
    def psubscribe(cls) -> Any:
        """
        Subscribe using pattern matching anchored on ``get_schema_name()``.
        """
        backend = cls._get_messaging_backend("psubscribe")
        return backend.psubscribe(cls.get_schema_name())

    @classmethod
    async def push(
        cls,
        *instances: Union[T, Any],
        side: QueueSide = QueueSide.RIGHT,
    ) -> int:
        """
        Push one or more model instances onto the queue identified by ``get_schema_name()``.
        """
        backend = cls._get_messaging_backend("push")
        payloads = [cls._to_payload(inst) for inst in instances]
        return await backend.push(
            cls.get_schema_name(),
            *payloads,
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
        Pop a single value from the queue identified by ``get_schema_name()``
        and reconstruct it as a model instance.
        """
        backend = cls._get_messaging_backend("pop")
        raw = await backend.pop(
            cls.get_schema_name(),
            timeout=timeout,
            side=side,
        )
        return cls._from_payload(raw)

    @classmethod
    async def enqueue(cls, instance: Union[T, Any]) -> int:
        """FIFO enqueue — push a model instance to the tail (right)."""
        backend = cls.get_backend()
        payload = cls._to_payload(instance)
        if hasattr(backend, "enqueue"):
            return await backend.enqueue(
                cls.get_schema_name(),
                payload,
            )
        return await cls.push(payload, side=QueueSide.RIGHT)

    @classmethod
    async def dequeue(
        cls: Type[T],
        timeout: Optional[float] = None,
    ) -> Optional[T]:
        """FIFO dequeue — pop from the head (left) and reconstruct model instance."""
        backend = cls.get_backend()
        if hasattr(backend, "dequeue"):
            raw = await backend.dequeue(
                cls.get_schema_name(),
                timeout=timeout,
            )
            return cls._from_payload(raw)
        return await cls.pop(timeout=timeout, side=QueueSide.LEFT)

    @classmethod
    async def queue_length(cls) -> int:
        """Return the number of items currently in the queue for this model identity."""
        backend = cls._get_messaging_backend("queue_length")
        return await backend.queue_length(cls.get_schema_name())

    @classmethod
    async def queue_range(
        cls: Type[T],
        start: int = 0,
        stop: int = -1,
    ) -> List[T]:
        """Return a slice of the queue with items deserialized into model instances."""
        backend = cls._get_messaging_backend("queue_range")
        raw_list = await backend.queue_range(
            cls.get_schema_name(),
            start=start,
            stop=stop,
        )
        return [cls._from_payload(raw) for raw in raw_list]

    @classmethod
    async def queue_peek(cls: Type[T]) -> Optional[T]:
        """Return the head item as a model instance without removing it."""
        items = await cls.queue_range(start=0, stop=0)
        return items[0] if items else None

    async def push_self(self, side: QueueSide = QueueSide.RIGHT) -> int:
        """
        Push this instance onto a queue.

        Convenience wrapper around ``cls.push(queue_name, self, side=side)``.

        Example:
            await hitl_entry.push_self("approvals")
        """
        return await self.push(self.get_schema_name(), self, side=side)

    async def publish_self(self) -> None:
        """
        Publish this instance to a pub/sub channel.

        Convenience wrapper around ``cls.publish(channel, self)``.

        Example:
            await audit_entry.publish_self("audit")
        """
        return await self.publish(channel, self)
