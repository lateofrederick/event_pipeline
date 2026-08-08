import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional, Tuple, TYPE_CHECKING, Type, Callable, Union

from redis.exceptions import RedisError

from volnux.backends.messaging.base import (
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    AsyncSubscriptionContext,
    QueueSide,
    Record,
    Message,
)
from volnux.backends.messaging.util import _BLOCKING_EXECUTOR

if TYPE_CHECKING:
    from volnux.backends.connectors.redis import RedisConnector

logger = logging.getLogger(__name__)


class RedisStorePubSubMixin(PubSubCapabilityMixin):

    connector: RedisConnector

    async def publish(self, channel: str, record: Record) -> int:
        try:
            self.connector._ensure_connected()  # type: ignore[attr-unresolved]

            payload = self._serialize_record(record)  # type: ignore[attr-unresolved]

            cursor = self.connector.get_cursor()

            receivers = await asyncio.to_thread(cursor.publish, channel, payload)
            return receivers
        except RedisError as exc:
            raise ConnectionError(f"Failed to publish to {channel}: {exc}") from exc

    def subscribe(
        self, *channels: str, record_class: Type[Record]
    ) -> "RedisStoreSubscriptionContext":
        return RedisStoreSubscriptionContext(
            backend=self,
            channels=list(channels),
            patterns=[],
            record_class=record_class,
        )

    def psubscribe(
        self, *patterns: str, record_class: Type[Record]
    ) -> "RedisStoreSubscriptionContext":
        return RedisStoreSubscriptionContext(
            self, [], list(patterns), record_class=record_class
        )


class RedisStoreSubscriptionContext(AsyncSubscriptionContext):

    def __init__(
        self,
        backend: "RedisStorePubSubMixin",
        channels: List[str],
        patterns: List[str],
        record_class: Type[Record],
    ):
        self.backend = backend
        self.channels = channels
        self.patterns = patterns
        self.record_class = record_class
        self._pubsub = None
        self._async_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._listener_task: Optional[asyncio.Task] = None
        self._loop = asyncio.get_running_loop()

    async def __aenter__(self) -> "RedisStoreSubscriptionContext":
        try:
            self.backend.connector._ensure_connected()  # type: ignore

            cursor = self.backend.connector.get_cursor()

            # Get pubsub object from the existing pooled client
            self._pubsub = await asyncio.to_thread(cursor.pubsub)

            if self.channels:
                await asyncio.to_thread(self._pubsub.subscribe, *self.channels)
            if self.patterns:
                await asyncio.to_thread(self._pubsub.psubscribe, *self.patterns)

            # Start the background bridge task
            self._listener_task = asyncio.create_task(self._bridge_listen())
            return self
        except Exception as exc:
            await self._cleanup()
            raise ConnectionError(f"Failed to subscribe: {exc}") from exc

    async def _bridge_listen(self):
        """Runs the blocking listen() in a dedicated thread pool and bridges to async."""

        def sync_listen():
            try:
                # pubsub.listen() is a blocking generator that yields messages
                for msg in self._pubsub.listen():
                    if msg["type"] in ("message", "pmessage"):
                        # Thread-safe bridge to the async event loop
                        self._loop.call_soon_threadsafe(
                            self._async_queue.put_nowait, msg
                        )
            except Exception as e:
                logger.debug("Redis listen loop terminated: %s", e)

        # Run in the dedicated blocking executor, NOT the default to_thread pool
        await self._loop.run_in_executor(_BLOCKING_EXECUTOR, sync_listen)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._cleanup()

    async def _cleanup(self):
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()

        if self._pubsub:
            try:
                if self.channels:
                    await asyncio.to_thread(self._pubsub.unsubscribe, *self.channels)
                if self.patterns:
                    await asyncio.to_thread(self._pubsub.punsubscribe, *self.patterns)
                await asyncio.to_thread(self._pubsub.close)
            except Exception:
                pass

    def __aiter__(self):
        return self

    async def __anext__(self) -> Message:
        # Pure async wait. Wakes up instantly when the bridge thread pushes a message.
        msg = await self._async_queue.get()

        channel = (
            msg["channel"].decode()
            if isinstance(msg["channel"], bytes)
            else msg["channel"]
        )

        pattern = msg.get("pattern")
        if isinstance(pattern, bytes):
            pattern = pattern.decode()

        return Message(
            **{
                "channel": channel,
                "pattern": pattern,
                "data": self.backend._deserialise_record(msg["data"], self.record_class),  # type: ignore[attr-unresolved]
                "raw": msg["data"],
            }
        )


class RedisStorePushPopMixin(PushPopCapabilityMixin):

    connector: RedisConnector

    async def push(
        self, key: str, *values: Record, side: QueueSide = QueueSide.RIGHT
    ) -> int:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]

            payloads = [self._serialize_record(v) for v in values]  # type: ignore[attr-unresolved]

            cursor = self.connector.get_cursor()

            if side == QueueSide.LEFT:
                return await asyncio.to_thread(cursor.lpush, key, *payloads)
            else:
                return await asyncio.to_thread(cursor.rpush, key, *payloads)
        except RedisError as exc:
            raise ConnectionError(f"Failed to push to {key}: {exc}") from exc

    async def pop(
        self,
        key: str,
        record_class: Type[Record],
        timeout: Optional[Union[float, int]] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Record]:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]

            loop = asyncio.get_running_loop()

            cursor = self.connector.get_cursor()

            if timeout is None:
                # Non-blocking pop (safe for default to_thread)
                if side == QueueSide.LEFT:
                    raw = await asyncio.to_thread(cursor.lpop, key)
                else:
                    raw = await asyncio.to_thread(cursor.rpop, key)
            else:
                # Blocking pop (MUST use dedicated executor to prevent thread-pool starvation)
                func: Callable[[str, Union[float, int]], Any] = (
                    cursor.blpop if side == QueueSide.LEFT else cursor.brpop
                )

                result = await loop.run_in_executor(
                    _BLOCKING_EXECUTOR, lambda: func(key, float(timeout))
                )

                if result is None:
                    return None
                _, raw = result

            return self._deserialise_record(raw, record_class) if raw is not None else None  # type: ignore
        except RedisError as exc:
            raise ConnectionError(f"Failed to pop from {key}: {exc}") from exc

    async def pop_many(
        self,
        key: str,
        record_class: type[Record],
        limit: Optional[int] = None,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> List[Any]:
        """
        Block until the queue has data, then drain a batch of items up to `limit`.
        """
        results = []

        first_item = await self.pop(key, record_class, timeout=timeout, side=side)
        if first_item is None:
            return []
        results.append(first_item)

        # Drain the rest non-blocking up to limit
        items_popped = 1
        while True:
            if limit is not None and items_popped >= limit:
                break

            result = await self.pop(key, record_class, timeout=timeout, side=side)

            if result is None:
                break

            results.append(result)

            items_popped += 1

        return results

    async def queue_length(self, key: str) -> int:
        try:
            self._ensure_connected()  # type: ignore[attr-unresolved]

            cursor = self.connector.get_cursor()

            return await asyncio.to_thread(cursor.llen, key)
        except RedisError as exc:
            raise ConnectionError(
                f"Failed to get queue length for {key}: {exc}"
            ) from exc

    async def queue_range(
        self, key: str, record_class: Type[Record], start: int = 0, stop: int = -1
    ) -> List[Record]:
        try:
            self._ensure_connected()  # type: ignore

            cursor = self.connector.get_cursor()

            raws = await asyncio.to_thread(cursor.lrange, key, start, stop)
            return [self._deserialise_record(r, record_class) for r in raws]  # type: ignore
        except RedisError as exc:
            raise ConnectionError(
                f"Failed to get queue range for {key}: {exc}"
            ) from exc
