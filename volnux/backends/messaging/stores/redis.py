import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Set, Type, Union
from redis import RedisError

from volnux.backends.messaging.base import (
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    QueueSide,
)


logger = logging.getLogger(__name__)


def _serialise(value: Any) -> str:
    """Serialise Python objects to JSON strings for Redis storage."""
    if isinstance(value, (str, bytes)):
        return value if isinstance(value, str) else value.decode()
    return json.dumps(value, default=str)


def _deserialise(raw: Optional[bytes]) -> Any:
    """Deserialise JSON strings back to Python objects."""
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        # Fall back to raw string if not valid JSON
        return raw.decode() if isinstance(raw, bytes) else raw


class RedisStorePubSubMixin(PubSubCapabilityMixin):
    """
    Pub/Sub implementation for RedisStoreBackend.

    Uses `self.connector.cursor` (the synchronous Redis client) wrapped with
    `asyncio.to_thread()` for PUBLISH commands. Subscription contexts create
    their own dedicated connections for SUBSCRIBE/PSUBSCRIBE.

    Contract
    --------
    - `publish()` uses `asyncio.to_thread()` with the pooled client.
    - `subscribe()` / `psubscribe()` return async context managers that manage
      dedicated connections (Redis SUBSCRIBE mode blocks the connection).
    """

    async def publish(self, channel: str, message: Any) -> int:
        """
        Publish a message to a Redis channel.

        Uses the backend's connection management and wraps the synchronous
        `PUBLISH` call with `asyncio.to_thread()`.
        """
        try:
            self._ensure_connected()
            payload = _serialise(message)
            # Wrap synchronous PUBLISH in to_thread
            receivers = await asyncio.to_thread(
                self.connector.cursor.publish, channel, payload
            )
            logger.debug("Published to %s — %d receiver(s)", channel, receivers)
            return receivers
        except RedisError as exc:
            logger.error("Redis PUBLISH to %s failed: %s", channel, exc)
            raise ConnectionError(f"Failed to publish to {channel}: {exc}")

    def subscribe(self, *channels: str) -> "RedisStoreSubscriptionContext":
        """
        Subscribe to one or more exact channel names.

        Returns an async context manager that creates a dedicated Redis
        connection (not from the store's pool).
        """
        return RedisStoreSubscriptionContext(
            backend=self,
            channels=list(channels),
            patterns=[],
        )

    def psubscribe(self, *patterns: str) -> "RedisStoreSubscriptionContext":
        """
        Subscribe using Redis glob patterns.

        Returns an async context manager that creates a dedicated Redis
        connection (not from the store's pool).
        """
        return RedisStoreSubscriptionContext(
            backend=self,
            channels=[],
            patterns=list(patterns),
        )


class RedisStoreSubscriptionContext:
    """
    Async context manager for Redis SUBSCRIBE / PSUBSCRIBE.

    Opens a dedicated Redis connection on `__aenter__`, issues SUBSCRIBE/PSUBSCRIBE,
    and yields an async iterator of messages. Closes the connection on `__aexit__`.

    All blocking Redis operations are wrapped with `asyncio.to_thread()`.
    """

    def __init__(
        self,
        backend: "RedisStoreBackend",
        channels: List[str],
        patterns: List[str],
    ):
        self.backend = backend
        self.channels = channels
        self.patterns = patterns
        self._client: Optional[Any] = None
        self._pubsub: Optional[Any] = None

    async def __aenter__(self) -> "RedisStoreSubscriptionContext":
        """
        Create a dedicated Redis connection and subscribe.
        All operations wrapped in to_thread.
        """
        try:
            import redis

            # Create connection config from backend's connector
            config = self.backend.connector.config

            # Create client in thread pool to avoid blocking
            self._client = await asyncio.to_thread(
                redis.Redis,
                host=config.get("host", "localhost"),
                port=config.get("port", 6379),
                db=config.get("database", 0),
                password=config.get("password"),
                decode_responses=False,
            )

            # Get pubsub in thread
            self._pubsub = await asyncio.to_thread(self._client.pubsub)

            # Subscribe in thread
            if self.channels:
                await asyncio.to_thread(self._pubsub.subscribe, *self.channels)
                logger.debug("Subscribed to channels: %s", self.channels)

            if self.patterns:
                await asyncio.to_thread(self._pubsub.psubscribe, *self.patterns)
                logger.debug("Subscribed to patterns: %s", self.patterns)

            return self

        except Exception as exc:
            logger.error("Failed to create Redis subscription: %s", exc)
            await self._cleanup()
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Unsubscribe and close the dedicated connection."""
        await self._cleanup()

    async def _cleanup(self):
        """Clean up subscription and connection resources."""
        if self._pubsub:
            try:
                if self.channels:
                    await asyncio.to_thread(self._pubsub.unsubscribe, *self.channels)
                if self.patterns:
                    await asyncio.to_thread(self._pubsub.punsubscribe, *self.patterns)
                await asyncio.to_thread(self._pubsub.close)
            except Exception as exc:
                logger.debug("Error closing pubsub: %s", exc)

        if self._client:
            try:
                await asyncio.to_thread(self._client.close)
            except Exception as exc:
                logger.debug("Error closing Redis client: %s", exc)

    def __aiter__(self):
        """Return self as async iterator."""
        return self

    async def __anext__(self):
        """
        Yield the next message from the subscription.

        Wraps blocking get_message() with to_thread and filters out
        subscription confirmation messages.
        """
        if self._pubsub is None:
            raise StopAsyncIteration

        while True:
            try:
                # Wrap blocking get_message in to_thread
                msg = await asyncio.to_thread(
                    self._pubsub.get_message,
                    ignore_subscribe_messages=True,
                    timeout=0.1,
                )

                if msg is None:
                    # Small sleep to prevent busy-wait
                    await asyncio.sleep(0.01)
                    continue

                if msg["type"] in ("message", "pmessage"):
                    return {
                        "channel": (
                            msg["channel"].decode()
                            if isinstance(msg["channel"], bytes)
                            else msg["channel"]
                        ),
                        "pattern": (
                            msg.get("pattern", b"").decode()
                            if isinstance(msg.get("pattern"), bytes)
                            else msg.get("pattern")
                        ),
                        "data": _deserialise(msg["data"]),
                    }
            except Exception as exc:
                logger.error("Error receiving pubsub message: %s", exc)
                raise StopAsyncIteration


class RedisStorePushPopMixin(PushPopCapabilityMixin):
    """
    Push/Pop queue implementation for RedisStoreBackend.

    Uses `self.connector.cursor` (synchronous Redis client) wrapped with
    `asyncio.to_thread()` for all LPUSH/RPUSH/LPOP/RPOP/BLPOP/BRPOP/
    LLEN/LRANGE commands.

    The backend's connection management (`_ensure_connected()`) and transaction
    support (`connector.get_pipeline()`) are reused where appropriate.
    """

    async def push(
        self,
        key: str,
        *values: Any,
        side: QueueSide = QueueSide.RIGHT,
    ) -> int:
        """
        Push one or more values onto a Redis list.

        Wraps synchronous LPUSH/RPUSH with asyncio.to_thread().
        """
        try:
            self._ensure_connected()
            payloads = [_serialise(v) for v in values]

            # Wrap the push operation in to_thread
            if side == QueueSide.LEFT:
                length = await asyncio.to_thread(
                    self.connector.cursor.lpush, key, *payloads
                )
            else:
                length = await asyncio.to_thread(
                    self.connector.cursor.rpush, key, *payloads
                )

            logger.debug(
                "%sPUSH %s — queue length now %d",
                "L" if side == QueueSide.LEFT else "R",
                key,
                length,
            )
            return length

        except RedisError as exc:
            logger.error("Redis PUSH to %s failed: %s", key, exc)
            raise ConnectionError(f"Failed to push to {key}: {exc}")

    async def pop(
        self,
        key: str,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Any]:
        """
        Pop a single value from a Redis list.

        Supports non-blocking (timeout=None) and blocking (timeout >= 0) modes.
        All operations wrapped with asyncio.to_thread().
        """
        try:
            self._ensure_connected()

            if timeout is None:
                # Non-blocking pop wrapped in to_thread
                if side == QueueSide.LEFT:
                    raw = await asyncio.to_thread(self.connector.cursor.lpop, key)
                else:
                    raw = await asyncio.to_thread(self.connector.cursor.rpop, key)
                return _deserialise(raw) if raw is not None else None

            # Blocking pop with float timeout wrapped in to_thread
            if side == QueueSide.LEFT:
                result = await asyncio.to_thread(
                    self.connector.cursor.blpop, key, timeout=float(timeout)
                )
            else:
                result = await asyncio.to_thread(
                    self.connector.cursor.brpop, key, timeout=float(timeout)
                )

            if result is None:
                return None

            _key, raw = result
            return _deserialise(raw)

        except RedisError as exc:
            logger.error("Redis POP from %s failed: %s", key, exc)
            raise ConnectionError(f"Failed to pop from {key}: {exc}")

    async def pop_many(
        self,
        *keys: str,
        timeout: float = 0,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[tuple[str, Any]]:
        """
        Block until any of the given queues has a value, then pop it.

        Raises ValueError if no keys are provided.
        All operations wrapped with asyncio.to_thread().
        """
        if not keys:
            raise ValueError("pop_many() requires at least one key")

        try:
            self._ensure_connected()

            # Wrap blocking multi-key pop in to_thread
            if side == QueueSide.LEFT:
                result = await asyncio.to_thread(
                    self.connector.cursor.blpop, list(keys), timeout=float(timeout)
                )
            else:
                result = await asyncio.to_thread(
                    self.connector.cursor.brpop, list(keys), timeout=float(timeout)
                )

            if result is None:
                return None

            key_raw, value_raw = result
            key = key_raw.decode() if isinstance(key_raw, bytes) else key_raw
            return key, _deserialise(value_raw)

        except RedisError as exc:
            logger.error("Redis POP_MANY from %s failed: %s", keys, exc)
            raise ConnectionError(f"Failed to pop from multiple keys: {exc}")

    async def queue_length(self, key: str) -> int:
        """
        Return the number of items in a Redis list.

        Wraps LLEN with asyncio.to_thread().
        """
        try:
            self._ensure_connected()
            return await asyncio.to_thread(self.connector.cursor.llen, key)
        except RedisError as exc:
            logger.error("Redis LLEN on %s failed: %s", key, exc)
            raise ConnectionError(f"Failed to get queue length for {key}: {exc}")

    async def queue_range(
        self,
        key: str,
        start: int = 0,
        stop: int = -1,
    ) -> List[Any]:
        """
        Return a slice of a Redis list without removing items.

        Wraps LRANGE with asyncio.to_thread().
        """
        try:
            self._ensure_connected()
            raws = await asyncio.to_thread(
                self.connector.cursor.lrange, key, start, stop
            )
            return [_deserialise(r) for r in raws]
        except RedisError as exc:
            logger.error("Redis LRANGE on %s failed: %s", key, exc)
            raise ConnectionError(f"Failed to get queue range for {key}: {exc}")
