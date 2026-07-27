import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Type, Union

from volnux.backends.messaging.base import (
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    QueueSide,
    Message,
)

logger = logging.getLogger(__name__)


def _serialise(value: Any) -> str:
    """Serialise Python objects to JSON strings."""
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def _deserialise(raw: Optional[str]) -> Any:
    """Deserialise JSON strings back to Python objects."""
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw


class PostgresPubSubMixin(PubSubCapabilityMixin):
    """
    PostgreSQL NOTIFY/LISTEN pub/sub implementation.

    Uses PostgreSQL's native NOTIFY/LISTEN for messaging within the same database.
    """

    async def publish(self, channel: str, message: Any) -> int:
        """Publish via PostgreSQL NOTIFY."""
        payload = _serialise(message)
        if len(payload.encode()) > 8000:
            raise ValueError(
                f"Payload exceeds 8000 byte limit ({len(payload.encode())} bytes)"
            )

        def _notify():
            self._ensure_connected()
            with self._get_cursor() as cursor:
                cursor.execute("SELECT pg_notify(%s, %s)", (channel, payload))

        await asyncio.to_thread(_notify)
        logger.debug("NOTIFY sent to channel %s", channel)
        return 0  # PostgreSQL doesn't report subscriber count

    def subscribe(self, *channels: str) -> "PostgresStoreSubscriptionContext":
        """Subscribe to exact channel names."""
        return PostgresStoreSubscriptionContext(backend=self, channels=list(channels))

    def psubscribe(self, *patterns: str):
        """PostgreSQL does not support pattern subscriptions."""
        raise NotImplementedError(
            "PostgreSQL LISTEN/NOTIFY does not support pattern subscriptions. "
            "Use exact channel names with subscribe()."
        )


class PostgresStoreSubscriptionContext:
    """Async context manager for PostgreSQL LISTEN."""

    def __init__(self, backend: "PostgresStoreBackend", channels: List[str]):
        self.backend = backend
        self.channels = channels
        self._conn = None
        self._stop_event = asyncio.Event()

    async def __aenter__(self) -> "PostgresStoreSubscriptionContext":
        """Open dedicated connection and LISTEN."""

        def _listen():
            import psycopg

            params = self.backend.connector._get_connection_params()
            self._conn = psycopg.connect(**params)
            self._conn.autocommit = True
            with self._conn.cursor() as cursor:
                for channel in self.channels:
                    cursor.execute(f"LISTEN {channel}")

        await asyncio.to_thread(_listen)
        logger.debug("PostgreSQL LISTEN: %s", self.channels)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """UNLISTEN and close connection."""
        self._stop_event.set()
        if self._conn:

            def _unlisten():
                try:
                    with self._conn.cursor() as cursor:
                        for channel in self.channels:
                            cursor.execute(f"UNLISTEN {channel}")
                    self._conn.close()
                except Exception as exc:
                    logger.debug("Error during UNLISTEN: %s", exc)

            await asyncio.to_thread(_unlisten)

    def __aiter__(self):
        return self

    async def __anext__(self):
        """Yield next notification."""
        if self._conn is None:
            raise StopAsyncIteration

        def _poll():
            import select

            if select.select([self._conn], [], [], 0.1) == ([], [], []):
                return None
            self._conn.poll()
            notifies = self._conn.notifies()
            return notifies.pop(0) if notifies else None

        while not self._stop_event.is_set():
            notify = await asyncio.to_thread(_poll)
            if notify:
                return {
                    "channel": notify.channel,
                    "pattern": None,
                    "data": _deserialise(notify.payload),
                }
            await asyncio.sleep(0.01)

        raise StopAsyncIteration


class PostgresPushPopMixin(PushPopCapabilityMixin):
    """
    Table-based queue implementation for PostgreSQL.

    Uses a `volnux_queues` table with SKIP LOCKED for concurrent-safe operations.
    """

    QUEUE_TABLE = "volnux_queues"

    def _ensure_queue_table(self):
        """Create queue table if it doesn't exist."""

        def _create():
            self._ensure_connected()
            with self._get_cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.QUEUE_TABLE} (
                        id BIGSERIAL PRIMARY KEY,
                        queue_key TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        INDEX idx_queue_key (queue_key, id)
                    )
                """
                )

        if not hasattr(self, "_queue_table_created"):
            asyncio.run(asyncio.to_thread(_create))
            self._queue_table_created = True

    async def push(
        self, key: str, *values: Any, side: QueueSide = QueueSide.RIGHT
    ) -> int:
        """Push to table-based queue."""
        self._ensure_queue_table()
        payloads = [_serialise(v) for v in values]

        def _push_sync():
            self._ensure_connected()
            with self._transaction():
                with self._get_cursor() as cursor:
                    for payload in payloads:
                        cursor.execute(
                            f"INSERT INTO {self.QUEUE_TABLE} (queue_key, payload) VALUES (%s, %s)",
                            (key, payload),
                        )
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {self.QUEUE_TABLE} WHERE queue_key = %s",
                        (key,),
                    )
                    return cursor.fetchone()[0]

        length = await asyncio.to_thread(_push_sync)
        logger.debug("Pushed to queue %s — length now %d", key, length)
        return length

    async def pop(
        self,
        key: str,
        timeout: Optional[float] = None,
        side: QueueSide = QueueSide.LEFT,
    ) -> Optional[Any]:
        """Pop from the table-based queue with SKIP LOCKED."""
        self._ensure_queue_table()

        def _pop_sync():
            self._ensure_connected()
            with self._transaction():
                with self._get_cursor() as cursor:
                    order = "ASC" if side == QueueSide.LEFT else "DESC"
                    cursor.execute(
                        f"""
                        DELETE FROM {self.QUEUE_TABLE}
                        WHERE id = (
                            SELECT id FROM {self.QUEUE_TABLE}
                            WHERE queue_key = %s
                            ORDER BY id {order}
                            FOR UPDATE SKIP LOCKED
                            LIMIT 1
                        )
                        RETURNING payload
                        """,
                        (key,),
                    )
                    row = cursor.fetchone()
                    return row[0] if row else None

        if timeout is None:
            raw = await asyncio.to_thread(_pop_sync)
            return _deserialise(raw) if raw else None

        # Blocking with timeout
        start = asyncio.get_event_loop().time()
        while True:
            raw = await asyncio.to_thread(_pop_sync)
            if raw:
                return _deserialise(raw)

            if timeout > 0 and (asyncio.get_event_loop().time() - start) >= timeout:
                return None

            await asyncio.sleep(0.1)

    async def pop_many(
        self, *keys: str, timeout: float = 0, side: QueueSide = QueueSide.LEFT
    ) -> Optional[tuple[str, Any]]:
        """Pop from first non-empty queue."""
        if not keys:
            raise ValueError("pop_many() requires at least one key")

        self._ensure_queue_table()
        start = asyncio.get_event_loop().time()

        while True:
            for key in keys:
                result = await self.pop(key, timeout=None, side=side)
                if result:
                    return key, result

            if timeout > 0 and (asyncio.get_event_loop().time() - start) >= timeout:
                return None

            await asyncio.sleep(0.1)

    async def queue_length(self, key: str) -> int:
        """Get queue length."""
        self._ensure_queue_table()

        def _length():
            self._ensure_connected()
            with self._get_cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {self.QUEUE_TABLE} WHERE queue_key = %s",
                    (key,),
                )
                return cursor.fetchone()[0]

        return await asyncio.to_thread(_length)

    async def queue_range(self, key: str, start: int = 0, stop: int = -1) -> List[Any]:
        """Get queue slice."""
        self._ensure_queue_table()

        def _range():
            self._ensure_connected()
            with self._get_cursor() as cursor:
                limit = stop - start + 1 if stop >= 0 else None
                query = f"SELECT payload FROM {self.QUEUE_TABLE} WHERE queue_key = %s ORDER BY id"
                if limit:
                    query += f" LIMIT {limit} OFFSET {start}"
                cursor.execute(query, (key,))
                return [_deserialise(row[0]) for row in cursor.fetchall()]

        return await asyncio.to_thread(_range)
