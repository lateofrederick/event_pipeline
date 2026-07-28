"""Redis-stream transport for governance events.

A thin wrapper over a Redis client that provides exactly two roles:

* the **producer** side (``GovernanceEventStream.publish``), used by the
  engine's reporter to append an event; and
* the **consumer** side (``ensure_group``, ``read``, ``ack``), used
  by the platform backend to drain the stream through a consumer group with
  at-least-once delivery.

The wrapper is deliberately small and honest: ``publish`` raises on a Redis
error rather than swallowing it. Isolation from the engine's hot path is the
*reporter's* responsibility, not the transport's — that way the backend
consumer still sees real failures instead of silent no-ops.
"""

from typing import List, Optional, Tuple

from redis.exceptions import ResponseError

from .events import GovernanceEvent

# Single stream all governance events are appended to. A single stream (rather
# than one per execution) keeps global ordering via the entry id and gives the
# consumer one place to read; correlation is done by ``execution_id`` on each
# event.
DEFAULT_STREAM_KEY = "volnux:governance:events"

# Consumer group the platform backend reads under. A group gives at-least-once
# delivery: entries stay pending until acked, so a consumer crash mid-projection
# redelivers rather than drops.
DEFAULT_CONSUMER_GROUP = "volnux-platform"


class GovernanceEventStream:
    """Publish and consume ``GovernanceEvent`` records over a Redis stream.

    Args:
        client: A connected ``redis.Redis`` client. Obtain one from the
            project's Redis connector via ``connector.get_cursor()``, or pass
            any object implementing the handful of stream commands used here
            (``xadd``, ``xgroup_create``, ``xreadgroup``, ``xack``).
        stream_key: The stream to read/write. Defaults to
            ``DEFAULT_STREAM_KEY``.
        maxlen: Optional cap on stream length. Left ``None`` by default:
            governance is a record of truth, so blind trimming that could drop
            unconsumed entries is unsafe. Trim by consumer progress instead
            (e.g. ``XTRIM`` to the minimum acked id) if retention needs bounding.
        approximate: When ``maxlen`` is set, use Redis' approximate (``~``)
            trimming, which is far cheaper and the usual choice.
    """

    def __init__(
        self,
        client,
        *,
        stream_key: str = DEFAULT_STREAM_KEY,
        max_len: Optional[int] = None,
        approximate: bool = True,
    ) -> None:
        self._client = client
        self._stream_key = stream_key
        self._max_len = max_len
        self._approximate = approximate

    @classmethod
    def from_connector(cls, connector, **kwargs) -> "GovernanceEventStream":
        """Build a stream from the project's Redis connector.

        Ensures the connector is connected, then borrows its client. Any keyword
        arguments are forwarded to ``GovernanceEventStream``.
        """
        if not connector.is_connected():
            connector.connect()
        return cls(connector.get_cursor(), **kwargs)

    @property
    def stream_key(self) -> str:
        return self._stream_key

    # -- Producer -----------------------------------------------------------

    def publish(self, event: GovernanceEvent) -> str:
        """Append an event to the stream and return its Redis entry id.

        Raises whatever the Redis client raises on failure; callers that must
        not be affected by a publish failure (the engine's signal hot path)
        wrap this in their own try/except.
        """
        entry_id = self._client.xadd(
            self._stream_key,
            event.to_stream_fields(),
            maxlen=self._max_len,
            approximate=self._approximate,
        )
        return entry_id

    # -- Consumer -----------------------------------------------------------

    def ensure_group(
        self, group: str = DEFAULT_CONSUMER_GROUP, *, start_id: str = "0"
    ) -> None:
        """Create the consumer group if it does not already exist.

        Idempotent: the "already exists" (``BUSYGROUP``) response is expected on
        every start after the first and is swallowed. ``mkstream=True`` creates
        the stream too, so the consumer can be started before the first event is
        ever published. ``start_id="0"`` reads the stream from the beginning;
        pass ``"$"`` to consume only events published after the group is created.
        """
        try:
            self._client.xgroup_create(
                self._stream_key, group, id=start_id, mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                return
            raise

    def read(
        self,
        group: str = DEFAULT_CONSUMER_GROUP,
        consumer: str = "default",
        *,
        count: int = 100,
        block_ms: Optional[int] = None,
    ) -> List[Tuple[str, GovernanceEvent]]:
        """Read up to ``count`` new (never-delivered) events for this consumer.

        Args:
            group: Consumer group to read under. Must already exist
                (``ensure_group``).
            consumer: This consumer's name within the group. Entries delivered
                here stay pending against this name until ``ack``-ed.
            count: Maximum number of entries to return in one call.
            block_ms: If set, block up to this many milliseconds waiting for new
                entries before returning empty; if ``None``, return immediately.

        Returns:
            A list of ``(entry_id, event)`` pairs in stream order. ``entry_id``
            is the Redis id needed to ``ack`` the entry.
        """
        response = self._client.xreadgroup(
            group,
            consumer,
            {self._stream_key: ">"},
            count=count,
            block=block_ms,
        )
        if not response:
            return []

        # xreadgroup returns [(stream_key, [(entry_id, {fields}), ...]), ...].
        # A single stream is read, so there is exactly one bucket.
        out: List[Tuple[str, GovernanceEvent]] = []
        for _stream_key, entries in response:
            for entry_id, fields in entries:
                # A trimmed/deleted entry can surface as (id, None); skip it.
                if fields is None:
                    continue
                out.append((entry_id, GovernanceEvent.from_stream_fields(fields)))
        return out

    def ack(self, group: str, *entry_ids: str) -> int:
        """Acknowledge processed entries so they leave the pending list.

        Returns the number of entries actually acknowledged. Call this only
        after the event has been durably projected into governance state, so a
        crash before projection results in redelivery rather than loss.
        """
        if not entry_ids:
            return 0
        return self._client.xack(self._stream_key, group, *entry_ids)
