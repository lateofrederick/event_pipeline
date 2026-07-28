"""Tests for :class:`GovernanceEventStream`.

These exercise the wrapper's use of the Redis streams API against a small
in-memory fake that reproduces the semantics the wrapper depends on: ordered
entries, consumer-group delivery cursors, at-least-once pending/ack, and the
``BUSYGROUP`` response on a duplicate group create. A live Redis is not needed
to prove the wrapper's own logic.
"""

import pytest
from redis.exceptions import ResponseError

from volnux.governance.events import EventType, GovernanceEvent
from volnux.governance.stream import GovernanceEventStream


class FakeRedisStream:
    """Minimal in-memory stand-in for the Redis streams commands used here."""

    def __init__(self):
        # Ordered list of (entry_id, fields).
        self._entries = []
        self._seq = 0
        # group -> {"cursor": int (index of last delivered), "pending": set()}
        self._groups = {}

    def xadd(self, name, fields, id="*", maxlen=None, approximate=True, **_):
        self._seq += 1
        entry_id = f"{self._seq}-0"
        self._entries.append((entry_id, dict(fields)))
        if maxlen is not None and len(self._entries) > maxlen:
            # Trim oldest to the cap (approximate flag irrelevant for the fake).
            self._entries = self._entries[-maxlen:]
        return entry_id

    def xgroup_create(self, name, groupname, id="0", mkstream=False):
        if groupname in self._groups:
            raise ResponseError("BUSYGROUP Consumer Group name already exists")
        # "0" delivers from the very start; "$" only entries added afterwards.
        cursor = len(self._entries) - 1 if id == "$" else -1
        self._groups[groupname] = {"cursor": cursor, "pending": set()}

    def xreadgroup(self, groupname, consumername, streams, count=None, block=None, **_):
        group = self._groups[groupname]
        start = group["cursor"] + 1
        new = self._entries[start:]
        if count is not None:
            new = new[:count]
        if not new:
            return None
        group["cursor"] = start + len(new) - 1
        for entry_id, _fields in new:
            group["pending"].add(entry_id)
        return [(list(streams)[0], list(new))]

    def xack(self, name, groupname, *ids):
        pending = self._groups[groupname]["pending"]
        acked = 0
        for entry_id in ids:
            if entry_id in pending:
                pending.discard(entry_id)
                acked += 1
        return acked

    # Test-only helpers.
    def pending_count(self, groupname):
        return len(self._groups[groupname]["pending"])


@pytest.fixture
def stream():
    return GovernanceEventStream(FakeRedisStream(), stream_key="test:events")


def _event(exec_id, seq):
    return GovernanceEvent(
        event_type=EventType.TASK_COMPLETED,
        execution_id=exec_id,
        task_id=f"task-{seq}",
        sequence=seq,
        payload={"status": "completed"},
    )


def test_publish_returns_entry_id(stream):
    entry_id = stream.publish(_event("exec-1", 1))

    assert isinstance(entry_id, str)


def test_ensure_group_is_idempotent(stream):
    stream.ensure_group("g")
    # Second call must not raise despite BUSYGROUP.
    stream.ensure_group("g")


def test_read_delivers_published_events_in_order(stream):
    stream.ensure_group("g")
    stream.publish(_event("exec-1", 1))
    stream.publish(_event("exec-1", 2))

    delivered = stream.read("g", "c1")

    assert [event.sequence for _id, event in delivered] == [1, 2]
    assert all(isinstance(event, GovernanceEvent) for _id, event in delivered)


def test_read_does_not_redeliver_after_advancing(stream):
    stream.ensure_group("g")
    stream.publish(_event("exec-1", 1))

    assert len(stream.read("g", "c1")) == 1
    # Nothing new since; a second read is empty (cursor advanced).
    assert stream.read("g", "c1") == []


def test_group_from_start_reads_events_published_before_it(stream):
    # A consumer started after events already exist must still see them.
    stream.publish(_event("exec-1", 1))
    stream.publish(_event("exec-1", 2))
    stream.ensure_group("late", start_id="0")

    assert len(stream.read("late", "c1")) == 2


def test_group_from_dollar_only_sees_new_events(stream):
    stream.publish(_event("exec-1", 1))
    stream.ensure_group("tail", start_id="$")
    stream.publish(_event("exec-1", 2))

    delivered = stream.read("tail", "c1")

    assert [event.sequence for _id, event in delivered] == [2]


def test_ack_clears_pending(stream):
    fake = stream._client
    stream.ensure_group("g")
    stream.publish(_event("exec-1", 1))
    delivered = stream.read("g", "c1")
    assert fake.pending_count("g") == 1

    entry_ids = [entry_id for entry_id, _event in delivered]
    acked = stream.ack("g", *entry_ids)

    assert acked == 1
    assert fake.pending_count("g") == 0


def test_ack_with_no_ids_is_a_noop(stream):
    stream.ensure_group("g")
    assert stream.ack("g") == 0


def test_count_limits_batch_size(stream):
    stream.ensure_group("g")
    for i in range(5):
        stream.publish(_event("exec-1", i))

    first = stream.read("g", "c1", count=2)
    assert len(first) == 2
    # Remaining are delivered on the next read.
    assert len(stream.read("g", "c1", count=10)) == 3


def test_maxlen_caps_stream_length():
    fake = FakeRedisStream()
    bounded = GovernanceEventStream(fake, stream_key="test:events", max_len=2)
    bounded.ensure_group("g")

    for i in range(3):
        bounded.publish(_event("exec-1", i))

    # Only the last two survive the cap.
    assert len(fake._entries) == 2
