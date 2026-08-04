"""Non-blocking delivery of governance events off the engine's hot path.

The reporter runs inside synchronous signal handlers on the execution path, so
it must never block on the transport. It hands each event's fields to this
publisher through a non-blocking ``submit()``; a single background thread drains
the buffer and performs the async send (building a ``GovernanceEvent`` and
enqueuing it over the provisioned backend).

If the buffer fills or the transport stalls, the engine is unaffected:
``submit()`` never waits, and an overflow drops the event with a warning rather
than back-pressuring a running workflow. Reporting is observational.

The send step is injected as a ``sink`` so the publisher's buffering and
threading can be exercised with a fake, and so the default sink (which builds the
model and enqueues it) is the only code that depends on a provisioned backend.
"""

import asyncio
import logging
import queue
import threading
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional

from .events import GovernanceEvent, encode_payload

logger = logging.getLogger(__name__)

# A sink receives one event's field dict and delivers it. Async because the
# messaging backend is async.
Sink = Callable[[Dict[str, Any]], Awaitable[None]]


async def enqueue_governance_event(fields: Dict[str, Any]) -> None:
    """Default sink: build a ``GovernanceEvent`` and enqueue it over the backend.

    Correlation ids that do not apply are normalised to empty strings and an
    untracked sequence to ``-1``, so the model's plain fields are always filled.
    """
    event = GovernanceEvent(
        event_type=fields.get("event_type", ""),
        event_id=fields.get("event_id") or str(uuid.uuid4()),
        occurred_at=float(fields.get("occurred_at") or time.time()),
        execution_id=fields.get("execution_id") or "",
        task_id=fields.get("task_id") or "",
        workflow_id=fields.get("workflow_id") or "",
        workflow_name=fields.get("workflow_name") or "",
        sequence=int(fields["sequence"]) if fields.get("sequence") is not None else -1,
        payload=encode_payload(fields.get("payload") or {}),
    )
    await GovernanceEvent.enqueue(event)


class GovernanceEventPublisher:
    """Buffer governance events and deliver them from a background thread.

    Args:
        sink: Coroutine that delivers one event's fields. Defaults to building a
            ``GovernanceEvent`` and enqueuing it.
        max_buffer: Bound on the in-process buffer. On overflow, ``submit`` drops
            the event rather than block.
    """

    def __init__(
        self,
        sink: Optional[Sink] = None,
        *,
        max_buffer: int = 10000,
        flush_source: Optional[Callable[[], List[Dict[str, Any]]]] = None,
        flush_interval: float = 1.0,
    ) -> None:
        self._sink = sink or enqueue_governance_event
        self._buffer: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=max_buffer)
        # Optional periodic pull, e.g. draining a sampler's reservoir. Delivered
        # on the same thread as buffered events, so no extra thread is spun up.
        self._flush_source = flush_source
        self._flush_interval = flush_interval
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    def start(self) -> None:
        """Start the delivery thread. Idempotent."""
        if self._started:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="governance-publisher", daemon=True
        )
        self._thread.start()
        self._started = True

    def submit(self, fields: Dict[str, Any]) -> None:
        """Hand an event to the delivery thread. Never blocks."""
        try:
            self._buffer.put_nowait(fields)
        except queue.Full:
            logger.warning(
                "Governance event buffer full; dropping %s",
                fields.get("event_type"),
            )

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        last_flush = time.monotonic()
        try:
            # Keep draining while running, and flush whatever remains on stop.
            while not self._stop.is_set() or not self._buffer.empty():
                if self._flush_source is not None:
                    now = time.monotonic()
                    if now - last_flush >= self._flush_interval:
                        self._flush(loop)
                        last_flush = now
                try:
                    fields = self._buffer.get(timeout=0.2)
                except queue.Empty:
                    continue
                self._deliver(loop, fields)
            # Final flush on stop so sampled telemetry is not lost.
            if self._flush_source is not None:
                self._flush(loop)
        finally:
            loop.close()

    def _flush(self, loop: "asyncio.AbstractEventLoop") -> None:
        try:
            pending = self._flush_source()  # type: ignore[misc]
        except Exception:  # noqa: BLE001 - a flush failure must not kill delivery
            logger.exception("Governance flush source failed")
            return
        for fields in pending:
            self._deliver(loop, fields)

    def _deliver(
        self, loop: "asyncio.AbstractEventLoop", fields: Dict[str, Any]
    ) -> None:
        try:
            loop.run_until_complete(self._sink(fields))
        except Exception:  # noqa: BLE001 - one bad event must not kill delivery
            logger.exception(
                "Failed to publish governance event %s", fields.get("event_type")
            )

    def stop(self, *, timeout: float = 5.0) -> None:
        """Signal the thread to flush the buffer and stop. Idempotent."""
        if not self._started:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._started = False
