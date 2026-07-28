"""The governance event contract.

This module is deliberately dependency-free: it imports nothing from the engine
and nothing from the governance model layer. It is the shared vocabulary that
crosses the process boundary between the engine (producer) and the platform
backend (consumer), so it must stay a plain, JSON-serializable data structure
that either side can depend on without pulling in the other.
"""

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


class EventType:
    """Canonical event-type identifiers.

    Names are namespaced ``"<subject>.<verb>"`` so a consumer can route on the
    subject prefix (``execution.*``, ``task.*``, ...) without matching every
    leaf. The comments record which side emits each type; the engine only ever
    publishes the "engine emits" ones, but the full vocabulary lives here so the
    consumer has a single authoritative list to project from.
    """

    # --- Execution lifecycle (engine emits) --------------------------------
    EXECUTION_STARTED = "execution.started"
    EXECUTION_COMPLETED = "execution.completed"
    EXECUTION_FAILED = "execution.failed"
    EXECUTION_PAUSED = "execution.paused"
    EXECUTION_RESUMED = "execution.resumed"
    EXECUTION_STOPPED = "execution.stopped"

    # --- Task/event lifecycle within an execution (engine emits) -----------
    # "task" here is a single event/node in the pipeline graph; it maps onto an
    # ExecutionTrace row on the consumer side.
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_RETRIED = "task.retried"

    # --- Human-in-the-loop (engine emits "requested"; backend owns the rest)-
    HITL_REQUESTED = "hitl.requested"

    # --- Mesh node health (engine emits) -----------------------------------
    NODE_HEARTBEAT = "node.heartbeat"
    NODE_DECOMMISSIONED = "node.decommissioned"


@dataclass(frozen=True)
class GovernanceEvent:
    """A single fact the engine reports for the platform to project.

    Attributes:
        event_id: Unique id for this event. Used by the consumer for
            idempotent projection — a Redis consumer group may redeliver an
            entry after a crash, and deduping on ``event_id`` makes that safe.
        event_type: One of the ``EventType`` constants.
        occurred_at: Unix timestamp (seconds) when the fact happened, set by
            the producer. This is the authoritative ordering key *within* a
            correlation id; the stream entry id orders events globally.
        workflow_id: The workflow the fact relates to, if any.
        workflow_name: Human-readable workflow name, for convenience/logging.
        execution_id: The runtime execution the fact relates to, if any. This
            is the correlation key that ties tasks and HITL events back to
            their execution.
        task_id: The task/event within the execution, for ``task.*`` events.
        sequence: Optional per-execution monotonic counter. Lets the consumer
            order same-millisecond events and detect gaps; ``None`` when the
            producer does not track one.
        payload: Event-specific data (status, error message, node metrics, HITL
            prompt, ...). Must be JSON-serialisable.
    """

    event_type: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: float = field(default_factory=time.time)
    workflow_id: Optional[str] = None
    workflow_name: Optional[str] = None
    execution_id: Optional[str] = None
    task_id: Optional[str] = None
    sequence: Optional[int] = None
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_stream_fields(self) -> Dict[str, str]:
        """Flatten to the string→string field map a Redis stream entry stores.

        ``None`` correlation fields are omitted rather than written as empty
        strings, so the consumer can distinguish "not applicable" from "blank".
        The payload is JSON-encoded under a single ``payload`` field to keep the
        entry flat while preserving nested structure.
        """
        fields: Dict[str, str] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "occurred_at": repr(self.occurred_at),
            "payload": json.dumps(self.payload, separators=(",", ":")),
        }
        if self.workflow_id is not None:
            fields["workflow_id"] = self.workflow_id
        if self.workflow_name is not None:
            fields["workflow_name"] = self.workflow_name
        if self.execution_id is not None:
            fields["execution_id"] = self.execution_id
        if self.task_id is not None:
            fields["task_id"] = self.task_id
        if self.sequence is not None:
            fields["sequence"] = str(self.sequence)
        return fields

    @classmethod
    def from_stream_fields(cls, fields: Dict[str, str]) -> "GovernanceEvent":
        """Reconstruct an event from a Redis stream field map.

        Tolerant by design: a missing or malformed ``payload`` yields ``{}``
        rather than raising, and unknown extra fields are ignored, so a
        consumer built against an older contract still reads newer entries.
        """
        raw_payload = fields.get("payload")
        try:
            payload = json.loads(raw_payload) if raw_payload else {}
            if not isinstance(payload, dict):
                payload = {}
        except (ValueError, TypeError):
            payload = {}

        occurred_at = fields.get("occurred_at")
        sequence = fields.get("sequence")

        return cls(
            event_type=fields.get("event_type", ""),
            event_id=fields.get("event_id", ""),
            occurred_at=_to_float(occurred_at, default=0.0),
            workflow_id=fields.get("workflow_id"),
            workflow_name=fields.get("workflow_name"),
            execution_id=fields.get("execution_id"),
            task_id=fields.get("task_id"),
            sequence=_to_int(sequence),
            payload=payload,
        )


def _to_float(value: Optional[str], *, default: float) -> float:
    """Parse a float field, falling back to ``default`` on missing/garbage."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _to_int(value: Optional[str]) -> Optional[int]:
    """Parse an optional int field, returning ``None`` on missing/garbage."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
