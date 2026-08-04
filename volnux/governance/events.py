"""The governance event contract.

A ``GovernanceEvent`` is a single fact the engine reports for the platform to
project (an execution started, a task completed, a node beat). It is a
transport-agnostic message: it mixes in the engine's messaging integration and
is delivered through whatever backend the deployment has provisioned (Redis,
Postgres, in-memory, ...), keyed by ``get_schema_name()``. There is no Redis or
any other transport hard-wired here.

The pattern mirrors ``volnux.backends.saga._dlq.DeadLetterEntry``: a model that
is both persistable (``KeyValueStoreIntegrationMixin``) and
publishable/queueable (``MessagingBackendIntegrationMixin``). The reporter
enqueues events with ``GovernanceEvent.enqueue(event)`` and a consumer drains
them with ``GovernanceEvent.dequeue()`` over the same configured backend.

Serialisation note: ``payload`` is carried as a JSON string rather than a nested
dict. The event's structured payload (status, error, node metrics, HITL prompt,
...) is JSON-encoded by the producer and decoded by the consumer via the
``payload_dict`` / ``with_payload`` helpers. Keeping the field a scalar string
keeps the model flat and avoids depending on nested-container coercion in the
model layer.
"""

import json
from typing import Any, Dict

from formax import BaseModel

from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin
from volnux.mixins.messaging import MessagingBackendIntegrationMixin


class EventType:
    """Canonical event-type identifiers.

    Names are namespaced ``"<subject>.<verb>"`` so a consumer can route on the
    subject prefix (``execution.*``, ``task.*``, ...) without matching every
    leaf. The engine only publishes the "engine emits" ones; the full vocabulary
    lives here so the consumer has a single authoritative list to project from.
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


# The queue/channel identity for governance events, shared by the engine
# producer and the platform consumer.
GOVERNANCE_SCHEMA = "volnux:governance:events"


class GovernanceEvent(
    KeyValueStoreIntegrationMixin, MessagingBackendIntegrationMixin, BaseModel
):
    """A single governance fact, delivered over the provisioned messaging backend.

    Fields are declared plainly (no in-model defaults) so construction stays
    predictable across backends; the reporter fills every field, using empty
    strings for correlation ids that do not apply to a given event and ``-1``
    for an untracked sequence.
    """

    event_type: str
    event_id: str
    occurred_at: float
    execution_id: str
    task_id: str
    workflow_id: str
    workflow_name: str
    sequence: int
    payload: str

    @classmethod
    def get_schema_name(cls) -> str:
        return GOVERNANCE_SCHEMA

    def payload_dict(self) -> Dict[str, Any]:
        """Decode the JSON ``payload`` back into a dict, tolerating garbage."""
        if not self.payload:
            return {}
        try:
            decoded = json.loads(self.payload)
        except (ValueError, TypeError):
            return {}
        return decoded if isinstance(decoded, dict) else {}


def encode_payload(payload: Dict[str, Any]) -> str:
    """JSON-encode an event payload for the wire, compactly and deterministically."""
    if not payload:
        return ""
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)
