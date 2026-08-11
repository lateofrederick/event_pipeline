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

The structured ``payload`` (status, error, node metrics, HITL prompt, ...) is a
plain mapping and stays one the whole way across: producers build a dict,
consumers read a dict, and the model layer handles coercion and serialisation.
"""

from typing import Any, Dict, Optional
from formax import BaseModel, Attrib, MiniAnnotated
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin
from volnux.mixins.messaging import MessagingBackendIntegrationMixin
from volnux.config import VolnuxConfig


volnux_config = VolnuxConfig.get_instance()


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

    ``event_type``, ``event_id`` and ``occurred_at`` are always present — every
    event has an identity and a time. The correlation fields are ``Optional``
    because they genuinely do not apply to every event: a ``node.heartbeat``
    belongs to no execution, and only ``task.*`` events name a task. ``None``
    means "not applicable" and is distinct from a blank value, so the consumer
    never has to tell a real id from a sentinel.

    ``node_id`` and ``project_id`` say *where* the fact came from: which mesh
    node emitted it and which project it belongs to. Both are constant for the
    life of the process, so the declared factories are only a fallback — the
    reporter fills them per event from the same config.
    """

    event_type: str
    event_id: str
    occurred_at: float
    execution_id: Optional[str]
    task_id: Optional[str]
    workflow_id: Optional[str]
    workflow_name: Optional[str]
    sequence: Optional[int]
    payload: Dict[str, Any]
    node_id: MiniAnnotated[str, Attrib(default_factory=lambda: volnux_config.get_node_id())]
    project_id: MiniAnnotated[str, Attrib(default_factory=lambda: volnux_config.get('PROJECT_ID'))]

    @classmethod
    def get_schema_name(cls) -> str:
        return GOVERNANCE_SCHEMA
