"""Outbound governance event feed.

This package is the engine's *outbound* side of the boundary with the Volnux
platform (control-plane) backend. The engine publishes durable, ordered facts
about what it is doing — executions starting and finishing, tasks running,
human-in-the-loop pauses, node heartbeats — onto a Redis stream. A separate
backend process consumes that stream and materializes governance records
(``Execution``, ``ExecutionTrace``, ``HITLRequest``, ``NodeHeartbeat``, audit
entries, ...).

Design boundary
---------------
The engine emits only *execution/task/node facts*. It holds no governance
concepts — organizations, users, approvals, tenancy — those are supplied and
resolved entirely by the consumer. Keeping this direction one-way and free of
governance semantics is what lets the engine run standalone with no backend at
all: if nothing is publishing (the reporter is not installed), the signals the
engine already fires simply have no listener and the engine behaves exactly as
before.

Why a durable stream (and not RPC or pub/sub)
---------------------------------------------
Reporting must never block or fail a running workflow, yet the governance
record of truth must be complete and correctly ordered. A synchronous RPC to
the backend would couple execution progress to backend liveness; fire-and-forget
pub/sub would silently drop the one event you cannot lose (``execution.completed``)
whenever the backend happens to be restarting. A Redis stream is durable and
ordered independently of the backend: the backend can be down for a deploy and
simply drain the backlog on recovery. Redis is already part of the stack, so
this adds no new infrastructure.
"""

from .events import EventType, GovernanceEvent
from .reporter import SignalGovernanceReporter, install_governance_reporter
from .stream import (
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_STREAM_KEY,
    GovernanceEventStream,
)

__all__ = [
    "EventType",
    "GovernanceEvent",
    "GovernanceEventStream",
    "SignalGovernanceReporter",
    "install_governance_reporter",
    "DEFAULT_STREAM_KEY",
    "DEFAULT_CONSUMER_GROUP",
]
