"""Backend to engine execution-control channel (Redis pub/sub).

The command counterpart to the governance reporting stream. Reporting flows
engine to backend over a durable stream; control flows backend to engine over
pub/sub, because a PAUSE/RESUME/CANCEL is a low-latency intent against a *live*
run, not a durable log to replay.

The channel is **execution-level**, keyed by the governance ``execution_id``
(which equals the running ``pipeline.id`` by the bridge's correlation rule), so
the backend controls a whole execution without knowing its internal task ids.
The engine side maps a received command onto a cooperative ``RunControl`` that
the run loop checks between steps.
"""

import json
import time
from dataclasses import dataclass, field

# Command verbs. Kept as plain strings so the wire form is transport-agnostic
# and the backend does not need to import an engine enum.
PAUSE = "pause"
RESUME = "resume"
CANCEL = "cancel"


def command_channel_name(execution_id: str) -> str:
    """The pub/sub channel a single execution's commands travel on."""
    return f"volnux:governance:cmd:{execution_id}"


@dataclass(frozen=True)
class ExecutionCommand:
    """One control intent aimed at a running execution."""

    execution_id: str
    command: str
    issued_at: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "execution_id": self.execution_id,
                "command": self.command,
                "issued_at": self.issued_at,
            }
        )

    @classmethod
    def from_json(cls, raw) -> "ExecutionCommand":
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        return cls(
            execution_id=data["execution_id"],
            command=data["command"],
            issued_at=float(data.get("issued_at", 0.0)),
        )


class CommandChannel:
    """Publish/subscribe execution commands over Redis pub/sub.

    Wraps a ``redis.Redis`` client. The backend uses ``publish``; the engine's
    dispatch worker uses ``subscribe`` to receive a run's commands.
    """

    def __init__(self, client) -> None:
        self._client = client

    def publish(self, command: ExecutionCommand) -> int:
        """Publish a command; returns the number of subscribers that received it."""
        return self._client.publish(
            command_channel_name(command.execution_id), command.to_json()
        )

    def subscribe(self, execution_id: str):
        """Return a subscribed ``PubSub`` for this execution's command channel.

        The caller drives it (``get_message``/``listen``) and closes it when the
        run ends.
        """
        pubsub = self._client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(command_channel_name(execution_id))
        return pubsub


class RunControl:
    """Cooperative control state for a single running execution.

    A command listener sets these flags; the run loop reads them between steps.
    Deliberately plain-bool based (no cross-thread asyncio primitives) so a
    listener running in a background thread can set them while an asyncio run
    loop reads them, relying only on the GIL for atomic bool access.
    """

    def __init__(self) -> None:
        self._paused = False
        self._cancelled = False

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def apply(self, command: str) -> None:
        """Fold a received command verb into the control state."""
        if command == CANCEL:
            self._cancelled = True
            self._paused = False
        elif command == PAUSE:
            if not self._cancelled:
                self._paused = True
        elif command == RESUME:
            self._paused = False
