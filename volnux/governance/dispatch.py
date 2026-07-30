"""Backend to engine dispatch queue (durable Redis stream).

The backend enqueues a ``DispatchRequest`` to start an execution on the engine;
an engine ``DispatchWorker`` (see ``worker.py``) consumes it, runs the pipeline
correlated to the governance ``execution_id``, and the reporter streams the run
back over the events stream. A durable stream + consumer group means a request
survives the worker being down and is delivered to exactly one worker.

The request is deliberately thin. It carries the ``execution_id`` (the
correlation key the engine stamps onto the pipeline) plus enough to identify the
work; the authoritative workflow definition is resolved engine-side. ``steps`` is
used by the stand-in runner while the Pointy compiler is being completed; a
production runner ignores it and builds the real pipeline from the workflow.
"""

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_DISPATCH_STREAM = "volnux:governance:dispatch"
DEFAULT_DISPATCH_GROUP = "volnux-engine"


def _decode_fields(fields) -> Dict[str, str]:
    """Normalize xreadgroup fields to a str->str dict (bytes or str clients)."""
    out = {}
    for key, value in fields.items():
        if isinstance(key, (bytes, bytearray)):
            key = key.decode("utf-8")
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        out[key] = value
    return out


def _loads(value, default):
    if value is None:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class DispatchRequest:
    """A request to start one execution on the engine."""

    execution_id: str
    workflow_name: str = ""
    steps: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)

    def to_stream_fields(self) -> Dict[str, str]:
        return {
            "execution_id": self.execution_id,
            "workflow_name": self.workflow_name,
            "steps": json.dumps(self.steps),
            "params": json.dumps(self.params),
        }

    @classmethod
    def from_stream_fields(cls, fields) -> "DispatchRequest":
        data = _decode_fields(fields)
        return cls(
            execution_id=data.get("execution_id", ""),
            workflow_name=data.get("workflow_name", ""),
            steps=_loads(data.get("steps"), []),
            params=_loads(data.get("params"), {}),
        )


class DispatchQueue:
    """A durable Redis-stream queue of dispatch requests.

    Wraps a ``redis.Redis`` client. The backend calls ``enqueue``; the engine
    worker calls ``ensure_group`` then ``consume``/``ack``.
    """

    def __init__(self, client, *, stream_key: str = DEFAULT_DISPATCH_STREAM) -> None:
        self._client = client
        self._stream_key = stream_key

    def enqueue(self, request: DispatchRequest) -> str:
        return self._client.xadd(self._stream_key, request.to_stream_fields())

    def ensure_group(
        self, group: str = DEFAULT_DISPATCH_GROUP, *, start_id: str = "0"
    ) -> None:
        from redis.exceptions import ResponseError

        try:
            self._client.xgroup_create(
                self._stream_key, group, id=start_id, mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                return
            raise

    def consume(
        self,
        group: str = DEFAULT_DISPATCH_GROUP,
        consumer: str = "worker",
        *,
        count: int = 10,
        block_ms: Optional[int] = None,
    ) -> List[Tuple[str, DispatchRequest]]:
        response = self._client.xreadgroup(
            group, consumer, {self._stream_key: ">"}, count=count, block=block_ms
        )
        out: List[Tuple[str, DispatchRequest]] = []
        for _stream_key, entries in response or []:
            for entry_id, fields in entries:
                if fields is None:
                    continue
                if isinstance(entry_id, (bytes, bytearray)):
                    entry_id = entry_id.decode("utf-8")
                out.append((entry_id, DispatchRequest.from_stream_fields(fields)))
        return out

    def ack(self, group: str, *entry_ids: str) -> int:
        if not entry_ids:
            return 0
        return self._client.xack(self._stream_key, group, *entry_ids)
