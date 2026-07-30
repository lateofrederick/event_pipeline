"""The engine-side dispatch worker.

Consumes ``DispatchRequest``s from the dispatch queue and runs each one, while
routing that execution's control commands (PAUSE/RESUME/CANCEL) onto a
cooperative ``RunControl`` the run loop honors. The actual run is delegated to an
injected ``runner`` coroutine, so the transport/worker here is production code
while the thing that *executes* the work can be swapped: a stand-in that emits
lifecycle signals today, the real Pointy pipeline once the compiler is complete.

Command delivery uses a short-lived background thread per run that drives the
Redis pub/sub subscription and folds each message into the run's ``RunControl``.
A thread (rather than an asyncio task) keeps the blocking pub/sub read off the
event loop, and ``RunControl`` is plain-bool based so the hand-off needs no
cross-thread asyncio primitives.
"""

import asyncio
import logging
import threading
from typing import Awaitable, Callable, Optional

from .control import CommandChannel, ExecutionCommand, RunControl
from .dispatch import DEFAULT_DISPATCH_GROUP, DispatchQueue, DispatchRequest

logger = logging.getLogger(__name__)

# A runner takes the request and the cooperative control handle and drives the
# run to completion (emitting lifecycle signals along the way).
Runner = Callable[[DispatchRequest, RunControl], Awaitable[None]]


class _CommandListener:
    """Drives a run's command pub/sub subscription on a background thread."""

    def __init__(self, channel: CommandChannel, execution_id: str, control: RunControl):
        self._pubsub = channel.subscribe(execution_id)
        self._execution_id = execution_id
        self._control = control
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                # Poll with a timeout so the loop can observe the stop flag and
                # the subscription can be torn down promptly when the run ends.
                message = self._pubsub.get_message(timeout=0.2)
                if not message or message.get("type") != "message":
                    continue
                try:
                    command = ExecutionCommand.from_json(message["data"])
                except (
                    Exception
                ):  # noqa: BLE001 - a malformed command must not kill the run
                    logger.exception("Discarding malformed execution command")
                    continue
                logger.info(
                    "Execution %s received command %s",
                    self._execution_id,
                    command.command,
                )
                self._control.apply(command.command)
        finally:
            try:
                self._pubsub.close()
            except Exception:  # noqa: BLE001
                pass

    def stop(self) -> None:
        self._stop.set()


class DispatchWorker:
    """Consume dispatch requests and run each with command routing attached."""

    def __init__(
        self,
        dispatch_queue: DispatchQueue,
        command_channel: CommandChannel,
        runner: Runner,
        *,
        group: str = DEFAULT_DISPATCH_GROUP,
        consumer_name: str = "worker-1",
    ) -> None:
        self._queue = dispatch_queue
        self._commands = command_channel
        self._runner = runner
        self._group = group
        self._consumer_name = consumer_name

    async def _handle(self, request: DispatchRequest) -> None:
        control = RunControl()
        listener = _CommandListener(self._commands, request.execution_id, control)
        listener.start()
        try:
            await self._runner(request, control)
        except Exception:  # noqa: BLE001 - one run failing must not stop the worker
            logger.exception("Dispatch runner failed for %s", request.execution_id)
        finally:
            listener.stop()

    async def run_forever(
        self,
        *,
        block_ms: int = 1000,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """Consume and run dispatch requests until ``stop_event`` is set."""
        await asyncio.to_thread(self._queue.ensure_group, self._group)
        active: set = set()
        while stop_event is None or not stop_event.is_set():
            batch = await asyncio.to_thread(
                self._queue.consume,
                self._group,
                self._consumer_name,
                block_ms=block_ms,
            )
            for entry_id, request in batch:
                # Runs proceed concurrently; ack immediately since the request
                # has been accepted for execution (the run's own progress is
                # reported over the events stream, not the dispatch queue).
                task = asyncio.create_task(self._handle(request))
                active.add(task)
                task.add_done_callback(active.discard)
                await asyncio.to_thread(self._queue.ack, self._group, entry_id)
        if active:
            await asyncio.gather(*active, return_exceptions=True)
