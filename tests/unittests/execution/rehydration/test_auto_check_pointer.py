import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from volnux.execution.rehydrator.checkpoint import AutoCheckPointer


class DummyContext:
    def __init__(self, state_id="ctx-1"):
        self.state_id = state_id
        self.persist = AsyncMock()


class AwaitableTask:
    def __init__(self):
        self.cancel = MagicMock()
        self._result = None

    def done(self):
        return False

    def __await__(self):
        async def _inner():
            return self._result

        return _inner().__await__()


@pytest.mark.asyncio
async def test_stop_cancels_running_task():
    checkpointer = AutoCheckPointer()
    task = AwaitableTask()
    checkpointer._checkpoint_task = task

    await checkpointer.stop()

    task.cancel.assert_called_once()
    assert checkpointer._checkpoint_task is None


@pytest.mark.asyncio
async def test_checkpoint_all_continues_when_one_context_fails():
    checkpointer = AutoCheckPointer()
    ctx1 = DummyContext("ctx-1")
    ctx2 = DummyContext("ctx-2")

    ctx1.persist.side_effect = [RuntimeError("boom"), None]
    checkpointer.register_context(ctx1)
    checkpointer.register_context(ctx2)

    await checkpointer.checkpoint_all()

    assert ctx1.persist.await_count >= 1
    ctx2.persist.assert_awaited_once()


@pytest.mark.asyncio
async def test_checkpoint_loop_calls_checkpoint_all_periodically():
    checkpointer = AutoCheckPointer(checkpoint_interval=0.01)
    checkpointer.checkpoint_all = AsyncMock()

    with patch(
        "volnux.execution.rehydrator.checkpoint.asyncio.sleep",
        side_effect=[None, asyncio.CancelledError()],
    ):
        with pytest.raises(asyncio.CancelledError):
            await checkpointer._checkpoint_loop()

    assert checkpointer.checkpoint_all.await_count == 1
