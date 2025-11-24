import concurrent
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class QueuedFuture(concurrent.futures.Future):
    """
    A custom Future that tracks a task while it is in the internal queue,
    then proxies to the real Future once submitted to the ProcessPoolExecutor.
    """

    def __init__(self, fn: Callable, args: tuple, kwargs: dict):
        super().__init__()
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self._real_future: Optional[concurrent.futures.Future] = None
        self._submit_time = None

    def set_real_future(self, real_future: concurrent.futures.Future):
        """Called by the submission loop when the task is finally submitted."""
        self._real_future = real_future
        self._submit_time = time.time()
        # Attach a callback to the real future to propagate results/exceptions
        real_future.add_done_callback(self._propagate_result)

    def _propagate_result(self, future: concurrent.futures.Future):
        """Propagates result or exception from the real future to this one."""
        if future.cancelled():
            self.cancel()
        elif future.exception():
            self.set_exception(future.exception())
        else:
            # Task completed successfully.
            self.set_result(future.result())

    def cancel(self):
        if self._real_future is None:
            return super().cancel()
        return self._real_future.cancel()

    def running(self):
        if self._real_future is None:
            return False  # Not running, waiting in queue
        return self._real_future.running()

    def done(self):
        if self._real_future is None:
            return super().done()
        return self._real_future.done()

    def result(self, timeout=None):
        if self._real_future is None:
            # If done() is True, result should be available via super().result()
            if self.done():
                return super().result(timeout=timeout)
            # If not done, but not yet submitted, the user should not call result()
            raise RuntimeError("Task result unavailable: not yet submitted by engine.")
        return self._real_future.result(timeout=timeout)
