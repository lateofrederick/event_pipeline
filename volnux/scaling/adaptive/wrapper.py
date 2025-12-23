import concurrent
import logging
import multiprocessing as mp
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from queue import Empty, Queue
from typing import Any, Callable, List, Optional

from .engine import AdaptiveScalingEngine
from .queue import QueuedFuture
from .scaling_config import ScalingConfig

logger = logging.getLogger(__name__)


class DynamicProcessPoolExecutor:
    """
    Dynamic ProcessPoolExecutor that scales workers without losing queued tasks.
    It manages the PBE instance, recreating it when the AdaptiveScalingEngine
    changes the target worker count.
    """

    def __init__(self, config: Optional[ScalingConfig] = None):
        self.config = config or ScalingConfig()
        self.scaling_engine = AdaptiveScalingEngine(self.config)
        self.current_worker_count = self.config.min_workers

        # Task management
        self._pending_tasks: Queue[QueuedFuture] = Queue()
        self._all_futures: List[QueuedFuture] = []

        # Executor management
        self._executor_lock = threading.Lock()
        self.executor: Optional[ProcessPoolExecutor] = None

        # Threads
        self._submitting = False
        self._submission_thread = None

        # Initialize
        self._rescale_executor(self.current_worker_count)
        self._start_submission_manager()
        self.scaling_engine.start_monitoring(self._scaling_callback)

    def _scaling_callback(self, metrics: dict):
        """
        Callback from the AdaptiveScalingEngine to execute the scaling action.
        """
        new_target = metrics["target_workers"]
        if new_target != self.current_worker_count:
            # Execute rescale if target changes
            self._rescale_executor(new_target)

    def _rescale_executor(self, new_worker_count: int):
        """
        Gracefully shuts down the current executor and starts a new one
        with the updated worker count.
        """
        with self._executor_lock:
            # 1. Shutdown old executor gracefully
            if self.executor:
                logger.info(
                    f"PBE scaling: Shutting down old pool (workers={self.current_worker_count})."
                )
                # wait=False allows the shutdown to be non-blocking.
                # The old PBE processes will terminate after their current tasks finish.
                self.executor.shutdown(wait=False)

                # 2. Update worker count
            self.current_worker_count = new_worker_count

            try:
                mp.set_start_method("spawn", force=True)  # <-- Add this line
            except RuntimeError:
                pass  # Already set

            # 3. Create new executor
            logger.info(f"PBE scaling: Starting new pool (workers={new_worker_count}).")
            self.executor = ProcessPoolExecutor(max_workers=new_worker_count)

    def _start_submission_manager(self):
        """Start background thread that manages task submission based on target workers"""
        self._submitting = True
        self._submission_thread = threading.Thread(
            target=self._submission_loop, daemon=True
        )
        self._submission_thread.start()

    def _submission_loop(self):
        """
        Continuously submits tasks from the internal queue to the active PBE,
        respecting the optimal batch size calculated by the engine.
        """
        while self._submitting:
            try:
                batch_size = self.scaling_engine.calculate_optimal_batch_size()
                tasks_submitted_in_cycle = 0

                # Submit up to B_size tasks
                for _ in range(batch_size):
                    try:
                        queued_future: QueuedFuture = self._pending_tasks.get(
                            timeout=0.01
                        )

                        # Submit to the active executor. The PBE's internal queue
                        # automatically handles the max_workers limit.
                        with self._executor_lock:
                            real_future = self.executor.submit(
                                queued_future._fn,
                                *queued_future._args,
                                **queued_future._kwargs,
                            )

                        queued_future.set_real_future(real_future)
                        tasks_submitted_in_cycle += 1

                    except Empty:
                        break  # No more pending tasks

                # Update queue length for the scaling engine
                self.scaling_engine.update_queue_length(self._pending_tasks.qsize())

                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in submission loop: {e}", exc_info=True)
                time.sleep(0.5)

    def submit(self, func: Callable, *args, **kwargs) -> QueuedFuture:
        """
        Submit a task. It is wrapped in a QueuedFuture and added to the pending queue.
        """
        queued_future = QueuedFuture(func, args, kwargs)
        self._pending_tasks.put(queued_future)
        self._all_futures.append(queued_future)
        self.scaling_engine.update_queue_length(self._pending_tasks.qsize())

        return queued_future

    def submit_batch(self, func: Callable, tasks: list) -> list[QueuedFuture]:
        """Submit a batch of tasks"""
        futures = []
        for task in tasks:
            args = task if isinstance(task, (list, tuple)) else (task,)
            queued_future = QueuedFuture(func, args, {})
            self._pending_tasks.put(queued_future)
            self._all_futures.append(queued_future)
            futures.append(queued_future)

        self.scaling_engine.update_queue_length(self._pending_tasks.qsize())
        return futures

    def wait_for_completion(self, timeout: Optional[float] = None):
        """Wait for all submitted tasks to complete"""

        # Wait for internal queue to empty (i.e., all tasks submitted to PBE)
        logger.info("Waiting for internal task queue to empty...")
        start_time = time.time()
        while not self._pending_tasks.empty():
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError("Timeout waiting for internal queue to drain.")
            time.sleep(0.5)

        # Wait for all real futures to complete
        logger.info(
            "Internal queue drained. Waiting for all running tasks to complete..."
        )
        for future in self._all_futures:
            if not future.done():
                try:
                    # Block on the result to ensure completion
                    future.result(timeout=timeout)
                except concurrent.futures.TimeoutError:
                    if timeout:
                        raise TimeoutError("Timeout waiting for task completion.")
        logger.info("All tasks completed.")

    def get_results(self) -> List[Any]:
        """Get results from all completed futures"""
        return [f.result() for f in self._all_futures if f.done()]

    def start_monitoring(self, callback: Optional[Callable[[dict], None]] = None):
        """Start monitoring with optional callback"""
        self.scaling_engine.start_monitoring(callback)

    def shutdown(self, wait: bool = True):
        """Shutdown executor and monitoring"""
        logger.info("Shutting down Dynamic Process Pool Executor...")
        self._submitting = False
        if self._submission_thread:
            self._submission_thread.join(timeout=5)

        self.scaling_engine.stop_monitoring()

        if self.executor:
            # This shuts down the currently active worker pool
            self.executor.shutdown(wait=wait)

        logger.info("Shutdown complete.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
