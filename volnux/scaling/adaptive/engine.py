import logging
import math
import threading
import time
from typing import Callable, Optional

from .scaling_config import ScalingConfig
from .system_monitor import SystemMonitor

logger = logging.getLogger(__name__)


class AdaptiveScalingEngine:
    """
    Adaptive scaling engine with real-time system monitoring
    """

    def __init__(self, config: Optional[ScalingConfig] = None):
        self.config = config or ScalingConfig()
        self.system_monitor = SystemMonitor()

        # Calculate maximum workers based on resource quota
        self.max_workers = self._calculate_max_workers()
        self.target_workers = self.config.min_workers

        # Tracking metrics
        self.task_queue_length = 0
        self.last_scale_action_time = time.time()
        self.last_scale_action = None  # 'up', 'down', or None
        self._lock = threading.RLock()

        # Monitoring thread
        self._monitoring = False
        self._monitor_thread = None

        logger.info(
            f"Engine Initialized. Max Workers (W_max): {self.max_workers} based on Quota."
        )

    def _calculate_max_workers(self) -> int:
        """
        Algorithm 1: Resource-to-Worker Mapping
        W_max = floor(Q_max / R_w). Takes the minimum of CPU and Memory constraints.
        """
        cpu_based = math.floor(self.config.max_cpu_quota / self.config.cpu_per_worker)
        memory_based = math.floor(
            self.config.max_memory_quota / self.config.memory_per_worker
        )

        # Take the minimum to respect the tightest constraint
        w_max = min(cpu_based, memory_based)
        return max(w_max, self.config.min_workers)

    def calculate_optimal_batch_size(self) -> int:
        """
        Algorithm 2: Optimal Batch Size Calculation
        B_size = Parallelism_Multiplier × W_current. Dynamically adjusted.
        """
        with self._lock:
            base_batch_size = self.config.parallelism_multiplier * self.target_workers

            # Dynamic adjustment based on queue feedback
            if self.task_queue_length < self.target_workers:
                # Queue is low, increase batch size to load faster
                adjusted_size = int(base_batch_size * 1.5)
            elif self.task_queue_length > (self.target_workers * 5):
                # Queue is very high, reduce batch size (or pause submission)
                adjusted_size = int(base_batch_size * 0.5)
            else:
                adjusted_size = base_batch_size

            return max(adjusted_size, self.config.min_workers)

    def _check_resource_constraints(self) -> dict:
        """Check if we're within resource constraints (measured in absolute cores/GB)"""

        cpu_usage_cores = self.system_monitor.get_average_cpu_usage()
        memory_usage_gb = self.system_monitor.get_total_memory_usage()

        # Check against quotas
        cpu_available = cpu_usage_cores < (
            self.config.max_cpu_quota * self.config.cpu_threshold_scale_up
        )
        cpu_underutilized = cpu_usage_cores < (
            self.config.max_cpu_quota * self.config.cpu_threshold_scale_down
        )
        memory_available = memory_usage_gb < (
            self.config.max_memory_quota * self.config.memory_threshold
        )

        return {
            "cpu_usage": cpu_usage_cores,
            "cpu_limit": self.config.max_cpu_quota,
            "cpu_available": cpu_available,
            "cpu_underutilized": cpu_underutilized,
            "memory_usage": memory_usage_gb,
            "memory_limit": self.config.max_memory_quota,
            "memory_available": memory_available,
        }

    def should_scale_up(self) -> tuple[bool, str]:
        """Determine if we should add more workers."""
        with self._lock:
            if self.target_workers >= self.max_workers:
                return False, "At max workers (W_max limit)"

            resources = self._check_resource_constraints()

            # 1. Resource Check: Must have quota available
            if not resources["cpu_available"]:
                return (
                    False,
                    f"CPU Quota usage too high: {resources['cpu_usage']:.2f}/{resources['cpu_limit']:.2f} cores",
                )
            if not resources["memory_available"]:
                return (
                    False,
                    f"Memory Quota usage too high: {resources['memory_usage']:.2f}/{resources['memory_limit']:.2f}GB",
                )

            # 2. Queue Pressure: If queue length is high relative to current workers
            queue_threshold = self.target_workers * self.config.scale_up_threshold
            if self.task_queue_length >= queue_threshold:
                return (
                    True,
                    f"Queue pressure: {self.task_queue_length} tasks (>{queue_threshold} threshold)",
                )

            return False, "No scale up needed"

    def should_scale_down(self) -> tuple[bool, str]:
        """Determine if we should remove idle workers."""
        with self._lock:
            if self.target_workers <= self.config.min_workers:
                return False, "At minimum workers"

            # 1. Workload Check: Don't scale down if there's work to do
            if self.task_queue_length > 0:
                return False, f"Queue has tasks: {self.task_queue_length}"

            resources = self._check_resource_constraints()

            # 2. Resource Check: Scale down if resources are underutilized AND queue is empty
            if resources["cpu_underutilized"] and self.task_queue_length == 0:
                time_since_last_scale = time.time() - self.last_scale_action_time

                # 3. Timeout Check: Wait for defined idle timeout
                if time_since_last_scale >= self.config.scale_down_timeout:
                    return (
                        True,
                        f"Low CPU usage: {resources['cpu_usage']:.2f} cores for >{self.config.scale_down_timeout}s",
                    )

            return False, "No scale down needed"

    def set_target_worker_count(self, count: int) -> bool:
        """Update target worker count safely."""
        with self._lock:
            count = max(self.config.min_workers, min(count, self.max_workers))

            if count != self.target_workers:
                old_count = self.target_workers
                self.target_workers = count
                self.last_scale_action_time = time.time()
                self.last_scale_action = "up" if count > old_count else "down"
                logger.info(f"Target workers adjusted: {old_count} -> {count}")
                return True
            return False

    def _monitor_loop(self, callback: Optional[Callable[[dict], None]]):
        """Real-time monitoring and adjustment loop"""
        while self._monitoring:
            try:
                # Update system information before decision making
                self.system_monitor.update_child_processes()

                metrics = self.get_metrics()

                # Real-time scaling decisions
                if self.config.aggressive_scaling:
                    should_up, up_reason = self.should_scale_up()
                    should_down, down_reason = self.should_scale_down()

                    if should_up:
                        new_target = min(self.target_workers + 1, self.max_workers)
                        self.set_target_worker_count(new_target)
                    elif should_down:
                        new_target = max(
                            self.target_workers - 1, self.config.min_workers
                        )
                        self.set_target_worker_count(new_target)

                # User callback (for the DynamicProcessPoolExecutor to act)
                if callback:
                    callback(metrics)

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}", exc_info=True)

            time.sleep(self.config.monitoring_interval)

    # --- Public API for Metrics and Control ---

    def update_queue_length(self, length: int):
        """Update the current task queue length"""
        with self._lock:
            self.task_queue_length = length

    def get_metrics(self) -> dict:
        """Get comprehensive metrics including system utilization"""
        with self._lock:
            resources = self._check_resource_constraints()
            should_up, up_reason = self.should_scale_up()
            should_down, down_reason = self.should_scale_down()

            return {
                "max_workers": self.max_workers,
                "target_workers": self.target_workers,
                "actual_workers": self.system_monitor.get_active_worker_count(),
                "task_queue_length": self.task_queue_length,
                "optimal_batch_size": self.calculate_optimal_batch_size(),
                "cpu_usage_cores": resources["cpu_usage"],
                "cpu_limit_cores": resources["cpu_limit"],
                "memory_usage_gb": resources["memory_usage"],
                "memory_limit_gb": resources["memory_limit"],
                "should_scale_up": should_up,
                "should_scale_down": should_down,
                "scale_up_reason": up_reason,
                "scale_down_reason": down_reason,
                "last_action": self.last_scale_action,
            }

    def start_monitoring(self, callback: Optional[Callable[[dict], None]] = None):
        """Start real-time monitoring and adjustment"""
        if self._monitoring:
            return
        self._monitoring = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, args=(callback,), daemon=True
        )
        self._monitor_thread.start()

    def stop_monitoring(self):
        """Stop the monitoring thread"""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=10)
