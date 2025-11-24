import logging
import os
import threading
from collections import deque

import psutil

logger = logging.getLogger(__name__)


class SystemMonitor:
    """Monitors actual system resource utilization in real-time"""

    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.cpu_core_history = deque(maxlen=10)  # Tracks cores consumed
        self.memory_gb_history = deque(maxlen=10)
        self.child_processes = []
        self._lock = threading.Lock()

    def update_child_processes(self):
        """Update list of child worker processes"""
        try:
            with self._lock:
                # Find all children recursively, typically including the PBE's worker processes
                self.child_processes = self.process.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            self.child_processes = []

    def get_total_cpu_usage(self) -> float:
        """
        Get total CPU usage of all workers, measured in absolute cores consumed.
        (e.g., 1.5 means 1.5 CPU cores are being used)
        """
        total_cpu_percent = 0.0

        with self._lock:
            # Sum the percentage usage of all worker children
            for child in self.child_processes:
                try:
                    if child.is_running():
                        # psutil.Process.cpu_percent returns a percentage since last call.
                        # On a multi-core machine, this can exceed 100 for a single process.
                        total_cpu_percent += child.cpu_percent(interval=0.01)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

        # Convert total percentage (where 100% = 1 core) to absolute cores consumed
        cores_consumed = total_cpu_percent / 100.0
        self.cpu_core_history.append(cores_consumed)
        return cores_consumed

    def get_average_cpu_usage(self) -> float:
        """Get average cores consumed over recent history"""
        if not self.cpu_core_history:
            return 0.0
        return sum(self.cpu_core_history) / len(self.cpu_core_history)

    def get_total_memory_usage(self) -> float:
        """Get total memory usage in GB"""
        # Sum memory usage of main process and all worker processes
        total_memory_bytes = self.process.memory_info().rss

        with self._lock:
            for child in self.child_processes:
                try:
                    if child.is_running():
                        total_memory_bytes += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

        total_memory_gb = total_memory_bytes / (1024**3)
        self.memory_gb_history.append(total_memory_gb)
        return total_memory_gb

    def get_active_worker_count(self) -> int:
        """Get count of active worker processes"""
        with self._lock:
            # Active workers are simply the number of child processes created by the PBE
            return len([p for p in self.child_processes if p.is_running()])
