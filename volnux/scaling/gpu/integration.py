import os
import time
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)
from enum import Enum
from typing import Any, Callable, List, Optional

import torch


class ExecutorType(Enum):
    """Supported executor types"""

    PROCESS = "process"
    THREAD = "thread"


class GPUTaskExecutor:
    """
    Modular GPU task execution framework.
    Accepts tasks with their executor type and handles GPU allocation.

    The task_func provided must accept two arguments:
    1. data: A single item from the task_data list.
    2. gpu_id: The assigned GPU ID (0, 1, 2, etc.) or -1 for CPU.
    """

    def __init__(self, auto_detect_gpus: bool = True):
        """
        Initialize GPU Task Executor

        Args:
            auto_detect_gpus: Automatically detect available GPUs
        """
        self.gpu_available = torch.cuda.is_available()
        # Ensure num_gpus is only called if cuda is available
        self.num_gpus = (
            torch.cuda.device_count() if auto_detect_gpus and self.gpu_available else 0
        )

        print(f"GPU Executor initialized:")
        print(f"  - CUDA available: {self.gpu_available}")
        print(f"  - Number of GPUs: {self.num_gpus}")

        if self.gpu_available and self.num_gpus > 0:
            for i in range(self.num_gpus):
                print(f"  - GPU {i}: {torch.cuda.get_device_name(i)}")

    def execute_task(
        self,
        task_func: Callable,
        task_data: List[Any],
        executor_type: ExecutorType = ExecutorType.PROCESS,
        max_workers: Optional[int] = None,
        gpu_distribution: str = "round_robin",
        **executor_kwargs,
    ) -> List[Any]:
        """
        Execute tasks on GPU with specified executor

        Args:
            task_func: Function to execute (must accept data and gpu_id)
            task_data: List of data items to process
            executor_type: Type of executor (PROCESS or THREAD)
            max_workers: Number of workers (defaults based on GPU/CPU count)
            gpu_distribution: Strategy for GPU allocation ('round_robin', 'single', 'least_memory_allocated')
            **executor_kwargs: Additional arguments for executor

        Returns:
            List of results
        """
        # Determine executor class and default workers
        if executor_type == ExecutorType.PROCESS:
            ExecutorClass = ProcessPoolExecutor
            # Default to one process per GPU, or CPU core count if no GPUs
            default_workers = (
                self.num_gpus if self.num_gpus > 0 else (os.cpu_count() or 4)
            )
        else:
            ExecutorClass = ThreadPoolExecutor
            # Default to 2 threads per GPU, or 4 threads if no GPUs
            default_workers = self.num_gpus * 2 if self.num_gpus > 0 else 4

        workers = max_workers or default_workers

        print(
            f"\nExecuting {len(task_data)} tasks with {executor_type.value} executor ({workers} workers)"
        )

        # Wrap the task function to handle GPU allocation and context setting
        def gpu_wrapped_task(task_item):
            data, task_idx = task_item
            gpu_id = self._get_gpu_id(task_idx, gpu_distribution)

            # CRITICAL: Set the CUDA device context for the current worker
            if gpu_id != -1 and executor_type == ExecutorType.PROCESS:
                try:
                    # This ensures the process starts with the correct CUDA context.
                    torch.cuda.set_device(gpu_id)
                except Exception as e:
                    # This should rarely fail in a robust setup
                    print(
                        f"Warning: Could not set CUDA device {gpu_id} in worker. Error: {e}"
                    )

            # Execute the user's task function
            return task_func(data, gpu_id)

        # Execute tasks
        with ExecutorClass(max_workers=workers, **executor_kwargs) as executor:
            # Prepare tasks with indices
            indexed_tasks = [(data, idx) for idx, data in enumerate(task_data)]

            # Submit all tasks
            futures = [
                executor.submit(gpu_wrapped_task, task) for task in indexed_tasks
            ]

            # Collect results as they complete
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Task failed: {e}")
                    results.append(
                        None
                    )  # Append None or an error marker for failed tasks

            return results

    def execute_with_custom_executor(
        self,
        task_func: Callable,
        task_data: List[Any],
        executor_instance: Any,  # Expects an instance with a .submit() method
        gpu_distribution: str = "round_robin",
    ) -> List[Any]:
        """
        Execute tasks with a custom executor instance provided by the framework.
        Note: The custom executor must manage its own context if using ProcessPool.

        Args:
            task_func: Function to execute
            task_data: List of data items
            executor_instance: Pre-configured executor instance
            gpu_distribution: GPU allocation strategy

        Returns:
            List of results
        """
        print(f"\nExecuting {len(task_data)} tasks with custom executor")

        def gpu_wrapped_task_custom(task_item):
            data, task_idx = task_item
            gpu_id = self._get_gpu_id(task_idx, gpu_distribution)

            # NOTE: We skip setting the device context here, relying on the user's
            # custom executor setup/process creation to handle CUDA context if needed.
            # The task_func MUST use the gpu_id parameter to select the device.

            return task_func(data, gpu_id)

        # Prepare tasks
        indexed_tasks = [(data, idx) for idx, data in enumerate(task_data)]

        # Submit to provided executor
        futures = [
            executor_instance.submit(gpu_wrapped_task_custom, task)
            for task in indexed_tasks
        ]

        # Collect results
        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Task failed: {e}")
                results.append(None)

        return results

    # --- Internal Methods ---

    def _get_gpu_id(self, task_idx: int, strategy: str) -> int:
        """
        Determine GPU ID based on allocation strategy

        Args:
            task_idx: Task index
            strategy: Allocation strategy

        Returns:
            GPU ID to use, or -1 for CPU fallback
        """
        if not self.gpu_available or self.num_gpus == 0:
            return -1  # CPU fallback

        if strategy == "round_robin":
            # Distribute tasks evenly across all available GPUs
            return task_idx % self.num_gpus
        elif strategy == "single":
            # Force all tasks onto the first GPU
            return 0
        elif strategy == "least_memory_allocated":
            # Find the GPU with the least memory allocated by the current process.
            # Warning: This is a weak heuristic and unreliable in a ProcessPoolExecutor setup.
            return self._get_least_loaded_gpu()
        else:
            print(
                f"Warning: Unknown GPU distribution strategy '{strategy}'. Defaulting to GPU 0."
            )
            return 0

    def _get_least_loaded_gpu(self) -> int:
        """
        Find GPU with least memory allocated by the current process.
        Returns GPU ID, or 0 as a fallback.
        """
        if not self.gpu_available or self.num_gpus == 0:
            return 0  # Should be caught by the calling function, but return 0 just in case.

        min_memory = float("inf")
        best_gpu = 0

        for i in range(self.num_gpus):
            try:
                # The memory reported here is ONLY from the current process's PyTorch context
                memory_allocated = torch.cuda.memory_allocated(i)
                if memory_allocated < min_memory:
                    min_memory = memory_allocated
                    best_gpu = i
            except Exception as e:
                # Handle potential errors during memory check
                print(f"Error checking memory for GPU {i}: {e}")

        return best_gpu
