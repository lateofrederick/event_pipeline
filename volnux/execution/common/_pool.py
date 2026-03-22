import logging
import multiprocessing
import threading
import warnings
from collections import defaultdict
from concurrent.futures import Future, ProcessPoolExecutor, wait, FIRST_EXCEPTION
from typing import TYPE_CHECKING, Any, Callable, Dict, Literal, Optional, Set, Union, Tuple, List

try:
    from celery import Celery
    from volnux.executors.celery import CeleryExecutor
except ImportError:
    warnings.warn(
        "Celery is not installed. Celery-based execution will not be available.",
        ImportWarning,
    )
    Celery = None

if TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext

logger = logging.getLogger(__name__)

BackendType = Literal["local", "celery"]


class _ContextFutureRegistry:
    """
    Thread-safe registry mapping context_id → set[Future].

    All public methods acquire the internal lock, so callers never need to
    hold an external lock while working with the registry.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Only created on first add; deleted when the set empties.
        self._store: Dict[str, Set[Future]] = {}

    def add(self, ctx_id: str, future: Future) -> None:
        with self._lock:
            if ctx_id not in self._store:
                self._store[ctx_id] = set()
            self._store[ctx_id].add(future)

    def discard(self, ctx_id: str, future: Future) -> None:
        with self._lock:
            bucket = self._store.get(ctx_id)
            if bucket is None:
                return
            bucket.discard(future)
            # Eagerly remove empty buckets to avoid memory accumulation.
            if not bucket:
                del self._store[ctx_id]

    def snapshot(self, ctx_id: str) -> Set[Future]:
        """Return a shallow copy of the futures for *ctx_id*."""
        with self._lock:
            return set(self._store.get(ctx_id, ()))

    def pop_all(self, ctx_id: str) -> Set[Future]:
        """Atomically remove and return all futures for *ctx_id*."""
        with self._lock:
            return self._store.pop(ctx_id, set())

    def active_count(self, ctx_id: str) -> int:
        with self._lock:
            return len(self._store.get(ctx_id, ()))

    def all_context_ids(self) -> List[str]:
        with self._lock:
            return list(self._store.keys())


class VolnuxPoolManager:
    """
    Singleton pool manager for the Volnux workflow execution engine.

    Lifecycle
    ---------
    One manager instance is created when the workflow starts and lives until
    the workflow ends.
    Call `initialize` once at startup and `shutdown` once at teardown.
    Between those two points, any component that holds a reference to
    the manager can submit tasks via `submit_task`.

    Backends
    --------
    * ``"local"`` —: class:`~concurrent.futures.ProcessPoolExecutor`
      (spawn context, suitable for CPU-bound steps).
    * ``"celery"`` —: class:`~volnux.executors.CeleryExecutor`
      (distributed workers, suitable for I/O-bound or long-running steps).

    Context tracking
    ----------------
    Every submitted future is tagged with a *context id* (the
    ``ExecutionContext.state_id``).  This allows the engine to:

    * **Drain** a context — wait for all its in-flight tasks to finish before
      moving to the next workflow state.
    * **Cancel** a context — revoke all pending futures when a branch is
      preempted or a timeout fires.
    """

    _instance: Optional["VolnuxPoolManager"] = None
    _singleton_lock = threading.Lock()  # guards singleton creation only

    def __new__(cls) -> "VolnuxPoolManager":
        with cls._singleton_lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                # All mutable state lives here; __init__ is never called.
                inst._init_state()
                cls._instance = inst
        return cls._instance

    def _init_state(self) -> None:
        """Set every field to its clean, un-initialized default."""
        self._executor: Optional[Union[ProcessPoolExecutor, CeleryExecutor]] = None
        self._backend_type: Optional[BackendType] = None
        self._initialized = False
        self._shut_down = False
        # Per-instance lock for state mutations *after* singleton creation.
        self._state_lock = threading.Lock()
        self._registry = _ContextFutureRegistry()

    def initialize(
        self,
        backend: BackendType = "local",
        *,
        max_workers: Optional[int] = None,
        celery_app: Optional[Celery] = None,
        queue: Optional[str] = None,
        task_soft_time_limit: Optional[int] = None,
        task_time_limit: Optional[int] = None,
    ) -> None:
        """
        Configure and start the executor.

        Must be called exactly once per workflow run before any task is
        submitted.  A second call with the **same** configuration is a
        no-op (idempotent restart-safety).  A second call with a
        **different** backend raises :class:`RuntimeError` to surface the
        misconfiguration immediately.

        Params:
        backend:
            ``"local"`` or ``"celery"``.
        max_workers:
            Worker count for the local backend (defaults to CPU count).
            Must be a positive integer if supplied.
        celery_app:
            Required for the ``"celery"`` backend.
        queue:
            Celery queue name; ``None`` uses the app default.
        task_soft_time_limit / task_time_limit:
            Forwarded to: class:`CeleryExecutor` when using the Celery backend.
        """
        backend = backend.lower()  # type: ignore[assignment]

        with self._state_lock:
            if self._shut_down:
                raise RuntimeError(
                    "VolnuxPoolManager has been shut down. "
                    "Call reset() before re-initializing."
                )

            if self._initialized:
                if self._backend_type == backend:
                    # Identical backend — safe to ignore.
                    return
                raise RuntimeError(
                    f"VolnuxPoolManager is already initialized with backend "
                    f"{self._backend_type!r}. "
                    f"Call reset() before switching to {backend!r}."
                )

            if backend == "local":
                if max_workers is not None and max_workers < 1:
                    raise ValueError(
                        f"max_workers must be a positive integer, got {max_workers}."
                    )
                num_workers = max_workers or multiprocessing.cpu_count()
                ctx = multiprocessing.get_context("spawn")
                self._executor = ProcessPoolExecutor(
                    max_workers=num_workers,
                    mp_context=ctx,
                )
                logger.info(
                    "VolnuxPoolManager (local) started — %d workers.",
                    num_workers,
                )

            elif backend == "celery":
                if celery_app is None:
                    raise ValueError(
                        "celery_app must be provided when backend='celery'."
                    )
                self._executor = CeleryExecutor(
                    celery_app=celery_app,
                    queue=queue,
                    task_soft_time_limit=task_soft_time_limit,
                    task_time_limit=task_time_limit,
                )
                logger.info(
                    "Volnux VolnuxPoolManager (celery) started — queue=%r.",
                    queue or "default",
                )

            else:
                raise ValueError(
                    f"Unknown backend {backend!r}. Choose 'local' or 'celery'."
                )

            self._backend_type = backend  # type: ignore[assignment]
            self._initialized = True

    def submit_task(
        self,
        context: "ExecutionContext",
        task_func: Callable,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future:
        """
        Submit *task_func* to the configured backend and track it under the
        context's state id.

        Parameters
        ----------
        context:
            The :class:`ExecutionContext` that owns this task.  Its
            ``state_id`` is used as the tracking key.
        task_func:
            Any clickable callable (local backend) or clickable function
            (Celery backend).
        *args / **kwargs:
            Forwarded verbatim to *task_func*.

        Returns
        -------
        Future
            A :class:`~concurrent.futures.Future` (or
            :class:`~volnux.executors.CeleryFuture`) that resolves to the
            return value of *task_func*.

        Raises
        ------
        RuntimeError
            If the manager has not been initialized or has been shut down.
        TypeError
            If *task_func* is not callable.
        """
        self._assert_ready()

        if not callable(task_func):
            raise TypeError(f"task_func must be callable, got {type(task_func)!r}.")

        ctx_id: str = context.state_id
        future = self._executor.submit(task_func, *args, **kwargs)  # type: ignore[union-attr]
        self._registry.add(ctx_id, future)

        # Remove from registry once settled — no external lock needed because
        # _ContextFutureRegistry.discard is internally synchronized.
        future.add_done_callback(lambda f: self._registry.discard(ctx_id, f))

        logger.debug("Task submitted — context=%r future=%r", ctx_id, future)
        return future

    def drain_context(
        self,
        context: "ExecutionContext",
        *,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Block until every in-flight task belonging to *context* has finished.

        This is the mechanism the engine uses to synchronize at workflow
        state boundaries — the engine calls ``drain_context`` before
        transitioning to the next state so no orphaned tasks bleed over.

        Params:
        timeout:
            Maximum seconds to wait.  ``None`` waits forever.

        Raises
        concurrent.futures.TimeoutError
            If *timeout* elapses before all futures settle.
        """
        ctx_id = context.state_id
        futures = self._registry.snapshot(ctx_id)
        if not futures:
            return

        logger.debug(
            "Draining context=%r — waiting on %d task(s).", ctx_id, len(futures)
        )
        done, not_done = wait(futures, timeout=timeout)

        if not_done:
            raise TimeoutError(
                f"drain_context timed out for context {ctx_id!r}: "
                f"{len(not_done)} task(s) still running."
            )

        logger.debug("Context %r drained successfully.", ctx_id)

    def cancel_context(
        self,
        context: "ExecutionContext",
        *,
        terminate: bool = False,
    ) -> Tuple[int, int]:
        """
        Attempt to cancel every pending task belonging to *context*.

        Used for branch preemption: when one branch of the workflow wins a
        race, the engine cancels the losing branch's outstanding tasks.

        Params:
            context:
                The `ExecutionContext` whose tasks should be cancelled.
            terminate:
                If ``True``, forcibly terminate already-running tasks (Celery
                backend only; has no effect on local ProcessPoolExecutor since
                running processes cannot be interrupted mid-flight via the
                standard Future API).

        Return:
            (cancelled, skipped): tuple[int, int]
                *cancelled* — number of futures successfully cancelled.
                *skipped*   — number of futures that were already running or done
                              and therefore could not be cancelled.
        """
        ctx_id = context.state_id
        # pop_all atomically removes the bucket so no new callbacks can re-add.
        futures = self._registry.pop_all(ctx_id)

        cancelled = skipped = 0
        for future in futures:
            # For CeleryFuture, cancel() delegates to control.revoke().
            # For ProcessPoolExecutor futures, cancel() works only if the task
            # hasn't started yet.
            if terminate and isinstance(self._executor, CeleryExecutor):
                task_id = getattr(future, "task_id", None)
                if task_id:
                    self._executor.revoke(task_id, terminate=True)
            if future.cancel():
                cancelled += 1
            else:
                skipped += 1

        logger.info(
            "cancel_context %r — cancelled=%d skipped=%d",
            ctx_id,
            cancelled,
            skipped,
        )
        return cancelled, skipped

    def wait_any(
        self,
        context: "ExecutionContext",
        *,
        timeout: Optional[float] = None,
    ) -> tuple[set[Future], set[Future]]:
        """
        Wait until *at least one* task in *context* finishes (or fails).

        Useful for fan-out patterns where the engine wants to react to the
        first completed task without waiting for the whole batch.

        Returns:
            (done, not_done): tuple[set[Future], set[Future]]
        """
        futures = self._registry.snapshot(context.state_id)
        if not futures:
            return set(), set()
        return wait(futures, timeout=timeout, return_when=FIRST_EXCEPTION)

    @property
    def is_initialized(self) -> bool:
        return self._initialized and not self._shut_down

    @property
    def backend_type(self) -> Optional[BackendType]:
        return self._backend_type

    def active_count(self, context: "ExecutionContext") -> int:
        """Return the number of unfinished tasks for *context*."""
        return self._registry.active_count(context.state_id)

    def active_context_ids(self) -> List[str]:
        """Return all context ids that still have live futures."""
        return self._registry.all_context_ids()

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        """
        Tear down the executor.

        Should be called once when the workflow finishes (or fails).
        After this call, the manager refuses new submissions.
        Call`reset`to bring it back to a pristine state (e.g. between integration tests
        or when restarting a workflow in the same process).

        Params:
            wait:
                Block until all submitted tasks have finished.
            cancel_futures:
                Attempt to cancel all pending tasks before waiting.
        """
        with self._state_lock:
            if self._shut_down:
                return  # idempotent
            self._shut_down = True

        if self._executor is not None:
            logger.info(
                "Volnux VolnuxPoolManager (%s) shutting down "
                "(wait=%s, cancel_futures=%s).",
                self._backend_type,
                wait,
                cancel_futures,
            )
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
            self._executor = None

        logger.info("Volnux VolnuxPoolManager shut down.")

    @classmethod
    def reset(cls) -> None:
        """
        Destroy the singleton and return to a pre-initialized state.

        Intended for testing and workflow-restart scenarios.  If the
        executor is still running, `shutdown` is called first.

        warning:
            Not safe to call while tasks are actively running in production.
        """
        with cls._singleton_lock:
            if cls._instance is not None:
                inst = cls._instance
                if inst._initialized and not inst._shut_down:
                    try:
                        inst.shutdown(wait=False, cancel_futures=True)
                    except Exception:
                        logger.exception("Error during implicit shutdown in reset().")
                cls._instance = None
        logger.debug("VolnuxPoolManager reset — singleton cleared.")

    def __enter__(self) -> "VolnuxPoolManager":
        return self

    def __exit__(self, *_: Any) -> None:
        self.shutdown(wait=True)

    def _assert_ready(self) -> None:
        if self._shut_down:
            raise RuntimeError(
                "VolnuxPoolManager has been shut down. "
                "Call reset() then initialize() to reuse."
            )
        if not self._initialized or self._executor is None:
            raise RuntimeError(
                "VolnuxPoolManager must be initialized before submitting tasks. "
                "Call initialize() first."
            )
