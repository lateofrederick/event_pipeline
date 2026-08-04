"""Bridge the engine's in-process lifecycle signals to governance events.

The engine already fires a rich set of ``volnux.signal.signals`` at every
lifecycle boundary: a pipeline run starting and ending, each task starting,
finishing and retrying, and a task suspending for human input. This module
subscribes to those signals, translates each into a governance event's fields,
and hands them to a ``GovernanceEventPublisher`` that delivers them off the hot
path over whatever messaging backend the deployment has provisioned.

Three properties are load-bearing and deliberate:

* **Opt-in.** Nothing here runs unless ``install_governance_reporter`` (or
  ``SignalGovernanceReporter.install``) is called. If it is never installed, the
  signals simply have no listener and the engine behaves exactly as it does
  standalone.

* **Non-blocking and exception-isolated.** Translation is cheap and pure; the
  actual send is deferred to the publisher's background thread, so a slow or
  failing transport never disturbs a running workflow. Failures are logged and
  swallowed; reporting is observational.

* **Retained for the process lifetime.** The signal system holds listeners by
  *weak reference*. On ``install`` the reporter registers itself in a module-level
  strong-reference registry, so it survives for the process lifetime even if the
  caller does not keep the returned instance; ``uninstall`` releases it.

Correlation
-----------
Every event is correlated by ``execution_id = pipeline.id``. One ``Pipeline``
instance corresponds to one run, and ``Pipeline`` exposes ``change_object_id()``,
so when the platform backend dispatches a run it can stamp the governance
``Execution`` id onto the pipeline, and every event this reporter emits then
carries that exact id. Run standalone, the id is simply the engine's own
pipeline id.
"""

import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from .events import EventType
from .publisher import GovernanceEventPublisher
from .sampling import EventSampler

logger = logging.getLogger(__name__)


# Strong references to every installed reporter. ``SoftSignal`` holds its
# listeners by *weak* reference, so without a strong reference somewhere an
# installed reporter (and its bound-method handlers) can be garbage-collected and
# silently stop reporting. Keeping it here guarantees an installed reporter lives
# for the process lifetime regardless of whether the caller retains the instance.
_installed_reporters: "List[SignalGovernanceReporter]" = []


# ---------------------------------------------------------------------------
# Field extraction
#
# Signals hand us live engine objects (a Pipeline, an ExecutionContext, a task
# event). These helpers pull correlation fields off them defensively: the
# reporter must never raise into the engine, so a missing or renamed attribute
# degrades to ``None`` rather than an AttributeError.
# ---------------------------------------------------------------------------


def _safe_str(value: Any) -> Optional[str]:
    """Stringify a value, mapping ``None`` through unchanged."""
    return None if value is None else str(value)


def _pipeline_of(source: Any) -> Any:
    """Return the pipeline for a Pipeline-or-ExecutionContext source.

    ``pipeline_execution_start`` hands us the pipeline directly; the task and
    end signals hand us an ``ExecutionContext`` whose ``.pipeline`` is the run.
    """
    if source is None:
        return None
    pipeline = getattr(source, "pipeline", None)
    return pipeline if pipeline is not None else source


def _execution_id(source: Any) -> Optional[str]:
    """The run correlation id — the pipeline's (settable) object id.

    This single function encodes the correlation-key decision for the whole
    bridge: everything downstream keys off whatever this returns, so a change of
    correlation strategy is localised here.
    """
    # TODO(correlation): revisit the correlation-key choice. Using pipeline.id
    # means "one Pipeline instance == one run", relying on the backend stamping
    # the governance Execution id via Pipeline.change_object_id() at dispatch.
    return _safe_str(getattr(_pipeline_of(source), "id", None))


def _workflow_id(context: Any) -> Optional[str]:
    return _safe_str(getattr(context, "workflow_id", None))


def _workflow_name(context: Any) -> Optional[str]:
    return _safe_str(getattr(context, "workflow_name", None))


def _task_id(event: Any) -> Optional[str]:
    """Identify a task/event within a run — prefer its name, fall back to id."""
    name = getattr(event, "name", None)
    if name:
        return _safe_str(name)
    return _safe_str(getattr(event, "id", None))


# ---------------------------------------------------------------------------
# Translators: signal payload -> governance event fields (a plain dict)
#
# One pure function per wired signal. They build no model and touch no backend,
# so they are trivially unit-testable; the status of a terminal execution event
# is taken from *which signal fired* rather than read from the async state
# manager, which keeps these synchronous and unambiguous.
# ---------------------------------------------------------------------------


def _fields(
    event_type: str,
    *,
    execution_id: Optional[str] = None,
    task_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
    workflow_name: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "execution_id": execution_id,
        "task_id": task_id,
        "workflow_id": workflow_id,
        "workflow_name": workflow_name,
        "payload": payload or {},
    }


def translate_execution_start(pipeline: Any = None, **_: Any) -> Dict[str, Any]:
    return _fields(
        EventType.EXECUTION_STARTED,
        execution_id=_execution_id(pipeline),
        workflow_id=_workflow_id(pipeline),
        workflow_name=_workflow_name(pipeline),
    )


def translate_execution_end(execution_context: Any = None, **_: Any) -> Dict[str, Any]:
    return _fields(
        EventType.EXECUTION_COMPLETED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
    )


def translate_execution_stopped(
    execution_context: Any = None, **_: Any
) -> Dict[str, Any]:
    # pipeline_stop fires when the run ended in the CANCELLED state.
    return _fields(
        EventType.EXECUTION_STOPPED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        payload={"reason": "cancelled"},
    )


def translate_execution_aborted(
    execution_context: Any = None, **_: Any
) -> Dict[str, Any]:
    # pipeline_shutdown fires when the run ended in the ABORTED state; the
    # platform records it as a failed execution and keeps the reason.
    return _fields(
        EventType.EXECUTION_FAILED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        payload={"reason": "aborted"},
    )


def translate_execution_failed(
    execution_context: Any = None, state: Any = None, **_: Any
) -> Dict[str, Any]:
    # event_execution_failed is emitted by ExecutionContext.failed() at the
    # execution level. It fires mid-run, before the unconditional
    # pipeline_execution_end (which maps to COMPLETED), so the backend
    # projector's terminal-state freeze keeps the run FAILED.
    return _fields(
        EventType.EXECUTION_FAILED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        payload={"state": _safe_str(state)},
    )


def translate_execution_paused(
    execution_context: Any = None, state: Any = None, **_: Any
) -> Dict[str, Any]:
    return _fields(
        EventType.EXECUTION_PAUSED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
    )


def translate_execution_resumed(
    execution_context: Any = None, state: Any = None, **_: Any
) -> Dict[str, Any]:
    return _fields(
        EventType.EXECUTION_RESUMED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
    )


def translate_task_started(
    event: Any = None, execution_context: Any = None, **_: Any
) -> Dict[str, Any]:
    return _fields(
        EventType.TASK_STARTED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        task_id=_task_id(event),
    )


def translate_task_completed(
    event: Any = None, execution_context: Any = None, **_: Any
) -> Dict[str, Any]:
    return _fields(
        EventType.TASK_COMPLETED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        task_id=_task_id(event),
    )


def translate_task_retried(
    event: Any = None,
    execution_context: Any = None,
    task_id: Any = None,
    retry_count: Any = None,
    max_attempts: Any = None,
    backoff: Any = None,
    **_: Any,
) -> Dict[str, Any]:
    resolved_task_id = _safe_str(task_id) or _task_id(event)
    return _fields(
        EventType.TASK_RETRIED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        task_id=resolved_task_id,
        payload={
            "retry_count": retry_count,
            "max_attempts": max_attempts,
            "backoff": backoff,
        },
    )


def translate_hitl_requested(
    execution_context: Any = None, request: Any = None, **_: Any
) -> Dict[str, Any]:
    # The suspension request carries the prompt/options and its own request_id
    # (the key the engine resumes on); those travel in the payload.
    return _fields(
        EventType.HITL_REQUESTED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        task_id=_safe_str(getattr(request, "task_id", None)),
        payload={
            "request_id": _safe_str(getattr(request, "request_id", None)),
            "title": _safe_str(getattr(request, "title", None)),
            "description": _safe_str(getattr(request, "description", None)),
            "options": list(getattr(request, "options", None) or []),
            "timeout_hours": getattr(request, "timeout_hours", None),
        },
    )


class SignalGovernanceReporter:
    """Connect lifecycle signals to a ``GovernanceEventPublisher``.

    Usage::

        reporter = SignalGovernanceReporter(GovernanceEventPublisher())
        reporter.install()
        # ... keep ``reporter`` referenced for the life of the process ...
        reporter.uninstall()  # optional, on shutdown

    Prefer ``install_governance_reporter``, which constructs, installs and
    returns the reporter in one call.
    """

    def __init__(
        self,
        publisher: GovernanceEventPublisher,
        sampler: Optional[EventSampler] = None,
    ) -> None:
        self._publisher = publisher
        self._sampler = sampler
        self._registrations: List[Tuple[Any, Any]] = []
        self._installed = False

    @property
    def installed(self) -> bool:
        return self._installed

    def install(self) -> None:
        """Start the publisher and subscribe to the lifecycle signals. Idempotent."""
        if self._installed:
            return

        from volnux.signal.signals import GenericSender

        self._publisher.start()
        for signal, handler in self._build_registrations():
            # GenericSender connects to *every* sender of the signal, which is
            # what we want: report all runs regardless of which class emitted.
            # We use GenericSender explicitly (rather than the None alias
            # connect() accepts) so uninstall's disconnect matches: disconnect()
            # does not apply the same None -> GenericSender mapping that connect
            # does, so a None-connected listener cannot be disconnected.
            signal.connect(GenericSender, handler)
            self._registrations.append((signal, handler))

        # Hold a strong reference so weakly-held signal listeners cannot be GC'd.
        if self not in _installed_reporters:
            _installed_reporters.append(self)
        self._installed = True

    def uninstall(self) -> None:
        """Disconnect from all signals and stop the publisher. Idempotent."""
        from volnux.signal.signals import GenericSender

        for signal, handler in self._registrations:
            signal.disconnect(GenericSender, handler)
        self._registrations.clear()
        if self in _installed_reporters:
            _installed_reporters.remove(self)
        self._publisher.stop()
        self._installed = False

    def _build_registrations(self) -> List[Tuple[Any, Any]]:
        """Pair each wired signal with the handler that translates it.

        Wired signals and their mappings:

        * pipeline run: start / end (completed) / stop (cancelled) /
          shutdown (aborted);
        * execution state transitions: failed / paused / resumed (emitted by
          ``ExecutionContext`` at the execution level, with the target state in
          the payload). Wiring ``failed`` also corrects the run status: a failed
          run still fires the unconditional ``pipeline_execution_end``
          (COMPLETED), but ``failed`` fires first and the projector freezes on
          the terminal FAILED;
        * task: start / end (completed) / retry;
        * HITL: a task suspending for human input.

        ``event_execution_cancelled``/``aborted`` are deliberately *not* wired:
        ``pipeline_stop``/``pipeline_shutdown`` already report those run endings,
        so the event-level twins would only duplicate them.
        """
        from volnux.signal import signals as sig

        return [
            (sig.pipeline_execution_start, self._on_execution_start),
            (sig.pipeline_execution_end, self._on_execution_end),
            (sig.pipeline_stop, self._on_execution_stopped),
            (sig.pipeline_shutdown, self._on_execution_aborted),
            (sig.event_execution_failed, self._on_execution_failed),
            (sig.event_execution_paused, self._on_execution_paused),
            (sig.event_execution_resumed, self._on_execution_resumed),
            (sig.event_execution_start, self._on_task_started),
            (sig.event_execution_end, self._on_task_completed),
            (sig.event_execution_retry, self._on_task_retried),
            (sig.hitl_requested, self._on_hitl_requested),
        ]

    # -- Signal handlers: translate, then hand off (isolated) ---------------

    def _on_execution_start(self, **kwargs: Any) -> None:
        self._emit(translate_execution_start(**kwargs))

    def _on_execution_end(self, **kwargs: Any) -> None:
        self._emit(translate_execution_end(**kwargs))

    def _on_execution_stopped(self, **kwargs: Any) -> None:
        self._emit(translate_execution_stopped(**kwargs))

    def _on_execution_aborted(self, **kwargs: Any) -> None:
        self._emit(translate_execution_aborted(**kwargs))

    def _on_execution_failed(self, **kwargs: Any) -> None:
        self._emit(translate_execution_failed(**kwargs))

    def _on_execution_paused(self, **kwargs: Any) -> None:
        self._emit(translate_execution_paused(**kwargs))

    def _on_execution_resumed(self, **kwargs: Any) -> None:
        self._emit(translate_execution_resumed(**kwargs))

    def _on_task_started(self, **kwargs: Any) -> None:
        self._emit(translate_task_started(**kwargs))

    def _on_task_completed(self, **kwargs: Any) -> None:
        self._emit(translate_task_completed(**kwargs))

    def _on_task_retried(self, **kwargs: Any) -> None:
        self._emit(translate_task_retried(**kwargs))

    def _on_hitl_requested(self, **kwargs: Any) -> None:
        self._emit(translate_hitl_requested(**kwargs))

    def _emit(self, fields: Optional[Dict[str, Any]]) -> None:
        """Stamp identity/time, sample, and hand off to the publisher (non-blocking).

        With a sampler, critical events pass straight through while telemetry is
        held back for the publisher's periodic reservoir flush; without one, every
        event goes straight to the publisher.
        """
        if fields is None:
            return
        fields.setdefault("event_id", str(uuid.uuid4()))
        fields.setdefault("occurred_at", time.time())
        try:
            if self._sampler is None:
                self._publisher.submit(fields)
            else:
                for event in self._sampler.offer(fields):
                    self._publisher.submit(event)
        except Exception:  # noqa: BLE001 - reporting must never raise into the engine
            logger.exception(
                "Failed to submit governance event %s", fields.get("event_type")
            )


def install_governance_reporter(
    publisher: Optional[GovernanceEventPublisher] = None,
    sampler: Optional[EventSampler] = None,
    *,
    enable_sampling: bool = True,
    flush_interval: float = 1.0,
) -> SignalGovernanceReporter:
    """Construct, install and return a reporter.

    The installed reporter is kept alive by a module-level registry, so the
    caller need not retain the returned instance (it is returned for convenience
    and so ``uninstall`` can be called).

    By default telemetry is reservoir-sampled: a default ``EventSampler`` is
    created and a default ``GovernanceEventPublisher`` is wired to flush it every
    ``flush_interval`` seconds. Pass ``enable_sampling=False`` to send every
    event. If you pass your own ``publisher`` together with a sampler, wire its
    ``flush_source`` to ``sampler.drain`` yourself; only a publisher created here
    is wired automatically.
    """
    if not enable_sampling:
        sampler = None
    elif sampler is None:
        sampler = EventSampler()

    if publisher is None:
        publisher = GovernanceEventPublisher(
            flush_source=(sampler.drain if sampler is not None else None),
            flush_interval=flush_interval,
        )

    reporter = SignalGovernanceReporter(publisher, sampler=sampler)
    reporter.install()
    return reporter
