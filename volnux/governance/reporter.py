"""Bridge the engine's in-process lifecycle signals to the governance stream.

The engine already fires a rich set of ``volnux.signal.signals`` at every
lifecycle boundary, a pipeline run starting and ending, each task starting,
finishing and retrying. This module subscribes to those signals and translates
each one into a ``GovernanceEvent`` published on the Redis stream for the
platform backend to project.

Three properties are load-bearing and deliberate:

* **Opt-in.** Nothing here runs unless ``install_governance_reporter`` (or
  ``SignalGovernanceReporter.install``) is called. If it is never installed,
  the signals simply have no listener and the engine behaves exactly as it does
  standalone. This keeps the reporter out of the engine's default path.

* **Exception-isolated.** A publish failure is logged and swallowed. Reporting
  is observational; a Redis hiccup or a malformed event must never disturb a
  running workflow. (The signal dispatcher already swallows listener
  exceptions, but we isolate here too so the intent is explicit and the log
  message is useful.)

* **Retained for the process lifetime.** The signal system holds listeners by
  *weak reference*, so the reporter must be kept alive by the caller, if it is
  garbage-collected, its handlers silently disconnect. ``install_governance_reporter``
  returns the instance precisely so the caller can hold onto it.

Correlation
-----------
Every event is correlated by ``execution_id = pipeline.id``. One ``Pipeline``
instance corresponds to one run, and ``Pipeline`` exposes ``change_object_id()``,
so when the platform backend dispatches a run it can stamp the governance
``Execution`` id onto the pipeline, and every event this reporter emits then
carries that exact id with no translation needed. Run standalone (no stamping),
the id is simply the engine's own pipeline id.
"""

import logging
from typing import Any, List, Optional, Tuple

from .events import EventType, GovernanceEvent
from .stream import GovernanceEventStream

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Field extraction
#
# Signals hand us live engine objects (a Pipeline, an ExecutionContext, a task
# event). These helpers pull the correlation fields off them defensively: the
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
    bridge (see the module "Correlation" section). Everything downstream — the
    backend projector, the stamping done at dispatch — keys off whatever this
    returns, so a change of correlation strategy is localised here.
    """
    # TODO(correlation): revisit the correlation-key choice. Using pipeline.id
    # means "one Pipeline instance == one run", relying on the backend stamping
    # the governance Execution id via Pipeline.change_object_id() at dispatch.
    # Accepted provisionally; confirm this is the identity we want before it is
    # depended on across the backend consumer and the dispatch path.
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
# Translators: signal payload -> GovernanceEvent
#
# One pure function per wired signal. Kept free of any Redis/stream concern so
# they can be unit-tested with plain stand-in objects. The status of a terminal
# execution event is taken from *which signal fired* rather than read from the
# async state manager, which keeps these synchronous and unambiguous.
# ---------------------------------------------------------------------------


def translate_execution_start(pipeline: Any = None, **_: Any) -> GovernanceEvent:
    return GovernanceEvent(
        event_type=EventType.EXECUTION_STARTED,
        execution_id=_execution_id(pipeline),
        workflow_id=_workflow_id(pipeline),
        workflow_name=_workflow_name(pipeline),
    )


def translate_execution_end(execution_context: Any = None, **_: Any) -> GovernanceEvent:
    return GovernanceEvent(
        event_type=EventType.EXECUTION_COMPLETED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
    )


def translate_execution_stopped(
    execution_context: Any = None, **_: Any
) -> GovernanceEvent:
    # pipeline_stop fires when the run ended in the CANCELLED state.
    return GovernanceEvent(
        event_type=EventType.EXECUTION_STOPPED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        payload={"reason": "cancelled"},
    )


def translate_execution_aborted(
    execution_context: Any = None, **_: Any
) -> GovernanceEvent:
    # pipeline_shutdown fires when the run ended in the ABORTED state. Aborted is
    # a hard, non-graceful stop; the platform records it as a failed execution
    # and keeps the specific reason in the payload.
    return GovernanceEvent(
        event_type=EventType.EXECUTION_FAILED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        payload={"reason": "aborted"},
    )


def translate_task_started(
    event: Any = None, execution_context: Any = None, **_: Any
) -> GovernanceEvent:
    return GovernanceEvent(
        event_type=EventType.TASK_STARTED,
        execution_id=_execution_id(execution_context),
        workflow_id=_workflow_id(execution_context),
        workflow_name=_workflow_name(execution_context),
        task_id=_task_id(event),
    )


def translate_task_completed(
    event: Any = None, execution_context: Any = None, **_: Any
) -> GovernanceEvent:
    return GovernanceEvent(
        event_type=EventType.TASK_COMPLETED,
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
) -> GovernanceEvent:
    # The retry signal carries an explicit task_id; fall back to the event's
    # own identity if it is absent.
    resolved_task_id = _safe_str(task_id) or _task_id(event)
    return GovernanceEvent(
        event_type=EventType.TASK_RETRIED,
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


class SignalGovernanceReporter:
    """Connect lifecycle signals to a ``GovernanceEventStream``.

    Args:
        stream: The stream to publish translated events to.

    Usage::

        reporter = SignalGovernanceReporter(stream)
        reporter.install()
        # ... keep ``reporter`` referenced for the life of the process ...
        reporter.uninstall()  # optional, on shutdown

    Prefer ``install_governance_reporter``, which constructs, installs and
    returns the reporter in one call.
    """

    def __init__(self, stream: GovernanceEventStream) -> None:
        self._stream = stream
        # Bound-method handlers are created once and retained here so the same
        # objects are used for connect and disconnect.
        self._registrations: List[Tuple[Any, Any]] = []
        self._installed = False

    @property
    def installed(self) -> bool:
        return self._installed

    def install(self) -> None:
        """Subscribe to the lifecycle signals. Idempotent.

        The signals are imported lazily here rather than at module load so that
        importing ``volnux.governance`` does not drag in the engine's signal
        module (and its transitive imports) unless a reporter is actually used.
        """
        if self._installed:
            return

        for signal, handler in self._build_registrations():
            # sender=None connects to *every* sender of the signal, which is
            # what we want: report all runs regardless of which class emitted.
            signal.connect(None, handler)
            self._registrations.append((signal, handler))

        self._installed = True

    def uninstall(self) -> None:
        """Disconnect from all signals. Idempotent."""
        for signal, handler in self._registrations:
            signal.disconnect(None, handler)
        self._registrations.clear()
        self._installed = False

    def _build_registrations(self) -> List[Tuple[Any, Any]]:
        """Pair each wired signal with the handler that translates it.

        Only signals with an unambiguous payload are wired here:

        * pipeline run: start / end (completed) / stop (cancelled) /
          shutdown (aborted);
        * task: start / end (completed) / retry.

        The ``event_execution_failed``/``paused``/``resumed``/``cancelled``/
        ``aborted`` signals carry ``task_profiles`` + a ``state`` object whose
        semantics need confirming against their emission sites before they can
        be mapped correctly, so they are intentionally left unwired for now.
        """
        from volnux.signal import signals as sig

        return [
            (sig.pipeline_execution_start, self._on_execution_start),
            (sig.pipeline_execution_end, self._on_execution_end),
            (sig.pipeline_stop, self._on_execution_stopped),
            (sig.pipeline_shutdown, self._on_execution_aborted),
            (sig.event_execution_start, self._on_task_started),
            (sig.event_execution_end, self._on_task_completed),
            (sig.event_execution_retry, self._on_task_retried),
        ]

    # -- Signal handlers: translate, then publish (isolated) ----------------

    def _on_execution_start(self, **kwargs: Any) -> None:
        self._emit(translate_execution_start(**kwargs))

    def _on_execution_end(self, **kwargs: Any) -> None:
        self._emit(translate_execution_end(**kwargs))

    def _on_execution_stopped(self, **kwargs: Any) -> None:
        self._emit(translate_execution_stopped(**kwargs))

    def _on_execution_aborted(self, **kwargs: Any) -> None:
        self._emit(translate_execution_aborted(**kwargs))

    def _on_task_started(self, **kwargs: Any) -> None:
        self._emit(translate_task_started(**kwargs))

    def _on_task_completed(self, **kwargs: Any) -> None:
        self._emit(translate_task_completed(**kwargs))

    def _on_task_retried(self, **kwargs: Any) -> None:
        self._emit(translate_task_retried(**kwargs))

    def _emit(self, event: Optional[GovernanceEvent]) -> None:
        """Publish an event, isolating any failure from the engine.

        A translator may return ``None`` to say "nothing to report"; that is a
        no-op. Any error from the stream is logged and swallowed so reporting
        can never propagate into the running workflow.
        """
        if event is None:
            return
        try:
            self._stream.publish(event)
        except Exception:  # noqa: BLE001 - reporting must never raise into the engine
            logger.warning(
                "Failed to publish governance event %s for execution %s",
                event.event_type,
                event.execution_id,
                exc_info=True,
            )


def install_governance_reporter(
    stream: GovernanceEventStream,
) -> SignalGovernanceReporter:
    """Construct, install and return a ``SignalGovernanceReporter``.

    The returned reporter **must be retained** by the caller for as long as
    reporting should continue — the signal system holds its handlers weakly, so
    dropping the reference silently stops reporting.
    """
    reporter = SignalGovernanceReporter(stream)
    reporter.install()
    return reporter
