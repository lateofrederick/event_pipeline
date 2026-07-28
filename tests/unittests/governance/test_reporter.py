"""Tests for the signal -> governance-event reporter.

The translators are pure and are tested directly with lightweight stand-in
objects (a fake pipeline / execution context / task event), which is all the
real signal payloads expose that the reporter reads. The reporter's wiring
(connect/disconnect, exception isolation) is tested against fake signals so a
live engine and Redis are not needed.
"""

import pytest

from volnux.governance.events import EventType, GovernanceEvent
from volnux.governance.reporter import (
    SignalGovernanceReporter,
    translate_execution_aborted,
    translate_execution_end,
    translate_execution_start,
    translate_execution_stopped,
    translate_task_completed,
    translate_task_retried,
    translate_task_started,
)


# --- Stand-ins for the live engine objects the signals carry ----------------


class FakePipeline:
    def __init__(self, pid, workflow_id=None, workflow_name=None):
        self.id = pid
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name


class FakeContext:
    def __init__(self, pipeline, workflow_id="wf-1", workflow_name="Customer ETL"):
        self.pipeline = pipeline
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name


class FakeEvent:
    def __init__(self, name=None, id=None):
        self.name = name
        self.id = id


# --- Translators ------------------------------------------------------------


def test_execution_start_uses_pipeline_id_as_execution_id():
    event = translate_execution_start(
        pipeline=FakePipeline("run-1", "wf-1", "Customer ETL")
    )

    assert event.event_type == EventType.EXECUTION_STARTED
    assert event.execution_id == "run-1"
    assert event.workflow_id == "wf-1"
    assert event.workflow_name == "Customer ETL"


def test_execution_end_reads_pipeline_id_through_the_context():
    context = FakeContext(FakePipeline("run-1"))

    event = translate_execution_end(execution_context=context)

    assert event.event_type == EventType.EXECUTION_COMPLETED
    assert event.execution_id == "run-1"
    assert event.workflow_id == "wf-1"


def test_stopped_and_aborted_carry_distinct_reasons():
    context = FakeContext(FakePipeline("run-1"))

    stopped = translate_execution_stopped(execution_context=context)
    aborted = translate_execution_aborted(execution_context=context)

    assert stopped.event_type == EventType.EXECUTION_STOPPED
    assert stopped.payload == {"reason": "cancelled"}
    assert aborted.event_type == EventType.EXECUTION_FAILED
    assert aborted.payload == {"reason": "aborted"}


def test_task_events_carry_task_id_from_event_name():
    context = FakeContext(FakePipeline("run-1"))

    started = translate_task_started(
        event=FakeEvent(name="ExtractCustomerData"), execution_context=context
    )
    completed = translate_task_completed(
        event=FakeEvent(name="ExtractCustomerData"), execution_context=context
    )

    assert started.event_type == EventType.TASK_STARTED
    assert started.task_id == "ExtractCustomerData"
    assert started.execution_id == "run-1"
    assert completed.event_type == EventType.TASK_COMPLETED
    assert completed.task_id == "ExtractCustomerData"


def test_task_id_falls_back_to_event_id_when_unnamed():
    context = FakeContext(FakePipeline("run-1"))

    event = translate_task_started(
        event=FakeEvent(name=None, id="task-42"), execution_context=context
    )

    assert event.task_id == "task-42"


def test_retry_prefers_explicit_task_id_and_records_attempt_data():
    context = FakeContext(FakePipeline("run-1"))

    event = translate_task_retried(
        event=FakeEvent(name="fallback"),
        execution_context=context,
        task_id="Enrich",
        retry_count=2,
        max_attempts=3,
        backoff=1.5,
    )

    assert event.event_type == EventType.TASK_RETRIED
    assert event.task_id == "Enrich"
    assert event.payload == {"retry_count": 2, "max_attempts": 3, "backoff": 1.5}


def test_translators_degrade_to_none_ids_rather_than_raising():
    # Missing/renamed attributes must not blow up a signal handler.
    event = translate_execution_start(pipeline=object())

    assert event.execution_id is None
    assert event.workflow_id is None


# --- Reporter wiring --------------------------------------------------------


class FakeSignal:
    """Records connect/disconnect the way SoftSignal would be driven."""

    def __init__(self, name):
        self.name = name
        self.listeners = []

    def connect(self, sender, listener):
        self.listeners.append(listener)

    def disconnect(self, sender, listener):
        if listener in self.listeners:
            self.listeners.remove(listener)


class RecordingStream:
    def __init__(self):
        self.published = []

    def publish(self, event):
        self.published.append(event)
        return "1-0"


class ExplodingStream:
    def publish(self, event):
        raise RuntimeError("redis is down")


@pytest.fixture
def wired_reporter(monkeypatch):
    """A reporter whose signals are fakes, so install/emit can be driven."""
    stream = RecordingStream()
    reporter = SignalGovernanceReporter(stream)

    signals = {
        "start": FakeSignal("pipeline_execution_start"),
        "end": FakeSignal("pipeline_execution_end"),
    }
    monkeypatch.setattr(
        reporter,
        "_build_registrations",
        lambda: [
            (signals["start"], reporter._on_execution_start),
            (signals["end"], reporter._on_execution_end),
        ],
    )
    return reporter, stream, signals


def test_install_connects_and_is_idempotent(wired_reporter):
    reporter, _stream, signals = wired_reporter

    reporter.install()
    reporter.install()  # second call must not double-connect

    assert reporter.installed is True
    assert len(signals["start"].listeners) == 1
    assert len(signals["end"].listeners) == 1


def test_uninstall_disconnects(wired_reporter):
    reporter, _stream, signals = wired_reporter

    reporter.install()
    reporter.uninstall()

    assert reporter.installed is False
    assert signals["start"].listeners == []
    assert signals["end"].listeners == []


def test_emitting_a_signal_publishes_a_translated_event(wired_reporter):
    reporter, stream, signals = wired_reporter
    reporter.install()

    # Drive the signal the way SoftSignal.emit would call the listener.
    listener = signals["start"].listeners[0]
    listener(signal=signals["start"], sender=object(), pipeline=FakePipeline("run-1"))

    assert len(stream.published) == 1
    assert stream.published[0].event_type == EventType.EXECUTION_STARTED
    assert stream.published[0].execution_id == "run-1"


def test_publish_failure_is_swallowed(monkeypatch):
    reporter = SignalGovernanceReporter(ExplodingStream())

    # A failing publish must not propagate — the engine cannot be harmed.
    reporter._emit(GovernanceEvent(event_type=EventType.EXECUTION_STARTED))


def test_emit_none_is_a_noop():
    reporter = SignalGovernanceReporter(RecordingStream())

    reporter._emit(None)

    assert reporter._stream.published == []
