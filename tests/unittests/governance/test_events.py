"""Contract tests for :class:`GovernanceEvent` serialisation.

These are pure (no Redis): they lock down the wire shape that crosses the
engine/backend boundary, since both sides depend on it staying stable.
"""

from volnux.governance.events import EventType, GovernanceEvent


def test_new_event_fills_id_and_timestamp():
    event = GovernanceEvent(event_type=EventType.EXECUTION_STARTED)

    assert event.event_type == "execution.started"
    assert event.event_id  # a uuid string
    assert event.occurred_at > 0
    assert event.payload == {}


def test_round_trip_preserves_all_fields():
    event = GovernanceEvent(
        event_type=EventType.TASK_COMPLETED,
        event_id="evt-1",
        occurred_at=1_700_000_000.5,
        workflow_id="wf-1",
        workflow_name="Customer ETL",
        execution_id="exec-1",
        task_id="ExtractCustomerData",
        sequence=3,
        payload={"status": "completed", "rows": 12847, "nested": {"a": 1}},
    )

    restored = GovernanceEvent.from_stream_fields(event.to_stream_fields())

    assert restored == event


def test_none_correlation_fields_are_omitted_not_blanked():
    # A node heartbeat has no workflow/execution/task correlation.
    event = GovernanceEvent(
        event_type=EventType.NODE_HEARTBEAT,
        payload={"node_id": "node-a", "current_load": 6},
    )

    fields = event.to_stream_fields()

    assert "workflow_id" not in fields
    assert "execution_id" not in fields
    assert "task_id" not in fields
    assert "sequence" not in fields
    # Reconstruction keeps them None.
    restored = GovernanceEvent.from_stream_fields(fields)
    assert restored.workflow_id is None
    assert restored.execution_id is None
    assert restored.sequence is None


def test_payload_survives_as_json():
    event = GovernanceEvent(
        event_type=EventType.HITL_REQUESTED,
        payload={
            "prompt": "Approve $47,500 transfer?",
            "options": ["approve", "reject"],
        },
    )

    fields = event.to_stream_fields()

    # Stored flat: payload is a single JSON string field.
    assert isinstance(fields["payload"], str)
    assert GovernanceEvent.from_stream_fields(fields).payload == event.payload


def test_from_stream_fields_tolerates_malformed_payload():
    restored = GovernanceEvent.from_stream_fields(
        {"event_type": "task.failed", "event_id": "e", "payload": "{not json"}
    )

    assert restored.payload == {}
    assert restored.event_type == "task.failed"


def test_from_stream_fields_tolerates_non_object_payload():
    # A JSON array is valid JSON but not a governance payload; coerce to {}.
    restored = GovernanceEvent.from_stream_fields(
        {"event_type": "task.failed", "payload": "[1, 2, 3]"}
    )

    assert restored.payload == {}


def test_from_stream_fields_defaults_missing_numbers():
    restored = GovernanceEvent.from_stream_fields({"event_type": "execution.started"})

    assert restored.occurred_at == 0.0
    assert restored.sequence is None
    assert restored.payload == {}


def test_from_stream_fields_ignores_unknown_extra_fields():
    # A newer producer may add fields an older consumer does not know about.
    restored = GovernanceEvent.from_stream_fields(
        {"event_type": "execution.started", "some_future_field": "x"}
    )

    assert restored.event_type == "execution.started"
