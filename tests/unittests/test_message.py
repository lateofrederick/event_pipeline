import pytest
import zlib
import json
from unittest.mock import patch, MagicMock
from volnux.executors.message import TaskMessage, serialize_object, deserialize_message

# Mocking config for checksum generation which relies on SECRET_KEY
@pytest.fixture(autouse=True)
def mock_config():
    with patch("volnux.executors.message.generate_signature") as mock_gen_sig:
        # Mock generate_signature to return a dummy signature
        mock_gen_sig.return_value = ("dummy_signature", "sha256")

        with patch("volnux.executors.message.verify_data") as mock_verify:
             mock_verify.return_value = True
             yield

def test_task_message_serialization_and_deserialization():
    """
    todo - investigate why this test case fails when args when having type typing.Dict and passes with dict
    """
    # Create a sample TaskMessage object
    task_message = TaskMessage(
        event="test_event",
        args={"key": "value", "number": 1},
        correlation_id="123"
    )

    # Serialize the TaskMessage object
    serialized_data = task_message.serialize()
    assert isinstance(serialized_data, bytes)

    # Deserialize the serialized data
    deserialized_data, is_task_message = deserialize_message(serialized_data)

    assert is_task_message
    assert isinstance(deserialized_data, TaskMessage)

    # Verify the deserialized object matches the original
    assert deserialized_data.event == task_message.event
    assert deserialized_data.args == task_message.args
    assert deserialized_data.correlation_id == task_message.correlation_id


def test_serialize_object_function():
    # Test serialization of a generic object using standalone function
    mock_obj = MagicMock()
    mock_obj.dump.return_value = {"key": "value"}

    serialized_data = serialize_object(mock_obj)
    assert isinstance(serialized_data, bytes)

    # Decompress and deserialize the data to verify correctness
    decompressed_data = zlib.decompress(serialized_data)
    data_dict = json.loads(decompressed_data)

    assert data_dict["key"] == "value"
    assert "_signature" in data_dict
    assert "_algorithm" in data_dict


def test_deserialize_message_with_invalid_data():
    # Test deserialization with invalid data
    invalid_data = b"not a valid serialized object"
    with pytest.raises(zlib.error):
        deserialize_message(invalid_data)

def test_deserialize_message_invalid_checksum():
    # Test with valid zlib but invalid checksum (mocked check)
    task_message = TaskMessage(event="t", args={})
    serialized_data = task_message.serialize()

    with patch("volnux.executors.message.verify_data") as mock_verify:
        mock_verify.return_value = False
        from volnux.exceptions import RemoteExecutionError

        with pytest.raises(RemoteExecutionError, match="INVALID_CHECKSUM"):
             deserialize_message(serialized_data)
