import json
import unittest
from unittest.mock import MagicMock, patch, ANY
import grpc
from concurrent import futures

from volnux.executors.grpc_executor import GRPCExecutor
from volnux.types import (
    TaskExecutionSuccessResponse,
    TaskExecutionErrorResponse,
    QueryEventPayload,
    QueryEventResponse
)
from volnux.protos import task_pb2

class TestGRPCExecutor(unittest.TestCase):
    def setUp(self):
        self.host = "localhost"
        self.port = 50051

        self.patcher_construct = patch("volnux.executors.base_remote_executor.BaseRemoteExecutor.construct_payload")
        self.mock_construct_payload = self.patcher_construct.start()

        mock_payload = MagicMock()
        mock_payload.type = "task"
        mock_payload.event_name = "test_event"
        mock_payload.args = {}
        mock_payload.correlation_id = "123"
        mock_payload.timeout = 30
        mock_payload.timestamp = 123456789.0
        mock_payload.client_id = "test_client"
        mock_payload.hmac = "signature"
        self.mock_construct_payload.return_value = mock_payload

        self.patcher_key = patch("volnux.executors.base_remote_executor.get_secret_key")
        self.mock_get_key = self.patcher_key.start()
        self.mock_get_key.return_value = b'test_secret'
        self.patcher_verify = patch("volnux.utils.verify_hmac")
        self.mock_verify = self.patcher_verify.start()
        self.mock_verify.return_value = True

        self.executor = GRPCExecutor(host=self.host, port=self.port)
        self.executor._stub = MagicMock()

    def tearDown(self):
        self.executor.close()
        self.patcher_construct.stop()
        self.patcher_key.stop()
        self.patcher_verify.stop()

    @patch("volnux.executors.grpc_executor.grpc")
    @patch("volnux.executors.grpc_executor.SecureSocketManager")
    def test_init_secure(self, mock_ssl_manager, mock_grpc):
        """Test initialization with SSL enabled."""
        mock_conf = MagicMock()
        mock_conf.USE_SSL = True

        with patch("volnux.executors.grpc_executor.CONF", mock_conf):
            executor = GRPCExecutor(host="localhost", port=50051)

            # Verify secure channel creation
            mock_ssl_manager.return_value.create_grpc_client_credentials.assert_called_once()
            mock_grpc.secure_channel.assert_called_once()
            executor.close()

    @patch("volnux.executors.grpc_executor.grpc")
    def test_init_insecure(self, mock_grpc):
        """Test initialization with SSL disabled (default)."""
        mock_conf = MagicMock()
        mock_conf.USE_SSL = False

        with patch("volnux.executors.grpc_executor.CONF", mock_conf):
            executor = GRPCExecutor(host="localhost", port=50051)

            # Verify insecure channel creation
            mock_grpc.insecure_channel.assert_called_once()
            executor.close()

    def test_submit_success(self):
        """Test successful unary task submission."""
        # Mock successful response
        expected_result = {"foo": "bar"}
        mock_response = task_pb2.SubmitTaskResponse(
            correlation_id="123",
            status="success",
            message="OK",
            code="200"
        )
        mock_response.result = json.dumps(expected_result).encode("utf-8")
        mock_response.completed_at = 100.0
        mock_response.hmac = "sig"

        # Mock future
        mock_future = MagicMock()
        mock_future.result.return_value = mock_response
        self.executor._stub.SubmitTask.future.return_value = mock_future

        future = self.executor.submit(lambda x: x, 1)

        # Trigger callback
        args, _ = mock_future.add_done_callback.call_args
        callback = args[0]
        callback(mock_future)

        # Check result
        self.assertTrue(future.done())
        result = future.result()
        self.assertEqual(result, expected_result)

    def test_submit_failure(self):
        """Test failed unary task submission."""
        mock_response = task_pb2.SubmitTaskResponse()
        mock_response.status = "error"
        mock_response.correlation_id = "123"
        mock_response.message = "Failed"
        mock_response.code = "ERR"

        mock_future = MagicMock()
        mock_future.result.return_value = mock_response
        self.executor._stub.SubmitTask.future.return_value = mock_future

        future = self.executor.submit(lambda: None)

        # Trigger callback
        args, _ = mock_future.add_done_callback.call_args
        callback = args[0]
        callback(mock_future)

        # Verify exception
        with self.assertRaises(Exception) as cm:
            future.result()
        self.assertIn("Failed", str(cm.exception))

    def test_submit_stream(self):
        """Test streaming submission."""
        mock_responses = [
            task_pb2.SubmitTaskResponse(status="success", result=json.dumps({"i": 1}).encode("utf-8")),
            task_pb2.SubmitTaskResponse(status="success", result=json.dumps({"i": 2}).encode("utf-8")),
        ]
        self.executor._stub.SubmitTaskStream.return_value = iter(mock_responses)

        results = list(self.executor.submit_stream(lambda: None))

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].result, {"i": 1})
        self.assertEqual(results[1].result, {"i": 2})

    def test_submit_batch_mixed(self):
        """Test batch submission with mixed success/failure."""
        mock_responses = [
            task_pb2.SubmitTaskResponse(status="success", correlation_id="1", result=json.dumps({"ok": True}).encode("utf-8")),
            task_pb2.SubmitTaskResponse(status="error", correlation_id="2", message="oops"),
        ]
        self.executor._stub.SubmitBatchTasks.return_value = iter(mock_responses)

        tasks = [(lambda: None, (), {})]
        results = list(self.executor.submit_batch(tasks))

        self.assertEqual(len(results), 2)
        self.assertIsInstance(results[0], TaskExecutionSuccessResponse)
        self.assertEqual(results[0].result, {"ok": True})

        self.assertIsInstance(results[1], TaskExecutionErrorResponse)
        self.assertEqual(results[1].message, "oops")

    def test_query_event_exists(self):
        """Test query event existence."""
        mock_resp = task_pb2.QueryEventResponse(
            event_name="foo",
            available=True,
            message="Found",
            metadata=json.dumps({"version": 1})
        )
        self.executor._stub.QueryEvent.return_value = mock_resp

        payload = QueryEventPayload(event_name="foo", client_id="test")
        response = self.executor.query_event_exists(payload)

        self.assertTrue(response.available)
        self.assertEqual(response.metadata, {"version": 1})
