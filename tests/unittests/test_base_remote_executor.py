import unittest
import sys
import json
import base64
import hmac
import hashlib
from unittest.mock import MagicMock, patch
from volnux.executors.base_remote_executor import BaseRemoteExecutor


mock_options = MagicMock()
sys.modules["volnux.parser.options"] = mock_options


class TestBaseRemoteExecutor(unittest.TestCase):
    def setUp(self):
        self.executor = BaseRemoteExecutor()

        patcher = patch('volnux.executors.base_remote_executor.CONF')
        self.mock_conf = patcher.start()
        self.mock_conf.SECRET_KEY = "test_secret_key"
        self.addCleanup(patcher.stop)

    def test_get_secret_key(self):
        """Test retrieving secret key from configuration"""
        from volnux.executors.base_remote_executor import get_secret_key

        self.mock_conf.SECRET_KEY = "string_key"
        self.assertEqual(get_secret_key(), b"string_key")

        self.mock_conf.SECRET_KEY = b"bytes_key"
        self.assertEqual(get_secret_key(), b"bytes_key")

    def test_generate_hmac(self):
        """Test HMAC generation"""
        data = {"test": "data"}
        signature, algorithm = self.executor._generate_hmac(data)

        expected_bytes = json.dumps(data, sort_keys=True).encode("utf-8")
        expected_signature = hmac.new(
            b"test_secret_key", expected_bytes, hashlib.sha256
        ).digest()
        expected_b64 = base64.b64encode(expected_signature).decode("utf-8")

        self.assertEqual(signature, expected_b64)
        self.assertEqual(algorithm, "sha256")

    def test_construct_payload(self):
        """Test payload construction and HMAC generation"""
        mock_message = MagicMock()
        mock_message.dump.return_value = {"task_id": "123", "args": [1, 2]}

        payload = self.executor.construct_payload(mock_message)

        self.assertIn("_signature", payload)
        self.assertIn("_algorithm", payload)
        self.assertEqual(payload["task_id"], "123")
        self.assertEqual(payload["_algorithm"], "sha256")

    def test_construct_payload_special_characters(self):
        """Test payload construction with special characters"""
        mock_message = MagicMock()
        # Including special characters and unicode
        mock_message.dump.return_value = {
            "task_id": "spec!@#$",
            "data": "test_data_😊"
        }

        payload = self.executor.construct_payload(mock_message)

        self.assertIn("_signature", payload)
        self.assertEqual(payload["data"], "test_data_😊")

        # Verify signature is generated for this specific payload
        # Note: we need to remove signature/algo fields to verify against _generate_hmac logic locally if we wanted strict verification,
        # but _generate_hmac is tested separately. Here we ensure it doesn't crash and returns valid structure.
        self.assertTrue(len(payload["_signature"]) > 0)

    def test_construct_payload_empty_args(self):
        """Test payload construction with empty args"""
        mock_message = MagicMock()
        mock_message.dump.return_value = {}

        payload = self.executor.construct_payload(mock_message)

        self.assertIn("_signature", payload)
        self.assertIn("_algorithm", payload)

    def test_construct_payload_unserializable(self):
        """Test payload construction with unserializable data"""
        mock_message = MagicMock()
        # Sets are not JSON serializable by default
        mock_message.dump.return_value = {"invalid": {1, 2, 3}}

        with self.assertRaises(ValueError) as context:
            self.executor.construct_payload(mock_message)

        self.assertEqual(str(context.exception), "Payload is not serializable")
