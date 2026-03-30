import asyncio
import json
import logging
import unittest
from unittest.mock import patch, MagicMock
import urllib.error
import zlib
import time

from volnux.executors.polling import HttpPollingClient, PollingTimeoutError

class TestHttpPollingClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.base_url = "http://localhost:8000"
        self.client = HttpPollingClient(
            base_url=self.base_url,
            interval=0.01,
            timeout=1.0
        )

    def test_polling_config_defaults(self):
        """Test that HttpPollingClient uses defaults from CONF when not specified."""
        from volnux.conf import ConfigLoader
        conf = ConfigLoader.get_lazily_loaded_config()
        
        client = HttpPollingClient(base_url=self.base_url)
        self.assertEqual(client.interval, conf.POLLING_INTERVAL)
        self.assertEqual(client.timeout, conf.POLLING_TIMEOUT)
        self.assertEqual(client.max_interval, conf.POLLING_MAX_INTERVAL)

    @patch("urllib.request.urlopen")
    async def test_poll_result_success(self, mock_urlopen):
        # Mock successful response (200 OK)
        correlation_id = "test_cid"
        expected_result = {"status": "success", "data": "result_value"}
        
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = json.dumps(expected_result).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        
        mock_urlopen.return_value = mock_response

        result = await self.client.poll_result(correlation_id)
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["data"], "result_value")
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    async def test_poll_result_pending_then_success(self, mock_urlopen):
        correlation_id = "test_cid"
        
        # 1. First call: 202 Pending
        mock_response_pending = MagicMock()
        mock_response_pending.getcode.return_value = 202
        # urllib raises HTTPError for non-2xx status codes
        error_202 = urllib.error.HTTPError(
            url="", code=202, msg="Accepted", hdrs={}, fp=None
        )
        
        # 2. Second call: 200 OK
        expected_result = {"status": "success", "data": "done"}
        mock_response_ok = MagicMock()
        mock_response_ok.getcode.return_value = 200
        mock_response_ok.read.return_value = json.dumps(expected_result).encode("utf-8")
        mock_response_ok.__enter__.return_value = mock_response_ok

        mock_urlopen.side_effect = [error_202, mock_response_ok]

        result = await self.client.poll_result(correlation_id)
        
        self.assertEqual(result["status"], "success")
        self.assertEqual(mock_urlopen.call_count, 2)

    @patch("urllib.request.urlopen")
    async def test_poll_result_timeout(self, mock_urlopen):
        correlation_id = "test_cid"
        
        # Always return 202 Pending
        error_202 = urllib.error.HTTPError(
            url="", code=202, msg="Accepted", hdrs={}, fp=None
        )
        mock_urlopen.side_effect = lambda *args, **kwargs: (lambda: (_ for _ in ()).throw(error_202))()
        
        # More robust way to mock repeated calls raising exception
        mock_urlopen.side_effect = error_202

        with self.assertRaises(PollingTimeoutError):
            await self.client.poll_result(correlation_id)

    @patch("urllib.request.urlopen")
    async def test_exponential_backoff(self, mock_urlopen):
        self.client.use_exponential_backoff = True
        self.client.interval = 0.01
        self.client.backoff_factor = 2.0
        
        correlation_id = "test_cid"
        error_202 = urllib.error.HTTPError(url="", code=202, msg="Accepted", hdrs={}, fp=None)
        
        # We need it to eventually succeed so it doesn't loop forever in this test
        expected_result = {"status": "success"}
        mock_response_ok = MagicMock()
        mock_response_ok.getcode.return_value = 200
        mock_response_ok.read.return_value = json.dumps(expected_result).encode("utf-8")
        mock_response_ok.__enter__.return_value = mock_response_ok
        
        mock_urlopen.side_effect = [error_202, error_202, mock_response_ok]
        
        start_time = time.time()
        await self.client.poll_result(correlation_id)
        duration = time.time() - start_time
        
        # Interval: 0.01, then 0.02. Total sleep should be around 0.03
        self.assertGreaterEqual(duration, 0.03)
