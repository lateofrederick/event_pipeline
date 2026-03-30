import abc
import asyncio
import logging
import socket
import struct
import time
import typing
import json
import urllib.request
import urllib.error
import zlib
from volnux.concurrency.async_utils import to_thread
from volnux.conf import ConfigLoader
from volnux.exceptions import PollingTimeoutError, RemoteExecutionError
from volnux.executors.message import TaskMessage, serialize_dict, deserialize_message

logger = logging.getLogger(__name__)
CONF = ConfigLoader.get_lazily_loaded_config()


class BasePollingClient(abc.ABC):
    """Interface for polling results from the manager."""

    @abc.abstractmethod
    async def poll_result(self, correlation_id: str) -> typing.Dict[str, typing.Any]:
        """Poll for a result using the correlation_id."""
        pass


class HttpPollingClient(BasePollingClient):
    """
    A client that polls an HTTP endpoint at a specified interval for a task result.

    This class is used to poll a manager over HTTP for the result of a task, retrying
    at regular intervals until either a result is obtained or a timeout occurs. It
    supports both fixed intervals and exponential backoff for retry intervals. The
    client can be configured with a base URL, polling interval, timeout duration,
    and optional exponential backoff settings.

    Attributes:
        base_url: The root URL of the HTTP endpoint to poll.
        interval: The base polling interval in seconds. Defaults to a
            configuration value or 1.0 if not provided.
        timeout: The maximum duration in seconds to wait for a result before
            considering the poll timeout. Defaults to a configuration value
            or 300.0 if not provided.
        use_exponential_backoff: A flag indicating whether exponential backoff
            should be used for retry intervals. Defaults to False.
        max_interval: The maximum interval in seconds between polls when
            exponential backoff is enabled. Defaults to a configuration value
            or 60.0 if not provided.
        backoff_factor: The factor by which the interval increases during each
            backoff step. Defaults to 2.0.

    Methods:
        poll_result:
            Initiates the polling process to fetch the task result. Retries
            until the task result is obtained or the timeout duration is reached.
    """

    def __init__(
            self,
            base_url: str,
            interval: typing.Optional[float] = None,
            timeout: typing.Optional[float] = None,
            use_exponential_backoff: bool = False,
            max_interval: typing.Optional[float] = None,
            backoff_factor: float = 2.0
    ):
        self.base_url = base_url.rstrip("/")
        self.interval = interval if interval is not None else getattr(CONF, "POLLING_INTERVAL", 1.0)
        self.timeout = timeout if timeout is not None else getattr(CONF, "POLLING_TIMEOUT", 300.0)
        self.use_exponential_backoff = use_exponential_backoff
        self.max_interval = max_interval if max_interval is not None else getattr(CONF, "POLLING_MAX_INTERVAL", 60.0)
        self.backoff_factor = backoff_factor

    def _make_request(self, correlation_id: str) -> typing.Tuple[int, typing.Optional[typing.Dict]]:
        url = f"{self.base_url}/poll/{correlation_id}"

        body = {"correlation_id": correlation_id, "timestamp": time.time()}

        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"}
        )

        try:
            with urllib.request.urlopen(req) as response:
                status = response.getcode()
                raw_data = response.read()
                try:
                    decompressed = zlib.decompress(raw_data)
                    resp_data = json.loads(decompressed.decode("utf-8"))
                except Exception as e:
                    logger.error(f"Failed to decompress response: {e}")
                    resp_data = json.loads(raw_data.decode("utf-8"))

                return status, resp_data
        except urllib.error.HTTPError as e:
            if e.code in (202, 404):
                return e.code, None
            raise e
        except urllib.error.URLError as e:
            logger.error(f"URLError during polling: {e}")
            raise e

    async def poll_result(self, correlation_id: str) -> typing.Dict[str, typing.Any]:
        start_time = time.time()
        current_interval = self.interval

        while True:
            if time.time() - start_time > self.timeout:
                raise PollingTimeoutError(f"Polling timed out after {self.timeout} seconds for {correlation_id}")

            status, data = await to_thread(self._make_request, correlation_id)

            if status == 200:
                return data
            elif status == 202:
                # Task pending
                pass
            elif status == 404:
                # Task not found
                logger.debug(f"Task {correlation_id} not found (404)")
            else:
                logger.warning(f"Unexpected status code {status} for {correlation_id}")

            await asyncio.sleep(current_interval)

            if self.use_exponential_backoff:
                current_interval = min(current_interval * self.backoff_factor, self.max_interval)


class TcpPollingClient(BasePollingClient):
    """
    A client that polls a manager over TCP for a task result.
    """

    def __init__(
            self,
            host: str,
            port: int,
            interval: typing.Optional[float] = None,
            timeout: typing.Optional[float] = None,
            use_exponential_backoff: bool = False,
            max_interval: typing.Optional[float] = None,
            backoff_factor: float = 2.0
    ):
        self.host = host
        self.port = port
        self.interval = interval if interval is not None else getattr(CONF, "POLLING_INTERVAL", 1.0)
        self.timeout = timeout if timeout is not None else getattr(CONF, "POLLING_TIMEOUT", 300.0)
        self.use_exponential_backoff = use_exponential_backoff
        self.max_interval = max_interval if max_interval is not None else getattr(CONF, "POLLING_MAX_INTERVAL", 60.0)
        self.backoff_factor = backoff_factor
        logger.debug(f"Initialized TcpPollingClient with host={host}, port={port}, interval={self.interval}, timeout={self.timeout}")

    def _poll_once(self, correlation_id: str) -> typing.Optional[typing.Dict]:
        """Send a POLL request over TCP and wait for response."""
        import struct
        try:
            logger.debug(f"Attempting to poll for {correlation_id} at {self.host}:{self.port}")
            with socket.create_connection((self.host, self.port), timeout=10) as sock:
                # Create a POLL message
                from volnux.executors.message import TaskMessage, serialize_dict, deserialize_message
                poll_msg = TaskMessage(
                    event="POLL",
                    args={},
                    correlation_id=correlation_id
                )
                from volnux.executors.message import serialize_object
                data = serialize_object(poll_msg)
                
                # Framing: 8 bytes length + data (matches send_data_over_socket)
                header = struct.pack("!Q", len(data))
                sock.sendall(header + data)
                logger.debug(f"POLL request sent for {correlation_id} ({len(data)} bytes)")
                
                # Receive response header (8 bytes)
                resp_header = b""
                sock.settimeout(5)  # Set a timeout for reading response
                while len(resp_header) < 8:
                    try:
                        chunk = sock.recv(8 - len(resp_header))
                    except socket.timeout:
                        logger.error(f"Timeout receiving response header for {correlation_id}")
                        return None
                    if not chunk:
                        logger.debug(f"Connection closed by server while waiting for header for {correlation_id}")
                        return None
                    resp_header += chunk
                
                resp_len = struct.unpack("!Q", resp_header)[0]
                logger.debug(f"Expecting {resp_len} bytes in response for {correlation_id}")
                
                resp_data = b""
                while len(resp_data) < resp_len:
                    try:
                        chunk = sock.recv(resp_len - len(resp_data))
                    except socket.timeout:
                        logger.error(f"Timeout receiving response data for {correlation_id}")
                        return None
                    if not chunk:
                        logger.debug(f"Connection closed by server while waiting for data for {correlation_id}")
                        break
                    resp_data += chunk
                
                if not resp_data:
                    logger.debug(f"Empty response received for {correlation_id}")
                    return None

                # Deserialize response
                # Note: For polling, we use deserialize_message which handles decompression and HMAC
                response, is_msg = deserialize_message(resp_data)
                logger.debug(f"Received response for {correlation_id}: {response} (is_msg={is_msg})")
                
                if isinstance(response, TaskMessage):
                     return response.dump(_format="dict")
                return response
        except (socket.error, RemoteExecutionError) as e:
            logger.error(f"TCP Polling error for {correlation_id}: {e}")
            return None

    async def poll_result(self, correlation_id: str) -> typing.Dict[str, typing.Any]:
        # Log for debugging purposes
        logger.info(f"TcpPollingClient starting to poll for {correlation_id} at {self.host}:{self.port}")
        start_time = time.time()
        current_interval = self.interval

        while True:
            if time.time() - start_time > self.timeout:
                raise PollingTimeoutError(f"TCP Polling timed out after {self.timeout} seconds for {correlation_id}")

            response = await to_thread(self._poll_once, correlation_id)

            if response:
                status = response.get("status")
                if status == "PENDING":
                    logger.info(f"Task {correlation_id} is PENDING")
                elif status == "NOT_FOUND":
                    logger.info(f"Task {correlation_id} not found on TCP manager")
                else:
                    logger.info(f"Task {correlation_id} COMPLETED with status {status}")
                    return response
            else:
                logger.debug(f"Poll request for {correlation_id} returned no response")

            await asyncio.sleep(current_interval)

            if self.use_exponential_backoff:
                current_interval = min(current_interval * self.backoff_factor, self.max_interval)
