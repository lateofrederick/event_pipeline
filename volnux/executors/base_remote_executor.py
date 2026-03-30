import concurrent
import time
import typing
import uuid
from concurrent.futures import Executor

from volnux.conf import ConfigLoader
from volnux.types import (
    QueryEventPayload,
    QueryEventResponse,
    TaskExecutionErrorResponse,
    TaskExecutionSuccessResponse,
    Payload,
)
from volnux.executors.polling import HttpPollingClient, TcpPollingClient

CONF = ConfigLoader.get_lazily_loaded_config()
ALGORITHM = "sha256"


def get_secret_key() -> bytes:
    """Retrieve the secret key from configuration and ensure it is bytes."""
    key = CONF.SECRET_KEY
    if isinstance(key, str):
        return key.encode("utf-8")
    return key


class BaseRemoteExecutor(Executor):
    def construct_payload(
        self, event_name: str, args: typing.Dict[str, typing.Any]
    ) -> Payload:
        """Construct the payload to send to the remote manager."""
        from volnux.utils import generate_hmac

        dict_data = {
            "type": "submission_event",
            "event_name": event_name,
            "args": args,
            "correlation_id": str(uuid.uuid4()),
            "timeout": CONF.REMOTE_EVENT_TIMEOUT or None,
            "timestamp": time.time(),
            "client_id": self._get_client_id(),
        }

        hmac, _ = generate_hmac(dict_data, get_secret_key())
        dict_data["hmac"] = hmac

        return Payload(**dict_data)

    def _get_client_id(self) -> str:
        """Get the client ID for the newly constructed payload."""
        import socket

        return socket.gethostname()

    def query_event_exists(self, data: QueryEventPayload) -> QueryEventResponse:
        """Query the remote manager for the existence of an event."""
        raise NotImplementedError

    async def poll_result(
        self,
        correlation_id: str,
        base_url: typing.Optional[str] = None,
        interval: typing.Optional[float] = None,
        timeout: typing.Optional[float] = None,
        use_exponential_backoff: bool = False,
        **kwargs
    ) -> typing.Dict[str, typing.Any]:
        """
        Poll for a result using the correlation_id.
        """

        url = base_url or getattr(CONF, "POLLING_BASE_URL", None)

        # Determine a client type based on url
        if url and (url.startswith("http://") or url.startswith("https://")):
            client = HttpPollingClient(
                base_url=url,
                interval=interval,
                timeout=timeout,
                use_exponential_backoff=use_exponential_backoff,
                **kwargs
            )
        else:
            # Default to TCP if no URL or if it looks like host:port
            host = getattr(self, "host", "localhost")
            port = getattr(self, "port", 8000)
            
            if url and ":" in url and not url.startswith("http"):
                parts = url.split(":")
                host = parts[0]
                port = int(parts[1])
            
            client = TcpPollingClient(
                host=host,
                port=port,
                interval=interval,
                timeout=timeout,
                use_exponential_backoff=use_exponential_backoff,
                **kwargs
            )
            
        return await client.poll_result(correlation_id)

    def parse_task_execution_response(
        self,
        response: typing.Union[
            TaskExecutionSuccessResponse, TaskExecutionErrorResponse
        ],
    ):
        """Parse the response from the remote manager."""
        from volnux.exceptions import RemoteExecutionError
        from volnux.utils import verify_hmac

        if isinstance(response, TaskExecutionErrorResponse):
            raise RemoteExecutionError(f"{response.code}: {response.message}")

        # Verify HMAC
        response_data = response.dump(_format="dict")
        if not verify_hmac(response_data, get_secret_key()):
            raise RemoteExecutionError("INVALID_HMAC")

        return response.result

    def submit(
        self, fn: typing.Callable, /, *args, **kwargs
    ) -> concurrent.futures.Future:
        raise NotImplementedError
