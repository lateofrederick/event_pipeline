from __future__ import annotations

import json
import logging
import typing
import grpc
from concurrent import futures

from volnux.conf import ConfigLoader
from volnux.executors.base_remote_executor import BaseRemoteExecutor
from volnux.protos import task_pb2, task_pb2_grpc
from volnux.security.ssl_manager import SecureSocketManager
from volnux.types import (
    QueryEventPayload,
    QueryEventResponse,
    TaskExecutionErrorResponse,
    TaskExecutionSuccessResponse,
)

if typing.TYPE_CHECKING:
    from volnux.security.ssl_config import SSLConfig

CONF = ConfigLoader.get_lazily_loaded_config()
logger = logging.getLogger(__name__)


class GRPCExecutor(BaseRemoteExecutor):
    """
    gRPC-based remote task executor.
    """

    def __init__(
        self,
        security_manager: typing.Optional[SecureSocketManager] = None,
        host: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        max_message_length: int = 4 * 1024 * 1024,  # 4MB default
    ):
        """
        Initialize the GrpcExecutor.

        Args:
            security_manager: Manager for SSL/TLS credentials
            host: Remote manager host
            port: Remote manager port
            max_message_length: Maximum gRPC message length in bytes
        """
        super().__init__()
        self._host = host or CONF.REMOTE_MANAGER_HOST or "localhost"
        self._port = port or CONF.REMOTE_MANAGER_GRPC_PORT
        self._max_message_length = max_message_length

        if security_manager:
            self._security_manager = security_manager
        else:
            self._security_manager = None
            # Initialize with default config if not provided AND SSL is enabled
            if getattr(CONF, "USE_SSL", False):
                from volnux.security.ssl_config import SSLConfig

                try:
                    ssl_config = SSLConfig(
                        cert_path=getattr(CONF, "SSL_CERT_PATH", None),
                        key_path=getattr(CONF, "SSL_KEY_PATH", None),
                        ca_cert_path=getattr(CONF, "SSL_CA_CERT_PATH", None),
                        verify_certificates=getattr(CONF, "SSL_VERIFY_CERTIFICATES", True),
                    )
                    self._security_manager = SecureSocketManager(ssl_config)
                except Exception as e:
                    logger.warning(f"Failed to initialize default SSL manager: {e}")


        self._channel = self._create_channel()
        self._stub = task_pb2_grpc.TaskExecutorStub(self._channel)

    def _create_channel(self) -> grpc.Channel:
        """Create a secure or insecure gRPC channel."""
        target = f"{self._host}:{self._port}"
        options = [
            ("grpc.max_send_message_length", self._max_message_length),
            ("grpc.max_receive_message_length", self._max_message_length),
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.keepalive_timeout_ms", 10000),
            ("grpc.keepalive_permit_without_calls", True),
        ]

        if getattr(CONF, "USE_SSL", False):
            credentials = self._security_manager.create_grpc_client_credentials()
            logger.debug(f"Creating secure gRPC channel to {target}")
            return grpc.secure_channel(target, credentials, options=options)
        else:
            logger.warning(
                f"Creating INSECURE gRPC channel to {target}. "
                "This should not be used in production."
            )
            return grpc.insecure_channel(target, options=options)

    def submit(
        self, fn: typing.Callable, /, *args, **kwargs
    ) -> futures.Future:
        """
        Submit a task to the remote manager via gRPC.

        Returns:
            A Future object representing the execution of the task.
        """
        # Resolve event name from function or use name if string
        event_name = getattr(fn, "__name__", str(fn))

        # Prepare arguments
        data = {}
        if args:
            data["args"] = args
        if kwargs:
            data["kwargs"] = kwargs

        # Construct payload
        payload = self.construct_payload(event_name, data)

        # Create gRPC request
        # Note: We serialize args mainly because the payload structure uses dict/json
        # but the proto defined args as bytes to handle arbitrary json structures safely.
        request = task_pb2.SubmitTaskRequest(
            type=payload.type,
            event_name=payload.event_name,
            args=json.dumps(payload.args).encode("utf-8"),
            correlation_id=payload.correlation_id,
            timeout=int(payload.timeout) if payload.timeout else 0,
            timestamp=payload.timestamp,
            client_id=payload.client_id,
            hmac=payload.hmac,
        )

        # Use the future API of the stub
        grpc_future = self._stub.SubmitTask.future(request)

        # We return a concurrent.futures.Future wrapper
        encoded_future = futures.Future()

        def callback(f):
            try:
                # Catch any grpc specific errors
                response = f.result()

                # Map proto response to internal response types
                if response.status == "success":
                    success_response = TaskExecutionSuccessResponse(
                        correlation_id=response.correlation_id,
                        result=json.loads(response.result),
                        completed_at=response.completed_at,
                        hmac=response.hmac
                    )
                    # Verify and return result
                    try:
                        result = self.parse_task_execution_response(success_response)
                        encoded_future.set_result(result)
                    except Exception as e:
                        encoded_future.set_exception(e)
                else:
                    error_response = TaskExecutionErrorResponse(
                        correlation_id=response.correlation_id,
                        status="error",
                        message=response.message,
                        code=response.code,
                        timestamp=response.timestamp
                    )
                    # This will raise the exception via parse_task_execution_response
                    try:
                        self.parse_task_execution_response(error_response)
                    except Exception as e:
                         encoded_future.set_exception(e)
            except grpc.RpcError as e:
                # Handle gRPC transport errors
                encoded_future.set_exception(e)
            except Exception as e:
                encoded_future.set_exception(e)

        grpc_future.add_done_callback(callback)
        return encoded_future

    def submit_stream(
        self, fn: typing.Callable, /, *args, **kwargs
    ) -> typing.Iterator[TaskExecutionSuccessResponse]:
        """
        Submit a task and get a stream of responses.

        Returns:
            An iterator of TaskExecutionSuccessResponse objects.
        """
        # Resolve event name
        event_name = getattr(fn, "__name__", str(fn))

        # Prepare arguments
        data = {}
        if args:
            data["args"] = args
        if kwargs:
            data["kwargs"] = kwargs

        # Construct payload
        payload = self.construct_payload(event_name, data)

        # Create gRPC request
        request = task_pb2.SubmitTaskRequest(
            type=payload.type,
            event_name=payload.event_name,
            args=json.dumps(payload.args).encode("utf-8"),
            correlation_id=payload.correlation_id,
            timeout=int(payload.timeout) if payload.timeout else 0,
            timestamp=payload.timestamp,
            client_id=payload.client_id,
            hmac=payload.hmac,
        )

        # Call streaming RPC
        try:
            response_iterator = self._stub.SubmitTaskStream(request)
            for response in response_iterator:
                if response.status == "success" or response.status == "partial": # Assuming partial for stream
                     yield TaskExecutionSuccessResponse(
                        correlation_id=response.correlation_id,
                        result=json.loads(response.result) if response.result else {},
                        completed_at=response.completed_at,
                        hmac=response.hmac
                    )
                elif response.status == "error":
                     # For stream, we might yield an error or raise it.
                     # Raising it stops the stream.
                     raise Exception(f"Task failed: {response.message} ({response.code})")
        except grpc.RpcError as e:
            logger.error(f"Streaming RPC failed: {e}")
            raise

    def query_event_exists(self, data: QueryEventPayload) -> QueryEventResponse:
        """Query if an event exists on the remote manager."""
        request = task_pb2.QueryEventRequest(
            type=data.type,
            event_name=data.event_name,
            client_id=data.client_id
        )

        try:
            response = self._stub.QueryEvent(request)
            return QueryEventResponse(
                event_name=response.event_name,
                available=response.available,
                message=response.message,
                metadata=json.loads(response.metadata) if response.metadata else {}
            )
        except grpc.RpcError as e:
            logger.error(f"Failed to query event: {e}")
            raise

    def close(self):
        """Close the gRPC channel."""
        if hasattr(self, "_channel") and self._channel:
            self._channel.close()

    def __del__(self):
        self.close()
