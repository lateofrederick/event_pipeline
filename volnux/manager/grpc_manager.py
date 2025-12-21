import logging
import typing
import grpc
import threading
from concurrent import futures
from .base import BaseManager, Protocol
from volnux.protos import task_pb2, task_pb2_grpc
from volnux.executors.message import TaskMessage, deserialize_message, serialize_dict, serialize_object

logger = logging.getLogger(__name__)


class TaskExecutorServicer(task_pb2_grpc.TaskExecutorServicer):
    """Implementation of TaskExecutor service."""

    def __init__(self, manager):
        self.manager = manager

    def Execute(self, request, context):
        """Execute a task and return the result."""
        try:
            # Reconstruct TaskMessage from request
            # Request has: task_id, fn, name, args, kwargs

            # Deserialize args/kwargs using message.py utils
            # The current gRPC executor serializes them individually.

            args_tuple, is_task = deserialize_message(request.args)
            kwargs_dict, is_task = deserialize_message(request.kwargs)

            # Assuming we only use kwargs for remote execution for now.
            combined_args = kwargs_dict if kwargs_dict else {}

            event_name = request.name

            # Construct TaskMessage locally
            task_msg = TaskMessage(
                event=event_name,
                args=combined_args,
                correlation_id=request.task_id if request.task_id else None
            )

            # Setup sync
            completion_event = threading.Event()
            client_context = {
                "event": completion_event,
                "result_container": {}
            }

            # Dispatch
            self.manager.handle_task(task_msg, Protocol.GRPC, client_context)

            # Wait
            if completion_event.wait(timeout=300): # TODO: usage configurable timeout
                result_data = client_context["result_container"].get("data")

                # result_data is 'status', 'result', etc.
                is_success = result_data.get("status") == "success"
                error_msg = result_data.get("message", "") if not is_success else ""

                # Serialize the inner result content
                inner_result = result_data.get("result")
                if isinstance(inner_result, dict):
                    serialized_result = serialize_dict(inner_result)
                else:
                    serialized_result = serialize_object(inner_result) if not isinstance(inner_result, bytes) else inner_result
                # Note: serialize_object returns bytes (compressed+signed)

                return task_pb2.TaskResponse(
                    success=is_success,
                    error=error_msg,
                    result=serialized_result
                )
            else:
                return task_pb2.TaskResponse(
                    success=False,
                    error="TASK_TIMEOUT",
                    result=b""
                )

        except Exception as e:
            logger.error(
                f"Error executing task {request.task_id}: {str(e)}",
                exc_info=e,
            )
            # Serialize generic error
            # We can't really serialize exception easily unless pickling, but serialize_object does json dump.
            # Convert to string.
            serialized_error = serialize_dict({"error": str(e)})
            return task_pb2.TaskResponse(
                success=False, error=str(e), result=serialized_error
            )

    def ExecuteStream(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "ExecuteStream not yet refactored")


class GRPCManager(BaseManager):
    """
    gRPC server that handles remote task execution requests.
    """

    def __init__(
        self,
        host: str,
        port: int,
        max_workers: int = 10,
        use_encryption: bool = False,
        server_cert_path: typing.Optional[str] = None,
        server_key_path: typing.Optional[str] = None,
        require_client_cert: bool = False,
        client_ca_path: typing.Optional[str] = None,
    ) -> None:
        super().__init__(host=host, port=port)
        self._max_workers = max_workers
        self._use_encryption = use_encryption
        self._server_cert_path = server_cert_path
        self._server_key_path = server_key_path
        self._require_client_cert = require_client_cert
        self._client_ca_path = client_ca_path
        self._server = None
        self._shutdown = False

    def _route_tcp_response(self, task_info: typing.Dict, result_data: typing.Dict):
        pass

    def _route_grpc_response(self, task_info: typing.Dict, result_data: typing.Dict):
        """
        Route response back to the gRPC handler waiting on event.
        """
        client_context = task_info.get("client_context")
        if not client_context:
            logger.error("No client context for GRPC response")
            return

        completion_event = client_context.get("event")
        result_container = client_context.get("result_container")

        if result_container is not None:
            result_container["data"] = result_data

        if completion_event:
            completion_event.set()

    def start(self, *args, **kwargs) -> None:
        """Start the gRPC server"""
        # Start BaseManager components
        super().start()

        try:
            # Create server
            self._server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=self._max_workers)
            )

            # Add servicer with connection to self
            task_pb2_grpc.add_TaskExecutorServicer_to_server(
                TaskExecutorServicer(self), self._server
            )

            # Configure encryption if enabled
            if self._use_encryption:
                if not (self._server_cert_path and self._server_key_path):
                    raise ValueError(
                        "Server certificate and key required for encryption"
                    )

                with open(self._server_key_path, "rb") as f:
                    private_key = f.read()
                with open(self._server_cert_path, "rb") as f:
                    certificate_chain = f.read()

                root_certificates = None
                if self._require_client_cert:
                    if not self._client_ca_path:
                        raise ValueError(
                            "Client CA required when client cert is required"
                        )
                    with open(self._client_ca_path, "rb") as f:
                        root_certificates = f.read()

                server_credentials = grpc.ssl_server_credentials(
                    ((private_key, certificate_chain),),
                    root_certificates=root_certificates,
                    require_client_auth=self._require_client_cert,
                )
                port = self._server.add_secure_port(
                    f"{self._host}:{self._port}", server_credentials
                )
            else:
                port = self._server.add_insecure_port(f"{self._host}:{self._port}")

            # Start server
            self._server.start()
            logger.info(f"gRPC server listening on {self._host}:{port}")

            self._server.wait_for_termination()

        except Exception as e:
            logger.error(f"Error starting gRPC server: {e}")
            raise

    def shutdown(self) -> None:
        """Shutdown the gRPC server"""
        super().shutdown()
        if self._server:
            self._server.stop(grace=5)  # 5 seconds grace period
            self._server = None
