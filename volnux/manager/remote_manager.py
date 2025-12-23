import socket
import zlib
import time
import errno
import ssl
import typing
import pickle
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from .base import BaseManager, Protocol
from nexus.conf import ConfigLoader
from nexus.utils import (
    send_data_over_socket,
    receive_data_from_socket,
    create_server_ssl_context,
)
from volnux.executors.message import TaskMessage, deserialize_message, serialize_dict

logger = logging.getLogger(__name__)

CONF = ConfigLoader.get_lazily_loaded_config()

DEFAULT_TIMEOUT = CONF.DEFAULT_CONNECTION_TIMEOUT
CHUNK_SIZE = CONF.DATA_CHUNK_SIZE
BACKLOG_SIZE = CONF.CONNECTION_BACKLOG_SIZE
QUEUE_SIZE = CONF.DATA_QUEUE_SIZE
PROJECT_ROOT = CONF.PROJECT_ROOT_DIR


class RemoteTaskManager(BaseManager):
    """
    Server that receives and executes tasks from RemoteExecutor clients.
    Supports SSL/TLS encryption and client certificate verification.
    """

    def __init__(
        self,
        host: str,
        port: int,
        cert_path: typing.Optional[str] = None,
        key_path: typing.Optional[str] = None,
        ca_certs_path: typing.Optional[str] = None,
        require_client_cert: bool = False,
        socket_timeout: float = DEFAULT_TIMEOUT,
    ):
        """
        Initialize the task manager.

        Args:
            host: Host to bind to
            port: Port to listen on
            cert_path: Path to server certificate file
            key_path: Path to server private key file
            ca_certs_path: Path to CA certificates for client verification
            require_client_cert: Whether to require client certificates
            socket_timeout: Socket timeout in seconds
        """
        super().__init__(host=host, port=port)
        self._cert_path = cert_path
        self._key_path = key_path
        self._ca_certs_path = ca_certs_path
        self._require_client_cert = require_client_cert
        self._socket_timeout = socket_timeout

        self._shutdown = False
        self._sock: typing.Optional[socket.socket] = None
        # self._process_context = mp.get_context("spawn")
        # self._process_pool = ProcessPoolExecutor(mp_context=self._process_context)
        self._thread_pool = ThreadPoolExecutor(max_workers=8)

    def _route_tcp_response(self, task_info: typing.Dict, result_data: typing.Dict):
        """Route response via TCP"""
        client_context = task_info.get("client_context")
        if not client_context or not isinstance(client_context, dict):
            logger.error("Invalid client context for TCP response")
            return

        client_sock = client_context.get("socket")

        if not client_sock:
            logger.error(f"No client socket found for TCP response (correlation_id={task_info.get('correlation_id')})")
            return

        try:
            # result_data is a dictionary, use serialize_dict
            response_data = serialize_dict(result_data)

            # We need to lock the socket write to avoid interleaving if multiple threads write to same socket?
            # Or assume 1 socket = 1 client thread?

            data_size = send_data_over_socket(
                client_sock,
                data=response_data,
                chunk_size=CHUNK_SIZE,
            )
            logger.info(f"Routed response via TCP, size: {data_size} bytes")

        except Exception as e:
            logger.error(f"Failed to send response via TCP: {e}. Re-raising for fallback.")
            raise

    def _route_grpc_response(self, task_info: typing.Dict, result_data: typing.Dict):
        pass # Not used in RemoteTaskManager

    def _route_xrpc_response(self, task_info: typing.Dict, result_data: typing.Dict):
        pass # Not used in RemoteTaskManager

    def _create_server_socket(self) -> socket.socket:
        """Create and configure the server socket with proper timeout and SSL if enabled"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # sock.settimeout(self._socket_timeout)
        sock.setblocking(True)

        if not (self._cert_path and self._key_path):
            return sock

        try:
            context = create_server_ssl_context(
                cert_path=self._cert_path,
                key_path=self._key_path,
                ca_certs_path=self._ca_certs_path,
                require_client_cert=self._require_client_cert,
            )

            return context.wrap_socket(sock, server_side=True)
        except (ssl.SSLError, OSError) as e:
            logger.error(f"Failed to create SSL context: {str(e)}", exc_info=e)
            raise

    def _handle_client(
        self, client_sock: socket.socket, client_addr: typing.Tuple[str, int]
    ) -> None:
        """Handle a client connection"""
        client_info = f"{client_addr[0]}:{client_addr[1]}"
        logger.info(f"New client connection from {client_info}")

        try:
            client_sock.settimeout(self._socket_timeout)

            while not self._shutdown:
                try:
                    # Receive task message
                    # This will block until data arrives or timeout
                    msg_data = receive_data_from_socket(client_sock, chunk_size=CHUNK_SIZE)

                    if not msg_data:
                        # Empty data usually means closed connection or client finished sending request
                        # If we assume persistent connection, we wait for more.
                        # If receive_data_from_socket returns empty bytes, it means connection closed.
                        break

                    task_message, is_task_message = deserialize_message(msg_data)
                    if not is_task_message:
                         logger.warning(f"Invalid message received from {client_info}")
                         continue

                    # Handle POLL event
                    if task_message.event == "POLL":
                        target_task_id = task_message.args.get("task_id")
                        if not target_task_id:
                             raise ValueError("POLL event requires 'task_id' in args")

                        from volnux.manager.result_store import get_result_store
                        from volnux.manager.base import get_client_task_registry

                        # 1. Check ResultStore (completed and waiting)
                        result = get_result_store().get(target_task_id)

                        if result:
                            # Found result, send it back immediately
                            response_data = serialize_dict(result)
                            send_data_over_socket(client_sock, data=response_data, chunk_size=CHUNK_SIZE)
                            logger.info(f"Polled result retrieved for {target_task_id}")
                        else:
                            # 2. Check Registry (still processing?)
                            registry = get_client_task_registry()
                            task_info = registry.get_task(target_task_id)

                            status = "PENDING" if task_info else "NOT_FOUND"
                            response_data = serialize_dict({
                                "correlation_id": target_task_id,
                                "status": status
                            })
                            send_data_over_socket(client_sock, data=response_data, chunk_size=CHUNK_SIZE)

                        # Continue loop (don't submit POLL as a task to executor)
                        continue

                    # Context just holds the socket now, no events needed
                    client_context = {
                        "socket": client_sock,
                    }

                    # Dispatch using BaseManager - fire and forget from this thread's perspective
                    # BaseManager will use the correlation_id from task_message (or generating one)
                    # and register this context.
                    self.handle_task(task_message, Protocol.TCP, client_context)

                except socket.timeout:
                    # Timeout on read is fine, just loop to check shutdown or keep alive
                    continue
                except (ConnectionResetError, BrokenPipeError):
                    logger.info(f"Client {client_info} disconnected")
                    break
                except Exception as e:
                    logger.error(f"Error reading/processing message from {client_info}: {e}", exc_info=e)
                    # Try to send error response
                    try:
                        error_result = {
                            "status": "error",
                            "code": "PROCESSING_ERROR",
                            "message": str(e)
                        }
                        response_data = serialize_dict(error_result)
                        send_data_over_socket(client_sock, data=response_data, chunk_size=CHUNK_SIZE)
                    except Exception:
                        pass
                    # If we can't parse messages, we should probably close connection
                    break

        except Exception as e:
            logger.error(f"Error handling connection {client_info}: {str(e)}", exc_info=e)
        finally:
            try:
                client_sock.close()
                logger.debug(f"Closed connection from {client_info}")
            except Exception:
                pass

    def start(self) -> None:
        """Start the task manager with proper error handling"""
        # Start BaseManager router
        super().start()

        try:
            self._sock = self._create_server_socket()
            self._sock.bind((self._host, self._port))
            self._sock.listen(BACKLOG_SIZE)

            logger.info(f"Task manager listening on {self._host}:{self._port}")

            while not self._shutdown:
                try:
                    client_sock, client_addr = self._sock.accept()

                    self._thread_pool.submit(
                        self._handle_client, client_sock, client_addr
                    )
                except socket.error as e:
                    # Check if the error is genuinely just "no connection ready"
                    if e.errno == errno.EAGAIN or e.errno == errno.EWOULDBLOCK:
                        # No connection pending, wait a bit and try again
                        time.sleep(0.1)
                        continue
                    elif e.errno == errno.EINTR:
                        # Interrupted system call (e.g., a signal was received)
                        logger.warning("Accept interrupted by signal, retrying...")
                        continue
                    else:
                        # A real, fatal error occurred
                        logger.error(f"Fatal socket error: {e}")
                        break
                # except socket.timeout:
                #     continue  # Allow checking a shutdown flag
                except Exception as e:
                    if not self._shutdown:
                        logger.error(
                            f"Error accepting client connection: {str(e)}", exc_info=e
                        )

        except Exception as e:
            logger.error(f"Fatal error in task manager: {str(e)}", exc_info=e)
            raise
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Gracefully shutdown the task manager"""
        if self._shutdown:
            return

        self._shutdown = True
        logger.info("Shutting down task manager...")

        # Shutdown BaseManager components
        super().shutdown()

        if self._sock:
            try:
                self._sock.close()
            except Exception as e:
                logger.error(f"Error closing server socket: {str(e)}", exc_info=e)

        if self._thread_pool:
            try:
                self._thread_pool.shutdown(wait=True)
            except Exception as e:
                logger.error(f"Error shutting down thread pool: {str(e)}", exc_info=e)

        logger.info("Task manager shutdown complete")
