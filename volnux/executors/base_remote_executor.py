import base64
import concurrent
import hashlib
import hmac
import json
import typing
import uuid
from concurrent.futures import Executor

from volnux.conf import ConfigLoader
from volnux.executors.message import TaskMessage

CONF = ConfigLoader.get_lazily_loaded_config()
ALGORITHM = "sha256"


def get_secret_key() -> bytes:
    """Retrieve the secret key from configuration and ensure it is bytes."""
    key = CONF.SECRET_KEY
    if isinstance(key, str):
        return key.encode("utf-8")
    return key


class BaseRemoteExecutor(Executor):
    def _generate_hmac(self, data: typing.Dict[str, typing.Any]) -> typing.Tuple[str, str]:
        """Generate a signature for the payload."""
        data_bytes = json.dumps(data, sort_keys=True).encode("utf-8")

        signature = hmac.new(
            get_secret_key(), data_bytes, getattr(hashlib, ALGORITHM)
        ).digest()

        return base64.b64encode(signature).decode("utf-8"), ALGORITHM

    def construct_payload(self, data: TaskMessage) -> typing.Dict[str, typing.Any]:
        """Construct the payload to send to the remote manager."""
        dict_data = data.dump(_format="dict")

        if not self._is_payload_serializable(dict_data):
            raise ValueError("Payload is not serializable")

        signature, algorithm = self._generate_hmac(dict_data)
        dict_data["_signature"] = signature
        dict_data["_algorithm"] = algorithm

        return dict_data

    def _is_payload_serializable(self, payload: typing.Dict[str, typing.Any]) -> bool:
        """Check if the payload can be serialized."""
        try:
            json.dumps(payload)
        except (TypeError, OverflowError) as e:
            return False
        return True

    def generate_correlation_id(self) -> str:
        """Generate a unique correlation ID."""
        return str(uuid.uuid4())

    def submit(
            self, fn: typing.Callable, /, *args, **kwargs
    ) -> concurrent.futures.Future:
        pass
