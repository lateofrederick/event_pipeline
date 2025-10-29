import json
import hmac
import hashlib
import typing
import zlib
import pickle
import base64
import cloudpickle
from dataclasses import dataclass, asdict
from typing import Dict


ALGORITHM = "sha256"
SECRET_KEY = "j43fheiue3xvheilmew-xmwy34mcuea"


@dataclass
class TaskMessage:
    """Message format for task communication"""

    task_id: str
    fn: typing.Callable
    args: tuple
    kwargs: dict
    client_cert: typing.Optional[bytes] = None
    encrypted: bool = False

    def serialize(self) -> bytes:
        return self.serialize_object(self)

    @staticmethod
    def serialize_object(obj) -> bytes:
        obj_dict = asdict(obj)
        obj_json = json.dumps(obj_dict, sort_keys=True)

        signature = hmac.new(
            SECRET_KEY,
            obj_json.encode("utf-8"),
            getattr(hashlib, ALGORITHM)
        ).digest()
        
        obj_dict["_signature"] = signature
        obj_dict["_algorithm"] = ALGORITHM
       
        data = json.dumps(obj_dict, sort_keys=True)
        return data.encode("utf-8")

    @staticmethod
    def deserialize(data: bytes) -> typing.Tuple[typing.Any, bool]:
        decompressed_data = zlib.decompress(data)
        decompressed_data = json.loads(decompressed_data)
        
        def verify_data(_data):
            """verify incoming data's signature to check if it is the expected signature."""
            if "_signature" not in _data:
                raise ValueError("signature not found in data")

            received_signature = base64.b64decode(_data["_signature"])

            verification_data = _data.copy()
            verification_data.pop("_signature", None)

            verification_json = json.dumps(verification_data, sort_keys=True)
            expected_signature = hmac.new(
                SECRET_KEY,
                verification_json.encode("utf-8"),
                getattr(hashlib, ALGORITHM)            
            ).digest()

            if not hmac.compare_digest(received_signature, expected_signature):
                raise ValueError("received signature does not match expected signature")
        
        verify_data(decompressed_data)
        
        # remove signature and algorithm
        decompressed_data.pop("_signature", None)
        decompressed_data.pop("_algorithm", None)
        decompressed_data = TaskMessage(**decompressed_data)

        return decompressed_data, isinstance(decompressed_data, TaskMessage)
