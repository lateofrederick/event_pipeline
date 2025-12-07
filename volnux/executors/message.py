import json
import typing
import zlib

from pydantic_mini import Attrib, BaseModel, MiniAnnotated

from nexus.exceptions import RemoteExecutionError

from .checksum import generate_signature, verify_data


def ensure_json_serializable(instance, v: typing.Any) -> typing.Any:
    try:
        json.dumps(v)
    except (TypeError, OverflowError) as e:
        raise ValueError(f"Value is not JSON serializable: {e}")
    return v


class TaskMessage(BaseModel):
    """Message format for task communication"""

    event: str
    args: MiniAnnotated[
        typing.Dict[str, typing.Any], Attrib(validators=[ensure_json_serializable])
    ]

    def serialize(self) -> bytes:
        return self.serialize_object(self)

    @staticmethod
    def serialize_object(obj) -> bytes:
        obj_dict = obj.dump(_format="dict")

        signature, algorithm = generate_signature(obj)
        obj_dict["_signature"] = signature
        obj_dict["_algorithm"] = algorithm

        data = json.dumps(obj_dict, sort_keys=True)
        return data.encode("utf-8")

    @staticmethod
    def deserialize(data: str) -> typing.Tuple[typing.Any, bool]:
        decompressed_data = zlib.decompress(data)
        decompressed_data = json.loads(decompressed_data)

        if not verify_data(decompressed_data):
            raise RemoteExecutionError("INVALID_CHECKSUM")

        # remove signature and algorithm
        decompressed_data.pop("_signature", None)
        decompressed_data.pop("_algorithm", None)
        decompressed_data = TaskMessage(**decompressed_data)

        return decompressed_data, isinstance(decompressed_data, TaskMessage)
