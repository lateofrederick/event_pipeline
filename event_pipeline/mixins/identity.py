import typing
from ..utils import generate_unique_id


class ObjectIdentityMixin:

    def __init__(self, *args, **kwargs):
        generate_unique_id(self)

    @property
    def id(self):
        return generate_unique_id(self)

    @property
    def __object_import_str__(self):
        return f"{self.__class__.__module__}.{self.__class__.__name__}"

    def get_state(self) -> typing.Dict[str, typing.Any]:
        raise NotImplementedError()

    def set_state(self, state: typing.Dict[str, typing.Any]) -> None:
        raise NotImplementedError()
