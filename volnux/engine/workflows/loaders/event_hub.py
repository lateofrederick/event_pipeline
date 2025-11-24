import typing

from volnux import Event
from volnux.base import EventType


class LoadFromEventHUB(Event):
    name = "event_hub"

    event_type = EventType.SYSTEM

    def process(
        self, *args: typing.Tuple[typing.Any], **kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        raise NotImplementedError()
