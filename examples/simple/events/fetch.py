import typing
from concurrent.futures import ThreadPoolExecutor
from event_pipeline import EventBase


class Fetch(EventBase):
    executor = ThreadPoolExecutor

    def process(self, name) -> typing.Tuple[bool, typing.Any]:
        print(f"Executed fetch event: {name}")
        raise ValueError
        return True, "Executed fetch event"
