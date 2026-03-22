import json
import typing
from abc import ABC, abstractmethod


class CheckpointManagerBase(ABC):
    @abstractmethod
    def save(self, event_instance: "EventBase") -> None:
        """Persists the current state of the event instance."""
        pass

    @abstractmethod
    def load(self, task_id: str) -> typing.Optional[dict]:
        """Retrieves the persisted state for a given task ID."""
        pass

    @abstractmethod
    def clear(self, task_id: str) -> None:
        """Removes the checkpoint once the task is fully completed."""
        pass

    @abstractmethod
    def register_resource(
        self, name: str, provider: callable, is_persistent: bool = True
    ):
        """Standardizes how users attach external resources."""
        pass
