from typing import TYPE_CHECKING

from .snapshot import EventCheckpointSnapshot
from .serializer import StateSerializer
from volnux.utils import get_obj_klass_import_str

if TYPE_CHECKING:
    from volnux import EventBase


class SnapshotBuilder:
    """Builds a snapshot of an event for checkpointing and rehydration.

    This class is responsible for serializing event data into a format suitable
    for checkpointing and rehydration. It uses a StateSerializer to handle
    the serialization of event attributes.
    """

    def __init__(self, serializer=StateSerializer):
        self.serializer = serializer

    async def build(self, event: "EventBase") -> EventCheckpointSnapshot:

        return EventCheckpointSnapshot(
            task_id=event._task_id,
            class_path=get_obj_klass_import_str(event),
            phase=event.get_phash(),
            init_args=self.serializer.serialize_init_args(event.get_init_args()),
            call_args=self.serializer.serialize_call_args(event.get_call_args()),
            retry_count=event._retry_count,
        )
