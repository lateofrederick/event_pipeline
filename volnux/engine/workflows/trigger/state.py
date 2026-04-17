import logging
import typing
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from formax import Attrib, BaseModel, MiniAnnotated, preformat, postformat

from volnux.backends.store import KeyValueStoreBackendBase
from .triggers import TriggerLifecycle
from volnux.mixins import KeyValueStoreIntegrationMixin


def datetime_to_str(dt: datetime) -> str:
    if isinstance(dt, str):
        return dt
    return dt.astimezone(timezone.utc).isoformat()


def str_to_datetime(dt: str) -> datetime:
    return datetime.fromisoformat(dt).replace(tzinfo=timezone.utc)


class TriggerStateRecord(KeyValueStoreIntegrationMixin, BaseModel):
    trigger_id: str
    workflow_name: str
    lifecycle: TriggerLifecycle
    enabled: bool
    fire_count: MiniAnnotated[int, Attrib(default=0)]
    error_count: MiniAnnotated[int, Attrib(default=0)]
    last_fired: Optional[str]
    # dirty=True means the CLI wrote a change the engine hasn't applied yet
    dirty: MiniAnnotated[bool, Attrib(default=False)]
    updated_at: MiniAnnotated[
        str, Attrib(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ]

    def __post_init__(
        self,
        autosave: bool = False,
        storage_backend: typing.Optional[KeyValueStoreBackendBase] = None,
    ) -> None:
        self.change_object_id(self.trigger_id)
        super().__post_init__(autosave=autosave, storage_backend=storage_backend)

    @preformat(["last_fired", "updated_at"], 1)
    def serialize_datetime(self, value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        return datetime_to_str(value)

    @postformat(["last_fired", "updated_at"], 1)
    def deserialize_datetime(self, value: Optional[str]) -> Optional[datetime]:
        if value is None:
            return None
        return str_to_datetime(value)

    @classmethod
    def get_backend_config(cls) -> Dict[str, Any]:
        return {
            "ENGINE": "volnux.backends.stores.sqlite_store.SqliteStoreBackend",
            "CONNECTOR_CONFIG": {
                "database": "volnux.db",
            },
        }

    @classmethod
    async def get_dirty(cls) -> List["TriggerStateRecord"]:
        records = await cls.filter_async(dirty=True)
        return records

    async def mark_clean(self, trigger_id: str) -> None:
        record = await self.get_or_none_async(record_id=trigger_id)
        if record is None:
            logging.warning(
                f"TriggerStateRecord not found for trigger_id: {trigger_id}"
            )
            return
        record.dirty = False
        await record.save_async()
