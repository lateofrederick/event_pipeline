import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from formax import Attrib, BaseModel, MiniAnnotated, preformat, postformat

from .triggers import TriggerLifecycle
from volnux.mixins import KeyValueStoreIntegrationMixin

logger = logging.getLogger(__name__)


def datetime_to_str(dt: datetime) -> str:
    if isinstance(dt, str):
        return dt
    return dt.astimezone(timezone.utc).isoformat()


def str_to_datetime(dt: str) -> datetime:
    return datetime.fromisoformat(dt).replace(tzinfo=timezone.utc)


class TriggerStateRecord(KeyValueStoreIntegrationMixin, BaseModel):
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

    @classmethod
    async def mark_clean(cls, trigger_id: str) -> None:
        record = await cls.get_or_none_async(record_id=trigger_id)
        if record is None:
            logging.warning(
                f"TriggerStateRecord not found for trigger_id: {trigger_id}"
            )
            return
        record.dirty = False
        await record.save_async()

    async def start(self):
        if self.lifecycle == TriggerLifecycle.ACTIVE:
            raise RuntimeError(
                f"Trigger '{self.id}' is already active and cannot be started again."
            )

        self.lifecycle = TriggerLifecycle.ACTIVE
        self.enabled = True
        self.dirty = True
        await self.save_async()
        logger.info(f"Trigger '{self.id}' started.")

    async def stop(self):
        if self.lifecycle == TriggerLifecycle.STOPPED:
            raise RuntimeError(
                f"Trigger '{self.id}' is already stopped and cannot be stopped again."
            )

        self.lifecycle = TriggerLifecycle.STOPPED
        self.enabled = False
        self.dirty = True
        await self.save_async()
        logger.info(f"Trigger '{self.id}' stopped.")

    async def pause(self):
        """
        Pause the trigger (temporarily disable).

        Raises:
            RuntimeError: If the trigger is not in the ACTIVE state.
        """

        if self.lifecycle not in (TriggerLifecycle.ACTIVE,):
            raise RuntimeError(
                f"Cannot pause trigger '{self.id}': "
                f"current lifecycle is '{self.lifecycle.value}', expected 'active'."
            )
        self.enabled = False
        self.lifecycle = TriggerLifecycle.PAUSED
        self.dirty = True
        await self.save_async()
        logger.info(f"Trigger '{self.id}' paused.")

    async def resume(self):
        """
        Resume a paused trigger.

        Raises:
            RuntimeError: If the trigger is not in the PAUSED state.
        """

        if self.lifecycle not in (TriggerLifecycle.PAUSED,):
            raise RuntimeError(
                f"Cannot resume trigger '{self.id}': "
                f"current lifecycle is '{self.lifecycle.value}', expected 'paused'."
            )
        self.enabled = True
        self.lifecycle = TriggerLifecycle.ACTIVE
        self.dirty = True
        await self.save_async()
        logger.info(f"Trigger '{self.id}' resumed.")
