from typing import Union
from datetime import datetime, timezone
from formax import BaseModel, MiniAnnotated, Attrib, InitStrategy, preformat, postformat

from volnux.mixins import KeyValueStoreIntegrationMixin


class GovernanceModel(KeyValueStoreIntegrationMixin, BaseModel):
    """Base model for all governance entities.

    Provides:
        - Self-persistence via KeyValueStoreIntegrationMixin
        - Automatic ForeignKey backreference registration
        - Standard timestamp fields
        - OrJSON serialization support
    """

    creation_time: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    updated_time: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __hash__(self) -> int:
        return hash(self.id)

    def touch(self) -> None:
        """Update the updated_time to now."""
        self.updated_time = datetime.now(timezone.utc).timestamp()

    @postformat(["creation_time", "updated_time"])
    def postformat_timestamps(self, value: float) -> datetime:
        return datetime.fromtimestamp(value, timezone.utc)

    @preformat(["creation_time", "updated_time"])
    def preformat_timestamps(self, value: Union[datetime, float]) -> float:
        if isinstance(value, datetime):
            return value.timestamp()
        return value
