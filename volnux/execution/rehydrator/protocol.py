from typing import Protocol

class Monitorable(Protocol):
    """
    Protocol for monitorable objects that can be periodically snapshotted.
    """
    async def create_snapshot(self, *args, **kwargs) -> "Snapshot": ...


class Snapshot(Protocol):
    """
    Protocol for snapshot objects that can be saved and restored.
    """

    async def save_async(self, force_inert: bool = False, ttl: int = 0): ...

    async def restore(self) -> object: ...