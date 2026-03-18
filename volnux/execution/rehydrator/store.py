# import json
# import logging
# import pickle
# import typing
#
# import redis.asyncio as aioredis
#
# logger = logging.getLogger(__name__)


import json
import logging
import typing

from volnux.execution.rehydrator.snapshot import ContextSnapshot
from volnux.execution.rehydrator.serializer import StateSerializer
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

logger = logging.getLogger(__name__)


class PersistentStateStoreMixin:
    """
    Snapshot persistence helper mixin.

    This mixin expects the host class to also inherit from
    KeyValueStoreIntegrationMixin so it can obtain the configured backend
    via get_backend().
    """

    SNAPSHOT_KEY_PREFIX = "volnux:context"
    WORKFLOW_KEY_PREFIX = "volnux:workflow"
    SNAPSHOT_TTL_SECONDS = 86400 * 7

    def _get_backend(self):
        """
        Get the configured backend from KeyValueStoreIntegrationMixin.
        """
        if not hasattr(self, "get_backend"):
            raise RuntimeError(
                "PersistentStateStoreMixin requires KeyValueStoreIntegrationMixin"
            )
        return self.get_backend()

    @staticmethod
    def _normalize_bytes(value: typing.Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    def _snapshot_key(self, state_id: str) -> str:
        return f"{self.SNAPSHOT_KEY_PREFIX}:{state_id}:snapshot"

    def _workflow_contexts_key(self, workflow_id: str) -> str:
        return f"{self.WORKFLOW_KEY_PREFIX}:{workflow_id}:contexts"

    def _workflow_metadata_key(self, workflow_id: str) -> str:
        return f"{self.WORKFLOW_KEY_PREFIX}:{workflow_id}:metadata"

    async def save_snapshot(self, snapshot: ContextSnapshot) -> None:
        """
        Persist a snapshot using the configured backend.
        """
        backend = self._get_backend()
        key = self._snapshot_key(snapshot.state_id)
        data = json.dumps(snapshot.to_dict())

        if hasattr(backend, "redis") and backend.redis is not None:
            async with backend.redis.pipeline(transaction=True) as pipe:
                await pipe.set(key, data)
                await pipe.sadd(
                    self._workflow_contexts_key(snapshot.workflow_id), snapshot.state_id
                )
                await pipe.expire(key, self.SNAPSHOT_TTL_SECONDS)
                await pipe.execute()
        else:
            # Generic fallback for key-value backends that expose async methods
            await backend.upsert("snapshots", snapshot.state_id, snapshot)
            if hasattr(backend, "connector") and hasattr(backend.connector, "set"):
                await backend.connector.set(key, data)

        logger.debug(f"Saved snapshot for context {snapshot.state_id}")

    async def get_snapshot(self, state_id: str) -> typing.Optional[ContextSnapshot]:
        """
        Retrieve a context snapshot by state id.
        """
        backend = self._get_backend()
        key = self._snapshot_key(state_id)

        if hasattr(backend, "redis") and backend.redis is not None:
            data = await backend.redis.get(key)
            if not data:
                return None
            snapshot_dict = json.loads(self._normalize_bytes(data))
            return ContextSnapshot.from_dict(snapshot_dict)

        if hasattr(backend, "get"):
            snapshot = await backend.get("snapshots", state_id, ContextSnapshot)  # type: ignore
            return snapshot

        return None

    async def get_active_snapshots(
        self,
        workflow_id: str,
        statuses: typing.Optional[typing.List[str]] = None,
    ) -> typing.List[ContextSnapshot]:
        """
        Retrieve all snapshots for a workflow, ordered by depth.
        """
        backend = self._get_backend()
        snapshots: typing.List[ContextSnapshot] = []

        if hasattr(backend, "redis") and backend.redis is not None:
            context_ids = await backend.redis.smembers(self._workflow_contexts_key(workflow_id))

            for context_id in context_ids:
                cid = self._normalize_bytes(context_id)
                snapshot = await self.get_snapshot(cid)
                if snapshot and (statuses is None or snapshot.status in statuses):
                    snapshots.append(snapshot)
        else:
            # Generic fallback if backend supports filtering/listing
            if hasattr(backend, "filter"):
                snapshots = await backend.filter("snapshots", ContextSnapshot, workflow_id=workflow_id)  # type: ignore
                if statuses is not None:
                    snapshots = [s for s in snapshots if s.status in statuses]

        snapshots.sort(key=lambda s: s.depth)
        return snapshots

    async def update_snapshot_field(
        self, state_id: str, field_path: str, value: typing.Any
    ) -> None:
        """
        Update a nested field in a snapshot and persist it back.
        """
        snapshot = await self.get_snapshot(state_id)
        if not snapshot:
            logger.warning(f"Snapshot {state_id} not found for update")
            return

        snapshot_dict = snapshot.to_dict()
        keys = field_path.split(".")
        target = snapshot_dict

        for key in keys[:-1]:
            target = target[key]

        target[keys[-1]] = value
        await self.save_snapshot(ContextSnapshot.from_dict(snapshot_dict))

    async def delete_snapshot(self, state_id: str, workflow_id: str) -> None:
        """
        Delete a snapshot and remove it from the workflow index.
        """
        backend = self._get_backend()

        if hasattr(backend, "redis") and backend.redis is not None:
            async with backend.redis.pipeline(transaction=True) as pipe:
                await pipe.delete(self._snapshot_key(state_id))
                await pipe.srem(self._workflow_contexts_key(workflow_id), state_id)
                await pipe.execute()
        else:
            if hasattr(backend, "delete"):
                await backend.delete("snapshots", state_id)

        logger.debug(f"Deleted snapshot for context {state_id}")

    async def cleanup_workflow(self, workflow_id: str) -> int:
        """
        Delete all snapshots for a workflow.
        Returns the number of contexts cleaned up.
        """
        backend = self._get_backend()
        count = 0

        if hasattr(backend, "redis") and backend.redis is not None:
            context_ids = await backend.redis.smembers(self._workflow_contexts_key(workflow_id))

            for context_id in context_ids:
                cid = self._normalize_bytes(context_id)
                await self.delete_snapshot(cid, workflow_id)
                count += 1

            await backend.redis.delete(self._workflow_metadata_key(workflow_id))
        else:
            snapshots = await self.get_active_snapshots(workflow_id)
            for snapshot in snapshots:
                await self.delete_snapshot(snapshot.state_id, workflow_id)
                count += 1

        return count

    async def save_task_template_snapshot(self, task_snapshot: typing.Any) -> None:
        """
        Persist a task template snapshot.
        """
        backend = self._get_backend()
        key = f"volnux:task:{task_snapshot.id}:snapshot"
        data = json.dumps(task_snapshot.to_dict())

        if hasattr(backend, "redis") and backend.redis is not None:
            await backend.redis.set(key, data)
        else:
            await backend.upsert("task_snapshots", task_snapshot.id, task_snapshot)

    async def get_task_template_snapshot(self, task_id: str) -> typing.Optional[typing.Any]:
        """
        Retrieve a task template snapshot by task id.
        """
        backend = self._get_backend()
        key = f"volnux:task:{task_id}:snapshot"

        if hasattr(backend, "redis") and backend.redis is not None:
            data = await backend.redis.get(key)
            if not data:
                return None
            return json.loads(self._normalize_bytes(data))

        if hasattr(backend, "get"):
            return await backend.get("task_snapshots", task_id, object)  # type: ignore

        return None


# class PersistentStateStore:
#     """
#     Redis-backed persistent storage for ExecutionContext snapshots.
#
#     Key Schema:
#         volnux:workflow:{workflow_id}:contexts -> Set of context IDs
#         volnux:context:{state_id}:snapshot -> Hash of snapshot data
#         volnux:context:{state_id}:lock -> Distributed lock key
#         volnux:workflow:{workflow_id}:metadata -> Workflow-level metadata
#     """
#
#     def __init__(self, redis_url: str = "redis://localhost:6379/0"):
#         self.redis_url = redis_url
#         self.redis: typing.Optional[aioredis.Redis] = None
#         self._serializer = StateSerializer()
#
#     async def connect(self):
#         """Initialize Redis connection pool"""
#         self.redis = await aioredis.from_url(
#             self.redis_url,
#             encoding="utf-8",
#             decode_responses=False,  # Handle binary data
#         )
#
#     async def disconnect(self):
#         """Close Redis connection"""
#         if self.redis:
#             await self.redis.close()
#
#     async def save_snapshot(self, snapshot: ContextSnapshot) -> None:
#         """
#         Persist a context snapshot to Redis.
#         Uses Redis Hash for efficient field updates.
#         """
#         key = f"volnux:context:{snapshot.state_id}:snapshot"
#
#         # Serialize to JSON
#         data = json.dumps(snapshot.to_dict())
#
#         async with self.redis.pipeline(transaction=True) as pipe:
#             # Store snapshot
#             await pipe.set(key, data)
#
#             # Add to workflow's context set
#             await pipe.sadd(
#                 f"volnux:workflow:{snapshot.workflow_id}:contexts", snapshot.state_id
#             )
#
#             # Set TTL (optional - for automatic cleanup)
#             await pipe.expire(key, 86400 * 7)  # 7 days
#
#             await pipe.execute()
#
#         logger.debug(f"Saved snapshot for context {snapshot.state_id}")
#
#     async def get_snapshot(self, state_id: str) -> typing.Optional[ContextSnapshot]:
#         """Retrieve a context snapshot"""
#         key = f"volnux:context:{state_id}:snapshot"
#         data = await self.redis.get(key)
#
#         if not data:
#             return None
#
#         snapshot_dict = json.loads(data)
#         return ContextSnapshot.from_dict(snapshot_dict)
#
#     async def get_active_snapshots(
#         self, workflow_id: str, statuses: typing.Optional[typing.List[str]] = None
#     ) -> typing.List[ContextSnapshot]:
#         """
#         Retrieve all context snapshots for a workflow, ordered by depth.
#
#         Args:
#             workflow_id: The workflow identifier
#             statuses: Optional filter for execution statuses
#
#         Returns:
#             List of snapshots sorted by depth (parents before children)
#         """
#         # Get all context IDs for this workflow
#         context_ids = await self.redis.smembers(
#             f"volnux:workflow:{workflow_id}:contexts"
#         )
#
#         snapshots = []
#         for context_id in context_ids:
#             snapshot = await self.get_snapshot(
#                 context_id.decode() if isinstance(context_id, bytes) else context_id
#             )
#             if snapshot:
#                 if statuses is None or snapshot.status in statuses:
#                     snapshots.append(snapshot)
#
#         # Sort by depth to ensure parents are processed before children
#         snapshots.sort(key=lambda s: s.depth)
#
#         return snapshots
#
#     async def update_snapshot_field(
#         self, state_id: str, field_path: str, value: typing.Any
#     ) -> None:
#         """
#         Update a specific field in a snapshot without full rewrite.
#
#         Args:
#             state_id: Context state ID
#             field_path: Dot-notation path (e.g., "status", "metrics.end_time")
#             value: New value
#         """
#         snapshot = await self.get_snapshot(state_id)
#         if not snapshot:
#             logger.warning(f"Snapshot {state_id} not found for update")
#             return
#
#         # Navigate to nested field
#         snapshot_dict = snapshot.to_dict()
#         keys = field_path.split(".")
#         target = snapshot_dict
#
#         for key in keys[:-1]:
#             target = target[key]
#
#         target[keys[-1]] = value
#
#         # Save updated snapshot
#         await self.save_snapshot(ContextSnapshot.from_dict(snapshot_dict))
#
#     async def delete_snapshot(self, state_id: str, workflow_id: str) -> None:
#         """Remove a snapshot from Redis"""
#         async with self.redis.pipeline(transaction=True) as pipe:
#             await pipe.delete(f"volnux:context:{state_id}:snapshot")
#             await pipe.srem(f"volnux:workflow:{workflow_id}:contexts", state_id)
#             await pipe.execute()
#
#     async def cleanup_workflow(self, workflow_id: str) -> int:
#         """
#         Delete all snapshots for a completed workflow.
#
#         Returns:
#             Number of contexts cleaned up
#         """
#         context_ids = await self.redis.smembers(
#             f"volnux:workflow:{workflow_id}:contexts"
#         )
#
#         count = 0
#         for context_id in context_ids:
#             cid = context_id.decode() if isinstance(context_id, bytes) else context_id
#             await self.delete_snapshot(cid, workflow_id)
#             count += 1
#
#         # Remove workflow metadata
#         await self.redis.delete(f"volnux:workflow:{workflow_id}:metadata")
#
#         return count
