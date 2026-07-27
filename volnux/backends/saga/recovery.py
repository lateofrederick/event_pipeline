import logging
import time
from typing import Optional, List, Dict, Any
from formax import BaseModel

from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

logger = logging.getLogger(__name__)


class SagaRecoveryState(KeyValueStoreIntegrationMixin, BaseModel):
    """
    Persisted snapshot for crash-recovery of active sagas.
    """

    saga_id: str
    saga_name: str
    current_step_index: int
    status: str
    steps_state: List[Dict[str, Any]]
    created_at: float
    updated_at: float

    @classmethod
    def get_schema_name(cls) -> str:
        return "volnux_saga_recovery"


class SagaRecoveryManager:
    """Handles durable checkpointing and process crash recovery for Sagas."""

    @classmethod
    async def save_state(
        cls,
        saga_id: str,
        saga_name: str,
        current_step_index: int,
        status: str,
        steps: List[Any],
        created_at: float,
    ) -> SagaRecoveryState:
        """Persists or updates the current point-in-time snapshot."""
        now = time.time()
        steps_state = [
            {
                "index": i,
                "name": s.name,
                "status": s.status.value,
                "attempts": s.attempts,
                "error": str(s.error) if s.error else None,
            }
            for i, s in enumerate(steps)
        ]

        state = SagaRecoveryState(
            saga_id=saga_id,
            saga_name=saga_name,
            current_step_index=current_step_index,
            status=status,
            steps_state=steps_state,
            created_at=created_at,
            updated_at=now,
        )

        # Upsert state using active-record persistence
        await state.save_async()
        logger.debug(
            "Saga recovery checkpoint saved: %s (step %d)", saga_id, current_step_index
        )
        return state

    @classmethod
    async def load_state(cls, saga_id: str) -> Optional[SagaRecoveryState]:
        """Fetches an interrupted saga snapshot by ID."""
        try:
            return await SagaRecoveryState.get_async(saga_id)
        except Exception as exc:
            logger.warning(
                "Failed to load recovery state for saga %s: %s", saga_id, exc
            )
            return None

    @classmethod
    async def delete_state(cls, saga_id: str) -> None:
        """Removes the checkpoint upon successful completion or DLQ escalation."""
        try:
            state = await SagaRecoveryState.get_async(saga_id)
            await state.delete_async()
            logger.debug("Saga recovery state cleaned up: %s", saga_id)
        except Exception as exc:
            logger.warning(
                "Failed to delete recovery state for saga %s: %s", saga_id, exc
            )
