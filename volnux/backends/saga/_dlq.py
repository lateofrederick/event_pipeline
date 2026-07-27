from typing import Any, Callable, Dict, List
from formax import BaseModel

from volnux.mixins.messaging import MessagingBackendIntegrationMixin
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


class DeadLetterEntry(
    KeyValueStoreIntegrationMixin, MessagingBackendIntegrationMixin, BaseModel
):
    saga_id: str
    saga_name: str
    timestamp: float
    failed_step_index: int
    failed_step_name: str
    original_error: str
    compensation_failures: List[Dict[str, Any]]
    steps_state: List[Dict[str, Any]]

    @classmethod
    def get_schema_name(cls) -> str:
        return "volnux:saga:dlq"
