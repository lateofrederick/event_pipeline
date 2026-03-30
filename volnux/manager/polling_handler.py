import json
import logging
import typing
import zlib
from volnux.manager.result_store import get_result_store
from volnux.manager.base import get_client_task_registry

logger = logging.getLogger(__name__)

class PollingResponse:
    def __init__(self, status: int, data: typing.Dict[str, typing.Any]):
        self.status = status
        self.data = data

async def handle_poll_request(correlation_id: str) -> PollingResponse:
    """
    Handle a polling request for a given correlation_id.
    
    Args:
        correlation_id: The task correlation ID.

    Returns:
        PollingResponse: Contains HTTP status code and response data.
    """
    # Check the ResultStore for a complete result
    result_store = get_result_store()
    result = result_store.get(correlation_id)
    
    if result:
        logger.debug(f"Polling SUCCESS for {correlation_id}")
        return PollingResponse(200, result)

    # Check ClientTaskRegistry for a pending task
    registry = get_client_task_registry()
    task_info = registry.get_task(correlation_id)
    
    if task_info:
        # Task is still in registry, so it's pending
        logger.debug(f"Polling PENDING for {correlation_id}")
        response_data = {
            "correlation_id": correlation_id,
            "status": "PENDING"
        }
        return PollingResponse(202, response_data)

    # 3. Not found
    logger.debug(f"Polling NOT_FOUND for {correlation_id}")
    response_data = {
        "correlation_id": correlation_id,
        "status": "NOT_FOUND"
    }
    return PollingResponse(404, response_data)
