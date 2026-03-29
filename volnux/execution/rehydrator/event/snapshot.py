import typing
from enum import IntEnum
from datetime import datetime, timezone
from formax import BaseModel, MiniAnnotated, Attrib, ValidationFlags

from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


class InitArgsTemplate(typing.TypedDict, total=False):
    # The context in which the task was created
    execution_context_id: typing.Optional[str]

    # task identity
    task_id: typing.Optional[str]

    previous_result: typing.List[typing.Union[str, dict]]

    stop_condition: typing.List[str]
    run_bypass_event_checks: bool

    # task configuration
    options: typing.Optional[dict]
    sequence_number: typing.Optional[int]
    kwargs: typing.Dict[str, typing.Any]


class CallArgsTemplate(typing.TypedDict, total=False):
    pass


class EventPhase(IntEnum):
    INITIALIZED = 0
    PRE_PROCESS = 1
    PROCESSING = 2
    POST_PROCESS = 3
    COMPLETED = 4


class ResourceState(typing.TypedDict, total=False):
    """Schema for user-registered external states (e.g., DB cursors, file offsets)."""

    resource_name: str
    data: dict
    provider_path: str  # The import path to the restoration logic


class EventCheckpointSnapshot(KeyValueStoreIntegrationMixin, BaseModel):
    """The 'Source of Truth' persisted in Redis for a specific Task ID."""

    task_id: str
    class_path: str  # e.g., "myapp.events.SendEmailTask"
    phase: EventPhase

    # Arguments used to re-instantiate the class via __init__
    init_args: MiniAnnotated[InitArgsTemplate, Attrib(default_factory=dict)]

    call_args: MiniAnnotated[CallArgsTemplate, Attrib(default_factory=dict)]

    # User-defined external states registered during the 'process' step typing.Dict[str, ResourceState]
    external_resources: MiniAnnotated[
        typing.Dict[str, ResourceState], Attrib(default_factory=dict)
    ]

    # Metadata for the orchestrator (e.g., when it was last touched)
    timestamp: MiniAnnotated[
        float, Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    ]

    ## `process` method return value
    # Execution state captured at the end of the PROCESSING phase
    execution_status: typing.Optional[bool]

    # The result of process() or the error raised
    # This might be a Dict, List, or a Serialized Exception Dict
    execution_result: typing.Any = None

    # Capturing the retry state so preemption doesn't reset attempt counters
    retry_count: int = 0

    class Config:
        validation = ValidationFlags.NONE

    def get_schema_name(cls) -> str:
        return "volnux:event:checkpoint"
