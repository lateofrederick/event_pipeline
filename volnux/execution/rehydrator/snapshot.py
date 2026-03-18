import typing
import logging
from dataclasses import dataclass, asdict
from formax import BaseModel

from volnux import __version__ as volnux_version
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

if typing.TYPE_CHECKING:
    from volnux.parser.operator import PipeType


logger = logging.getLogger(__name__)


class TaskTemplate(typing.TypedDict, total=False):
    # task identity
    task_id: str
    task_type: typing.Literal["normal", "group"]

    # Event information
    event_name: str
    event_class_import_path: str

    # Sink/Deferred Task Information
    sink_node: typing.Optional["TaskTemplate"]
    sink_pipe: typing.Optional["PipeType"]

    # task configuration and states
    options: typing.Dict[str, typing.Any]
    condition_node: typing.List[typing.Dict[str, typing.Any]]
    sequence_number: int
    descriptor: typing.Optional[int]
    descriptor_pipe_type: typing.Optional[str]

    # Grouped task information
    chain: typing.List["TaskTemplate"]
    strategy: typing.Literal["single", "multiple"]


class QueueTaskTemplate(typing.TypedDict, total=True):
    task: TaskTemplate
    position_in_queue: int



@dataclass
class TraversalSnapshot:
    """
    Captures the state of the execution queue at snapshot time.
    """

    # Current task being executed (maybe mid-flight)
    current_task_id: typing.Optional[str] # The active task ID when the snapshot was taken
    current_task_event_name: typing.Optional[str]  # Event name of the current task for debugging
    current_task_checkpoint: typing.Optional[dict]  # For idempotency (i.e. The internal state of the task when it was checkpointed)

    # Remaining tasks in queue (LIFO order preserved)
    # Serialized PipelineTask objects
    queue_snapshot: typing.List[QueueTaskTemplate] # Remaining tasks in the queue at snapshot time

    # Queue position tracking
    # Position in the original queue
    queue_index: int # The index of the current task in the original queue
    total_queue_size: int # The total size of the original queue

    # Sink nodes (deferred execution)
    # Serialized sink tasks
    sink_nodes: typing.List[QueueTaskTemplate]

    # Engine state markers
    tasks_processed: int  # How many tasks completed before snapshot?
    is_multitask_context: bool  # Was this a parallel execution group?


class ContextSnapshot(KeyValueStoreIntegrationMixin, BaseModel):
    """
    Serializable snapshot of ExecutionContext state.
    This is the canonical data structure for rehydration.
    """

    # Identity
    state_id: str
    workflow_id: str

    # Hierarchy (Tree Structure)
    parent_id: typing.Optional[str]
    child_ids: typing.List[str]
    depth: int

    # Horizontal Links (Doubly-Linked List)
    previous_context_id: typing.Optional[str]
    next_context_id: typing.Optional[str]

    # Task Queue State
    traversal: TraversalSnapshot

    # Pipeline Reference
    pipeline_id: str
    pipeline_class_path: str  # e.g., "myapp.pipelines.DataProcessingPipeline"

    # Execution State
    status: str
    errors: typing.List[str]  # Serialized exception messages
    results: typing.List[dict]  # Serialized EventResult objects

    # Metrics
    metrics: typing.Dict[str, typing.Any]

    # Metadata
    snapshot_timestamp: float
    snapshot_version: str = volnux_version

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: typing.Dict[str, typing.Any]) -> "ContextSnapshot":
        data["traversal"] = TraversalSnapshot(**data["traversal"])
        return cls(**data)

    def set_state(self, state: typing.Dict[str, typing.Any]) -> None:
        self.from_dict(state)

    def get_state(self) -> typing.Dict[str, typing.Any]:
        state = self.__dict__.copy()
        traversal_state = state["traversal"].__dict__.copy()
        state["traversal"] = traversal_state
        return state
