from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Protocol,
    Tuple,
    Type,
    Union,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from volnux.result import EventResult
    from volnux.execution.context import ExecutionContext
    from volnux.mixins.event import RetryPolicy
    from volnux.execution.rehydrator.event.snapshot import EventPhase
    from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager
    from volnux.execution.rehydrator.event.snapshot import (
        EventCheckpointSnapshot,
        ResourceState,
    )

    from volnux.execution.rehydrator.event.resources import ResourceProvider


class BaseEvent(Protocol):
    """
    Represents the base interface for event processing, including retry mechanisms,
    resource management, and execution status tracking.

    This abstract class defines the contract for managing execution retries,
    handling resources, creating snapshots, and processing events through various
    stages. It serves as a template for building systems with event-oriented
    processing workflows. The class encapsulates utility methods for retrying
    tasks, registering and restoring resources, managing checkpoints, and handling
    execution outcomes.

    :ivar retry_policy: Defines the retry policy for the event.
    :type retry_policy: RetryPolicy
    :ivar exec_result: The result of the execution.
    :type exec_result: Any
    :ivar exec_status: Indicates the success or failure status of the execution.
    :type exec_status: bool
    :ivar checkpoint_manager: Manages checkpoints for event processing.
    :type checkpoint_manager: VolnuxCheckPointManager
    :ivar run_bypass_event_checks: Indicates whether event checks should be bypassed.
    :type run_bypass_event_checks: bool
    """

    # Retry configuration
    _retry_count: int
    retry_policy: "RetryPolicy"

    # state management
    _execution_context: "ExecutionContext"
    _task_id: str
    _sequence_number: int

    # Execution status
    _phase: "EventPhase"
    exec_result: Any
    exec_status: bool
    checkpoint_manager: "VolnuxCheckPointManager"

    # Resource management
    run_bypass_event_checks: bool
    _init_args: dict
    _call_args: dict
    _external_resources: Dict[str, "ResourceState"]

    # communication
    _paused: bool
    _command_channel: Any

    def init_retry(self) -> Union["RetryPolicy", None]:
        """
        Initialize the retry policy for the given context.

        This method is used to configure the retry policy settings, if applicable.
        It can return a specific instance of a RetryPolicy, or None if no retries
        are to be applied.

        :return: An instance of RetryPolicy if a retry policy is defined,
            else returns None.
        :rtype: Union[RetryPolicy, None]
        """
        ...

    def register_resource(
        self,
        resource_name: str,
        resource: Any,
        provider: Union[str, Type["ResourceProvider"]],
    ) -> None:
        """
        Registers a resource with a specified name, resource instance, and provider.
        This function associates a resource with a given provider, enabling the system
        to manage or utilize the resource effectively.

        :param resource_name: The name assigned to identify the resource.
        :type resource_name: str
        :param resource: The resource instance to be registered.
        :type resource: Any
        :param provider: The provider associated with the resource, which can be
            a string identifier or a type of ResourceProvider.
        :type provider: Union[str, Type[ResourceProvider]]
        :return: None
        """
        ...

    def restore_resource(
        self, resource_name: str, resource_config: "ResourceState"
    ) -> None:
        """
        Restores the state of a specified resource using the provided configuration.

        This method reinitializes or reconfigures a resource to a previous or desired
        state defined by the resource configuration.

        :param resource_name: The name of the resource to be restored.
        :param resource_config: The configuration object containing the specific state
            details for the resource.
        :return: None
        """
        ...

    def can_bypass_current_event(self) -> Tuple[bool, Any]: ...

    def is_exhausted(self) -> bool: ...

    def is_retryable(self, exception: Exception) -> bool: ...

    async def _sleep_for_backoff(self) -> float: ...

    async def _retry(
        self,
        func: Callable[[Any], Awaitable[Tuple[bool, Any]]],
        /,
        *args: Tuple[Any],
        **kwargs: Dict[str, Any],
    ) -> Tuple[bool, Any]: ...

    async def process(self, *args, **kwargs) -> Tuple[bool, Any]: ...

    def on_success(self, execution_result: Any) -> "EventResult": ...

    def on_failure(self, execution_result: Any) -> "EventResult": ...

    async def create_snapshot(self) -> "EventCheckpointSnapshot": ...

    async def enqueue_checkpoint(self) -> None: ...

    async def _run_step(self, step, *args, **kwargs) -> Any: ...

    async def _process_wrapper(
        self, *args, **kwargs
    ) -> Union["EventResult", tuple]: ...

    def _get_steps(self) -> List[Callable[..., Any]]: ...
