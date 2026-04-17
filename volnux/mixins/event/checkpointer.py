import logging
import inspect
from typing import (
    List,
    Dict,
    Tuple,
    Any,
    Callable,
    Union,
    Type,
    Optional,
    TYPE_CHECKING,
)
from dataclasses import dataclass

from volnux.import_utils import import_string
from volnux.constants import EMPTY
from volnux.config import VolnuxConfig
from volnux.result import EventResult
from volnux.mixins._phase_decorator import phase_step
from volnux.signal.signals import event_init
from volnux.concurrency.async_utils import to_thread
from volnux.parser.options import Options, StopCondition
from volnux.exceptions import MaxRetryError, SuspendTask, SwitchTask
from volnux.utils import get_function_call_args
from volnux.execution.rehydrator.event.snapshot import EventPhase
from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager
from volnux.execution.rehydrator.event.snapshot import (
    EventCheckpointSnapshot,
    ResourceState,
)
from volnux.execution.rehydrator.event.builder import SnapshotBuilder
from volnux.execution.rehydrator.event.resources import ResourceProvider
from volnux.mixins.protocols.event import BaseEvent as _BaseEvent

if TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext


logger = logging.getLogger(__name__)

conf = VolnuxConfig.get_instance()


@dataclass
class StopConditionProcessor:
    """
    Processor for handling stop conditions with improved error handling and flexibility.
    Attributes:
        stop_condition: The condition that determines when to stop processing
        exception: Any exception that occurred during processing
        message: Optional message for logging or debugging
    """

    stop_condition: Union[StopCondition, List[StopCondition]]
    exception: Optional[Exception] = None
    message: Optional[str] = None

    def should_stop(self, success: bool = True) -> bool:
        """
        Determine if processing should stop based on the current state.
        Args:
            success: Whether the operation was successful
        Returns:
            bool: True if processing should stop, False otherwise
        """
        if self.stop_condition == StopCondition.NEVER:
            return False

        if isinstance(self.stop_condition, (list, tuple)):
            return any(
                self._evaluate_single_condition(cond, success)
                for cond in self.stop_condition
            )

        return self._evaluate_single_condition(self.stop_condition, success)

    def _evaluate_single_condition(
        self, condition: StopCondition, success: bool
    ) -> bool:
        """Evaluate a single stop condition."""
        if condition == StopCondition.NEVER:
            return False
        elif condition == StopCondition.ON_SUCCESS:
            return success and self.exception is None
        elif condition == StopCondition.ON_ERROR:
            return not success or self.exception is not None
        elif condition == StopCondition.ON_ANY:
            return True
        else:
            logger.warning(f"Unknown stop condition: {condition}")
            return False

    def on_success(self) -> bool:
        """
        Handle successful operation completion.
        Returns:
            bool: True if processing should stop
        """
        self.exception = None
        should_stop = self.should_stop(success=True)

        if should_stop:
            self._log_stop_decision("success")

        return should_stop

    def on_error(self, exception: Exception, message: Optional[str] = None) -> bool:
        """
        Handle error during operation.
        Args:
            exception: The exception that occurred
            message: Optional additional message
        Returns:
            bool: True if processing should stop
        """
        self.exception = exception
        if message:
            self.message = message

        should_stop = self.should_stop(success=False)

        if should_stop:
            self._log_stop_decision("error")
        return should_stop

    def reset(self) -> None:
        """Reset the processor state for reuse."""
        self.exception = None
        self.message = None

    def _log_stop_decision(self, event_type: str) -> None:
        """Log the stop decision with context."""
        context = {
            "event_type": event_type,
            "stop_condition": self.stop_condition,
            "has_exception": self.exception is not None,
            "message": self.message,
        }

        if self.exception:
            logger.info(
                f"Stopping on {event_type} due to {self.stop_condition}", extra=context
            )
        else:
            logger.debug(
                f"Stopping on {event_type} due to {self.stop_condition}", extra=context
            )

    def get_status(self) -> Dict[str, Any]:
        """Get the current processor status for debugging."""
        return {
            "stop_condition": self.stop_condition,
            "has_exception": self.exception is not None,
            "exception_type": type(self.exception).__name__ if self.exception else None,
            "message": self.message,
        }


class EventCheckPointingMixin:

    @phase_step(EventPhase.INITIALIZED)
    def _setup_event(
        self: _BaseEvent,
        execution_context: "ExecutionContext",
        task_id: str,
        *args: Tuple[Any],
        checkpoint_manager: Optional[VolnuxCheckPointManager] = None,
        previous_result: Union[List[EventResult], EMPTY] = EMPTY,
        stop_condition: StopCondition = StopCondition.NEVER,
        run_bypass_event_checks: bool = False,
        options: Optional["Options"] = None,
        sequence_number: Optional[int] = None,
    ):
        """
        Initialize resource tracking and set up the event's required configurations. This method
        prepares the event for its execution by setting various attributes such as task-related
        details, previous results, stop conditions, and optional configurations.

        :param execution_context: The execution context within which the event operates.
        :type execution_context: ExecutionContext
        :param task_id: Unique identifier for the task associated with this event.
        :type task_id: str
        :param args: Additional positional arguments passed to the event setup.
        :type args: Tuple[Any]
        :param checkpoint_manager: Optional checkpoint manager for managing event states. If not
            provided, no checkpointing will be performed.
        :type checkpoint_manager: Optional[VolnuxCheckPointManager]
        :param previous_result: A list of previous event results or EMPTY if no prior results
            exist.
        :type previous_result: Union[List[EventResult], EMPTY]
        :param stop_condition: Condition under which event execution is terminated. Defaults
            to `StopCondition.NEVER`.
        :type stop_condition: StopCondition
        :param run_bypass_event_checks: Flag indicating if event checks should be bypassed.
            Defaults to False.
        :type run_bypass_event_checks: bool
        :param options: Optional configuration or settings for the event.
        :type options: Optional[Options]
        :param sequence_number: Optional sequence number indicating the order of the event.
        :type sequence_number: Optional[int]
        :return: None
        """

        self._external_resources: Dict[str, ResourceState] = {}

        self._execution_context = execution_context

        # Task ID
        self._task_id = task_id
        self._sequence_number = sequence_number

        # Configurations
        self.options = options

        # The previous result of the event, if any.
        self.previous_result = previous_result
        self.stop_condition = StopConditionProcessor(stop_condition=stop_condition)
        self.run_bypass_event_checks = run_bypass_event_checks

        # Retry configuration
        self._retry_count = 0
        self.init_retry()

        # The executor used to execute the event.
        self.exec_status: bool = False
        self.exec_result: Any = None

        self._init_args = get_function_call_args(self.__class__.__init__, locals())  # type: ignore
        self._init_args.pop("checkpoint_manager", None)
        self._call_args = EMPTY

        self._phase: EventPhase = EventPhase.INITIALIZED
        self.checkpoint_manager = checkpoint_manager

        event_init.emit(sender=self.__class__, event=self, init_kwargs=self._init_args)

    @phase_step(EventPhase.PRE_PROCESS)
    def _pre_process(self: _BaseEvent, *args, **kwargs):
        """
        Handles the pre-processing phase of an event within the event lifecycle. This method
        is executed with the intention of determining whether the event execution should proceed
        or be bypassed, based on custom conditions.

        The behavior of this method can be controlled by the `run_bypass_event_checks` attribute
        of the event instance. If this attribute is set to `True`, it attempts to validate event
        bypass prerequisites by calling the `can_bypass_current_event` method. If these checks
        indicate that the event can be skipped, the event execution completes successfully without
        proceeding further, and the bypass conditions are logged.

        :param args: Positional arguments passed to the pre-processing handler.
        :type args: tuple
        :param kwargs: Keyword arguments passed to the pre-processing handler.
        :type kwargs: dict
        :return: If event bypass checks indicate skipping the event, returns the success
            result containing a dictionary with the bypass status, data, and status flag.
        :rtype: dict, optional
        """
        if self.run_bypass_event_checks:
            try:
                should_skip, data = self.can_bypass_current_event()
            except Exception as e:
                logger.error(
                    "Error in event setup status checks: %s", str(e), exc_info=e
                )
                raise

            if should_skip:
                execution_result = {
                    "status": 1,
                    "skip_event_execution": should_skip,
                    "data": data,
                }
                return self.on_success(execution_result)

    @phase_step(EventPhase.PROCESSING)
    async def _process(self: _BaseEvent, *args, **kwargs):
        if self.retry_policy is None:
            self.exec_status, self.exec_result = await self._process_wrapper(
                *args, **kwargs
            )
        else:
            try:
                self.exec_status, exec_result = await self._retry(
                    self._process_wrapper, *args, **kwargs
                )
            except MaxRetryError as e:
                logger.error(str(e), exc_info=e.exception)
                self.exec_status, self.exec_result = False, e.exception
            except Exception as e:
                if not isinstance(e, SwitchTask):
                    logger.error(str(e), exc_info=e)
                self.exec_status, self.exec_result = False, e

    @phase_step(EventPhase.POST_PROCESS)
    def _post_process(self: _BaseEvent, *args, **kwargs) -> EventResult:
        result = (
            self.on_success(self.exec_result)
            if self.exec_status
            else self.on_failure(self.exec_result)
        )
        return result

    @phase_step(EventPhase.COMPLETED)
    def _completed(self, *args, **kwargs):
        """Cleanup resources after completion."""
        for resource_name, resource_config in self._external_resources.items():
            provider_path = resource_config.get("provider_path")
            if provider_path:
                try:
                    provider_class = import_string(provider_path)
                    if issubclass(provider_class, ResourceProvider):
                        # Get the restored resource
                        resource = getattr(self, f"_{resource_name}", None)
                        if resource:
                            provider_class.cleanup(resource)
                            logger.debug(f"Cleaned up resource '{resource_name}'")
                except Exception as e:
                    logger.warning(f"Failed to cleanup resource '{resource_name}': {e}")

        return None

    def _get_steps(self) -> List[Callable]:
        """
        Retrieves the list of internal processing steps for the object's lifecycle.

        :return: List of method references representing the sequence of internal steps.
        :rtype: list[Callable]
        """
        return [
            self._setup_event,
            self._pre_process,
            self._process,
            self._post_process,
            self._completed,
        ]

    async def _run_step(self, step, *args, **kwargs):
        """
        Executes a single step of a process, supporting both synchronous and asynchronous steps.
        Provides detailed logging for debugging purposes, including the step name and associated
        task identifier.

        :param step: Step function to be executed. Can be a coroutine or a regular
            synchronous function.
        :type step: Callable
        :param args: Positional arguments to pass to the step function.
        :param kwargs: Keyword arguments to pass to the step function.
        :return: The result of the executed step function.
        :rtype: Any
        """
        step_name = step.__name__
        logger.debug("Running step %s for task_id=%s", step_name, self._task_id)

        if inspect.iscoroutinefunction(step):
            return await step(*args, **kwargs)
        return await to_thread(step, *args, **kwargs)

    async def _process_wrapper(
        self: _BaseEvent, *args, **kwargs
    ) -> Union[EventResult, tuple]:
        """
        Execute the process method of the event with provided arguments and handle
        results returned by the process method. Ensures that the result adheres
        to the required format.

        The process method can be either asynchronous or synchronous. If it's
        synchronous, it will be executed in a different thread using the to_thread
        utility.

        Validation of the process result ensures:
          - The result is not None.
          - The result is either an instance of EventResult or a tuple type.
          - If it's a tuple, the first element of the tuple must be a boolean.

        :param args: Positional arguments to be passed to the event's process method.
        :type args: Any
        :param kwargs: Keyword arguments to be passed to the event's process method.
        :type kwargs: Any
        :return: The result returned by the process method. This could be an instance
                 of EventResult or a tuple.
        :rtype: Union[EventResult, tuple]
        :raises ValueError: If the process result is None, does not adhere to the
                            required format, or if a tuple does not have a boolean
                            as its first element.
        """
        if inspect.iscoroutinefunction(self.process):
            result = await self.process(*args, **kwargs)
        else:
            result = await to_thread(self.process, *args, **kwargs)

        is_event_result = isinstance(result, EventResult)
        is_tuple = isinstance(result, tuple)

        if result is None:
            raise ValueError("Process result cannot be None")
        if not is_event_result and not is_tuple:
            raise ValueError(
                "Process result must be a tuple with two elements or an instance of EventResult"
            )
        if is_tuple and not isinstance(result[0], bool):
            raise ValueError("First element of process result must be a boolean")

        if is_event_result:
            # result = cast(EventResult, result)
            return result.success, result

        return result

    def register_resource(
        self,
        resource_name: str,
        resource: Any,
        provider: Union[str, Type[ResourceProvider]],
    ) -> None:
        """
        Register an external resource for checkpointing.

        The provider must be a subclass of ResourceProvider implementing:
        - save_state(resource) -> dict: Serialize resource state
        - restore_state(data: dict) -> resource: Recreate resource from state
        - cleanup(resource) (optional): Clean up resource

        Args:
            resource_name: Unique identifier for this resource
            resource: The resource object to checkpoint
            provider: Either:
                - Import path string (e.g., "myapp.FileProvider")
                - ResourceProvider class directly

        Raises:
            TypeError: If provider is not a ResourceProvider subclass
            ValueError: If provider doesn't implement required methods

        Example:
            ```python
            from volnux.execution.rehydrator.builtin_providers import FileHandleProvider

            file_handle = open("data.txt", "r")

            # Using class directly
            self.register_resource("data_file", file_handle, FileHandleProvider)

            # Or using import string
            self.register_resource(
                "data_file",
                file_handle,
                "volnux.execution.rehydrator.builtin_providers.FileHandleProvider"
            )
            ```
        """
        try:
            # Import provider if string
            if isinstance(provider, str):
                provider_class = import_string(provider)
                provider_path = provider
            else:
                provider_class = provider
                provider_path = f"{provider.__module__}.{provider.__name__}"

            # Validate provider is a ResourceProvider subclass
            if not issubclass(provider_class, ResourceProvider):
                raise TypeError(
                    f"Provider must be a subclass of ResourceProvider, "
                    f"got {type(provider_class)}"
                )

            # Validate required methods exist
            if not hasattr(provider_class, "save_state"):
                raise ValueError(
                    f"Provider {provider_class.__name__} must implement save_state() method"
                )

            if not hasattr(provider_class, "restore_state"):
                raise ValueError(
                    f"Provider {provider_class.__name__} must implement restore_state() method"
                )

            # Save resource state using provider
            resource_data = provider_class.save_state(resource)

            # Validate that saved data is a dict
            if not isinstance(resource_data, dict):
                raise ValueError(
                    f"Provider save_state() must return a dict, "
                    f"got {type(resource_data)}"
                )

            # Store in external_resources
            self._external_resources[resource_name] = {
                "resource_name": resource_name,
                "data": resource_data,
                "provider_path": provider_path,
            }

            logger.debug(
                f"Registered resource '{resource_name}' with provider {provider_class.__name__}"
            )

        except ImportError as e:
            logger.error(f"Failed to import provider {provider}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to register resource '{resource_name}': {e}")
            raise

    def restore_resource(
        self, resource_name: str, resource_config: "ResourceState"
    ) -> None:
        """
        Restore an external resource from checkpoint data.

        This method is called automatically during event resumption.

        Args:
            resource_name: Name of the resource to restore
            resource_config: ResourceState dict with restoration data
        """
        try:
            provider_path = resource_config.get("provider_path")
            if not provider_path:
                logger.warning(
                    f"No provider_path for resource '{resource_name}', skipping restoration"
                )
                return

            # Import and validate provider
            provider_class = import_string(provider_path)

            if not issubclass(provider_class, ResourceProvider):
                raise TypeError(
                    f"Provider {provider_path} is not a ResourceProvider subclass"
                )

            # Restore resource using provider
            resource_data = resource_config.get("data", {})
            restored_resource = provider_class.restore_state(resource_data)

            # Attach to event instance (convention: prefix with underscore)
            setattr(self, f"_{resource_name}", restored_resource)

            logger.info(
                f"Restored resource '{resource_name}' using provider {provider_class.__name__}"
            )

        except ImportError as e:
            logger.error(
                f"Failed to import provider for resource '{resource_name}': {e}",
                exc_info=True,
            )
            # Don't raise - allow event to continue without this resource
        except Exception as e:
            logger.error(
                f"Failed to restore resource '{resource_name}': {e}", exc_info=True
            )

    async def __call__(self: _BaseEvent, *args, **kwargs) -> EventResult:
        """
        Executes a sequence of steps in a checkpointed and resumable manner. The method manages
        task execution phases, handles preemption scenarios, and ensures continuation from
        checkpoints when necessary. Each step in the sequence is executed in order unless the
        current phase indicates it has already been completed.

        :param args: Positional arguments to be passed to the steps.
        :type args: tuple
        :param kwargs: Keyword arguments to be passed to the steps.
        :type kwargs: dict
        :return: The result of the final step in the sequence.
        :rtype: EventResult
        :raises SuspendTask: Raised if the execution is preempted due to external conditions.
        :raises Exception: Propagates any errors encountered during the execution of a step.
        """
        self._call_args = get_function_call_args(self.__class__.__call__, locals())
        # self._args, self._kwargs = args, kwargs

        logger.debug(
            "Starting checkpointed execution for task_id=%s at phase=%s",
            self._task_id,
            getattr(self._phase, "name", None),
        )

        result = None

        for step in self._get_steps():
            step_phase = getattr(step, "_phase", None)

            if (
                self._phase is not None
                and step_phase is not None
                and step_phase <= self._phase
            ):
                logger.debug(
                    "Skipping step %s for task_id=%s because phase %s is already complete",
                    step.__name__,
                    self._task_id,
                    step_phase.name,
                )
                continue

            if self._execution_context.should_preempt(self._task_id):
                logger.info(
                    "Preempting task_id=%s before step %s at phase=%s",
                    self._task_id,
                    step.__name__,
                    getattr(self._phase, "name", None),
                )
                await self.enqueue_checkpoint()
                raise SuspendTask(self)

            logger.debug(
                "Running step %s for task_id=%s",
                step.__name__,
                self._task_id,
            )

            try:
                result = await self._run_step(step, *args, **kwargs)
            except Exception:
                logger.exception(
                    "Step %s failed for task_id=%s; checkpoint preserved at phase=%s",
                    step.__name__,
                    self._task_id,
                    getattr(self._phase, "name", None),
                )
                raise

            await self.enqueue_checkpoint()

            logger.debug(
                "Checkpoint saved for task_id=%s after step %s; phase=%s",
                self._task_id,
                step.__name__,
                self._phase.name,
            )

        # self.checkpoint_manager.delete_checkpoint(self._task_id)
        logger.info("Execution completed for task_id=%s", self._task_id)
        return result

    def get_phase(self) -> EventPhase:
        return self._phase

    async def enqueue_checkpoint(self) -> None:
        if self.checkpoint_manager is None:
            logger.warning("Checkpoint manager is not available. Skipping enqueue.")
            # Enforce checkpointing if configured
            if conf.get("CHECKPOINT_REQUIRED", default=False):
                raise RuntimeError(
                    "Checkpointing is required but manager is unavailable"
                )
            return

        try:
            self.checkpoint_manager.enqueue(await self.create_snapshot())
        except Exception as e:
            logger.error(
                "Failed to create or enqueue checkpoint for task_id=%s: %s",
                self._task_id,
                str(e),
                exc_info=e,
            )
            # Re-raise if checkpointing is critical
            if conf.get("CHECKPOINT_REQUIRED", default=False):
                raise

    async def create_snapshot(self: _BaseEvent) -> EventCheckpointSnapshot:
        return await SnapshotBuilder().build(self)

    # @classmethod
    # def resume_task(cls, task_id, checkpoint_manager, execution_context):
    #     # 1. Pull data from Redis
    #     data = checkpoint_manager.load(task_id)
    #     if not data:
    #         return None
    #
    #     event_cls = _event_registry.get(data["class_path"])
    #
    #     instance = event_cls(
    #         execution_context=execution_context, task_id=task_id, **data["init_args"]
    #     )
    #
    #     instance._phase = EventPhase(data["phase"])
    #     instance._exec_result = data["exec_result"]
    #     instance._execution_status = data["exec_status"]
    #
    #     # 5. User-side resource restoration
    #     for res_name, res_config in data["user_resources"].items():
    #         # Trigger the user's defined restoration logic
    #         instance.restore_resource(res_name, res_config)
    #
    #     return instance
