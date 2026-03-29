import logging
import inspect
import multiprocessing as mp
import time
import typing
from asgiref.sync import async_to_sync
from concurrent.futures import Executor, ProcessPoolExecutor
from dataclasses import dataclass, field

from volnux.conf import ConfigLoader
from volnux.parser.executor_config import ExecutorInitializerConfig
from volnux.constants import MAX_BACKOFF, MAX_BACKOFF_FACTOR, MAX_RETRIES
from volnux.exceptions import MaxRetryError
from volnux.executors.default import DefaultExecutor
from volnux.executors.tcp import RemoteExecutor
from volnux.utils import get_function_call_args
from volnux.signal.signals import event_execution_retry, event_execution_retry_done

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext


logger = logging.getLogger(__name__)

conf = VolnuxConfig.get_instance()


class _BaseEvent(typing.Protocol):
    _retry_count: int
    _execution_context: "ExecutionContext"
    retry_policy: "RetryPolicy"
    _task_id: str

    def is_exhausted(self) -> bool: ...

    def is_retryable(self, exception: Exception) -> bool: ...

    def _sleep_for_backoff(self) -> float: ...


class RetryConfigDict(typing.TypedDict, total=False):
    max_attempts: int
    backoff_factor: float
    max_backoff: float
    retry_on_exceptions: typing.List[typing.Type[Exception]]


@dataclass
class RetryPolicy:
    max_attempts: int = field(
        init=True, default=conf.get("MAX_EVENT_RETRIES", default=MAX_RETRIES)
    )
    backoff_factor: float = field(
        init=True,
        default=conf.get("MAX_EVENT_BACKOFF_FACTOR", default=MAX_BACKOFF_FACTOR),
    )
    max_backoff: float = field(
        init=True, default=conf.get("MAX_EVENT_BACKOFF", default=MAX_BACKOFF)
    )
    retry_on_exceptions: typing.List[typing.Type[Exception]] = field(
        default_factory=list
    )


class RetryMixin:
    """
    Provides retry handling capabilities for operations.

    This mixin allows defining and managing retry policies for operations that might fail
    and need to be retried. It includes support for exponential backoff, retryable
    exception checking, and maximum retry attempts. The mixin is intended to be combined
    with other classes to provide retry functionality for their methods.

    :ivar retry_policy: The retry policy configuration used to control retry behavior.
                        Can either be a dictionary or an instance of RetryPolicy.
    :type retry_policy: Optional[Union[RetryPolicy, Dict[str, Any], None]]
    """

    retry_policy: typing.Union[
        typing.Optional[RetryPolicy], typing.Dict[str, typing.Any], None
    ] = None

    def init_retry(self) -> typing.Union[RetryPolicy, None]:
        if isinstance(self.retry_policy, dict):
            retry_policy = typing.cast(dict, self.retry_policy)
            self.retry_policy = RetryPolicy(**retry_policy)
        return self.retry_policy

    def config_retry_policy(
        self,
        max_attempts: int,
        backoff_factor: float = MAX_BACKOFF_FACTOR,
        max_backoff: float = MAX_BACKOFF,
        retry_on_exceptions: typing.Union[
            typing.List[typing.Type[Exception]], typing.Type[Exception], None
        ] = None,
    ) -> None:
        """
        Configures the retry policy for the event.
        Args:
            max_attempts (int): Maximum number of retry attempts.
            backoff_factor (float): Factor for calculating backoff time.
            max_backoff (float): Maximum backoff time.
            retry_on_exceptions (Union[Tuple[Type[Exception]], Type[Exception], None]): Exceptions that trigger a retry.
        Returns:
            None
        """
        config: RetryConfigDict = {
            "max_attempts": max_attempts,
            "backoff_factor": backoff_factor,
            "max_backoff": max_backoff,
            "retry_on_exceptions": [],
        }
        if retry_on_exceptions:
            retry_exceptions: typing.Sequence[typing.Type[Exception]] = (
                retry_on_exceptions
                if isinstance(retry_on_exceptions, (tuple, list))
                else [retry_on_exceptions]
            )
            config["retry_on_exceptions"].extend(retry_exceptions)

        self.retry_policy = RetryPolicy(**config)

    def get_backoff_time(self: _BaseEvent) -> float:
        if self.retry_policy is None or self._retry_count <= 1:
            return 0

        backoff_value = self.retry_policy.backoff_factor * (
            2 ** (self._retry_count - 1)
        )

        return typing.cast(float, min(backoff_value, self.retry_policy.max_backoff))

    def _sleep_for_backoff(self) -> float:
        backoff = self.get_backoff_time()
        if backoff <= 0:
            return 0
        time.sleep(backoff)
        return backoff

    def is_retryable(self, exception: Exception) -> bool:
        if self.retry_policy is None:
            return False
        exception_evaluation = not self.retry_policy.retry_on_exceptions or any(
            [
                isinstance(exception, exc)
                and exception.__class__.__name__ == exc.__name__
                for exc in self.retry_policy.retry_on_exceptions
                if exc
            ]
        )
        return isinstance(exception, Exception) and exception_evaluation

    def is_exhausted(self: _BaseEvent) -> bool:
        return (
            self.retry_policy is None
            or self._retry_count >= self.retry_policy.max_attempts
        )

    def retry(
        self: _BaseEvent,
        func: typing.Callable[[typing.Any], typing.Tuple[bool, typing.Any]],
        /,
        *args: typing.Tuple[typing.Any],
        **kwargs: typing.Dict[str, typing.Any],
    ) -> typing.Tuple[bool, typing.Any]:
        if self.retry_policy is None:
            return func(*args, **kwargs)

        exception_causing_retry = None

        while True:
            if self.is_exhausted():
                event_execution_retry_done.emit(
                    sender=self._execution_context.__class__,
                    event=self,
                    execution_context=self._execution_context,
                    task_id=self._task_id,
                    max_attempts=self.retry_policy.max_attempts,
                )

                raise MaxRetryError(
                    attempt=self._retry_count,
                    exception=exception_causing_retry,
                    reason="Retryable event is already exhausted: actual error:{reason}".format(
                        reason=str(exception_causing_retry)
                    ),
                )

            logger.info(
                "Retrying event {}, attempt {}...".format(
                    self.__class__.__name__, self._retry_count
                )
            )

            try:
                self._retry_count += 1
                if inspect.iscoroutinefunction(func):
                    return async_to_sync(func)(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            except MaxRetryError:
                # ignore this
                break
            except Exception as exc:
                if self.is_retryable(exc):
                    if exception_causing_retry is None:
                        exception_causing_retry = exc
                    back_off = self._sleep_for_backoff()

                    event_execution_retry.emit(
                        sender=self._execution_context.__class__,
                        event=self,
                        backoff=back_off,
                        retry_count=self._retry_count,
                        max_attempts=self.retry_policy.max_attempts,
                        execution_context=self._execution_context,
                        task_id=self._task_id,
                    )
                    continue
                raise

        return False, None


class ExecutorInitializerMixin:
    executor: typing.Type[Executor] = DefaultExecutor

    executor_config: typing.Optional[ExecutorInitializerConfig] = None

    @classmethod
    def get_executor_class(cls) -> typing.Type[Executor]:
        return cls.executor

    def get_executor_initializer_config(self) -> ExecutorInitializerConfig:
        if self.executor_config:
            if isinstance(self.executor_config, dict):
                self.executor_config = ExecutorInitializerConfig.from_dict(
                    self.executor_config
                )
        else:
            self.executor_config = ExecutorInitializerConfig()
        return self.executor_config

    def is_multiprocessing_executor(self) -> bool:
        """Check if using multiprocessing or remote executor"""
        return (
            self.get_executor_class() == ProcessPoolExecutor
            or self.get_executor_class() == RemoteExecutor
        )

    def get_executor_context(
        self, ctx: typing.Optional[typing.Dict[str, typing.Any]] = None
    ) -> typing.Dict[str, typing.Any]:
        """
        Retrieves the execution context for the event's executor.

        This method determines the appropriate execution context (e.g., multiprocessing context)
        based on the executor class used for the event. If the executor is configured to use
        multiprocessing, the context is set to "spawn". Additionally, any parameters required
        for the executor's initialization are fetched and added to the context.

        The resulting context dictionary is used to configure the executor for the event execution.

        Returns:
            dict: A dictionary containing the execution context for the event's executor,
                  including any necessary parameters for initialization and multiprocessing context.

        """
        executor = self.get_executor_class()
        context = dict()
        if self.is_multiprocessing_executor():
            context["mp_context"] = mp.get_context("spawn")
        elif hasattr(executor, "get_context"):
            context["mp_context"] = executor.get_context("spawn")  # type: ignore
        params = get_function_call_args(
            executor.__init__, self.get_executor_initializer_config()
        )
        context.update(params)
        if ctx and isinstance(ctx, dict):
            context.update(ctx)
        return context
