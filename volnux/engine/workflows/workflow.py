"""
Workflow Configuration System

WorkflowConfig is ONLY for infrastructure/registry configuration (like Django's AppConfig).
User business logic (steps, pipelines, events) lives in workflow files.

Structure:
workflows/
├── docker_registry/
│   ├── __init__.py
│   ├── workflow.py      # WorkflowConfig - ONLY registries/infrastructure
│   ├── events.py         # USER CODE - event definitions
│   ├── pipeline.py      # USER CODE - pipeline logic
│   └── pointy.pty       # USER CODE - workflow structure
"""

import logging
import types
import typing
import inspect
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional, Callable, Awaitable, Dict, Type, Literal

from .registry import (
    WorkflowSource,
    get_workflow_registry,
)
from volnux.executors.utils.registry import get_global_executor_registry
from volnux.result import ResultSet as TriggerSet
from volnux.pipeline import Pipeline, BatchPipeline
from volnux.config import VolnuxConfig
from volnux.import_utils import load_module_from_path, load_multiple_submodules

if typing.TYPE_CHECKING:
    from volnux.executors import BaseExecutor
    from .trigger.triggers import TriggerBase, TriggerActivation
    from .trigger.triggers.base import TriggerType
    from volnux.executors.utils.registry import ExecutorRegistry

logger = logging.getLogger(__name__)

system_conf = VolnuxConfig.get_instance()


class WorkflowExecutionError(Exception):
    """Exception raised when pipeline execution fails."""


class WorkflowNotfound(Exception):
    """Workflow was not found"""


class TriggerRegistry:
    """Central registry for managing all triggers."""

    def __init__(self):
        self._triggers: TriggerSet["TriggerBase"] = TriggerSet()

    def register(
        self,
        trigger: "TriggerBase",
        trigger_activation_callback: Callable[["TriggerActivation"], Awaitable[None]],
    ) -> None:
        """
        Register a trigger.
        Args:
            trigger: Instance of a trigger
            trigger_activation_callback: callback to handle trigger when activated
        Raises:
            ValueError: If the trigger already exists
        """
        from .trigger.triggers import TriggerLifecycle

        trigger_qs = self._triggers.filter(trigger_type=trigger.trigger_type)
        if trigger_qs.first():
            raise ValueError(f"Trigger type {trigger.trigger_type.value} already added")

        trigger.set_activation_callback(trigger_activation_callback)
        trigger.state.lifecycle = TriggerLifecycle.INITIALIZED
        self._triggers.add(trigger)
        logger.info(
            f"Registered trigger {trigger.trigger_id} for workflow {trigger.workflow_name}"
        )

    def unregister(self, trigger_id: str):
        """
        Unregister a trigger.
        Args:
            trigger_id: trigger id
        Raises:
            ValueError: if the trigger does not exists
        """
        trigger = self.get_trigger(trigger_id)
        if trigger is None:
            raise ValueError(f"Trigger {trigger_id} not found")

        self._triggers.discard(trigger)
        logger.info(f"Unregistered trigger {trigger_id}")

    def get_trigger(self, trigger_id: str) -> Optional["TriggerBase"]:
        """
        Get a trigger by ID.
        Args:
            trigger_id: trigger's id
        Returns:
            trigger instance if it exist
        """
        try:
            return typing.cast("TriggerBase", self._triggers.get(id=trigger_id))
        except KeyError:
            return None


class WorkflowConfig(ABC):
    """
    Base class for workflow configuration.

    This class serves as a foundational component for managing and configuring workflows. It
    establishes the structure, default settings, and executor registration mechanisms required
    to implement a comprehensive workflow management system. Subclasses are expected to
    override certain attributes and implement the `ready` method to customize workflow behavior.

    :ivar name: Name of the workflow configuration. Must be overridden in subclass.
    :type name: str
    :ivar verbose_name: Human-readable name of the workflow configuration.
    :type verbose_name: Optional[str]
    :ivar version: Version of the workflow. Defaults to "1.0.0".
    :type version: str
    :ivar mode: Workflow mode defining its structural representation. Defaults to "CFG".
    :type mode: Literal["DAG", "CFG"]
    :ivar path: Path to the workflow’s configuration file or directory. Set automatically by registries.
    :type path: Optional[Path]
    :ivar default_timeout: Default timeout setting for tasks (in milliseconds). Defaults to 300000.
    :type default_timeout: int
    :ivar default_retries: Default number of retries for tasks. Defaults to 3.
    :type default_retries: int
    :ivar default_auto_cleanup: Flag to determine if auto-cleanup should be performed. Defaults to False.
    :type default_auto_cleanup: bool

    Example:
        class SimpleConfig(WorkflowConfig):
            name = 'simple'
            verbose_name = 'Simple Configuration'

            def ready(self):
                # Register registries (infrastructure)
                self.register_registry(...)

                # Set defaults
                self.default_timeout = 60000
    """

    # Attributes to override in subclass
    name: str = None
    verbose_name: Optional[str] = None
    version: str = "1.0.0"
    mode: Literal["DAG", "CFG"] = "CFG"

    # Paths (automatically set by registry)
    path: Optional[Path] = None

    # Default settings (can override)
    default_timeout: int = 300000
    default_retries: int = 3
    default_auto_cleanup: bool = False

    def __init__(self, workflow_path: Optional[Path] = None):
        """Initialize workflow configuration."""

        # Set by loaders
        self.is_executable = False

        if self.name is None:
            raise ValueError("WorkflowConfig.name must be set")

        if self.verbose_name is None:
            self.verbose_name = self.name.replace("_", " ").title()

        self.path = workflow_path
        if self.path is None:
            raise ValueError("WorkflowConfig.path must be set")

        self.module: Optional[types.ModuleType] = None

        # Registry storage
        self._registry = None
        self._settings: VolnuxConfig = system_conf

        self._loaded_modules: typing.Dict[str, types.ModuleType] = {}

        self.triggers: TriggerRegistry = TriggerRegistry()

        # Initialize the executor registry for this workflow
        self._executor_registry = get_global_executor_registry()

        # Call ready hook for infrastructure setup
        self.ready()

    def get_executor_registry(self) -> "ExecutorRegistry":
        """
        Get the executor registry for this workflow.

        Returns:
            ExecutorRegistry: The executor registry instance
        """
        return self._executor_registry

    def register_executor(
        self,
        label: str,
        executor_class: Type["BaseExecutor"],
        *,
        override: bool = False,
        reinit_callback: Optional[Callable[[], "BaseExecutor"]] = None,
        shared: bool = False,
        auto_shutdown: bool = True,
        health_check_enabled: bool = True,
    ) -> None:
        """
        Register a custom executor for the workflow.

        This method enables the registration of executor classes with a specific
        label to be used within the workflow. The feature helps streamline the
        configuration and execution of tasks, allowing users to refer to executors
        using labels instead of importing classes directly. The executors can be
        configured with options such as re-initialization callbacks, shared usage,
        and more.

        :param label: A string used to uniquely identify the executor in the
            workflow. Common labels might denote specific task execution
            environments (e.g., "gpu", "redis-queue").
        :type label: str

        :param executor_class: The executor class to register under the
            specified label. The class should inherit from `BaseExecutor` and
            implement the required interface for execution.
        :type executor_class: Type["BaseExecutor"]

        :param override: Specifies if an already existing registration with
            the same label should be overridden. Defaults to `False`.
        :type override: bool

        :param reinit_callback: An optional callable that will be used to
            reinitialize the executor when necessary. This is particularly
            useful for dynamic workflows where executor instances may need to
            be created or reset.
        :type reinit_callback: Optional[Callable[[], "BaseExecutor"]]

        :param shared: Indicates if the registered executor should be shared
            among multiple tasks or workflows. If set to `True`, the same instance
            may be reused. Defaults to `False`.
        :type shared: bool

        :param auto_shutdown: If `True`, enables automatic shutdown for the
            executor when the workflow concludes. This helps manage resources
            effectively. Defaults to `True`.
        :type auto_shutdown: bool

        :param health_check_enabled: If `True`, enables periodic health checks
            for the executor to verify availability and functionality. Defaults
            to `True`.
        :type health_check_enabled: bool

        :return: This method does not return any value as it updates the
            workflow's executor registry.
        :rtype: None

        Example:
            >>> from myapp.executors import GPUExecutor, AccraCeleryExecutor
            >>>
            >>> def ready(self):
            ...     # Now you can use "gpu" in your events
            ...     self.register_executor("gpu", GPUExecutor)
            ...
            ...     # And "accra-celery" for named Celery queues
            ...     self.register_executor("accra-celery", AccraCeleryExecutor)

            Then in your event:
            >>> class ProcessImage(EventBase):
            ...     executor = "gpu"  # Uses GPUExecutor
            ...
            ...     async def process(self, image):
            ...         return True, processed_image
        """
        registry = self.get_executor_registry()
        registry.register(
            label,
            executor_class,
            override=override,
            reinit_callback=reinit_callback,
            shared=shared,
            auto_shutdown=auto_shutdown,
            health_check_enabled=health_check_enabled,
        )
        logger.info(
            f"Registered executor '{label}' for workflow '{self.name}': "
            f"{executor_class.__module__}.{executor_class.__name__}"
        )

    def register_executor_factory(
        self,
        pattern: str,
        factory: Callable[..., Type["BaseExecutor"]],
        *,
        override: bool = False,
    ) -> None:
        """
        Register a factory function for dynamic executor creation.

        Useful for executors that need runtime configuration based on the label.

        Args:
            pattern: Pattern with placeholders (e.g., "redis-{queue}", "celery-{region}")
            factory: Function that creates executor class from extracted parameters
            override: Allow overriding existing factories

        Example:
            >>> def redis_executor_factory(queue: str):
            ...     class RedisQueueExecutor(BaseExecutor):
            ...         queue_name = queue
            ...         # ... implementation
            ...     return RedisQueueExecutor
            >>>
            >>> def ready(self):
            ...     self.register_executor_factory("redis-{queue}", redis_executor_factory)

            Now you can use:
            - executor = "redis-orders"  -> creates executor with queue="orders"
            - executor = "redis-payments" -> creates executor with queue="payments"
        """
        registry = self.get_executor_registry()
        registry.register_factory(pattern, factory, override=override)
        logger.info(
            f"Registered executor factory pattern '{pattern}' for workflow '{self.name}'"
        )

    def register_executor_alias(self, alias: str, target: str) -> None:
        """
        Create an alias for an existing executor label.

        Args:
            alias: The new alias name
            target: The existing label to point to

        Example:
            >>> def ready(self):
            ...     self.register_executor("gpu-v100", V100Executor)
            ...     self.register_executor_alias("gpu", "gpu-v100")  # Default GPU
        """
        registry = self.get_executor_registry()
        registry.alias(alias, target)
        logger.debug(
            f"Created executor alias '{alias}' -> '{target}' for workflow '{self.name}'"
        )

    def get_executor(self, label: str, **kwargs) -> Optional[Type["BaseExecutor"]]:
        """
        Resolve the executor class from the label for this workflow.

        Args:
            label: The executor label
            **kwargs: Additional arguments for factory executors

        Returns:
            Executor class or None if not found
        """
        registry = self.get_executor_registry()
        return registry.get(label, **kwargs)

    def list_executors(self) -> Dict[str, str]:
        """
        List all registered executors for this workflow.

        Returns:
            Dict mapping labels to executor class names
        """
        registry = self.get_executor_registry()
        return registry.list_executors()

    @abstractmethod
    def ready(self):
        """
        Override this to register infrastructure resources.

        SHOULD DO:
        - Register registries
        - Set default configurations
        - Initialize connections
        - Load environment variables
        """
        pass

    def get_registry(self) -> "WorkflowRegistry":
        if self._registry is None:
            self._registry = get_workflow_registry()
        return self._registry

    def register_registry_source(self, source: "WorkflowSource") -> None:
        """Register a registry source (infrastructure resource)."""
        self.get_registry().add_workflow_source(source)

    def register_trigger(self, trigger: "TriggerBase") -> None:
        """Register a trigger."""
        from volnux.engine.workflows.trigger import get_trigger_engine

        engine = get_trigger_engine()
        engine.register(trigger)

    def set_setting(self, key: str, value: Any):
        """Set a configuration setting."""
        self._settings.add(key, value)

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a configuration setting."""
        return self._settings.get(key, default)

    def _load_workflow_module(self) -> typing.Optional[types.ModuleType]:
        """
        Load a workflow module from a path.
        Returns:
             (WorkflowConfig) workflow module
        Raises:
            ImportError: if the workflow module cannot be loaded
        """
        if not self.path or not self.path.exists():
            raise ImportError(
                "Not validate workflow module path found for workflow configuration"
            )

        if not self.module:
            workflow_init_file = self.path / "__init__.py"
            self.module = load_module_from_path(self.name, workflow_init_file)
        return self.module

    def discover_workflow_submodules(self):
        """
        Load workflow module components
        Raises:
            RuntimeError: if the workflow module cannot be loaded
        """
        if self._loaded_modules:
            return self._loaded_modules

        try:
            module = self._load_workflow_module()
        except ImportError as e:
            raise RuntimeError(
                f"Failed to load workflow module from path: {self.path}"
            ) from e

        self._loaded_modules = load_multiple_submodules(
            module, self.path, ["events", "pipeline", "batch_pipeline"]
        )
        return self._loaded_modules

    def get_event_module(self):
        """Get the event module (user code)."""
        return self._loaded_modules.get("events")

    def get_pipeline_module(self):
        """Get the pipeline module (user code)."""
        return self._loaded_modules.get("pipeline")

    def get_batch_pipeline_module(self):
        """Get the batch pipeline module (user code)."""
        return self._loaded_modules.get("batch_pipeline")

    def check(self) -> List[str]:
        """
        Check configuration for issues.
        Only validates infrastructure, not user business logic.
        """
        issues = []

        if not self.is_executable:
            issues.append(f"Workflow '{self.name}' is not executable")

        # Check registries
        # if not self.get_registry().get_workflow_config(self.name):
        #     issues.append(f"Workflow '{self.name}' has no registries registered")
        #
        # for name, registry in self.get_registry().get_workflow_source(self.name):
        #     if not registry.location:
        #         issues.append(f"Registry '{name}' has no location configured")
        #
        #     if registry.credentials and not registry.credentials.is_valid():
        #         issues.append(f"Registry '{name}' has invalid credentials")

        # Check user code exists (but don't validate its logic)
        if not self.get_event_module():
            issues.append(f"Workflow '{self.name}' has no event")

        if not self.get_pipeline_module():
            issues.append(f"Workflow '{self.name}' has no pipeline")

        return issues

    def get_pipeline_class(self) -> typing.Type[Pipeline]:
        """
        Get the pipeline class (user code).
        Returns:
            (Pipeline) pipeline class
        Raises:
            RuntimeError: if the pipeline class cannot be loaded or found
        """
        module = self.get_pipeline_module()
        if not module:
            raise RuntimeError(f"Workflow '{self.name}' has no pipeline defined")

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                inspect.isclass(attr)
                and attr != Pipeline
                and issubclass(attr, Pipeline)
            ):
                return typing.cast(typing.Type[Pipeline], attr)
        raise RuntimeError(f"Workflow '{self.name}' has no pipeline defined")

    def get_batch_pipeline_class(self) -> typing.Type[BatchPipeline]:
        """
        Get the batch pipeline class (user code).
        Returns:
            (BatchPipeline) batch pipeline class
        Raises:
            RuntimeError: if batch pipeline class cannot be loaded or found
        """
        module = self.get_batch_pipeline_module()
        if not module:
            raise RuntimeError(f"Workflow '{self.name}' has no batch pipeline defined")

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                inspect.isclass(attr)
                and issubclass(attr, BatchPipeline)
                and attr != BatchPipeline
            ):
                return typing.cast(typing.Type[BatchPipeline], attr)

        raise RuntimeError(f"Workflow '{self.name}' has no batch pipeline defined")

    def run_workflow(
        self,
        params: typing.Dict[str, typing.Any],
        run_type: typing.Literal["batch", "single"] = "single",
    ) -> typing.Union[Pipeline, BatchPipeline, None]:
        """
        Run a workflow.

        Args:
            params (dict): workflow parameters
            run_type (str): workflow run type ('batch' or 'single')
        Returns:
            Pipeline or BatchPipeline or None
        Raises:
            RuntimeError: if a workflow run type is not 'batch' or 'single'
            WorkflowExecutionError: if workflow execution fails
        """
        issues = self.check()
        if issues:
            for issue in issues:
                logger.warning(f"  <UNK> {issue}")
            return None

        if run_type == "single":
            pipeline_class = self.get_pipeline_class()
            try:
                pipeline = pipeline_class(**params)
                pipeline.start(force_rerun=True)
                return pipeline
            except Exception as e:
                logger.error(f"  <UNK> {e}")
                raise WorkflowExecutionError("Failed to run workflow") from e
        elif run_type == "batch":
            batch_pipeline_class = self.get_batch_pipeline_class()
            try:
                batch_pipeline = batch_pipeline_class(**params)
                batch_pipeline.execute()
                return batch_pipeline
            except Exception as e:
                logger.error(f"  <UNK> {e}")
                raise WorkflowExecutionError("Failed to run batched workflow") from e
        else:
            raise RuntimeError(f"Unknown run type '{run_type}'")
