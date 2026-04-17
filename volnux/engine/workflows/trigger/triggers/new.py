import logging
import asyncio
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timezone  # fix-8: import timezone
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Callable, Awaitable

from volnux.mixins import ObjectIdentityMixin

logger = logging.getLogger(__name__)


class TriggerType(Enum):
    """Types of triggers supported by the framework."""

    SCHEDULE = "schedule"
    EVENT = "event"
    CONDITION = "condition"
    WORKFLOW_CHAIN = "workflow_chain"
    MANUAL = "manual"
    WEBHOOK = "webhook"


@dataclass
class Event:
    """
    Standard event structure for the event bus.

    All events flowing through the system use this structure.
    """

    event_id: str
    event_type: str
    source: str
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize event to dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "metadata": self.metadata,
            "correlation_id": self.correlation_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Event":
        """Deserialize event from dictionary."""
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            source=data["source"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            data=data.get("data", {}),
            metadata=data.get("metadata", {}),
            correlation_id=data.get("correlation_id"),
        )


@dataclass
class TriggerActivation:
    """
    Data passed when a trigger activates.

    Contains all context needed for workflow execution.
    """

    trigger_id: str
    activated_at: datetime
    activation_source: TriggerType
    # fix-5: explicit default factories prevent shared mutable state across activations
    workflow_params: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class TriggerLifecycle(Enum):
    """Lifecycle states of a trigger."""

    CREATED = "created"
    INITIALIZED = "initialized"
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


# fix-10: metaclass hook that enforces trigger_type on every concrete subclass
class _TriggerBaseMeta(type(ABC)):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)


class TriggerBase(ObjectIdentityMixin, ABC):
    """
    Base trigger class.

    Concrete subclasses MUST define a class-level ``trigger_type`` attribute:

        class MyTrigger(TriggerBase):
            trigger_type = TriggerType.SCHEDULE
            ...
    """

    # fix-10: sentinel so __init_subclass__ can detect missing overrides
    trigger_type: TriggerType

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Only enforce on concrete (non-abstract) subclasses
        if not getattr(cls, "__abstractmethods__", None):
            if not isinstance(getattr(cls, "trigger_type", None), TriggerType):
                raise TypeError(
                    f"{cls.__name__} must define a class-level "
                    f"'trigger_type' attribute of type TriggerType."
                )

    def __init__(
        self,
        workflow_name: str,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__()
        self.workflow_name = workflow_name
        self.workflow_params = workflow_params or {}
        self.enabled = enabled
        self.metadata = metadata or {}

        self.lifecycle = TriggerLifecycle.CREATED
        self.fire_count = 0
        self.last_fired: Optional[datetime] = None
        self.error_count = 0

        self._on_activate: Optional[Callable[[TriggerActivation], Awaitable[None]]] = (
            None
        )

    @property
    def trigger_id(self) -> str:
        # fix-9: guard against ObjectIdentityMixin not setting .id
        id_val = getattr(self, "id", None)
        if id_val is None:
            raise AttributeError(
                f"{type(self).__name__}.trigger_id is None — "
                "ensure ObjectIdentityMixin.__init__() was called via super().__init__()."
            )
        return id_val

    def set_activation_callback(
        self, callback: Callable[[TriggerActivation], Awaitable[None]]
    ) -> None:
        """
        Set callback to invoke when trigger activates.

        The engine provides this callback during registration.

        Raises:
            TypeError: If callback is not an async callable.
        """
        if not asyncio.iscoroutinefunction(callback):
            raise TypeError("Activation callback must be a coroutine function.")

        # fix-6: warn if an existing callback is being replaced
        if self._on_activate is not None:
            logger.warning(
                f"Trigger '{self.trigger_id}': replacing existing activation callback. "
                "Ensure this trigger is not registered with multiple engines."
            )

        self._on_activate = callback

    @abstractmethod
    async def start(self):
        """
        Start the trigger's activation mechanism.

        Examples:
        - Event trigger: Subscribe to event bus
        - Schedule trigger: Start timer/cron
        - Manual trigger: Register API endpoint
        - Condition trigger: Start polling loop
        """
        pass

    @abstractmethod
    async def stop(self):
        """
        Stop the trigger's activation mechanism.

        Clean up resources (unsubscribe, cancel timers, etc.)
        """
        pass

    async def activate(self, **activation_data):
        """
        Called internally when trigger condition is met.

        Builds a TriggerActivation and invokes the registered callback.

        Raises:
            RuntimeError: If no activation callback has been set.
        """
        if not self.enabled:
            logger.debug(f"Trigger {self.trigger_id} is disabled, skipping activation")
            return

        # fix-1: validate callback BEFORE mutating state so fire_count / last_fired
        # are never incremented on a no-callback path
        if self._on_activate is None:
            self.error_count += 1  # fix-3: count the failure
            raise RuntimeError(
                f"Trigger '{self.trigger_id}' has no activation callback set. "
                "Register the trigger with the engine before activating it."
            )

        # fix-2: detect key collisions between workflow_params and activation_data
        collisions = set(self.workflow_params) & set(activation_data)
        if collisions:
            logger.warning(
                f"Trigger '{self.trigger_id}': activation_data keys {collisions} "
                "overlap with workflow_params and will override them."
            )

        self.fire_count += 1
        self.last_fired = datetime.now(
            timezone.utc
        )  # fix-8: timezone-aware UTC timestamp

        activation = TriggerActivation(
            trigger_id=self.trigger_id,
            activated_at=self.last_fired,
            activation_source=self.get_activation_source(),
            # fix-2: explicit merge order documented; activation_data wins intentionally
            workflow_params={**self.workflow_params, **activation_data},
            metadata=dict(
                self.metadata
            ),  # fix-5: copy to prevent cross-activation mutation
        )

        logger.info(f"Trigger {self.trigger_id} activated (fires: {self.fire_count})")

        try:
            await self._on_activate(activation)
        except Exception as e:
            self.error_count += 1
            logger.error(
                f"Activation callback failed for trigger '{self.trigger_id}': {e}"
            )
            raise

    def get_activation_source(self) -> TriggerType:
        """
        Return the source type of this trigger.

        Raises:
            AttributeError: If the subclass did not define trigger_type.
        """
        # fix-4: explicit guard with a helpful message instead of a bare AttributeError
        trigger_type = getattr(self, "trigger_type", None)
        if not isinstance(trigger_type, TriggerType):
            raise AttributeError(
                f"{type(self).__name__} does not define a valid 'trigger_type' "
                f"class attribute. Got: {trigger_type!r}"
            )
        return trigger_type

    def pause(self):
        """
        Pause the trigger (temporarily disable).

        Raises:
            RuntimeError: If the trigger is not in ACTIVE state.
        """
        # fix-7: guard invalid lifecycle transitions
        if self.lifecycle not in (TriggerLifecycle.ACTIVE,):
            raise RuntimeError(
                f"Cannot pause trigger '{self.trigger_id}': "
                f"current lifecycle is '{self.lifecycle.value}', expected 'active'."
            )
        self.enabled = False
        self.lifecycle = TriggerLifecycle.PAUSED
        logger.info(f"Trigger '{self.trigger_id}' paused.")

    def resume(self):
        """
        Resume a paused trigger.

        Raises:
            RuntimeError: If the trigger is not in the PAUSED state.
        """
        # fix-7: only allow resume from PAUSED
        if self.lifecycle not in (TriggerLifecycle.PAUSED,):
            raise RuntimeError(
                f"Cannot resume trigger '{self.trigger_id}': "
                f"current lifecycle is '{self.lifecycle.value}', expected 'paused'."
            )
        self.enabled = True
        self.lifecycle = TriggerLifecycle.ACTIVE
        logger.info(f"Trigger '{self.trigger_id}' resumed.")
