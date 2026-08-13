import logging
import threading
from contextlib import contextmanager
from typing import (
    Any,
    ClassVar,
    Dict,
    Optional,
    Type,
    TypeVar,
    cast,
    TYPE_CHECKING,
    Generator,
)

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.exceptions import (
    ImproperlyConfigured,
    SerializationError,
)
from volnux.backends.storage_route import StorageRoute
from volnux.import_utils import import_string
from volnux.mixins.identity import ObjectIdentityMixin
from volnux.utils import get_obj_klass_import_str

if TYPE_CHECKING:
    from volnux.config import VolnuxConfig

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="BackendConnectionIntegrationMixin")


class BackendConnectionIntegrationMixin(ObjectIdentityMixin):
    """
    Mixin providing foundational backend connectivity, configuration loading,
    and lifecycle management across storage and messaging integration mixins.

    Provides a clean, unified interface for initializing database/broker connections
    without coupling models to specific persistence paradigms (KeyValue, Messaging, Relational).

    :ivar _backend_store: Class-level backend store instance shared across instances.
    :ivar _backend_config: Configuration dictionary for the active backend.
    """

    # Class-level backend store instance (shared across all instances)
    _backend_store: ClassVar[Optional[KeyValueStoreBackendBase]] = None
    _backend_config: ClassVar[Optional[Dict[str, Any]]] = None
    _lock: ClassVar[threading.RLock] = threading.RLock()

    def __post_init__(
        self,
        **kwargs: Any,
    ) -> None:
        """
        Base post-initialization hook. Ensures backend connectivity upon model instantiation.

        Subclasses can override this method, call super().__post_init__(...), and append
        paradigm-specific initialization logic.
        """

        if self._backend_store is None:
            self._initialize_backend()

        # Paradigm-specific hook for subclasses
        self._after_backend_init(**kwargs)

    def _after_backend_init(self, **kwargs: Any) -> None:
        """
        Optional lifecycle hook for specialized mixins (e.g., ORM autosave, queue verification).
        The default implementation is a no-op.
        """
        pass

    @classmethod
    @contextmanager
    def change_backend(
        cls: Type[T],
        storage_backend: KeyValueStoreBackendBase,
    ) -> Generator[Type[T], None, None]:
        """Temporarily or permanently switch the class-level storage backend.

        Acts as a thread-safe context manager. Inside the ``with`` block,
        all operations on this class (and its instances) use the provided
        ``storage_backend``.

        Upon exiting the context block, the original backend store and config
        are atomically restored, even if an exception was raised inside the block.

        Usage:
            >>> mock_backend = RedisStoreBackend(host="mock-host")
            >>> with User.change_backend(mock_backend):
            ...     user = User.get("user_123")  # Uses mock_backend
            ...
            >>> user = User.get("user_123")  # Automatically restored to real_backend
        """
        with cls._lock:
            previous_backend = cls._backend_store

            try:
                cls._backend_store = storage_backend
                yield cls
            finally:
                cls._backend_store = previous_backend

    def __getstate__(self) -> Dict[str, Any]:
        """Prepare an object for serialization.

        Returns:
            Dictionary representation of the object state.

        Raises:
            SerializationError: If the object cannot be serialized.
        """
        try:
            state = self.get_state()
        except NotImplementedError:
            raise SerializationError(
                f"Cannot serialise object of type {self.__class__.__name__!r}"
            )

        if hasattr(self, "_id"):
            state["id"] = self._id

        if hasattr(self, "_backend_store") and self._backend_store is not None:
            state["_backend_class"] = get_obj_klass_import_str(self._backend_store)

        # Remove non-serializable attributes
        state.pop("_backend_store", None)
        state.pop("_backend_config", None)

        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Restore the object state after deserialization.

        Args:
            state: Dictionary containing object state.

        Raises:
            SerializationError: If the object cannot be deserialized.
        """
        # Remove backend class info (will be reinitialized)
        state.pop("_backend_class", None)

        try:
            self.set_state(state)
        except NotImplementedError:
            raise SerializationError(
                f"Cannot deserialized object of type {self.__class__.__name__!r}"
            )

        # Ensure the backend is initialized for this class
        if self._backend_store is None:
            self._initialize_backend()

    @classmethod
    def get_volnux_config(cls) -> "VolnuxConfig":
        """Get the global VolnuxConfig instance."""
        from volnux.config import VolnuxConfig

        return VolnuxConfig.get_instance()

    @classmethod
    def get_backend_config(cls) -> Dict[str, Any]:
        """Get the backend configuration for this class from VolnuxConfig."""
        return cls.get_volnux_config().KEY_VALUE_STORE_CONFIG

    @classmethod
    def _initialize_backend(cls) -> None:
        """Initialize the backend store instance for this class.

        Reads ENGINE and CONNECTOR_CONFIG from get_backend_config() and
        instantiates the backend store driver.
        """
        try:
            backend_config = cls.get_backend_config()
            cls._backend_config = backend_config

            backend_class_path: Optional[str] = backend_config.get("ENGINE")
            if not backend_class_path:
                raise ImproperlyConfigured(
                    f"Backend ENGINE not configured for {cls.__name__}"
                )

            backend_class = import_string(backend_class_path)
            connector_config = cast(
                Dict[str, Any], backend_config.get("CONNECTOR_CONFIG", {})
            )

            cls._backend_store = backend_class(**connector_config)

            # Ensure underlying connector is connected
            if hasattr(cls._backend_store, "connector") and hasattr(
                cls._backend_store.connector, "connect"
            ):
                if not cls._backend_store.connector.is_connected():
                    cls._backend_store.connector.connect()

            logger.info(
                f"Initialized backend store: {backend_class.__name__} for class {cls.__name__}"
            )

        except Exception as e:
            logger.error(f"Failed to initialize backend for {cls.__name__}: {e}")
            raise ImproperlyConfigured(f"Backend initialization failed: {e}") from e

    @classmethod
    def get_backend(cls) -> Any:
        """Get or initialize the shared backend store instance."""
        if cls._backend_store is None:
            cls._initialize_backend()
        return cls._backend_store

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        """Get the StorageRoute definition for this class."""
        return StorageRoute(components=["volnux", cls.__name__])

    @classmethod
    def get_schema_name(cls) -> str:
        """Get the resolved schema name/channel for this class."""
        backend = cls.get_backend()
        return cls.get_storage_route().resolve(backend)

    @classmethod
    def close_backend(cls) -> None:
        """Close backend connections upon application shutdown."""
        if cls._backend_store is not None:
            try:
                if hasattr(cls._backend_store, "close"):
                    cls._backend_store.close()
                elif hasattr(cls._backend_store, "connector") and hasattr(
                    cls._backend_store.connector, "disconnect"
                ):
                    cls._backend_store.connector.disconnect()

                logger.info(f"Closed backend connection for {cls.__name__}")
            except Exception as e:
                logger.warning(f"Error closing backend for {cls.__name__}: {e}")
            finally:
                cls._backend_store = None
                cls._backend_config = None
