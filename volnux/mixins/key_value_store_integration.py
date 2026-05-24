import logging
import typing
import re
from contextlib import contextmanager
from functools import wraps
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
    Set,
    Tuple,
    Union,
    get_args,
    TYPE_CHECKING,
    ForwardRef,
)

from formax import Attrib
from formax.typing import get_type_hints, evaluate_forward_ref

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.backends.formax_fk import OnDelete
from volnux.exceptions import (
    ObjectExistError,
    ObjectProtectedError,
    ObjectDoesNotExist,
    ImproperlyConfigured,
    SerializationError,
)
from volnux.import_utils import import_string
from volnux.mixins.identity import ObjectIdentityMixin
from volnux.utils import get_obj_klass_import_str
from volnux.concurrency.async_utils import to_thread

if TYPE_CHECKING:
    from volnux.config import VolnuxConfig

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="KeyValueStoreIntegrationMixin")


def _resolve_foreign_keys_for_class(cls: Type["KeyValueStoreIntegrationMixin"]) -> None:
    """
    Scan a class for ForeignKey fields and register backreferences.

    Detects fields tagged with _volnux_fk metadata on their Attrib
    and calls register_backreference on the target model.

    Args:
        cls: The class to scan (a KeyValueStoreIntegrationMixin subclass).
    """

    try:
        hints = get_type_hints(cls, include_extras=True)
    except Exception:
        return

    for field_name, hint in hints.items():
        args = get_args(hint)
        if len(args) != 2:
            continue

        has_native_fk = False
        attrib = args[1]

        if isinstance(attrib, Attrib):
            fk_meta = attrib.metadata
            if not fk_meta:
                break

            target_model: Optional["KeyValueStoreIntegrationMixin"] = fk_meta.get(
                "target_model"
            )
            if not target_model:
                break

            if isinstance(target_model, ForwardRef):
                target_model = evaluate_forward_ref(target_model, None, None)
                fk_meta["target_model"] = target_model
                attrib.metadata = fk_meta

            if target_model == cls:
                has_native_fk = True
                fk_meta["has_native_fk"] = has_native_fk

                attrib.metadata = fk_meta

            reverse_name = fk_meta.get("reverse_name")
            if not reverse_name:
                # Auto-generate: class_name + "_" + field_name
                source_name = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()
                reverse_name = f"{source_name}_{field_name}"

            target_model.register_backreference(
                field_name=field_name,
                field_attrib=attrib,
                referencing_model=cls,
                reverse_name=reverse_name,
                has_native_fk=has_native_fk,
            )
            break


class _ReverseRelationDescriptor:
    """
    Descriptor that provides reverse relation access.

    Enables patterns like:
        user.workflows # All workflows created by this user
        user.audit_entries # All audit entries by this user
    """

    def __init__(
        self,
        referencing_model: Union[Type["KeyValueStoreIntegrationMixin"], str],
        foreign_key_field: str,
    ):
        self.referencing_model = referencing_model
        self.foreign_key_field = foreign_key_field
        self._cache: Dict[str, "KeyValueStoreIntegrationMixin"] = {}

    def get_model(self) -> Type["KeyValueStoreIntegrationMixin"]:
        if isinstance(self.referencing_model, str):
            self.referencing_model = cast(
                Type["KeyValueStoreIntegrationMixin"],
                import_string(self.referencing_model),
            )
        return self.referencing_model

    def __get__(self, instance, owner=None):
        if instance is None:
            return self

        if str(instance.id) in self._cache:
            return self._cache[str(instance.id)]

        model_class = self.get_model()

        # Build the filter: {foreign_key_field: instance}
        # The foreign key field stores {"object_id": instance.id, ...}
        # We need to filter on object_id within the JSONB field
        filter_key = f"{self.foreign_key_field}__object_id"

        model_instance = model_class.filter(**{filter_key: str(instance.id)})
        self._cache[str(instance.id)] = model_instance  # type: ignore[assignment]
        return model_instance

    def __set__(self, instance, value):
        raise AttributeError("Reverse relations are read-only")

    def __delete__(self, instance):
        raise AttributeError("Reverse relations cannot be deleted")

    def __repr__(self) -> str:
        return (
            f"<ReverseRelation: {self.referencing_model}" f".{self.foreign_key_field}>"
        )


def backend_operation(
    auto_save: bool = False, force_insert: bool = False, ttl: Optional[int] = None
):
    """Decorator for methods that perform backend operations.

    Args:
        auto_save: If True, automatically save the object after the operation.
        force_insert: If True, always attempt insert (raises error if exists).
        ttl: Time-to-live in seconds for the operation result. None for no expiration.

    Example:
        >>> @backend_operation(auto_save=True)
        ... def update_status(self, status: str):
        ...     self.status = status
    """

    def decorator(method: Callable) -> Callable:
        @wraps(method)
        def wrapper(self: "KeyValueStoreIntegrationMixin", *args, **kwargs):
            result = method(self, *args, **kwargs)
            if auto_save:
                self.save(force_insert=force_insert, ttl=ttl)
            return result

        return wrapper

    return decorator


class KeyValueStoreIntegrationMixin(ObjectIdentityMixin):
    """
    Mixin to enable backend persistence for classes.

    Provides an interface for classes to integrate backend storage by utilizing
    a key-value store. This allows objects to be persistently stored, retrieved,
    and updated via a backend configured in the application settings. This mixin
    also supports flexible backend initialization and schema management, ensuring
    seamless integration with diverse storage systems.

    :ivar _backend_store: Class-level backend store instance used for backend operations.
    :type _backend_store: ClassVar[Optional[KeyValueStoreBackendBase]]
    :ivar _backend_config: Configuration settings for the backend.
    :type _backend_config: ClassVar[Optional[Dict[str, Any]]]

    The backend is configured via CONFIG.KEY_VALUE_STORE_CONFIG.

    Example:
        >>> @dataclass
        ... class User(KeyValueStoreIntegrationMixin):
        ...     name: str
        ...     email: str
        ...     status: str = "active"
        ...
        >>> user = User(name="Alice", email="alice@example.com")
        >>> user.save()  # Automatically persisted
        >>>
        >>> loaded = User.get("user_123")  # Load from backend
        >>> loaded.status = "inactive"
        >>> loaded.save()  # Update in backend
    """

    # Class-level backend store instance (shared across all instances)
    _backend_store: ClassVar[Optional[KeyValueStoreBackendBase]] = None
    _backend_config: ClassVar[Optional[Dict[str, Any]]] = None

    # Backreference registry
    # Maps field_name -> set of (model_class, reverse_name, Attrib, has_native_fk) tuples
    # Example: {"created_by": {(Workflow, "workflows", attrib, False), (AuditEntry, "audit_entries", attrib, True)}}
    _backreferences: ClassVar[Dict[str, Set[Tuple[Type, str, Attrib, bool]]]] = {}

    def __init_subclass__(cls, **kwargs):
        """Register ForeignKey backreferences when a subclass is defined.

        At this point, both the source class and the target model are
        fully defined. We scan annotations for ForeignKey fields and
        register backreferences on the target models.
        """
        super().__init_subclass__(**kwargs)
        _resolve_foreign_keys_for_class(cls)

    @classmethod
    def register_backreference(
        cls,
        field_name: str,
        field_attrib: Attrib,
        referencing_model: Type["KeyValueStoreIntegrationMixin"],
        reverse_name: Optional[str] = None,
        has_native_fk: bool = False,
    ) -> None:
        """Register that another model references this model via a field.

        This enables reverse relation accessors. Called automatically by
        ForeignKeyField when a model class is defined.

        Args:
            field_name: The field name on the referencing model.
            field_attrib: The attribute descriptor for the foreign key field.
            referencing_model: The model class that references this model.
            reverse_name: Name for the reverse accessor. If None, defaults
                         to the referencing model's class name in snake_case
                         with 's' appended.
            has_native_fk: Whether the referencing model has a native foreign key field.
        """
        if reverse_name is None:
            name = re.sub(r"(?<!^)(?=[A-Z])", "_", referencing_model.__name__).lower()
            reverse_name = f"{name}s"

        if field_name not in cls._backreferences:
            cls._backreferences[field_name] = set()

        cls._backreferences[field_name].add(
            (referencing_model, reverse_name, field_attrib, has_native_fk)
        )

        # Create the reverse accessor on this model
        if not hasattr(cls, reverse_name):
            setattr(
                cls,
                reverse_name,
                _ReverseRelationDescriptor(
                    referencing_model=referencing_model,
                    foreign_key_field=field_name,
                ),
            )

    def __post_init__(
        self,
        autosave: bool = False,
        storage_backend: typing.Optional[KeyValueStoreBackendBase] = None,
    ) -> None:
        """Initialize the model with backend integration.

        This method is called during object initialization to set up
        the backend connection and perform initial save.

        Raises:
            ImproperlyConfigured: If backend initialization fails.
        """
        ObjectIdentityMixin.__init__(self)

        if storage_backend is not None and isinstance(
            storage_backend, KeyValueStoreBackendBase
        ):
            pass

        if self._backend_store is None:
            self._initialize_backend()

        if autosave and not self._is_loaded_from_backend():
            try:
                self.save()
            except Exception as e:
                logger.warning(f"Failed to auto-save new object: {e}")

    def _on_delete_hook(self) -> None:
        """Process all backreferences before deletion."""
        for field_name, references in self._backreferences.items():
            for reference_tuple in references:
                referencing_model, reverse_name, field_attrib, has_native_fk = cast(
                    Tuple[Type["KeyValueStoreIntegrationMixin"], str, Attrib, bool],
                    reference_tuple,
                )

                action: OnDelete = field_attrib.metadata.get(
                    "on_delete", OnDelete.PROTECT
                )

                if has_native_fk:
                    # Database handles CASCADE, SET NULL, SET DEFAULT automatically
                    # We only need to handle PROTECT (which is NO ACTION in SQL)
                    if action == OnDelete.PROTECT:
                        raise ObjectProtectedError(
                            f"Cannot delete {self.__class__.__name__}({self.id}): "
                            f"{referencing_model.__name__} objects via '{field_name}'. "
                            f"Protected by native foreign key constraint."
                        )

                    # For other on_delete values, the database handles it
                    continue

                try:
                    action.operation_handler(
                        self.id,
                        self.__class__.__name__,
                        referencing_model,
                        field_name,
                        field_attrib,
                    )
                except Exception as e:
                    logger.error(f"Failed to process backreference: {e}")
                    raise

    @classmethod
    def get_migration_dir(cls) -> typing.Optional[str]:
        """Get the directory where migrations are stored for this class."""
        return None

    @classmethod
    def get_volnux_config(cls) -> "VolnuxConfig":
        from volnux.config import VolnuxConfig

        return VolnuxConfig.get_instance()

    @classmethod
    def get_backend_config(cls) -> Dict[str, Any]:
        """Get the backend configuration for this class."""
        return cls.get_volnux_config().KEY_VALUE_STORE_CONFIG

    @classmethod
    def _initialize_backend(cls) -> None:
        """Initialize the backend store for this class.

        This method is called once per class to set up the backend connection.
        It reads configuration from CONFIG and creates the appropriate backend store.

        Raises:
            StopProcessingError: If backend initialization fails.
        """
        try:
            backend_config = cls.get_backend_config()
            cls._backend_config = backend_config

            backend_class_path = backend_config.get("ENGINE")
            if not backend_class_path:
                raise ImproperlyConfigured("Backend ENGINE not configured")

            backend_class = import_string(backend_class_path)

            connector_config = cast(
                Dict[str, Any], backend_config.get("CONNECTOR_CONFIG", {})
            )

            cls._backend_store = backend_class(**connector_config)

            # Ensure the backend is connected
            if hasattr(cls._backend_store.connector, "connect"):
                if not cls._backend_store.connector.is_connected():
                    cls._backend_store.connector.connect()

            logger.info(
                f"Initialized backend store: {backend_class.__name__} "
                f"for class {cls.__name__}"
            )

        except Exception as e:
            logger.error(f"Failed to initialize backend: {e}")
            raise ImproperlyConfigured(f"Backend initialization failed: {e}") from e

    @classmethod
    def get_backend(cls) -> KeyValueStoreBackendBase:
        """Get the backend store instance.

        Returns:
            The backend store instance.

        Raises:
            RuntimeError: If the backend is not initialized.
        """
        if cls._backend_store is None:
            cls._initialize_backend()
        return cls._backend_store

    def change_storage_backend(self, backend: "KeyValueStoreIntegrationMixin"):
        pass

    @classmethod
    def get_schema_name(cls) -> str:
        """Get the schema name for this class.

        By default, uses the class name. Can be overridden for custom schemas.

        Returns:
            The schema name to use for backend storage.
        """
        return f"volnux_{cls.__name__}"

    def _is_loaded_from_backend(self) -> bool:
        """Check if this instance was loaded from the backend.

        Returns:
            True if loaded from the backend, False if newly created.
        """
        return getattr(self, "_loaded_from_backend", False)

    def _mark_as_loaded(self) -> None:
        """Mark this instance as loaded from the backend."""
        self._loaded_from_backend = True

    def save(
        self, force_insert: bool = False, ttl: typing.Optional[int] = None
    ) -> None:
        """Save this object to the backend store.

        Performs an insert if the record doesn't exist, or an update if it does.
        This is an upsert operation.

        Args:
            force_insert: If True, always attempt insert (raises error if exists).
            ttl: Optional TTL for the new record.

        Raises:
            ObjectExistError: If force_insert is True and the record already exists.
        """
        try:
            backend = self.get_backend()
            schema_name = self.get_schema_name()

            if force_insert:
                backend.insert(schema_name, self.id, self, ttl=ttl)
                logger.debug(f"Inserted {self.__class__.__name__}:{self.id}")
            else:
                # Use upsert for save operation
                if hasattr(backend, "upsert"):
                    backend.upsert(schema_name, self.id, self)
                else:
                    # Fallback: try insert, if fails then update
                    try:
                        backend.insert(schema_name, self.id, self, ttl=ttl)
                    except ObjectExistError:
                        backend.update(schema_name, self.id, self)

                logger.debug(f"Saved {self.__class__.__name__}:{self.id}")

            self._mark_as_loaded()

        except ObjectExistError:
            raise
        except Exception as e:
            logger.error(f"Failed to save {self.__class__.__name__}:{self.id}: {e}")
            raise

    async def save_async(
        self, force_insert: bool = False, ttl: typing.Optional[int] = None
    ) -> None:
        """Save this object to the backend store."""
        await to_thread(self.save, force_insert=force_insert, ttl=ttl)

    def update(self) -> None:
        """Update this object in the backend store.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist in the backend.
        """
        try:
            backend = self.get_backend()
            backend.update(self.get_schema_name(), self.id, self)
            logger.debug(f"Updated {self.__class__.__name__}:{self.id}")
        except Exception as e:
            logger.error(f"Failed to update {self.__class__.__name__}:{self.id}: {e}")
            raise

    async def update_async(self) -> None:
        """Update this object in the backend store."""
        await to_thread(self.update)

    def delete(self) -> None:
        """Delete this object from the backend store.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist in the backend.
        """
        try:
            backend = self.get_backend()
            self._on_delete_hook()
            backend.delete(self.get_schema_name(), self.id)
            logger.debug(f"Deleted {self.__class__.__name__}:{self.id}")
        except Exception as e:
            logger.error(f"Failed to delete {self.__class__.__name__}:{self.id}: {e}")
            raise

    async def delete_async(self) -> None:
        """Delete this object from the backend store."""
        await to_thread(self.delete)

    def reload(self) -> None:
        """Reload this object's data from the backend store.

        Updates the current instance with fresh data from the backend.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist in the backend.
        """
        try:
            backend = self.get_backend()
            backend.reload(self.get_schema_name(), self)
            logger.debug(f"Reloaded {self.__class__.__name__}:{self.id}")
            self._mark_as_loaded()
        except Exception as e:
            logger.error(f"Failed to reload {self.__class__.__name__}:{self.id}: {e}")
            raise

    async def reload_async(self) -> None:
        """Reload this object's data from the backend store."""
        await to_thread(self.reload)

    def refresh(self) -> None:
        """Alias for reload(). Refresh data from the backend."""
        self.reload()

    def exists(self) -> bool:
        """Check if this object exists in the backend store.

        Returns:
            True if the record exists, False otherwise.
        """
        try:
            backend = self.get_backend()
            return backend.exists(self.get_schema_name(), self.id)
        except Exception as e:
            logger.error(
                f"Failed to check existence of {self.__class__.__name__}:{self.id}: {e}"
            )
            return False

    @classmethod
    def get(cls, record_id: str) -> T:
        """Get an object by its ID from the backend store.

        Args:
            record_id: The ID of the record to retrieve.

        Returns:
            An instance of the class loaded from the backend.

        Raises:
            ObjectDoesNotExist: If the record doesn't exist.
        """
        try:
            backend = cls.get_backend()
            instance = backend.get(cls.get_schema_name(), record_id, cls)
            instance._mark_as_loaded()
            logger.debug(f"Retrieved {cls.__name__}:{record_id}")
            return instance
        except Exception as e:
            logger.error(f"Failed to get {cls.__name__}:{record_id}: {e}")
            raise

    @classmethod
    async def get_async(cls, record_id: str) -> T:
        return await to_thread(cls.get, record_id=record_id)

    @classmethod
    def get_or_none(cls, record_id: str) -> Optional[T]:
        """Get an object by ID, returning None if it doesn't exist.

        Args:
            record_id: The ID of the record to retrieve.

        Returns:
            An instance of the class, or None if not found.
        """
        try:
            return cls.get(record_id)
        except ObjectDoesNotExist:
            return None

    @classmethod
    async def get_or_none_async(cls, record_id: str) -> Optional[T]:
        return await to_thread(cls.get_or_none, record_id=record_id)

    @classmethod
    def filter(cls: Type[T], **filters: Any) -> List[T]:
        """Filter objects by the given criteria.

        Args:
            **filters: Field-value pairs to filter by.

        Returns:
            List of instances matching the filters.

        Example:
            >>> active_users = User.filter(status="active")
            >>> admins = User.filter(role="admin", status="active")
        """
        try:
            backend = cls.get_backend()
            instances = backend.filter(cls.get_schema_name(), cls, **filters)

            # Mark all instances as loaded
            for instance in instances:
                instance._mark_as_loaded()

            logger.debug(f"Filtered {cls.__name__}: found {len(instances)} records")
            return instances
        except Exception as e:
            logger.error(f"Failed to filter {cls.__name__}: {e}")
            raise

    @classmethod
    async def filter_async(cls, **filters: Any) -> List[T]:
        return await to_thread(cls.filter, **filters)

    @classmethod
    def all(cls) -> List[T]:
        """Get all objects of this class from the backend.

        Returns:
            List of all instances.
        """
        return cls.filter()

    @classmethod
    async def all_async(cls) -> List[T]:
        return await to_thread(cls.all)

    @classmethod
    def count(cls, **filters: Any) -> int:
        """Count objects matching the given filters.

        Args:
            **filters: Optional field-value pairs to filter by.

        Returns:
            Number of matching records.
        """
        try:
            backend = cls.get_backend()
            return backend.count(cls.get_schema_name(), **filters)
        except Exception as e:
            logger.error(f"Failed to count {cls.__name__}: {e}")
            raise

    @classmethod
    async def count_async(cls, **filters: Any) -> int:
        return await to_thread(cls.count, **filters)

    @classmethod
    def exists_in_backend(cls, record_id: str) -> bool:
        """Check if a record with the given ID exists.

        Args:
            record_id: The ID to check.

        Returns:
            True if exists, False otherwise.
        """
        try:
            backend = cls.get_backend()
            return backend.exists(cls.get_schema_name(), record_id)
        except Exception as e:
            logger.error(f"Failed to check existence: {e}")
            return False

    @classmethod
    async def exists_in_backend_async(cls, record_id: str) -> bool:
        return await to_thread(cls.exists_in_backend, record_id=record_id)

    @classmethod
    def bulk_create(cls, instances: List[T]) -> None:
        """Create multiple instances in a single batch operation.

        Args:
            instances: List of instances to create.

        Raises:
            Exception: If bulk creation fails.
        """
        try:
            backend = cls.get_backend()

            if hasattr(backend, "bulk_insert"):
                # Use native bulk insert if available
                records = {instance.id: instance for instance in instances}
                backend.bulk_insert(cls.get_schema_name(), records)
            else:
                # Fallback: insert one by one
                for instance in instances:
                    instance.save(force_insert=True)

            # Mark all as loaded
            for instance in instances:
                instance._mark_as_loaded()

            logger.info(f"Bulk created {len(instances)} {cls.__name__} instances")
        except Exception as e:
            logger.error(f"Failed to bulk create {cls.__name__}: {e}")
            raise

    @classmethod
    def bulk_delete(cls, record_ids: List[str]) -> None:
        """Delete multiple records in a single batch operation.

        Args:
            record_ids: List of record IDs to delete.

        Raises:
            Exception: If bulk deletion fails.
        """
        try:
            backend = cls.get_backend()

            if hasattr(backend, "bulk_delete"):
                # Use native bulk delete if available
                backend.bulk_delete(cls.get_schema_name(), record_ids)
            else:
                # Fallback
                for record_id in record_ids:
                    backend.delete(cls.get_schema_name(), record_id)

            logger.info(f"Bulk deleted {len(record_ids)} {cls.__name__} records")
        except Exception as e:
            logger.error(f"Failed to bulk delete {cls.__name__}: {e}")
            raise

    @classmethod
    def clear_all(cls) -> None:
        """Delete all records of this class from the backend.

        Warning: This is a destructive operation!
        """
        try:
            backend = cls.get_backend()
            if hasattr(backend, "clear_schema"):
                backend.clear_schema(cls.get_schema_name())
            else:
                # Fallback: get all IDs and delete
                all_instances = cls.all()
                record_ids = [instance.id for instance in all_instances]
                cls.bulk_delete(record_ids)  # type: ignore

            logger.warning(f"Cleared all {cls.__name__} records from backend")
        except Exception as e:
            logger.error(f"Failed to clear {cls.__name__} records: {e}")
            raise

    @contextmanager
    def atomic(self):
        """Context manager for atomic operations.

        Changes are only saved if the context exits successfully.

        Example:
            >>> with user.atomic():
            ...     user.status = "inactive"
            ...     user.last_login = datetime.now()
            ...     # Changes saved only if no exception
        """
        original_state = self.__getstate__()
        try:
            yield self
            self.save()
        except Exception as e:
            # Restore the original state on error
            self.__setstate__(original_state)
            logger.error(f"Atomic operation failed, state restored: {e}")
            raise

    @classmethod
    @contextmanager
    def transaction(cls):
        """Context manager for backend transactions.

        Only supported by backends with transaction support.

        Example:
            >>> with User.transaction():
            ...     user1.save()
            ...     user2.save()
            ...     # Both saved atomically
        """
        backend = cls.get_backend()
        connector = backend.connector

        # Check if the backend supports transactions
        if not hasattr(connector, "transaction"):
            logger.warning(
                f"Backend {backend.__class__.__name__} doesn't support transactions"
            )
            yield
            return

        # Use backend's transaction support
        with connector.transaction():
            yield

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
    def close_backend(cls) -> None:
        """Close the backend connection.

        This should be called when the application shuts down.
        """
        if cls._backend_store is not None:
            try:
                if hasattr(cls._backend_store, "close"):
                    cls._backend_store.close()
                elif hasattr(cls._backend_store.connector, "disconnect"):
                    cls._backend_store.connector.disconnect()

                logger.info(f"Closed backend for {cls.__name__}")
            except Exception as e:
                logger.warning(f"Error closing backend: {e}")
            finally:
                cls._backend_store = None
                cls._backend_config = None

    def __repr__(self) -> str:
        """String representation of the object."""
        exists_str = "exists" if self.exists() else "new"
        return f"<{self.__class__.__name__}:{self.id} [{exists_str}]>"
