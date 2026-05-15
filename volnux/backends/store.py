"""
Key-Value Store Backend Interface.

This module provides the abstract base class for implementing key-value store backends
with support for CRUD operations, filtering, and record management.
"""

import abc
import os
import typing
import zlib
import orjson as json
import threading
import logging
from yoyo import read_migrations, get_backend
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Type, Union, Optional

from formax import BaseModel

from .connection import BackendConnectorBase
from volnux.exceptions import SerializationError

if TYPE_CHECKING:
    from .formax_fk import OnDelete
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


logger = logging.getLogger(__name__)


class YoyoMigrationsMixin:

    def schema_exists(self, schema_name: str) -> bool:
        raise NotImplementedError

    def create_schema(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        raise NotImplementedError

    def ensure_schema(
        self,
        schema_name: str,
        record: "KeyValueStoreIntegrationMixin",
        dry_run: bool = False,
    ) -> int:
        """
        Ensure the base schema exists, then apply pending Yoyo migrations.

        Args:
            schema_name: Schema/table name.
            record: Sample model used to derive initial columns.
            dry_run: If True, only report what would happen.

        Returns:
            Number of Yoyo migrations applied, or would be applied in dry_run mode.
        """
        schema_exists = self.schema_exists(schema_name)
        if not schema_exists:
            if dry_run:
                logger.info(
                    "Dry run: would create schema '%s' and apply migrations from {folder name}.",
                    schema_name,
                )
            else:
                self.create_schema(
                    schema_name=schema_name,
                    record=record,
                )

        if not hasattr(record, "get_migration_dir"):
            return 1 if schema_exists else 0

        migrations_dir = record.get_migration_dir()

        # check if migrations dir is empty
        if not os.listdir(migrations_dir):
            return 0

        self._validate_migrations_dir(migrations_dir)
        return self.run_migrations(migrations_dir=migrations_dir, dry_run=dry_run)

    def _get_migration_backend(self) -> typing.Any:
        return get_backend(self.connector.get_uri())

    def _validate_migrations_dir(self, migrations_dir: str) -> None:
        if not os.path.isdir(migrations_dir):
            raise FileNotFoundError(
                f"Migrations directory does not exist: {migrations_dir}"
            )

    def _get_pending_migrations(
        self, migrations_dir: str
    ) -> typing.Tuple[typing.Any, typing.Any, typing.Any]:
        backend = self._get_migration_backend()
        migrations = read_migrations(migrations_dir)
        return backend, migrations, backend.to_apply(migrations)

    def _get_rollback_migrations(
        self, migrations_dir: str, count: typing.Optional[int] = None
    ) -> typing.Tuple[typing.Any, typing.Any, typing.Any]:
        backend = self._get_migration_backend()
        migrations = read_migrations(migrations_dir)

        if not hasattr(backend, "to_rollback"):
            raise NotImplementedError(
                f"{backend.__class__.__name__} does not expose rollback selection support."
            )

        to_rollback = backend.to_rollback(migrations)
        if count is not None:
            to_rollback = to_rollback[:count]

        return backend, migrations, to_rollback

    def _execute_migration_batch(
        self,
        backend: typing.Any,
        migrations: typing.Any,
        migrations_dir: str,
        action: str,
    ) -> int:
        if not migrations:
            if action == "apply":
                logger.debug("No new migration files to apply.")
            else:
                logger.debug("No applied migrations to roll back.")
            return 0

        try:
            with backend.lock():
                if action == "apply":
                    backend.apply_migrations(migrations)
                elif action == "rollback":
                    if hasattr(backend, "rollback_migrations"):
                        backend.rollback_migrations(migrations)
                    elif hasattr(backend, "rollback"):
                        backend.rollback(migrations)
                    else:
                        raise NotImplementedError(
                            f"{backend.__class__.__name__} does not support rollback execution."
                        )
                else:
                    raise ValueError(f"Unknown migration action: {action}")
        except Exception:
            if action == "apply":
                logger.exception("Failed to apply migrations from %s", migrations_dir)
            else:
                logger.exception(
                    "Failed to roll back migrations from %s", migrations_dir
                )
            raise

        return len(migrations)

    def run_migrations(self, migrations_dir: str, dry_run: bool = False) -> int:
        """
        Apply all pending migrations from the given directory.

        Args:
            migrations_dir: Path to the migrations directory.
            dry_run: If True, only calculate what would be applied.

        Returns:
            Number of migrations that would be, or were, applied.
        """
        self._validate_migrations_dir(migrations_dir)

        backend, _, to_apply = self._get_pending_migrations(migrations_dir)

        if dry_run:
            logger.info(
                "Dry run: would apply %s migration(s) from %s.",
                len(to_apply),
                migrations_dir,
            )
            return len(to_apply)

        applied_count = self._execute_migration_batch(
            backend=backend,
            migrations=to_apply,
            migrations_dir=migrations_dir,
            action="apply",
        )

        if applied_count:
            logger.info("Applied %s new migration(s).", applied_count)

        return applied_count

    def rollback_migrations(
        self,
        migrations_dir: str,
        count: int = 1,
        dry_run: bool = False,
    ) -> int:
        """
        Roll back the last `count` applied migrations.

        Args:
            migrations_dir: Path to the migrations directory.
            count: Number of migrations to roll back.
            dry_run: If True, only calculate what would be rolled back.

        Returns:
            Number of migrations that would be, or were, rolled back.
        """
        self._validate_migrations_dir(migrations_dir)

        if count <= 0:
            logger.debug("Rollback count is <= 0; nothing to do.")
            return 0

        backend, _, to_rollback = self._get_rollback_migrations(
            migrations_dir=migrations_dir,
            count=count,
        )

        if dry_run:
            logger.info(
                "Dry run: would roll back %s migration(s) from %s.",
                len(to_rollback),
                migrations_dir,
            )
            return len(to_rollback)

        rolled_back_count = self._execute_migration_batch(
            backend=backend,
            migrations=to_rollback,
            migrations_dir=migrations_dir,
            action="rollback",
        )

        if rolled_back_count:
            logger.info("Rolled back %s migration(s).", rolled_back_count)

        return rolled_back_count

    def rollback_all_migrations(
        self,
        migrations_dir: str,
        dry_run: bool = False,
    ) -> int:
        """
        Roll back all applied migrations.

        Args:
            migrations_dir: Path to the migrations directory.
            dry_run: If True, only calculate what would be rolled back.

        Returns:
            Number of migrations that would be, or were, rolled back.
        """
        self._validate_migrations_dir(migrations_dir)

        backend, _, to_rollback = self._get_rollback_migrations(
            migrations_dir=migrations_dir,
            count=None,
        )

        if dry_run:
            logger.info(
                "Dry run: would roll back all %s migration(s) from %s.",
                len(to_rollback),
                migrations_dir,
            )
            return len(to_rollback)

        rolled_back_count = self._execute_migration_batch(
            backend=backend,
            migrations=to_rollback,
            migrations_dir=migrations_dir,
            action="rollback",
        )

        if rolled_back_count:
            logger.info("Rolled back all %s migration(s).", rolled_back_count)

        return rolled_back_count


class KeyValueStoreBackendBase(abc.ABC):
    """Abstract base class for key-value store backends.

    This class defines the interface for backend storage implementations,
    providing thread-safe operations for managing records in a schema-based
    key-value store.

    Attributes:
        connector_klass: The connector class to use for backend connections.
        connector: The active backend connector instance.
    """

    connector_klass: Type[BackendConnectorBase]

    NAMESPACE_SEPARATOR = ":"

    RESERVED_FIELDS = {"_id", "_backend", "_schema_name"}

    def __init__(
        self, namespace_prefix: typing.Optional[str] = None, **connector_config: Any
    ) -> None:
        """Initialize the backend with connector configuration.

        Args:
            **connector_config: Configuration parameters passed to the connector.
        """
        self._namespace_prefix = namespace_prefix
        self.connector = self.connector_klass(**connector_config)
        self._connector_lock = threading.RLock()

    @contextmanager
    def _acquire_lock(self):
        """Context manager for thread-safe operations."""
        self._connector_lock.acquire()
        try:
            yield
        finally:
            self._connector_lock.release()

    @staticmethod
    def _create_filter_predicate(**filter_kwargs: Any) -> Callable[[Any], bool]:
        """Create a filter predicate function from keyword arguments.

        Args:
            **filter_kwargs: Attribute-value pairs to match against records.

        Returns:
            A predicate function that returns True if a record matches all criteria.

        Example:
            >>> predicate = _create_filter_predicate(status="active", age=25)
            >>> predicate(record)  # Returns True if record.status == "active" and record.age == 25
        """

        def predicate(record: Any) -> bool:
            return all(
                hasattr(record, key) and getattr(record, key) == value
                for key, value in filter_kwargs.items()
            )

        return predicate

    def close(self) -> None:
        """Close the backend connection and release resources."""
        with self._acquire_lock():
            if hasattr(self, "connector") and self.connector is not None:
                self.connector.disconnect()

    def __enter__(self) -> "KeyValueStoreBackendBase":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit with automatic cleanup."""
        self.close()

    def _build_key(self, schema_name: str, record_key: str) -> str:
        """Build a fully qualified key with namespace.

        Args:
            schema_name: The schema namespace.
            record_key: The record key within the schema.

        Returns:
            Fully-qualified key string.
        """
        parts = []
        if self._namespace_prefix:
            parts.append(self._namespace_prefix)
        parts.extend([schema_name, record_key])
        return self.NAMESPACE_SEPARATOR.join(parts)

    def _prepare_record_data(
        self, record: "KeyValueStoreIntegrationMixin", record_key: str
    ) -> Dict[str, Any]:
        """Prepare record data for database insertion.

        Args:
            record: The record to prepare.
            record_key: The key for the record.

        Returns:
            Dictionary of field names to values.
        """
        record_data: Dict[str, Any] = {"id": record_key}

        for field_name, value in record.__getstate__().items():
            if field_name in self.RESERVED_FIELDS:
                continue

            if isinstance(value, (dict, list)):
                record_data[field_name] = value
            elif value is not None:
                record_data[field_name] = value
            else:
                record_data[field_name] = None

        return record_data

    def _serialize_record(self, record: "KeyValueStoreIntegrationMixin") -> bytes:
        """Serialize a record to bytes.

        Args:
            record: The record to serialize.

        Returns:
            Serialized record as bytes.

        Raises:
            SerializationError: If serialization fails.
        """
        try:
            state = record.__getstate__()
            return json.dumps(state, default=str)
        except Exception as e:
            logger.error(f"Failed to serialize record: {e}")
            raise SerializationError(f"Serialization failed: {e}")

    def _deserialize_record(
        self, data: bytes, record_klass: Type["KeyValueStoreIntegrationMixin"]
    ) -> "KeyValueStoreIntegrationMixin":
        """Deserialize bytes to a record object.

        Args:
            data: Serialized record data.
            record_klass: The class to instantiate.

        Returns:
            Deserialized record instance.

        Raises:
            SerializationError: If deserialization fails.
        """
        try:
            state = json.loads(data)
            record = record_klass.__new__(record_klass)
            record.__setstate__(state)
            return record
        except Exception as e:
            logger.error(f"Failed to deserialize record: {e}")
            raise SerializationError(f"Deserialization failed: {e}")

    def create_native_fk_constraint(
        self,
        source_backend: "KeyValueStoreBackendBase",
        source_schema: str,
        source_field: str,
        target_schema: str,
        target_field: str,
        on_delete: "OnDelete",
        nullable: bool,
    ) -> None:
        """
        Create a native foreign key constraint between two schemas in the backend.

        This function establishes a foreign key relationship between a source schema/field
        and a target schema/field. The parameters define the behavior of the constraint,
        such as the action on delete and whether the field can be nullable.

        :param source_backend: The backend system where the schema resides. Must
            be an instance of KeyValueStoreBackendBase.
        :param source_schema: The name of the schema in the source backend to which
            the foreign key constraint will be applied.
        :param source_field: The field in the source schema that will reference the target
            field as a foreign key.
        :param target_schema: The name of the schema containing the target field,
            which the source field will reference.
        :param target_field: The specific field in the target schema that will be
            referenced by the source field.
        :param on_delete: Specifies the action to be taken in the source field
            when the referenced target record is deleted.
        :param nullable: Indicates if the source field is allowed to be nullable.
        :return: This function does not return any value.
        """

        raise NotImplementedError("Native foreign key constraints are not supported.")

    def supports_foreign_keys(self) -> bool:
        """Check if the backend supports foreign key constraints.

        Returns:
            True if foreign key constraints are supported, False otherwise.
        """
        return False

    @abc.abstractmethod
    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The unique key identifying the record.

        Returns:
            True if the record exists, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: Optional[int] = None,
    ) -> None:
        """Insert a new record into the store.

        Args:
            schema_name: The schema/namespace to insert into.
            record_key: The unique key for the new record.
            record: The record object to insert.
            ttl: Optional TTL for the new record.

        Raises:
            KeyError: If a record with the same key already exists.
        """
        raise NotImplementedError

    def bulk_insert(
        self,
        schema_name: str,
        records: Dict[str, "KeyValueStoreIntegrationMixin"],
        ttl: Optional[int] = None,
    ) -> int:
        """Insert multiple records in a single operation.

        Args:
            schema_name: The schema to insert into.
            records: Dictionary mapping record keys to record objects.
            ttl: Optional TTL for all inserted records.

        Raises:
            SerializationError: If serialization fails.
            ConnectionError: If Redis operation fails.
        """
        pass

    @abc.abstractmethod
    def update(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        """Update an existing record in the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The key of the record to update.
            record: The updated record object.

        Raises:
            KeyError: If the record does not exist.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, schema_name: str, record_key: str) -> None:
        """Delete a record from the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The key of the record is to delete.

        Raises:
            KeyError: If the record does not exist.
        """
        raise NotImplementedError

    def bulk_delete(self, schema_name: str, record_keys: typing.List[str]) -> int:
        """Delete multiple records in a single operation.

        Args:
            schema_name: The schema containing the records.
            record_keys: List of record keys to delete.

        Raises:
            ConnectionError: If Redis operation fails.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(
        self,
        schema_name: str,
        record_key: Union[str, int],
        record_klass: Type["KeyValueStoreIntegrationMixin"],
    ) -> Optional["KeyValueStoreIntegrationMixin"]:
        """Retrieve a single record from the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The key of the record to retrieve.
            record_klass: The class to instantiate the record with.

        Returns:
            The record instance if found, None otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def filter(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> Iterable["KeyValueStoreIntegrationMixin"]:
        """Filter records matching the specified criteria.

        Args:
            schema_name: The schema/namespace to filter within.
            record_klass: The class to instantiate records with.
            **filter_kwargs: Attribute-value pairs to filter by.

        Returns:
            An iterable of matching record instances.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def count(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema/namespace to count within.
            record_klass: The class to instantiate records with.
            **filter_kwargs: Optional attribute-value pairs to filter by.

        Returns:
            The number of matching records.
        """
        raise NotImplementedError

    @staticmethod
    def load_record(
        record_state: bytes, record_klass: Type["KeyValueStoreIntegrationMixin"]
    ) -> "KeyValueStoreIntegrationMixin":
        """Load a record from its serialized state.

        Args:
            record_state: The serialized record data.
            record_klass: The class to instantiate the record with.

        Returns:
            The instantiated record object.

        Raises:
            SerializationError: If deserialization fails.
        """
        try:
            state = json.loads(record_state)
            record = record_klass.__new__(record_klass)
            record.__setstate__(state)
            return record
        except Exception as e:
            raise SerializationError(f"Failed to load record: {e}")

    @abc.abstractmethod
    def reload(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> "KeyValueStoreIntegrationMixin":
        """Reload a record's data from the backend.

        Args:
            schema_name: The schema/namespace containing the record.
            record: The record to reload.

        Returns:
            The reloaded record instance.

        Raises:
            KeyError: If the record no longer exists.
        """
        raise NotImplementedError

    def upsert(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        """Insert or update a record (upsert operation).

        Args:
            schema_name: The schema/namespace for the operation.
            record_key: The key of the record.
            record: The record object to upsert.
        """
        if self.exists(schema_name, record_key):
            self.update(schema_name, record_key, record)
        else:
            self.insert(schema_name, record_key, record)

    def list_all(
        self, schema_name: str, record_klass: Type["KeyValueStoreIntegrationMixin"]
    ) -> Iterable["KeyValueStoreIntegrationMixin"]:
        """List all records in a schema.

        Args:
            schema_name: The schema/namespace to list from.
            record_klass: The class to instantiate records with.

        Returns:
            An iterable of all records in the schema.
        """
        return self.filter(schema_name, record_klass)
