import logging
from typing import Type, cast, TYPE_CHECKING

from .fields import OnDelete

if TYPE_CHECKING:
    from .store import YoyoMigrationsMixin
    from volnux.backends.store import KeyValueStoreBackendBase
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin
    from volnux.backends.stores.postgres import PostgresStoreBackend
    from volnux.backends.stores.sqlite import SqliteStoreBackend

logger = logging.getLogger(__name__)


def _apply_foreign_key_constraint(
    cls,
    field_name: str,
    target_model: Type["KeyValueStoreIntegrationMixin"],
    on_delete: OnDelete,
    nullable: bool,
) -> None:
    """Apply the strongest possible foreign key constraint.

    If both models are on the same backend that supports native FK constraints,
    a database-level constraint is generated. Otherwise, the software-level
    constraint via _on_delete_hook and backreferences is used.

    This is called during schema creation / migration, not at runtime.
    """
    source_backend = cls.get_backend()
    target_backend = target_model.get_backend()

    if _can_use_native_fk(source_backend, target_backend):
        _create_native_fk_constraint(
            source_backend=source_backend,
            source_schema=cls.get_schema_name(),
            source_field=field_name,
            target_schema=target_model.get_schema_name(),
            target_field="id",
            on_delete=on_delete,
            nullable=nullable,
        )
        logger.info(
            "Applied native FK constraint: %s.%s -> %s.id [%s]",
            cls.get_schema_name(),
            field_name,
            target_model.get_schema_name(),
            on_delete.value,
        )
    else:
        # Cross-backend or backend without FK support — software constraint
        target_model.register_backreference(
            field_name=field_name,
            field_attrib=None,
            referencing_model=cls,
            reverse_name=None,  # Auto-generated
        )
        logger.info(
            "Applied software FK constraint: %s.%s -> %s [%s]",
            cls.get_schema_name(),
            field_name,
            target_model.get_schema_name(),
            on_delete.value,
        )


def _can_use_native_fk(
    source_backend: "KeyValueStoreBackendBase",
    target_backend: "KeyValueStoreBackendBase",
) -> bool:
    """Check if native foreign key constraints can be used.

    Requirements:
    1. Both backends are the same instance (same connection)
    2. The backend supports native FK constraints
    3. Both schemas are in the same database
    """
    # Same backend type
    if type(source_backend) is not type(target_backend):
        return False

    # Backend supports native FK
    if not hasattr(source_backend, "supports_foreign_keys"):
        return False
    if not source_backend.supports_foreign_keys():
        return False

    # Same database (check connection details)
    if not _same_database(source_backend, target_backend):
        return False

    return True


def _same_database(
    backend_a: "KeyValueStoreBackendBase",
    backend_b: "KeyValueStoreBackendBase",
) -> bool:
    """Check if two backends point to the same database."""
    conn_a = backend_a.connector
    conn_b = backend_b.connector

    # Compare connection parameters
    if hasattr(conn_a, "config") and hasattr(conn_b, "config"):
        config_a = conn_a.config
        config_b = conn_b.config

        return (
            config_a.host == config_b.host
            and config_a.port == config_b.port
            and config_a.database == config_b.database
        )

    return False


def _create_native_fk_constraint(
    source_backend: "KeyValueStoreBackendBase",
    source_schema: str,
    source_field: str,
    target_schema: str,
    target_field: str,
    on_delete: OnDelete,
    nullable: bool,
) -> None:
    """Create a native database-level foreign key constraint.

    Generates appropriate DDL for the backend type.
    """
    # Map on_delete to database-specific action
    on_delete_sql = {
        OnDelete.CASCADE: "CASCADE",
        OnDelete.SET_NULL: "SET NULL",
        OnDelete.SET_DEFAULT: "SET DEFAULT",
        OnDelete.PROTECT: "NO ACTION",  # Database will raise error
        OnDelete.DO_NOTHING: "NO ACTION",
    }.get(on_delete, "NO ACTION")

    constraint_name = f"fk_{source_schema}_{source_field}_{target_schema}"

    if isinstance(source_backend, PostgresStoreBackend):
        ddl = (
            f"ALTER TABLE {source_schema} "
            f"ADD CONSTRAINT {constraint_name} "
            f"FOREIGN KEY ({source_field}_object_id) "
            f"REFERENCES {target_schema}(id) "
            f"ON DELETE {on_delete_sql} "
            f"{'NOT VALID' if not source_backend._validate_immediately else ''}"
        )

    elif isinstance(source_backend, SqliteStoreBackend):
        ddl = (
            f"ALTER TABLE {source_schema} "
            f"ADD CONSTRAINT {constraint_name} "
            f"FOREIGN KEY ({source_field}_object_id) "
            f"REFERENCES {target_schema}(id) "
            f"ON DELETE {on_delete_sql}"
        )

    else:
        # Backend supports FK but we don't have DDL generation for it
        # Fall back to software constraint
        raise ValueError(
            f"Native FK not implemented for {type(source_backend).__name__}"
        )

    source_backend.connector.execute_query(ddl)


def migrate_models(
    *models: Type["KeyValueStoreIntegrationMixin"], dry_run=False
) -> None:
    """
    Migrates models to the database by creating the necessary tables and constraints.

    Args:
        *models: Variable number of KeyValueStoreIntegrationMixin subclasses to migrate.
        dry_run: If True, print logs

    Raises:
        TypeError: If any model is not a subclass of KeyValueStoreIntegrationMixin.
    """

    # Validate all models before migrating any (fail-fast)
    for model in models:
        if not issubclass(model, KeyValueStoreIntegrationMixin):
            raise TypeError(
                f"Model {model.__name__} is not a subclass of KeyValueStoreIntegrationMixin"
            )

    for model in models:
        try:
            model_backend = cast(object, model.get_backend())
            model_backend = cast(YoyoMigrationsMixin, model_backend)
        except Exception as e:
            logger.error("Failed to get backend for %s: %s", model.__name__, e)
            continue

        if not hasattr(model_backend, "ensure_schema"):
            logger.warning(
                "Backend %s does not support model migration.",
                model_backend.__class__.__name__,
            )
            continue

        num_applied = model_backend.ensure_schema(
            model.get_schema_name(), model, dry_run=dry_run
        )

        if num_applied > 0:
            logger.info("Applied %d migrations to %s.", num_applied, model.__name__)
        else:
            logger.debug("No migrations to apply for %s.", model.__name__)
