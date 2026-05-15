import orjson as json
import logging
import sqlite3
from types import UnionType
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    TYPE_CHECKING,
)

from formax import BaseModel
from formax.typing import get_type

from volnux.backends.connectors.sqlite import SqliteConnector
from volnux.backends.store import KeyValueStoreBackendBase, YoyoMigrationsMixin
from volnux.exceptions import (
    ObjectDoesNotExist,
    ObjectExistError,
    SqlOperationError,
    SerializationError,
)

if TYPE_CHECKING:
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


logger = logging.getLogger(__name__)


class SqliteStoreBackend(YoyoMigrationsMixin, KeyValueStoreBackendBase):
    """SQLite-backed key-value store implementation.

    This backend uses SQLite tables to store records, with automatic schema
    creation and migration support. Records are serialized and
    stored in a dedicated column, while individual fields are also stored
    for efficient querying.

    Example:
        >>> backend = SqliteStoreBackend(database="myapp.db")
        >>> backend.insert("users", "user_1", user_record)
        >>> user = backend.get("users", UserModel, "user_1")
        >>> active_users = backend.filter("users", UserModel, status="active")
    """

    connector_klass = SqliteConnector

    def __init__(self, **connector_config: Any):
        """Initialize the SQLite store backend.

        Args:
            **connector_config: Configuration passed to SqliteConnector.
        """
        super().__init__(**connector_config)

        if not self.connector.is_connected():
            self.connector.connect()

        self._schema_cache: Dict[str, bool] = {}

    def _ensure_connected(self) -> None:
        """Ensure the SQLite connection is active.

        Raises:
            ConnectionError: If a connection cannot be established.
        """
        if not self.connector.is_connected():
            logger.warning("SQLite connection lost, attempting to reconnect...")
            self.connector.connect()

    def _invalidate_schema_cache(self, schema_name: Optional[str] = None) -> None:
        """Invalidate the schema cache.

        Args:
            schema_name: Specific schema to invalidate, or None for all.
        """
        if schema_name:
            self._schema_cache.pop(schema_name, None)
        else:
            self._schema_cache.clear()



    def schema_exists(self, schema_name: str) -> bool:
        """Check if a schema (table) exists in the database.

        Args:
            schema_name: The name of the schema to check.

        Returns:
            True if the schema exists, False otherwise.
        """
        if schema_name in self._schema_cache:
            return self._schema_cache[schema_name]

        self._ensure_connected()

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (schema_name,),
            )
            exists = cursor.fetchone() is not None
            cursor.close()

            self._schema_cache[schema_name] = exists
            return exists
        except sqlite3.Error as e:
            logger.error(f"Error checking schema existence: {e}")
            return False

    def _map_python_type_to_sql(self, field_type: Any) -> str:
        """Map Python types to SQLite types.

        Args:
            field_type: The Python type annotation.

        Returns:
            The corresponding SQLite type as a string.
        """
        field_type = get_type(field_type)

        type_mapping = {
            bool: "BOOLEAN",
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            bytes: "BLOB",
        }

        return type_mapping.get(field_type, "TEXT")  # Default to TEXT for JSON

    def _is_optional_field(self, field_type: Any) -> bool:
        """Check if a field type allows None."""
        origin = get_origin(field_type)
        if origin in (Union, UnionType):
            return type(None) in get_args(field_type)
        return False

    def create_schema(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        """Create a schema (table) based on a record's structure.

        Args:
            schema_name: The name of the schema to create.
            record: A sample record to derive the schema from.

        Raises:
            ObjectExistError: If schema exists and if_not_exists is False.
            SqlOperationError: If schema creation fails.
        """
        self._ensure_connected()

        try:
            fields = ["id TEXT PRIMARY KEY"]

            record_type_hints = get_type_hints(record.__class__)

            # Add fields from record annotations
            for field_name, field_type in record_type_hints.items():
                if field_name.startswith("_"):
                    continue  # Skip private fields

                sql_type = self._map_python_type_to_sql(field_type)
                is_optional = self._is_optional_field(field_type)

                field_def = f"{field_name} {sql_type}"
                if not is_optional:
                    field_def += " NOT NULL"

                fields.append(field_def)

            # Add a special column for the serialized record state
            fields.append("_record_state BLOB NOT NULL")

            # Create table
            fields_str = ", ".join(fields)
            create_table_sql = f"CREATE TABLE {schema_name} ({fields_str})"

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(create_table_sql)
                cursor.close()

            self._invalidate_schema_cache(schema_name)
            logger.info(f"Created schema '{schema_name}' with {len(fields)} fields")

        except sqlite3.Error as e:
            logger.error(f"Error creating schema '{schema_name}': {e}")
            raise SqlOperationError(f"Error creating schema: {e}")

    def drop_schema(self, schema_name: str) -> None:
        """Drop a schema (table) from the database.

        Args:
            schema_name: The name of the schema to drop.

        Raises:
            SqlOperationError: If schema drop fails.
        """
        self._ensure_connected()

        try:
            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(f"DROP TABLE {schema_name}")
                cursor.close()

            self._invalidate_schema_cache(schema_name)
            logger.info(f"Dropped schema '{schema_name}'")

        except sqlite3.Error as e:
            logger.error(f"Error dropping schema '{schema_name}': {e}")
            raise SqlOperationError(f"Error dropping schema: {e}")

    def list_schemas(self) -> List[str]:
        """List all schemas (tables) in the database.

        Returns:
            List of schema names.
        """
        self._ensure_connected()

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return schemas
        except sqlite3.Error as e:
            logger.error(f"Error listing schemas: {e}")
            raise SqlOperationError(f"Error listing schemas: {e}")

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
        """Create a foreign key constraint between two tables."""
        from ..formax_fk import OnDelete

        on_delete_sql = {
            OnDelete.CASCADE: "CASCADE",
            OnDelete.SET_NULL: "SET NULL",
            OnDelete.SET_DEFAULT: "SET DEFAULT",
            OnDelete.PROTECT: "NO ACTION",  # Database will raise error
            OnDelete.DO_NOTHING: "NO ACTION",
        }.get(on_delete, "NO ACTION")

        constraint_name = f"fk_{source_schema}_{source_field}_{target_schema}"

        ddl = (
            f"ALTER TABLE {source_schema} "
            f"ADD CONSTRAINT {constraint_name} "
            f"FOREIGN KEY ({source_field}_object_id) "
            f"REFERENCES {target_schema}(id) "
            f"ON DELETE {on_delete_sql}"
        )
        with self.connector.transaction():
            cursor = self.connector.get_cursor()
            cursor.execute(ddl)
            cursor.close()

    def supports_foreign_keys(self) -> bool:
        return True

    # def _serialize_record(self, record: BaseModel) -> bytes:
    #     """Serialize a record to bytes.
    #
    #     Args:
    #         record: The record to serialize.
    #
    #     Returns:
    #         Serialized record as bytes.
    #
    #     Raises:
    #         SerializationError: If serialization fails.
    #     """
    #     try:
    #         state = record.__getstate__()
    #         return pickle.dumps(state, protocol=self.PICKLE_PROTOCOL)
    #     except Exception as e:
    #         logger.error(f"Failed to serialize record: {e}")
    #         raise SerializationError(f"Serialization failed: {e}")

    # def _deserialize_record(
    #     self, data: bytes, record_klass: Type[BaseModel]
    # ) -> BaseModel:
    #     """Deserialize bytes to a record object.
    #
    #     Args:
    #         data: Serialized record data.
    #         record_klass: The class to instantiate.
    #
    #     Returns:
    #         Deserialized record instance.
    #
    #     Raises:
    #         SerializationError: If deserialization fails.
    #     """
    #     try:
    #         state = pickle.loads(data)
    #         record = record_klass.__new__(record_klass)
    #         record.__setstate__(state)
    #         return record
    #     except Exception as e:
    #         logger.error(f"Failed to deserialize record: {e}")
    #         raise SerializationError(f"Deserialization failed: {e}")

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
                record_data[field_name] = json.dumps(value)
            elif value is not None:
                record_data[field_name] = value
            else:
                record_data[field_name] = None

        record_data["_record_state"] = self._serialize_record(record)

        return record_data

    def _convert_key_type(self, record_key: Union[str, int]) -> Union[str, int]:
        """Convert the record key to the appropriate type.

        Args:
            record_key: The key to convert.

        Returns:
            Converted key (tries int, falls back to str).
        """
        if isinstance(record_key, str):
            try:
                return int(record_key)
            except ValueError:
                return record_key
        return record_key

    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record is to check.

        Returns:
            True if the record exists, False otherwise.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            return False

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                f"SELECT 1 FROM {schema_name} WHERE id = ? LIMIT 1",
                (record_key,),
            )
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except sqlite3.Error as e:
            logger.error(f"Error checking record existence: {e}")
            return False

    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: Optional[int] = None,
    ) -> None:
        """Insert a new record into the store.

        Args:
            schema_name: The schema to insert into.
            record_key: The unique key for the record.
            record: The record object to insert.
            ttl: Time to live in seconds for the record. If None, no TTL is set.

        Raises:
            ObjectExistError: If a record with the same key already exists.
            SerializationError: If serialization fails.
            SqlOperationError: If insertion fails.
        """
        self._ensure_connected()

        try:
            self.ensure_schema(schema_name, record)

            if self.exists(schema_name, record_key):
                raise ObjectExistError(
                    f"Record '{record_key}' already exists in schema '{schema_name}'"
                )

            record_data = self._prepare_record_data(record, record_key)

            fields = list(record_data.keys())
            placeholders = ["?" for _ in fields]
            values = [record_data[field] for field in fields]

            insert_sql = f"""
                INSERT INTO {schema_name} ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
            """

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(insert_sql, values)
                cursor.close()

            logger.debug(f"Inserted record '{record_key}' into schema '{schema_name}'")

        except ObjectExistError:
            raise
        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error inserting record: {e}")
            raise SqlOperationError(f"Error inserting record: {e}")

    def update(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> None:
        """Update an existing record in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record is to update.
            record: The updated record object.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If serialization fails.
            SqlOperationError: If the update fails.
        """
        self._ensure_connected()
        record_key = str(self._convert_key_type(record_key))

        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' does not exist in schema '{schema_name}'"
            )

        try:

            record_data = self._prepare_record_data(record, record_key)

            set_fields = [
                f"{field} = ?" for field in record_data.keys() if field != "id"
            ]
            values = [
                record_data[field] for field in record_data.keys() if field != "id"
            ]
            values.append(record_key)

            update_sql = f"""
                UPDATE {schema_name}
                SET {', '.join(set_fields)}
                WHERE id = ?
            """

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(update_sql, values)
                cursor.close()

            logger.debug(f"Updated record '{record_key}' in schema '{schema_name}'")

        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error updating record: {e}")
            raise SqlOperationError(f"Error updating record: {e}")

    def upsert(self, schema_name: str, record_key: str, record: BaseModel) -> None:
        """Insert or update a record (upsert operation).

        Args:
            schema_name: The schema for the operation.
            record_key: The key of the record.
            record: The record object to upsert.

        Raises:
            SerializationError: If serialization fails.
            SqlOperationError: If upsert fails.
        """
        self._ensure_connected()

        try:
            self.ensure_schema(schema_name, record)

            record_data = self._prepare_record_data(record, record_key)

            fields = list(record_data.keys())
            placeholders = ["?" for _ in fields]
            update_fields = [
                f"{field} = excluded.{field}" for field in fields if field != "id"
            ]
            values = [record_data[field] for field in fields]

            upsert_sql = f"""
                INSERT INTO {schema_name} ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT(id) DO UPDATE SET
                {', '.join(update_fields)}
            """

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(upsert_sql, values)
                cursor.close()

            logger.debug(f"Upserted record '{record_key}' in schema '{schema_name}'")

        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error upserting record: {e}")
            raise SqlOperationError(f"Error upserting record: {e}")

    def delete(self, schema_name: str, record_key: str) -> None:
        """Delete a record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record is to delete.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SqlOperationError: If deletion fails.
        """
        self._ensure_connected()
        record_key = str(self._convert_key_type(record_key))

        try:
            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(f"DELETE FROM {schema_name} WHERE id = ?", (record_key,))

                if cursor.rowcount == 0:
                    raise ObjectDoesNotExist(
                        f"Record '{record_key}' does not exist in schema '{schema_name}'"
                    )

                cursor.close()

            logger.debug(f"Deleted record '{record_key}' from schema '{schema_name}'")

        except ObjectDoesNotExist:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error deleting record: {e}")
            raise SqlOperationError(f"Error deleting record: {e}")

    def get(
        self,
        schema_name: str,
        record_key: Union[str, int],
        record_klass: Type[BaseModel],
    ) -> Optional[BaseModel]:
        """Retrieve a single record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to retrieve.
            record_klass: The class to instantiate the record with.

        Returns:
            The record instance.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If deserialization fails.
            SqlOperationError: If retrieval fails.
        """
        self._ensure_connected()
        record_key = str(self._convert_key_type(record_key))

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                f"SELECT _record_state FROM {schema_name} WHERE id = ?", (record_key,)
            )
            row = cursor.fetchone()
            cursor.close()

            if row is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )

            record = self._deserialize_record(row[0], record_klass)
            logger.debug(f"Retrieved record '{record_key}' from schema '{schema_name}'")
            return record

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error getting record: {e}")
            raise SqlOperationError(f"Error getting record: {e}")

    def _build_sql_filter(self, filter_kwargs: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """Build SQL WHERE clause from filter kwargs.

        Args:
            filter_kwargs: Dictionary of field filters.

        Returns:
            Tuple of (where_clause, parameters).
        """
        if not filter_kwargs:
            return "1", []

        conditions = []
        parameters = []

        for key, value in filter_kwargs.items():
            if "__" in key:
                field, operator = key.rsplit("__", 1)

                operator_map = {
                    "exact": ("= ?", [value]),
                    "contains": ("LIKE ?", [f"%{value}%"]),
                    "startswith": ("LIKE ?", [f"{value}%"]),
                    "endswith": ("LIKE ?", [f"%{value}"]),
                    "icontains": ("LIKE ? COLLATE NOCASE", [f"%{value}%"]),
                    "istartswith": ("LIKE ? COLLATE NOCASE", [f"{value}%"]),
                    "iendswith": ("LIKE ? COLLATE NOCASE", [f"%{value}"]),
                    "gt": ("> ?", [value]),
                    "gte": (">= ?", [value]),
                    "lt": ("< ?", [value]),
                    "lte": ("<= ?", [value]),
                    "ne": ("!= ?", [value]),
                    "isnull": ("IS NULL" if value else "IS NOT NULL", []),
                }

                if operator == "in":
                    placeholders = ",".join(["?" for _ in value])
                    conditions.append(f"{field} IN ({placeholders})")
                    parameters.extend(value)
                elif operator in operator_map:
                    sql_op, params = operator_map[operator]
                    conditions.append(f"{field} {sql_op}")
                    parameters.extend(params)
                else:
                    logger.warning(f"Unknown filter operator: {operator}")
                    conditions.append(f"{field} = ?")
                    parameters.append(value)
            else:
                conditions.append(f"{key} = ?")
                parameters.append(value)

        where_clause = " AND ".join(conditions)
        return where_clause, parameters

    def filter(
        self,
        schema_name: str,
        record_klass: Type[BaseModel],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        **filter_kwargs: Any,
    ) -> List[BaseModel]:
        """Filter records matching the specified criteria.

        Args:
            schema_name: The schema to filter within.
            record_klass: The class to instantiate records with.
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            order_by: Field to order by (prefix with '-' for descending).
            **filter_kwargs: Attribute-value pairs to filter by.

        Returns:
            List of matching record instances.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SerializationError: If deserialization fails.
            SqlOperationError: If query fails.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:

            where_clause, parameters = self._build_sql_filter(filter_kwargs)
            query = f"SELECT _record_state FROM {schema_name}"

            if where_clause != "1":
                query += f" WHERE {where_clause}"

            if order_by:
                if order_by.startswith("-"):
                    query += f" ORDER BY {order_by[1:]} DESC"
                else:
                    query += f" ORDER BY {order_by} ASC"

            if limit is not None:
                query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"

            cursor = self.connector.get_cursor()
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
            cursor.close()

            results = []
            for row in rows:
                try:
                    record = self._deserialize_record(row[0], record_klass)
                    results.append(record)
                except SerializationError as e:
                    logger.warning(f"Skipping corrupted record in '{schema_name}': {e}")
                    continue

            logger.debug(f"Filtered {len(results)} records from schema '{schema_name}'")
            return results

        except ObjectDoesNotExist:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error filtering records: {e}")
            raise SqlOperationError(f"Error filtering records: {e}")

    def count(self, schema_name: str, **filter_kwargs: Any) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema to count within.
            **filter_kwargs: Optional attribute-value pairs to filter by.

        Returns:
            The number of matching records.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SqlOperationError: If the count fails.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:
            where_clause, parameters = self._build_sql_filter(filter_kwargs)
            query = f"SELECT COUNT(*) FROM {schema_name}"

            if where_clause != "1":
                query += f" WHERE {where_clause}"

            cursor = self.connector.get_cursor()
            cursor.execute(query, parameters)
            result = cursor.fetchone()
            cursor.close()

            return int(result[0]) if result else 0

        except sqlite3.Error as e:
            logger.error(f"Error counting records: {e}")
            raise SqlOperationError(f"Error counting records: {e}")

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

    def reload(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> "KeyValueStoreIntegrationMixin":
        """Reload a record's data from the backend.

        Args:
            schema_name: The schema containing the record.
            record: The record to reload.

        Returns:
            The reloaded record instance.

        Raises:
            ObjectDoesNotExist: If the record no longer exists.
            SerializationError: If deserialization fails.
            SqlOperationError: If reload fails.
        """
        if not hasattr(record, "id"):
            raise ValueError("Record must have an 'id' attribute for reload")

        record_key = str(self._convert_key_type(record.id))
        self._ensure_connected()

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                f"SELECT _record_state FROM {schema_name} WHERE id = ?", (record_key,)
            )
            row = cursor.fetchone()
            cursor.close()

            if row is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' no longer exists in schema '{schema_name}'"
                )

            state = json.loads(row[0])
            record.__setstate__(state)

            logger.debug(f"Reloaded record '{record_key}' from schema '{schema_name}'")
            return record

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error reloading record: {e}")
            raise SqlOperationError(f"Error reloading record: {e}")
