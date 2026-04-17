import copy
import threading
import typing

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.backends.connection import BackendConnectorBase
from volnux.exceptions import ObjectDoesNotExist, ObjectExistError


class DummyConnector(BackendConnectorBase):

    def __init__(self, **_: typing.Any):
        pass

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def ping(self) -> bool:
        return True

    def get_cursor(self) -> typing.Any:
        return {}

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def begin_transaction(self) -> None:
        pass


class InMemoryKeyValueStoreBackend(KeyValueStoreBackendBase):
    """
    In-memory implementation of the KeyValueStoreBackend.

    This class provides a simple in-memory key-value storage solution. It acts
    as a backend for managing data associated with different schemas and offers
    CRUD functionalities. The data is stored in memory, and no persistence is
    provided. It is suitable for testing and scenarios where persistence is
    not required.

    :ivar connector_klass: Specifies the default connector class for the
        backend. This is set to `DummyConnector`.
    :type connector_klass: type
    """

    connector_klass = DummyConnector

    def __init__(self, namespace_prefix: typing.Optional[str] = None, **_: typing.Any):
        super().__init__(namespace_prefix)
        self._storage: typing.Dict[str, typing.Dict[str, typing.Any]] = {}

    def close(self) -> None:
        with self._acquire_lock():
            self._storage.clear()

    def create_filter_predicate(
        self, **filter_kwargs: typing.Any
    ) -> typing.Callable[[typing.Any], bool]:
        return self._create_filter_predicate(**filter_kwargs)

    def _get_schema_bucket(self, schema_name: str) -> typing.Dict[str, typing.Any]:
        return self._storage.setdefault(schema_name, {})

    def exists(self, schema_name: str, record_key: str) -> bool:
        with self._acquire_lock():
            return record_key in self._get_schema_bucket(schema_name)

    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: typing.Any,
        ttl: typing.Optional[int] = None,
    ) -> None:
        del ttl
        with self._acquire_lock():
            bucket = self._get_schema_bucket(schema_name)
            if record_key in bucket:
                raise ObjectExistError(
                    f"Record '{record_key}' already exists in schema '{schema_name}'"
                )
            bucket[record_key] = copy.deepcopy(record)

    def update(self, schema_name: str, record_key: str, record: typing.Any) -> None:
        with self._acquire_lock():
            bucket = self._get_schema_bucket(schema_name)
            if record_key not in bucket:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )
            bucket[record_key] = copy.deepcopy(record)

    def delete(self, schema_name: str, record_key: str) -> None:
        with self._acquire_lock():
            bucket = self._get_schema_bucket(schema_name)
            if record_key not in bucket:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )
            del bucket[record_key]

    def get(
        self,
        schema_name: str,
        record_key: typing.Union[str, int],
        record_klass: typing.Type[typing.Any],
    ) -> typing.Optional[typing.Any]:
        del record_klass
        with self._acquire_lock():
            bucket = self._get_schema_bucket(schema_name)
            if str(record_key) not in bucket:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )
            return copy.deepcopy(bucket[str(record_key)])

    def filter(
        self,
        schema_name: str,
        record_klass: typing.Type[typing.Any],
        **filter_kwargs: typing.Any,
    ) -> typing.Iterable[typing.Any]:
        del record_klass
        predicate = self.create_filter_predicate(**filter_kwargs)
        with self._acquire_lock():
            bucket = self._get_schema_bucket(schema_name)
            return [
                copy.deepcopy(value) for value in bucket.values() if predicate(value)
            ]

    def count(
        self,
        schema_name: str,
        record_klass: typing.Optional[typing.Type[typing.Any]] = None,
        **filter_kwargs: typing.Any,
    ) -> int:
        del record_klass
        return len(
            list(
                self.filter(
                    schema_name,
                    typing.cast(typing.Type[typing.Any], object),
                    **filter_kwargs,
                )
            )
        )

    def reload(self, schema_name: str, record: typing.Any) -> typing.Any:
        fresh = self.get(schema_name, record.id, record.__class__)
        record.__dict__.update(fresh.__dict__)
        return record

    def bulk_get(
        self,
        schema_name: str,
        record_keys: typing.List[str],
        record_klass: typing.Type[typing.Any],
    ) -> typing.List[typing.Any]:
        results: typing.List[typing.Any] = []
        for record_key in record_keys:
            try:
                results.append(self.get(schema_name, record_key, record_klass))
            except ObjectDoesNotExist:
                continue
        return results

    def clear_schema(self, schema_name: str) -> None:
        with self._acquire_lock():
            self._storage.pop(schema_name, None)
