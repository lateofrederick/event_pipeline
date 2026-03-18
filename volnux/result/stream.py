import logging
import math
import typing
from dataclasses import dataclass, field

from volnux.backends.store import KeyValueStoreBackendBase
from volnux.backends.stores.inmemory_store import InMemoryKeyValueStoreBackend
from volnux.mixins import KeyValueStoreIntegrationMixin

try:
    from typing import TypeAlias  # noqa: F401
except ImportError:
    from typing_extensions import TypeAlias

__all__ = ["ResultStream"]

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="KeyValueStoreIntegrationMixin")

Result: TypeAlias = typing.Hashable  # Placeholder for a Result type


@dataclass
class ResultStream(typing.Generic[T]):
    """
    Hybrid lazy-loading result stream with a transient in-memory tier and an
    optional persisted tier.

    Resolution order:
    1. in-memory backend
    2. persisted backend (if the object is marked as persisted)
    """

    model_klass: typing.Type[T]
    transaction_id: str
    chunk_size: int = 100
    memory_backend: typing.Optional[KeyValueStoreBackendBase] = field(
        default=None, repr=False
    )
    persisted_backend: typing.Optional[KeyValueStoreBackendBase] = field(
        default=None, repr=False
    )

    # Internal state
    _keys: typing.List[str] = field(default_factory=list, repr=False)
    _key_set: set = field(default_factory=set, repr=False)
    _predicates: typing.List[typing.Callable[[T], bool]] = field(
        default_factory=list, repr=False
    )

    def __post_init__(self) -> None:
        if self.chunk_size < 1:
            raise ValueError(f"chunk_size must be >= 1, got {self.chunk_size}")

        if self.memory_backend is None:
            self.memory_backend = InMemoryKeyValueStoreBackend(
                namespace_prefix=self.transaction_id
            )

        if self.persisted_backend is None:
            try:
                self.persisted_backend = self.model_klass.get_backend()
            except Exception:
                self.persisted_backend = None

    @classmethod
    def _make(
        cls,
        model_klass: typing.Type[T],
        transaction_id: str,
        keys: typing.List[str],
        predicates: typing.List[typing.Callable[[T], bool]],
        chunk_size: int,
        memory_backend: KeyValueStoreBackendBase,
        persisted_backend: typing.Optional[KeyValueStoreBackendBase],
    ) -> "ResultStream[T]":
        """Internal factory that bypasses add() for bulk key assignment."""
        stream: ResultStream[T] = cls.__new__(cls)
        stream.model_klass = model_klass
        stream.transaction_id = transaction_id
        stream.chunk_size = chunk_size
        stream.memory_backend = memory_backend
        stream.persisted_backend = persisted_backend
        stream._keys = list(keys)
        stream._key_set = set(keys)
        stream._predicates = list(predicates)
        return stream

    def _schema_name(self) -> str:
        return self.model_klass.get_schema_name()

    def _memory_get_or_none(self, record_id: str) -> typing.Optional[T]:
        try:
            return typing.cast(
                T,
                self.memory_backend.get(
                    self._schema_name(), record_id, self.model_klass
                ),
            )
        except Exception:
            return None

    def _persisted_get_or_none(self, record_id: str) -> typing.Optional[T]:
        if self.persisted_backend is None:
            return None
        try:
            return typing.cast(
                T,
                self.persisted_backend.get(
                    self._schema_name(), record_id, self.model_klass
                ),
            )
        except Exception:
            return None

    def add(self, instance: T, persist: typing.Optional[bool] = None) -> None:
        """
        Adds an instance to the stream.

        The result is always written to the in-memory tier first.
        If `persist` is True, or the instance advertises `is_persisted=True`,
        it is also mirrored to the durable backend.
        """
        key = instance.id
        should_persist = (
            persist if persist is not None else getattr(instance, "is_persisted", False)
        )

        self.memory_backend.upsert(self._schema_name(), key, instance)

        if should_persist and self.persisted_backend is not None:
            self.persisted_backend.upsert(self._schema_name(), key, instance)
            if hasattr(instance, "is_persisted"):
                instance.is_persisted = True

        if key not in self._key_set:
            self._keys.append(key)
            self._key_set.add(key)

    def persist(self, instance: T) -> None:
        """Persist an already tracked instance to the durable backend."""
        if self.persisted_backend is None:
            raise RuntimeError(
                f"No persisted backend configured for {self.model_klass.__name__}"
            )

        self.persisted_backend.upsert(self._schema_name(), instance.id, instance)
        if hasattr(instance, "is_persisted"):
            instance.is_persisted = True
        self.memory_backend.upsert(self._schema_name(), instance.id, instance)

        if instance.id not in self._key_set:
            self._keys.append(instance.id)
            self._key_set.add(instance.id)

    def filter(self, **filter_kwargs) -> "VirtualResultStream[T]":
        """
        Registers a filter predicate to be applied lazily during iteration.
        Returns a new stream; the original is not mutated.
        """
        backend = self.persisted_backend or self.memory_backend
        new_predicate: typing.Callable[[T], bool] = backend.create_filter_predicate(
            **filter_kwargs
        )
        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates + [new_predicate],
            chunk_size=self.chunk_size,
            memory_backend=self.memory_backend,
            persisted_backend=self.persisted_backend,
        )

    def where(self, predicate: typing.Callable[[T], bool]) -> "VirtualResultStream[T]":
        """
        Lower-level alternative to filter(): register any callable predicate
        directly without going through a backend.
        """
        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates + [predicate],
            chunk_size=self.chunk_size,
            memory_backend=self.memory_backend,
            persisted_backend=self.persisted_backend,
        )

    def shard(self, num_shards: int) -> typing.List["VirtualResultStream[T]"]:
        """
        Partitions the ID list into N sub-streams for parallel processing.
        """
        if num_shards <= 1 or not self._keys:
            return [self]

        total = len(self._keys)
        shard_size = math.ceil(total / num_shards)
        actual_shards = math.ceil(total / shard_size)

        if actual_shards < num_shards:
            logger.warning(
                "Requested %d shards but only %d keys available; "
                "returning %d shard(s).",
                num_shards,
                total,
                actual_shards,
            )

        return [
            ResultStream._make(
                model_klass=self.model_klass,
                transaction_id=self.transaction_id,
                keys=self._keys[i : i + shard_size],
                predicates=self._predicates,
                chunk_size=self.chunk_size,
                memory_backend=self.memory_backend,
                persisted_backend=self.persisted_backend,
            )
            for i in range(0, total, shard_size)
        ]

    def __iter__(self) -> typing.Iterator[T]:
        """
        Terminal operation. Fetches objects in batches from the hybrid storage
        stack and applies predicates in memory.
        """
        for i in range(0, len(self._keys), self.chunk_size):
            batch_ids = self._keys[i : i + self.chunk_size]
            for instance in self._fetch_batch(batch_ids):
                if all(predicate(instance) for predicate in self._predicates):
                    yield instance

    def first(self) -> typing.Optional[T]:
        """Returns the first filtered result, or None if the stream is empty."""
        return next(iter(self), None)

    def has_results(self) -> bool:
        """Returns True if at least one object survives the filter pipeline."""
        return self.first() is not None

    def __len__(self) -> int:
        """Returns the number of tracked IDs before filtering."""
        return len(self._keys)

    def _fetch_batch(self, batch_ids: typing.List[str]) -> typing.List[T]:
        """
        Resolves a batch using memory-first lookup and persisted fallback.
        """
        results: typing.List[T] = []

        for rid in batch_ids:
            try:
                instance = self._memory_get_or_none(rid)
                if instance is None:
                    instance = self._persisted_get_or_none(rid)
                    if instance is not None:
                        self.memory_backend.upsert(self._schema_name(), rid, instance)

                if instance is not None:
                    results.append(instance)
            except Exception:
                logger.exception(
                    "Failed to fetch %s(id=%r) from stream storage; skipping.",
                    self.model_klass.__name__,
                    rid,
                )

        return results

    def evict_memory(self, record_id: typing.Optional[str] = None) -> None:
        """Evict one or all in-memory copies without touching durable storage."""
        if record_id is not None:
            try:
                self.memory_backend.delete(self._schema_name(), record_id)
            except Exception:
                return
            return

        for rid in list(self._keys):
            try:
                self.memory_backend.delete(self._schema_name(), rid)
            except Exception:
                continue

    def __repr__(self) -> str:
        persisted_count = 0
        for rid in self._keys:
            instance = self._memory_get_or_none(rid) or self._persisted_get_or_none(rid)
            if instance is not None and getattr(instance, "is_persisted", False):
                persisted_count += 1

        return (
            f"<VirtualResultStream: {self.model_klass.__name__} | "
            f"Tx: {self.transaction_id} | "
            f"Keys: {len(self._keys)} | "
            f"Persisted: {persisted_count} | "
            f"Filters: {len(self._predicates)}>"
        )
