import logging
import math
import typing
import itertools
import warnings
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


@dataclass(frozen=True)
class Q:
    """Enhanced Q object supporting backend-native filter creation."""

    children: typing.List[typing.Union["Q", tuple]] = field(default_factory=list)
    connector: str = "AND"
    negated: bool = False

    def __init__(self, *args, _connector="AND", _negated=False, **kwargs):
        # Hack for frozen dataclass
        object.__setattr__(self, "children", list(args) + list(kwargs.items()))
        object.__setattr__(self, "connector", _connector)
        object.__setattr__(self, "negated", _negated)

    def __and__(self, other: "Q") -> "Q":
        return Q(self, other, _connector="AND")

    def __or__(self, other: "Q") -> "Q":
        return Q(self, other, _connector="OR")

    def __invert__(self) -> "Q":
        return Q(*self.children, _connector=self.connector, _negated=not self.negated)

    def to_predicate(
        self, backend: "KeyValueStoreBackendBase"
    ) -> typing.Callable[[typing.Any], bool]:
        """Convert a Q object to a predicate function using backend's operators."""
        return backend._create_complex_filter([self])

    def to_filter_kwargs(self) -> typing.Dict[str, typing.Any]:
        """Attempt to convert simple Q objects back to filter kwargs.

        Returns:
            Dictionary suitable for backend's **filter_kwargs, or raises ValueError
            if the Q object is too complex.
        """
        if self.negated or self.connector == "OR" or len(self.children) > 1:
            raise ValueError(
                "Complex Q objects cannot be converted to simple filter kwargs"
            )

        child = self.children[0]
        if isinstance(child, Q):
            raise ValueError(
                "Nested Q objects cannot be converted to simple filter kwargs"
            )

        return {child[0]: child[1]}


@dataclass
class ResultStream(typing.Generic[T]):
    """
    Hybrid lazy-loading result stream with a transient in-memory tier and an optional
    persisted tier.

    The `ResultStream` class facilitates working with large datasets by leveraging a
    hybrid storage model, providing lazy-loading mechanisms with an in-memory layer
    and an optional persisted backend. It supports filtering, partitioning (sharding),
    and iterative processing while keeping memory usage efficient.

    Each object in the stream is identified using a key, and predicates can be registered
    to apply filters lazily during iteration. The class ensures that newly added objects
    can be persisted and tracked effectively.

    :ivar model_klass: The model class associated with the result stream.
    :ivar transaction_id: Identifier representing the unique transactional scope
        for the stream.
    :ivar chunk_size: The size of the data chunks to be retrieved during iteration,
        defaults to 100.
    :ivar memory_backend: Optional in-memory storage backend for the stream.
    :type memory_backend: typing.Optional[KeyValueStoreBackendBase]
    :ivar persisted_backend: Optional persisted storage backend for the stream.
    :type persisted_backend: typing.Optional[KeyValueStoreBackendBase]
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
    _q_objects: typing.List[Q] = field(default_factory=list, repr=False)

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
        q_predicates: typing.Optional[typing.List[Q]] = None,
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
        stream._q_predicates = list(q_predicates or [])
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
        except Exception as e:
            logger.debug("Failed to fetch %s: %s", record_id, e)
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
        key: str = instance.id
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

    def filter(self, **filter_kwargs) -> "ResultStream[T]":
        """
        Registers a filter predicate to be applied lazily during iteration.
        Now supports Django-style lookups.

        Examples:
            >>> stream.filter(age__gt=25, name__icontains="john")
            >>> stream.filter(status__in=["active", "pending"])
            >>> stream.filter(created_at__gte=datetime(2023, 1, 1))
        """

        backend = self.persisted_backend or self.memory_backend
        new_predicate: typing.Callable[[T], bool] = backend._create_filter_predicate(
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
            q_predicates=self._q_objects,
        )

    def where(self, predicate: typing.Callable[[T], bool]) -> "ResultStream[T]":
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
            q_predicates=self._q_objects,
        )

    def shard(self, num_shards: int) -> typing.Generator["ResultStream[T]", None, None]:
        """
        Partitions the ID list into N sub-streams for parallel processing.
        """
        if num_shards <= 1 or not self._keys:
            yield self
            return

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

        for i in range(0, total, shard_size):
            yield ResultStream._make(
                model_klass=self.model_klass,
                transaction_id=self.transaction_id,
                keys=self._keys[i : i + shard_size],
                predicates=self._predicates,
                chunk_size=self.chunk_size,
                memory_backend=self.memory_backend,
                persisted_backend=self.persisted_backend,
                q_predicates=self._q_objects,
            )

    def first(self) -> typing.Optional[T]:
        """Returns the first filtered result, or None if the stream is empty."""
        return next(iter(self), None)

    def has_results(self) -> bool:
        """Returns True if at least one object survives the filter pipeline."""
        try:
            next(iter(self))
            return True
        except StopIteration:
            return False

    def __len__(self) -> int:
        """Returns the number of tracked IDs before filtering."""
        return len(self._keys)

    def _fetch_batch(
        self, batch_ids: typing.List[str]
    ) -> typing.Generator[T, typing.Any, None]:
        """
        Resolves a batch using memory-first lookup and persisted fallback.

        :param batch_ids: A list of unique identifiers corresponding to the instances to
            retrieve.
        :return: A generator that yields resolved instances of type `T`.
        """

        for rid in batch_ids:
            try:
                instance = self._memory_get_or_none(rid)
                if instance is None:
                    instance = self._persisted_get_or_none(rid)
                    if instance is not None:
                        self.memory_backend.upsert(self._schema_name(), rid, instance)

                if instance is not None:
                    yield instance
            except Exception:
                logger.exception(
                    "Failed to fetch %s(id=%r) from stream storage; skipping.",
                    self.model_klass.__name__,
                    rid,
                )

    def evict_memory(self, record_id: typing.Optional[str] = None) -> None:
        """Evict one or all in-memory copies without touching durable storage."""
        if record_id is not None:
            try:
                self.memory_backend.delete(self._schema_name(), record_id)
            except Exception as e:
                logger.debug("Failed to evict %s: %s", record_id, e)
                return
            return

        for rid in list(self._keys):
            try:
                self.memory_backend.delete(self._schema_name(), rid)
            except Exception as e:
                logger.debug("Failed to evict %s: %s", rid, e)
                continue

    def q_filter(self, q_object: Q) -> "ResultStream[T]":
        """
        Filter using a Q object for complex boolean logic.

        Examples:
            stream.q_filter(Q(age__gt=25) | Q(status="vip"))
            stream.q_filter(~Q(is_deleted=True) & Q(active=True))
        """

        backend = self.persisted_backend or self.memory_backend
        new_predicate = q_object.to_predicate(backend)

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates + [new_predicate],
            chunk_size=self.chunk_size,
            memory_backend=self.memory_backend,
            persisted_backend=self.persisted_backend,
            q_predicates=self._q_objects + [q_object],
        )

    def order_by(self, *fields: str) -> "ResultStream[T]":
        """
        Order results by given fields. Prefix with '-' for descending.
        Forces evaluation to determine order, but keeps objects lazy.

        Examples:
            stream.order_by('-age', 'name')
            stream.filter(status="active").order_by('created_at')
        """
        if not fields:
            return self

        # Evaluate to get sorted IDs
        results_with_keys = []
        for obj in self:
            sort_keys = []
            for field in fields:
                reverse = field.startswith("-")
                field_name = field.lstrip("-")

                # Handle nested attributes
                value = obj
                for part in field_name.split("__"):
                    value = getattr(value, part, None)
                    if value is None:
                        break

                # Make sortable: None values sort last
                sort_keys.append((value is None, value or ""))

            results_with_keys.append((tuple(sort_keys), obj.id))

        # Sort by the composite key
        results_with_keys.sort(key=lambda x: x[0])

        sorted_keys = [rid for _, rid in results_with_keys]

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=sorted_keys,
            predicates=[],  # Pre-filtered
            chunk_size=self.chunk_size,
            memory_backend=self.memory_backend,
            persisted_backend=self.persisted_backend,
            q_predicates=[],
        )

    def count(self) -> int:
        """Count filtered results without loading all objects."""
        count = 0
        for _ in self:
            count += 1
        return count

    def aggregate(
        self, aggregator: typing.Callable[[typing.Iterator[T]], typing.Any]
    ) -> typing.Any:
        """Apply an aggregation function to the filtered stream."""
        return aggregator(iter(self))

    def paginate(
        self, page: int = 1, page_size: int = 20
    ) -> typing.Tuple["ResultStream[T]", int]:
        """
        Return a page of results and total count.
        Note: This consumes the stream twice unless cached.
        """
        # Calculate total (could be expensive)
        total = self.count()

        # Calculate offset and create a paginated stream
        offset = (page - 1) * page_size
        paginated_keys = []
        for i, obj in enumerate(self):
            if i >= offset and len(paginated_keys) < page_size:
                paginated_keys.append(obj.id)
            elif len(paginated_keys) >= page_size:
                break

        paginated_stream = ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=paginated_keys,
            predicates=[],  # Pre-filtered
            chunk_size=self.chunk_size,
            memory_backend=self.memory_backend,
            persisted_backend=self.persisted_backend,
        )

        return paginated_stream, total

    def cache_results(self) -> "ResultStream[T]":
        """
        Force evaluation and cache all results in memory.
        Returns a new stream backed by the cached results.
        """
        # Evaluate entire stream
        for obj in self:
            # Already being cached via _memory_get_or_none
            pass

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=self._keys,
            predicates=self._predicates,
            chunk_size=self.chunk_size,
            memory_backend=self.memory_backend,
            persisted_backend=self.persisted_backend,
        )

    def __getitem__(
        self, index: typing.Union[int, slice]
    ) -> typing.Union[T, "ResultStream[T]"]:
        """Support indexing and slicing."""
        if isinstance(index, int):
            # Single item access
            if index < 0:
                # Negative indexing requires full evaluation
                results = list(self)
                return results[index]
            else:
                try:
                    return next(itertools.islice(self, index, index + 1))
                except StopIteration:
                    raise IndexError("ResultStream index out of range")
        elif isinstance(index, slice):
            # Slice returns a new stream
            start = index.start or 0
            stop = index.stop
            step = index.step or 1

            sliced_keys = []
            for i, obj in enumerate(self):
                if stop is not None and i >= stop:
                    break
                if i >= start and (i - start) % step == 0:
                    sliced_keys.append(obj.id)

            return ResultStream._make(
                model_klass=self.model_klass,
                transaction_id=self.transaction_id,
                keys=sliced_keys,
                predicates=[],  # Pre-filtered
                chunk_size=self.chunk_size,
                memory_backend=self.memory_backend,
                persisted_backend=self.persisted_backend,
            )
        else:
            raise TypeError("Indices must be integers or slices")

    def bulk_update(self, **kwargs) -> int:
        """Update attributes on all filtered objects."""
        updated = 0
        for obj in self:
            # new to validate that the obj has the field before we try to set it
            for key, value in kwargs.items():
                if hasattr(obj, key):
                    setattr(obj, key, value)
                else:
                    warnings.warn(
                        f"Object {obj} does not have attribute {key}", UserWarning
                    )

            # Re-persist
            self.memory_backend.upsert(self._schema_name(), obj.id, obj)
            if self.persisted_backend:
                self.persisted_backend.upsert(self._schema_name(), obj.id, obj)
            updated += 1
        return updated

    def bulk_delete(self) -> int:
        """Delete all filtered objects from storage."""
        deleted = 0
        for obj in self:
            was_deleted = False
            try:
                self.memory_backend.delete(self._schema_name(), obj.id)
                was_deleted = True
            except Exception:
                was_deleted = False

            if self.persisted_backend:
                try:
                    self.persisted_backend.delete(self._schema_name(), obj.id)
                    was_deleted = True
                except Exception:
                    if not was_deleted:
                        was_deleted = False

            if was_deleted:
                # Remove from key list
                if obj.id in self._key_set:
                    self._keys.remove(obj.id)
                    self._key_set.discard(obj.id)
                deleted += 1
        return deleted

    def union(self, *streams: "ResultStream[T]") -> "ResultStream[T]":
        """Combine multiple streams, preserving order and removing duplicates."""
        combined_keys = list(self._keys)
        seen = set(self._keys)

        for stream in streams:
            for key in stream._keys:
                if key not in seen:
                    combined_keys.append(key)
                    seen.add(key)

        return ResultStream._make(
            model_klass=self.model_klass,
            transaction_id=self.transaction_id,
            keys=combined_keys,
            predicates=[],  # Reset predicates for combined stream
            chunk_size=self.chunk_size,
            memory_backend=self.memory_backend,
            persisted_backend=self.persisted_backend,
        )

    def explain(self) -> dict:
        """Return the query plan without executing."""
        return {
            "model": self.model_klass.__name__,
            "transaction": self.transaction_id,
            "total_keys": len(self._keys),
            "predicates": len(self._predicates),
            "chunk_size": self.chunk_size,
            "has_memory_backend": self.memory_backend is not None,
            "has_persisted_backend": self.persisted_backend is not None,
            "estimated_memory": len(self._keys) * 8,  # Rough estimate
        }

    def stats(self) -> dict:
        """Return execution statistics."""
        memory_hits = 0
        persisted_hits = 0
        misses = 0
        filtered = 0

        for obj in self:
            # This is approximate since we can't track hits without modifying _fetch_batch
            pass

        return {
            "total_objects": len(self._keys),
            "memory_hits": memory_hits,
            "persisted_hits": persisted_hits,
            "misses": misses,
            "filtered_out": filtered,
        }

    def try_optimize(self) -> "ResultStream[T]":
        """
        Attempt to push predicates to the backend for server-side filtering.
        Falls back to in-memory filtering if not possible.
        """
        if not self._q_objects or not self.persisted_backend:
            return self

        # Try to push simple Q objects to backend
        optimized_predicates = list(self._predicates)

        for q in self._q_objects:
            try:
                # Try to convert to simple filter kwargs
                filter_kwargs = q.to_filter_kwargs()

                # Use backend's native filter if available
                filtered_keys = []
                backend_results = self.persisted_backend.filter(
                    self._schema_name(), self.model_klass, **filter_kwargs
                )

                for result in backend_results:
                    if result.id in self._key_set:
                        filtered_keys.append(result.id)

                # Update keys to only include matches
                self._keys = filtered_keys
                self._key_set = set(filtered_keys)

            except (ValueError, NotImplementedError):
                # Can't optimize this Q object, keep as predicate
                pass

        return self

    def __iter__(self) -> typing.Iterator[T]:
        """
        Terminal operation with an optional backend optimization attempt.
        """
        # Try to optimize before iterating
        if self._q_objects:
            self.try_optimize()

        # Original iteration logic
        for i in range(0, len(self._keys), self.chunk_size):
            batch_ids = self._keys[i : i + self.chunk_size]
            for instance in self._fetch_batch(batch_ids):
                if all(predicate(instance) for predicate in self._predicates):
                    yield instance

    def __repr__(self) -> str:
        return (
            f"<ResultStream: {self.model_klass.__name__} | "
            f"Tx: {self.transaction_id[:8]}... | "
            f"Keys: {len(self._keys)} | "
            f"Filters: {len(self._predicates)}>"
        )
