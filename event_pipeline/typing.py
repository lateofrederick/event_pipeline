from typing import (
    TypeAlias,
    Union,
    TypeVar,
    Callable,
    Collection,
    Any,
    Iterator,
    Generator,
    Optional,
    Deque,
    List,
)
from enum import Enum
from event_pipeline.parser.protocols import TaskProtocol, TaskGroupingProtocol


TaskType: TypeAlias = Union[TaskProtocol, TaskGroupingProtocol]


class ConfigState(Enum):
    """Configuration state indicators"""

    UNSET = "unset"


T = TypeVar("T")
ConfigurableValue: TypeAlias = Union[T, None, ConfigState]

BatchProcessType: TypeAlias = Callable[
    [
        Union[Collection[Any], Any],
        Optional[Union[int, float]],
    ],
    Union[Iterator[Any], Generator[Any, None, None]],
]
