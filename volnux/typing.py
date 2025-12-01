from enum import Enum
from typing import (
    Any,
    Callable,
    Collection,
    Generator,
    Iterator,
    Optional,
    TypeVar,
    Union,
)

try:
    from typing import TypeAlias
except ImportError:
    from typing_extensions import TypeAlias


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
