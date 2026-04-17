import logging
from typing import Dict, Any, Optional, Union, List, Hashable, Tuple

from .snapshot import InitArgsTemplate, CallArgsTemplate
from volnux.parser.options import StopCondition

from volnux.result import EventResult
from volnux.utils import get_obj_klass_import_str


logger = logging.getLogger(__name__)


class _ExecResultSerializer:

    # Primitive types that are JSON-serializable
    PRIMITIVE_TYPES = (int, str, float, bool, type(None))

    def __init__(self):
        from volnux.execution.rehydrator.engine.serializer import StateSerializer

        self.engine_serializer = StateSerializer

    def serialize_exec_result(self, result: Hashable) -> Any:
        """
        Serialize execution result for checkpointing.

        Args:
            result: The execution result to serialize

        Returns:
            Serialized result (JSON-compatible structure)

        Raises:
            TypeError: If result contains non-serializable types

        Example:
            >>> serializer = EventSerializer()
            >>> serializer.serialize_exec_result(42)
            42
            >>> serializer.serialize_exec_result([1, 2, EventResult(...)])
            [1, 2, {...}]
        """
        if result is None:
            return None

        if isinstance(result, self.PRIMITIVE_TYPES):
            return result

        if isinstance(result, EventResult):
            return self._serialize_event_result(result)

        if isinstance(result, (list, tuple)):
            return self._serialize_sequence(result, type(result))

        if isinstance(result, set):
            return self._serialize_set(result)

        if isinstance(result, dict):
            return self._serialize_mapping(result)

        # Handle custom objects
        return self._serialize_object(result)

    def _serialize_event_result(self, result: EventResult) -> Dict[str, Any]:
        """
        Serialize an EventResult object.

        Args:
            result: EventResult instance to serialize

        Returns:
            Serialized EventResult dictionary
        """

        return self.state_serializer.serialize_result(result)

    def _serialize_sequence(
        self, sequence: Union[List, Tuple], original_type: type
    ) -> Union[List, Dict[str, Any]]:
        """
        Serialize a sequence (list or tuple).

        Args:
            sequence: List or tuple to serialize
            original_type: The original type (list or tuple)

        Returns:
            Serialized sequence (list) or dict with type info if tuple
        """
        serialized_items = []

        for item in sequence:
            try:
                serialized_item = self.serialize_exec_result(item)
                serialized_items.append(serialized_item)
            except Exception as e:
                logger.warning(
                    f"Failed to serialize item {item} in sequence: {e}. "
                    "Storing as None."
                )
                serialized_items.append(None)

        if original_type is tuple:
            return {"__type__": "tuple", "items": serialized_items}

        return serialized_items

    def _serialize_set(self, set_obj: set) -> Dict[str, Any]:
        """
        Serialize a set.

        Args:
            set_obj: Set to serialize

        Returns:
            Dictionary with type info and serialized items
        """
        serialized_items = []

        for item in set_obj:
            try:
                # Sets can only contain hashable items
                if not isinstance(item, Hashable):
                    logger.warning(f"Skipping non-hashable item in set: {type(item)}")
                    continue

                serialized_item = self.serialize_exec_result(item)
                serialized_items.append(serialized_item)
            except Exception as e:
                logger.warning(f"Failed to serialize item {item} in set: {e}")

        return {"__type__": "set", "items": serialized_items}

    def _serialize_mapping(self, mapping: Dict) -> Dict[str, Any]:
        """
        Serialize a dictionary/mapping.

        Args:
            mapping: Dictionary to serialize

        Returns:
            Serialized dictionary
        """
        serialized_dict = {}

        for key, value in mapping.items():
            # Keys must be strings for JSON compatibility
            if not isinstance(key, (str, int, float, bool, type(None))):
                logger.warning(
                    f"Converting non-primitive dict key {key} ({type(key)}) to string"
                )
                str_key = str(key)
            else:
                str_key = key

            try:
                serialized_value = self.serialize_exec_result(value)
                serialized_dict[str_key] = serialized_value
            except Exception as e:
                logger.warning(
                    f"Failed to serialize value for key '{key}': {e}. "
                    "Storing as None."
                )
                serialized_dict[str_key] = None

        return serialized_dict

    def _serialize_object(self, obj: Any) -> Dict[str, Any]:
        """
        Serialize a custom object using __getstate__ or __dict__.

        Args:
            obj: Custom object to serialize

        Returns:
            Dictionary with class path and state

        Raises:
            TypeError: If object cannot be serialized
        """
        try:
            class_path = get_obj_klass_import_str(obj)

            # Try __getstate__ first (pickle protocol)
            if hasattr(obj, "__getstate__"):
                state = obj.__getstate__()

                # Recursively serialize the state
                serialized_state = self.serialize_exec_result(state)

                return {
                    "__type__": "custom_object",
                    "__class_path__": class_path,
                    "state": serialized_state,
                    "uses_getstate": True,
                }

            # Fallback to __dict__
            if hasattr(obj, "__dict__"):
                state_dict = obj.__dict__.copy()

                # Recursively serialize each attribute
                serialized_dict = {}
                for key, value in state_dict.items():
                    try:
                        serialized_dict[key] = self.serialize_exec_result(value)
                    except Exception as e:
                        logger.warning(
                            f"Failed to serialize attribute '{key}' of {class_path}: {e}"
                        )
                        serialized_dict[key] = None

                return {
                    "__type__": "custom_object",
                    "__class_path__": class_path,
                    "state": serialized_dict,
                    "uses_getstate": False,
                }

            # Cannot serialize
            logger.error(f"Object {obj} ({type(obj)}) has no __getstate__ or __dict__")
            raise TypeError(
                f"Cannot serialize object of type {type(obj).__name__}: "
                "no __getstate__ or __dict__ available"
            )

        except Exception as e:
            logger.error(f"Failed to serialize object {obj}: {e}", exc_info=True)
            raise TypeError(f"Cannot serialize object: {e}") from e

    def deserialize_exec_result(self, data: Any) -> Any:
        """
        Deserialize execution result from checkpoint.

        Args:
            data: Serialized data to deserialize

        Returns:
            Deserialized result

        Raises:
            ValueError: If data format is invalid
            ImportError: If class cannot be imported during object restoration
        """
        if data is None:
            return None

        # Primitives - return as-is
        if isinstance(data, self.PRIMITIVE_TYPES):
            return data

        # Check for type markers
        if isinstance(data, dict) and "__type__" in data:
            type_marker = data["__type__"]

            if type_marker == "tuple":
                items = [self.deserialize_exec_result(item) for item in data["items"]]
                return tuple(items)

            if type_marker == "set":
                items = [self.deserialize_exec_result(item) for item in data["items"]]
                return set(items)

            if type_marker == "EventResult":
                return self._deserialize_event_result(data)

            if type_marker == "custom_object":
                return self._deserialize_object(data)

        # Regular dict - recursively deserialize values
        if isinstance(data, dict):
            return {
                key: self.deserialize_exec_result(value) for key, value in data.items()
            }

        # Regular list - recursively deserialize items
        if isinstance(data, list):
            return [self.deserialize_exec_result(item) for item in data]

        # Unknown type - return as-is
        logger.warning(f"Unknown data type during deserialization: {type(data)}")
        return data

    def _deserialize_event_result(self, data: Dict[str, Any]) -> EventResult:
        """Deserialize an EventResult object."""
        if self.state_serializer and hasattr(
            self.state_serializer, "deserialize_result"
        ):
            return self.state_serializer.deserialize_result(data)

        # Manual deserialization
        from volnux.import_utils import import_string

        class_path = data.get("__class_path__")
        if not class_path:
            raise ValueError("Missing __class_path__ in EventResult data")

        result_class = import_string(class_path)

        # Use the appropriate restoration method
        if "data" in data and hasattr(result_class, "from_dict"):
            return result_class.from_dict(data["data"])

        if "state" in data:
            instance = result_class.__new__(result_class)
            if hasattr(instance, "__setstate__"):
                instance.__setstate__(data["state"])
            else:
                instance.__dict__.update(data["state"])
            return instance

        raise ValueError(f"Cannot deserialize EventResult from data: {data}")

    def _deserialize_object(self, data: Dict[str, Any]) -> Any:
        """Deserialize a custom object."""
        from volnux.import_utils import import_string

        class_path = data.get("__class_path__")
        if not class_path:
            raise ValueError("Missing __class_path__ in object data")

        obj_class = import_string(class_path)
        instance = obj_class.__new__(obj_class)

        state = self.deserialize_exec_result(data["state"])

        if data.get("uses_getstate", False) and hasattr(instance, "__setstate__"):
            instance.__setstate__(state)
        else:
            # Restore via __dict__
            if isinstance(state, dict):
                instance.__dict__.update(state)
            else:
                raise ValueError(
                    f"Expected dict for __dict__ restoration, got {type(state)}"
                )

        return instance


class StateSerializer:
    """
    Handles the serialization and deserialization of event state data.
    """

    @classmethod
    def serialize_init_args(cls, init_arg_dict: Dict[str, Any]) -> InitArgsTemplate:
        from volnux.execution.rehydrator.engine.serializer import (
            StateSerializer as EngineSerializer,
        )

        execution_context = init_arg_dict.get("execution_context")
        options = init_arg_dict.get("options")
        results = init_arg_dict.get("previous_result", [])
        keyword_args = init_arg_dict.get("kwargs", {})

        return {
            "execution_context_id": (
                execution_context.state_id if execution_context else None
            ),
            "task_id": init_arg_dict.get("task_id"),
            "stop_condition": cls._serialize_stop_condition(
                init_arg_dict.get("stop_condition")
            ),
            "run_bypass_event_checks": init_arg_dict.get(
                "run_bypass_event_checks", False
            ),
            "options": options.as_dict() if options else None,
            "sequence_number": init_arg_dict.get("sequence_number"),
            "kwargs": cls._build_keyword_args(keyword_args),
            "previous_result": [
                EngineSerializer.serialize_result(result) for result in results
            ],
        }

    def serialize_call_args(self, call_arg_dict: Dict[str, Any]):
        pass

    @staticmethod
    def serialize_exec_result(result: Hashable):
        return _ExecResultSerializer().serialize_exec_result(result)

    @classmethod
    def _serialize_stop_condition(
        cls, stop_condition: Union[StopCondition, List[StopCondition], None]
    ) -> List[str]:
        """
        Serializes a given stop condition or a list of stop conditions into a list of strings suitable
        for internal processing. This method accepts either a single StopCondition object, a list of
        StopCondition objects, or None. The serialized output is a flattened list where each element
        represents the string value of the stop condition.

        :param stop_condition: The stop condition or a list of stop conditions to be serialized.
            Can accept a single StopCondition, a list/tuple of StopCondition, or None.
        :type stop_condition: Union[StopCondition, List[StopCondition], None]

        :return: A list of string values representing the serialized stop conditions.
        :rtype: List[str]
        """
        if stop_condition is None:
            return [StopCondition.NEVER.value]
        if isinstance(stop_condition, (list, tuple)):
            conditions = []
            for cond in stop_condition:
                conditions.extend(cls._serialize_stop_condition(cond))

            return conditions
        else:
            return [stop_condition.value]

    @staticmethod
    def _build_keyword_args(kwargs: Dict[str, Any]) -> Dict[str, Any]:
        data = {}
        for k, v in kwargs.items():
            if isinstance(v, dict):
                data[k] = StateSerializer._build_keyword_args(v)
            elif isinstance(v, list):
                data[k] = [
                    (
                        StateSerializer._build_keyword_args(item)
                        if isinstance(item, dict)
                        else item
                    )
                    for item in v
                ]
            elif isinstance(v, tuple):
                data[k] = tuple(
                    (
                        StateSerializer._build_keyword_args(item)
                        if isinstance(item, dict)
                        else item
                    )
                    for item in v
                )
            else:
                data[k] = v
        return data
