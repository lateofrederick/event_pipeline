import logging
from typing import Dict, Any, Optional, Union, List, Hashable, Tuple

from volnux.result import EventResult
from volnux.utils import get_obj_klass_import_str


logger = logging.getLogger(__name__)


class ExecResultSerializer:

    # Primitive types that are JSON-serializable
    PRIMITIVE_TYPES = (int, str, float, bool, type(None))

    def __init__(self):
        from volnux.execution.rehydrator.engine.serializer import StateSerializer

        self.state_serializer = StateSerializer

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
            >>> serializer = ExecResultSerializer()
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
