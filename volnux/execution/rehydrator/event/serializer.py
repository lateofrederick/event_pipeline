from typing import Dict, Any, Optional, Union, List

from .snapshot import InitArgsTemplate, CallArgsTemplate
from volnux.parser.options import StopCondition


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

    def serialize_external_resources(self, external_resources: Dict[str, Any]):
        pass

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
