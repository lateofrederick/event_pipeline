import logging
from enum import Enum
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)


class LLMProvider(Enum):
    """Supported LLM provider identifiers."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GEMINI = "gemini"
    MISTRAL = "mistral"
    OLLAMA = "ollama"
    VLLM = "vllm"
    CUSTOM = "custom"


class LLMProviderAdapterBase:
    """
    Abstract interface for LLM provider adapters.

    The framework ships adapters for OpenAI, Anthropic, Gemini, Mistral,
    Ollama, and vLLM.

    All methods are async. Sync providers are wrapped in ``asyncio.to_thread``.
    """

    provider_name: str

    @classmethod
    def __init_subclass__(cls, **kwargs):
        from .registry import LLMProviderRegistry

        super().__init_subclass__(**kwargs)
        LLMProviderRegistry.register(cls.provider_name, cls)

    async def complete(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int,
        tools: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Call the LLM and return a standardised response dict::

            {
                "content":       str,
                "action":        AgentAction | None,
                "tool_name":     str | None,
                "tool_args":     dict | None,
                "tool_call_id":  str | None,
                "token_usage":   {"prompt": int, "completion": int, "total": int},
                "finish_reason": str,
                "raw":           dict,
            }
        """
        raise NotImplementedError

    async def validate_response(
        self,
        response: Dict[str, Any],
        context: Dict[str, Any],
    ) -> bool:
        """
        Validate the response for hallucinations or constraint violations.
        Default returns True. Override for provider-level validation.
        """
        return True
