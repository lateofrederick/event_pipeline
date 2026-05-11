from typing import Any, Dict, List, Optional

from .base import LLMProviderAdapterBase
from ..base import AgentAction
from volnux.exceptions import LLMProviderError


class GeminiProviderAdapter(LLMProviderAdapterBase):
    """
    Google Gemini provider adapter.

    Supports Gemini 1.5 Pro, Gemini 1.5 Flash, Gemini 2.0 Flash, and
    Gemini 2.5 Pro via the ``google-generativeai`` SDK.

    Gemini's chat API maintains a stateful session internally. This adapter
    creates a fresh session per ``complete()`` call, replaying the message
    history to reconstruct context. This is stateless from Volnux's
    perspective — the agent's ``_messages`` list is the source of truth,
    consistent with OpenAI and Anthropic adapters.

    Role mapping
    ------------
    Volnux uses OpenAI-style roles (``system``, ``user``, ``assistant``,
    ``tool``). Gemini uses ``user`` and ``model``. This adapter maps:
      - ``system``    → prepended to the first ``user`` message
      - ``assistant`` → ``model``
      - ``tool``      → ``user`` (with a [Tool result] prefix)

    Tool use
    --------
    Gemini's function calling returns ``FunctionCall`` parts in the response.
    Tool schemas are converted from OpenAI's ``parameters`` format to
    Gemini's ``FunctionDeclaration`` format.

    Requirements: ``pip install google-generativeai``
    """

    provider_name = "gemini"

    async def complete(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float,
        max_tokens: int,
        tools: Optional[List[Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            import google.generativeai as genai
            from google.generativeai.types import (
                ContentDict,
                FunctionDeclaration,
                Tool as GeminiTool,
                GenerationConfig,
            )
        except ImportError:
            raise LLMProviderError(
                "google-generativeai package not installed. "
                "Run: pip install google-generativeai"
            )

        system_content = next(
            (m["content"] for m in messages if m["role"] == "system"), ""
        )
        non_system = [m for m in messages if m["role"] != "system"]

        # Gemini requires alternating user/model turns. Consecutive messages
        # of the same role are merged to satisfy this constraint.
        gemini_history: List[ContentDict] = []

        for msg in non_system[:-1]:  # history — all but the last message
            role = msg["role"]

            if role == "assistant":
                gemini_role = "model"
                text = msg["content"]
            elif role == "tool":
                gemini_role = "user"
                text = f"[Tool result]: {msg['content']}"
            else:
                gemini_role = "user"
                text = msg["content"]

            # Merge consecutive same-role messages (Gemini requirement)
            if gemini_history and gemini_history[-1]["role"] == gemini_role:
                existing = gemini_history[-1]["parts"]
                if isinstance(existing, list):
                    existing.append(text)
                else:
                    gemini_history[-1]["parts"] = [existing, text]
            else:
                gemini_history.append(
                    {
                        "role": gemini_role,
                        "parts": [text],
                    }
                )

        # The last non-system message is the current user prompt
        last_msg = non_system[-1] if non_system else {"role": "user", "content": ""}
        current_prompt = last_msg["content"]

        # Prepend system content to the first user message if present
        if system_content and gemini_history:
            first = gemini_history[0]
            if first["role"] == "user":
                parts = first["parts"]
                if isinstance(parts, list):
                    parts.insert(0, f"[System]: {system_content}\n\n")
                else:
                    first["parts"] = f"[System]: {system_content}\n\n{parts}"
        elif system_content:
            current_prompt = f"[System]: {system_content}\n\n{current_prompt}"

        gemini_tools = None
        if tools:
            declarations = []
            for t in tools:
                params_schema = t.get("parameters", {})
                declarations.append(
                    FunctionDeclaration(
                        name=t["name"],
                        description=t.get("description", ""),
                        parameters=params_schema,
                    )
                )
            gemini_tools = [GeminiTool(function_declarations=declarations)]

        generation_config = GenerationConfig(
            temperature=temperature,
            max_output_tokens=max_tokens,
        )

        gen_model = genai.GenerativeModel(
            model_name=model,
            generation_config=generation_config,
            tools=gemini_tools,
        )

        chat = gen_model.start_chat(history=gemini_history)

        try:
            # run_async wraps the sync send_message in a thread
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: chat.send_message(current_prompt),
            )
        except Exception as exc:
            raise LLMProviderError(f"Gemini API error: {exc}") from exc

        action = AgentAction.THINK
        tool_name = None
        tool_args = None
        content_text = ""

        candidate = response.candidates[0] if response.candidates else None
        if candidate:
            for part in candidate.content.parts:
                if hasattr(part, "function_call") and part.function_call:
                    fc = part.function_call
                    action = AgentAction.TOOL_CALL
                    tool_name = fc.name
                    # fc.args is a MapComposite — convert to plain dict
                    tool_args = dict(fc.args) if fc.args else {}
                elif hasattr(part, "text") and part.text:
                    content_text = part.text

        usage_meta = getattr(response, "usage_metadata", None)
        prompt_tok = getattr(usage_meta, "prompt_token_count", 0) if usage_meta else 0
        output_tok = (
            getattr(usage_meta, "candidates_token_count", 0) if usage_meta else 0
        )

        finish_reason = str(candidate.finish_reason) if candidate else "unknown"

        return {
            "content": content_text,
            "action": action,
            "tool_name": tool_name,
            "tool_args": tool_args,
            "tool_call_id": None,
            "token_usage": {
                "prompt": prompt_tok,
                "completion": output_tok,
                "total": prompt_tok + output_tok,
            },
            "finish_reason": finish_reason,
            "raw": str(response),
        }
