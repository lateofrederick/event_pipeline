f"""
volnux/examples/research_agent.py

A simple but complete example of an AgentEventBase subclass
using Ollama as the LLM provider.

Demonstrates:
- Correct INIT_PARAMS_SCHEMA usage (public config only)
- Private state initialised lazily (NOT in schema)
- build_prompt() implementation
- process() delegating to _run_agent_loop()
- Two governed tool events (FetchWebPageEvent, SummariseTextEvent)
- communicate() hook for optional human approval before processing
- detect_hallucination() for domain validation
- on_step_complete() hook for custom logging
- Pointy-Lang workflow definition

To run locally:
    1. Install Ollama: https://ollama.ai
    2. Pull a model: ollama pull llama3
    3. pip install volnux httpx
    4. Run: volnux run research --topic="quantum computing"
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Type

from volnux.event.agent import AgentEventBase
from volnux.event.agent._state import ReasoningStep, ToolCallRecord, AgentResult
from volnux.event import EventBase
from volnux.execution.context import ExecutionContext, ExecutionMetrics

# from volnux.event import EventCategory

logger = logging.getLogger(__name__)


class FetchWebPageEvent(EventBase):
    """
    Fetches the text content of a web page.

    Configured with ThreadPoolExecutor — I/O-bound network call
    should not block the asyncio event loop.
    """

    executor = "thread_pool"

    EXTRA_INIT_PARAMS_SCHEMA = {
        "url": {
            "type": str,
            "required": True,
            "description": "The URL to fetch.",
        },
    }

    async def process(self, url: str, **kwargs) -> Tuple[bool, Any]:
        try:
            import httpx

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                # Return first 3000 chars — enough for summarization
                return True, {
                    "url": url,
                    "status_code": response.status_code,
                    "content": response.text[:3000],
                }
        except Exception as exc:
            logger.warning("FetchWebPageEvent: failed to fetch %s: %s", url, exc)
            return False, {"url": url, "error": str(exc)}


class SummariseTextEvent(EventBase):
    """
    Extracts key facts from a block of text using simple heuristics.
    No LLM call — pure Python computation.

    The agent calls this after fetching a page to reduce the content
    before including it in its next LLM prompt, keeping token usage low.
    """

    EXTRA_INIT_PARAMS_SCHEMA = {
        "text": {
            "type": str,
            "required": True,
            "description": "Text to summarise.",
        },
        "max_sentences": {
            "type": int,
            "required": False,
            "default": 5,
            "description": "Maximum sentences to extract.",
        },
    }

    async def process(
        self,
        text: str,
        max_sentences: int = 5,
        **kwargs,
    ) -> Tuple[bool, Any]:
        import re

        sentences = re.split(r"(?<=[.!?])\s+", text.strip())
        # Score sentences by length — longer = more content
        scored = sorted(
            [(len(s), s) for s in sentences if len(s) > 40],
            reverse=True,
        )
        top = [s for _, s in scored[:max_sentences]]
        return True, {"summary": " ".join(top), "sentence_count": len(top)}


class ResearchAgent(AgentEventBase):
    """
    A research agent that investigates a topic by fetching web pages,
    summarising their content, and producing a structured research brief.

    Uses Ollama (llama3 by default) as the LLM provider.
    Runs entirely locally — no external API keys required.

    Tools
    -----
    FetchWebPageEvent    — fetch web page content
    SummariseTextEvent   — extract key facts from text

    Pointy-Lang usage
    -----------------
    ResearchAgent[
        topic="quantum computing breakthroughs 2026",
        llm_model="llama3",
        max_reasoning_steps=10,
    ] (
        0 -> HandleResearchFailure -> NotifyUser,
        1 -> FormatAndSaveReport,
        2 -> HandleTimeout -> EscalateToOperator,
    )
    """

    llm_provider = "ollama"
    llm_model = "llama3"  # "gemini-3.5-flash"
    temperature = 0.1  # Low — factual research, not creative
    max_tokens = 2048

    prompt_version = "1.0.0"
    max_reasoning_steps = 10
    tool_timeout_seconds = 15.0

    tools: List[Type[EventBase]] = [
        FetchWebPageEvent,
        SummariseTextEvent,
    ]

    permitted_handoffs: List[str] = []

    # event_type = EventType.AI
    # categories = frozenset({EventCategory.AI})

    system_prompt = """
    You are a focused research agent. Your job is to investigate a topic,
    gather information from relevant web pages, and produce a concise,
    factual research brief.

    Instructions:
    - Use FetchWebPageEvent to retrieve web page content.
    - Use SummariseTextEvent to extract key facts before including
      content in your reasoning — this keeps your context efficient.
    - Reason step by step. Cite the URLs you fetched.
    - When you have enough information (3-5 sources minimum),
      produce a FINISH response with a structured brief.
    - Be factual. Do not speculate beyond what the sources say.
    - If you cannot find relevant information, say so clearly.

    Brief format:
    TOPIC: <topic>
    SOURCES: <list of URLs consulted>
    KEY FINDINGS:
    - <finding 1>
    - <finding 2>
    - <finding 3>
    SUMMARY: <2-3 sentence summary>
    CONFIDENCE: <high|medium|low> — based on source quality
    """.strip()

    # ── INIT_PARAMS_SCHEMA ─────────────────────────────────────────────────────
    # Public configuration declared in Pointy-Lang [annotations].
    # The user supplies these — the engine does NOT.
    # Private state (_reasoning_trace etc.) is NOT declared here.
    # It is initialised lazily in _run_agent_loop() via hasattr guards.

    EXTRA_INIT_PARAMS_SCHEMA = {
        "topic": {
            "type": str,
            "required": True,
            "description": "The research topic to investigate.",
        },
        "llm_model": {
            "type": str,
            "required": False,
            "default": "llama3",
            "description": "Ollama model name. Run `ollama list` to see available models.",
        },
        "max_reasoning_steps": {
            "type": int,
            "required": False,
            "default": 10,
            "description": "Maximum ReAct loop iterations.",
        },
        "require_human_approval": {
            "type": bool,
            "required": False,
            "default": False,
            "description": (
                "If True, the agent suspends in communicate() and waits "
                "for human approval before processing begins."
            ),
        },
    }

    async def process(
        self,
        topic: str,
        **kwargs,
    ) -> Tuple[bool, Any]:
        """
        Runs the ReAct loop to research the topic.

        Receives `topic` from Pipeline injection.
        Receives `approved_by` from communicate() return value
        (present only when require_human_approval=True).
        """

        return await self._run_agent_loop(topic=topic, **kwargs)

    def build_prompt(self, topic: str, **kwargs) -> str:
        return (
            f"Research the following topic thoroughly and produce a "
            f"structured research brief:\n\n"
            f"TOPIC: {topic}\n\n"
            f"Start by identifying 3-5 relevant URLs to fetch, "
            f"then use FetchWebPageEvent to retrieve their content, "
            f"SummariseTextEvent to extract key facts, and finally "
            f"produce your FINISH response with the complete brief."
        )

    def detect_hallucination(
        self,
        response: Dict[str, Any],
        context: Dict[str, Any],
    ) -> bool:
        """
        Reject FINISH responses that claim findings without any tool calls.

        A research agent that produces findings without fetching any pages
        is fabricating sources. Reject these responses.
        """
        from volnux.event.agent._state import AgentAction

        content = str(response.get("content", ""))
        action = response.get("action")

        if action == AgentAction.FINISH:
            # If agent is finishing but has not called any tools yet,
            # reject — findings without sources are hallucinations.
            if not self._tool_call_records:
                logger.warning(
                    "ResearchAgent: FINISH without any tool calls — "
                    "rejecting as potential hallucination."
                )
                return False

        return True

    def on_step_complete(self, step: ReasoningStep) -> None:
        logger.info(
            "ResearchAgent step %d [%s]: tokens=%s latency=%.0fms",
            step.step_index,
            step.step_type.value,
            step.token_usage.get("total", "?") if step.token_usage else "?",
            step.latency_ms or 0,
        )

    def on_tool_result(self, record: ToolCallRecord) -> None:
        status = "✓" if record.error is None else "✗"
        logger.info(
            "ResearchAgent tool %s %s [%.0fms]: %s",
            record.tool_class_name,
            status,
            record.latency_ms or 0,
            record.error or "success",
        )

    def on_agent_complete(self, result: Optional[AgentResult]) -> None:
        if result and result.success:
            logger.info(
                "ResearchAgent completed: %d steps, %d tokens, "
                "%d tool calls, %.0fms",
                result.steps_taken,
                result.total_tokens,
                len(self._tool_call_records),
                result.total_latency_ms,
            )
        else:
            logger.error(
                "ResearchAgent failed: %s",
                result.content if result else "no result",
            )


def main():
    import asyncio

    agent = ResearchAgent(
        task_id="dsdsdd",
        execution_context=ExecutionContext(task_profiles=[], pipeline=None),
    )
    print(asyncio.run(agent(topic="quantum computing")))
