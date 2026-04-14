# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Agent orchestration patterns — ReAct, tool-calling, map-reduce.

Provides reusable AI agent patterns that integrate with Gravtory's
checkpoint and step system for crash-proof agent execution.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger("gravtory.ai.agents")


# ── Agent tool definition ─────────────────────────────────────────


@dataclass
class AgentTool:
    """A tool that an agent can invoke.

    Args:
        name: Unique tool name (e.g. ``"search"``).
        description: Human-readable description for the LLM.
        function: Async callable that executes the tool.
        parameters: JSON Schema describing the function's parameters.
    """

    name: str
    description: str
    function: Callable[..., Awaitable[str]]
    parameters: dict[str, Any] = field(default_factory=dict)


# ── ReAct agent ───────────────────────────────────────────────────


@dataclass
class ReActIteration:
    """One iteration of the ReAct loop."""

    thought: str = ""
    action: str = ""
    action_input: str = ""
    observation: str = ""


class ReActAgent:
    """ReAct agent that alternates between thinking and acting.

    Each iteration is stored in :attr:`history` so it can be
    checkpointed.  On crash, replay from the last checkpoint.

    Usage::

        agent = ReActAgent(
            tools=[search_tool, calc_tool],
            max_iterations=10,
        )
        result = await agent.run(
            query="What is the GDP of France?",
            llm_fn=my_llm_call,
        )

    The *llm_fn* receives ``(system_prompt, user_prompt)`` and returns
    the LLM's text response.
    """

    def __init__(
        self,
        tools: list[AgentTool],
        *,
        max_iterations: int = 10,
    ) -> None:
        self._tools: dict[str, AgentTool] = {t.name: t for t in tools}
        self._max_iterations = max_iterations
        self._history: list[ReActIteration] = []

    @property
    def history(self) -> list[ReActIteration]:
        """Return the iteration history."""
        return list(self._history)

    @property
    def tools(self) -> dict[str, AgentTool]:
        """Return registered tools."""
        return dict(self._tools)

    def _build_system_prompt(self) -> str:
        tool_list = "\n".join(f"  - {t.name}: {t.description}" for t in self._tools.values())
        return (
            "You are a ReAct agent. For each step you MUST output:\n"
            "Thought: <your reasoning>\n"
            "Action: <tool_name>\n"
            "Action Input: <input for the tool>\n\n"
            "OR, if you have the final answer:\n"
            "Thought: I now know the final answer.\n"
            "Final Answer: <answer>\n\n"
            f"Available tools:\n{tool_list}"
        )

    def _build_user_prompt(self, query: str) -> str:
        lines = [f"Question: {query}\n"]
        for it in self._history:
            lines.append(f"Thought: {it.thought}")
            if it.action:
                lines.append(f"Action: {it.action}")
                lines.append(f"Action Input: {it.action_input}")
                lines.append(f"Observation: {it.observation}")
        return "\n".join(lines)

    @staticmethod
    def parse_response(text: str) -> dict[str, str]:
        """Parse an LLM response into structured fields.

        Returns a dict that may contain keys: ``thought``, ``action``,
        ``action_input``, ``final_answer``.
        """
        result: dict[str, str] = {}
        for line in text.strip().splitlines():
            stripped = line.strip()
            if stripped.lower().startswith("thought:"):
                result["thought"] = stripped.split(":", 1)[1].strip()
            elif stripped.lower().startswith("action:"):
                result["action"] = stripped.split(":", 1)[1].strip()
            elif stripped.lower().startswith("action input:"):
                result["action_input"] = stripped.split(":", 1)[1].strip()
            elif stripped.lower().startswith("final answer:"):
                result["final_answer"] = stripped.split(":", 1)[1].strip()
        return result

    async def run(
        self,
        query: str,
        llm_fn: Callable[[str, str], Awaitable[str]],
    ) -> str:
        """Run the ReAct loop until a final answer or max iterations.

        Args:
            query: The user's question.
            llm_fn: Async function ``(system_prompt, user_prompt) -> str``.

        Returns:
            The agent's final answer string.
        """
        self._history.clear()
        system = self._build_system_prompt()

        for _i in range(self._max_iterations):
            user = self._build_user_prompt(query)
            response = await llm_fn(system, user)
            parsed = self.parse_response(response)

            if "final_answer" in parsed:
                return parsed["final_answer"]

            iteration = ReActIteration(
                thought=parsed.get("thought", ""),
                action=parsed.get("action", ""),
                action_input=parsed.get("action_input", ""),
            )

            # Execute tool
            tool = self._tools.get(iteration.action)
            if tool is not None:
                try:
                    iteration.observation = await tool.function(
                        iteration.action_input,
                    )
                except Exception as exc:
                    iteration.observation = f"Error: {exc}"
                    logger.warning(
                        "Tool '%s' failed: %s",
                        iteration.action,
                        exc,
                    )
            else:
                iteration.observation = (
                    f"Unknown tool '{iteration.action}'. Available: {', '.join(self._tools)}"
                )

            self._history.append(iteration)

        return f"Max iterations ({self._max_iterations}) reached without final answer."


# ── Tool-calling loop ─────────────────────────────────────────────


class ToolCallingLoop:
    """OpenAI function-calling style agent loop.

    The LLM decides which tools to call and with what arguments.
    Each tool call result is appended to the conversation and the
    loop continues until the LLM produces a non-tool response.

    Usage::

        loop = ToolCallingLoop(tools=[search, calc], max_calls=20)
        result = await loop.run("What is 2+2?", llm_fn=my_fn)
    """

    def __init__(
        self,
        tools: list[AgentTool],
        *,
        max_calls: int = 20,
    ) -> None:
        self._tools: dict[str, AgentTool] = {t.name: t for t in tools}
        self._max_calls = max_calls
        self._call_log: list[dict[str, str]] = []

    @property
    def call_log(self) -> list[dict[str, str]]:
        """Return the history of tool calls and results."""
        return list(self._call_log)

    async def run(
        self,
        query: str,
        llm_fn: Callable[[str, str], Awaitable[str]],
    ) -> str:
        """Execute the tool-calling loop.

        Args:
            query: The user's query.
            llm_fn: Async ``(system, user) -> str``.  When the LLM
                wants to call a tool it should respond with
                ``CALL: tool_name(input)``.

        Returns:
            The LLM's final (non-tool-call) response.
        """
        tool_desc = "\n".join(f"  - {t.name}: {t.description}" for t in self._tools.values())
        system = (
            "You may call tools by responding with:\n"
            "CALL: tool_name(input)\n\n"
            "When you have the final answer, respond normally "
            "(without CALL:).\n\n"
            f"Available tools:\n{tool_desc}"
        )
        self._call_log.clear()
        conversation = query

        for _i in range(self._max_calls):
            response = await llm_fn(system, conversation)

            if not response.strip().upper().startswith("CALL:"):
                return response

            # Parse tool call
            call_line = response.strip().split("\n", 1)[0]
            call_body = call_line.split(":", 1)[1].strip()

            tool_name, _, tool_input = call_body.partition("(")
            tool_input = tool_input.rstrip(")")
            tool_name = tool_name.strip()

            tool = self._tools.get(tool_name)
            if tool is not None:
                try:
                    result = await tool.function(tool_input)
                except Exception as exc:
                    result = f"Error: {exc}"
            else:
                result = f"Unknown tool: {tool_name}"

            self._call_log.append(
                {
                    "tool": tool_name,
                    "input": tool_input,
                    "result": result,
                }
            )
            conversation += f"\n\nTool result ({tool_name}): {result}"

        return f"Max tool calls ({self._max_calls}) reached."


# ── LLM Map-Reduce ────────────────────────────────────────────────


class LLMMapReduce:
    """Process large documents by splitting into chunks.

    1. Split document into chunks of *chunk_size* characters.
    2. Process each chunk with an LLM (map phase).
    3. Combine chunk results with an LLM (reduce phase).

    Usage::

        mr = LLMMapReduce(chunk_size=2000)
        summary = await mr.run(
            document=long_text,
            map_prompt="Summarize this section:\\n\\n{chunk}",
            reduce_prompt="Combine these summaries:\\n\\n{results}",
            llm_fn=my_fn,
        )
    """

    def __init__(self, *, chunk_size: int = 2000) -> None:
        self._chunk_size = max(1, chunk_size)

    def split(self, document: str) -> list[str]:
        """Split *document* into chunks of :attr:`chunk_size` chars."""
        return [
            document[i : i + self._chunk_size] for i in range(0, len(document), self._chunk_size)
        ]

    async def run(
        self,
        document: str,
        map_prompt: str,
        reduce_prompt: str,
        llm_fn: Callable[[str, str], Awaitable[str]],
    ) -> str:
        """Execute map-reduce over a document.

        Args:
            document: The full document text.
            map_prompt: Prompt template with ``{chunk}`` placeholder.
            reduce_prompt: Prompt template with ``{results}`` placeholder.
            llm_fn: Async ``(system, user) -> str``.

        Returns:
            The combined/reduced result.
        """
        chunks = self.split(document)
        if not chunks:
            return ""

        # Map phase
        chunk_results: list[str] = []
        for idx, chunk in enumerate(chunks):
            prompt = map_prompt.format(chunk=chunk)
            result = await llm_fn("", prompt)
            chunk_results.append(result)
            logger.debug("Map phase: processed chunk %d/%d", idx + 1, len(chunks))

        # Reduce phase
        if len(chunk_results) == 1:
            return chunk_results[0]

        combined = "\n---\n".join(chunk_results)
        prompt = reduce_prompt.format(results=combined)
        return await llm_fn("", prompt)
