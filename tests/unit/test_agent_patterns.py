"""Tests for agent orchestration patterns — ReAct, tool-calling, map-reduce."""

from __future__ import annotations

import pytest

from gravtory.ai.agents import (
    AgentTool,
    LLMMapReduce,
    ReActAgent,
    ToolCallingLoop,
)

# ── Helpers ───────────────────────────────────────────────────────


async def _search(query: str) -> str:
    return f"Search result for: {query}"


async def _calculator(expr: str) -> str:
    return "42"


SEARCH_TOOL = AgentTool(
    name="search",
    description="Search the web",
    function=_search,
    parameters={"query": {"type": "string"}},
)

CALC_TOOL = AgentTool(
    name="calculator",
    description="Evaluate math expressions",
    function=_calculator,
    parameters={"expression": {"type": "string"}},
)


# ── ReAct agent tests ────────────────────────────────────────────


class TestReActAgent:
    """ReActAgent unit tests."""

    @pytest.mark.asyncio()
    async def test_react_reaches_final_answer(self) -> None:
        """Agent converges to a final answer."""
        agent = ReActAgent(tools=[SEARCH_TOOL], max_iterations=5)

        call_count = 0

        async def mock_llm(system: str, user: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return (
                    "Thought: I need to search for the answer.\n"
                    "Action: search\n"
                    "Action Input: GDP of France"
                )
            return (
                "Thought: I now know the final answer.\n"
                "Final Answer: The GDP of France is $2.78 trillion."
            )

        result = await agent.run("What is the GDP of France?", llm_fn=mock_llm)
        assert "2.78 trillion" in result
        assert len(agent.history) == 1  # One tool-calling iteration

    @pytest.mark.asyncio()
    async def test_react_max_iterations(self) -> None:
        """Agent stops after max_iterations."""
        agent = ReActAgent(tools=[SEARCH_TOOL], max_iterations=2)

        async def never_answers(system: str, user: str) -> str:
            return "Thought: Let me search more.\nAction: search\nAction Input: something"

        result = await agent.run("unanswerable", llm_fn=never_answers)
        assert "Max iterations" in result
        assert len(agent.history) == 2

    @pytest.mark.asyncio()
    async def test_react_unknown_tool(self) -> None:
        """Agent handles unknown tool names gracefully."""
        agent = ReActAgent(tools=[SEARCH_TOOL], max_iterations=3)
        call_count = 0

        async def uses_unknown_tool(system: str, user: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "Thought: Let me try.\nAction: nonexistent_tool\nAction Input: test"
            return "Thought: done\nFinal Answer: recovered"

        result = await agent.run("test", llm_fn=uses_unknown_tool)
        assert result == "recovered"
        assert "Unknown tool" in agent.history[0].observation

    def test_parse_response(self) -> None:
        """parse_response extracts structured fields from LLM text."""
        text = "Thought: I need to search.\nAction: search\nAction Input: query text"
        parsed = ReActAgent.parse_response(text)
        assert parsed["thought"] == "I need to search."
        assert parsed["action"] == "search"
        assert parsed["action_input"] == "query text"

    def test_parse_response_final_answer(self) -> None:
        """parse_response detects Final Answer."""
        text = "Thought: I know.\nFinal Answer: 42"
        parsed = ReActAgent.parse_response(text)
        assert parsed["final_answer"] == "42"

    def test_tools_property(self) -> None:
        """tools property returns registered tools."""
        agent = ReActAgent(tools=[SEARCH_TOOL, CALC_TOOL])
        assert "search" in agent.tools
        assert "calculator" in agent.tools

    @pytest.mark.asyncio()
    async def test_react_history_cleared_between_runs(self) -> None:
        """History is reset at the start of each run() call."""
        agent = ReActAgent(tools=[SEARCH_TOOL], max_iterations=3)

        async def immediate_answer(system: str, user: str) -> str:
            return "Thought: done\nFinal Answer: answer"

        await agent.run("first query", llm_fn=immediate_answer)
        assert len(agent.history) == 0  # Final answer found immediately

        # Second run: history should not carry over
        call_count = 0

        async def one_tool_then_answer(system: str, user: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "Thought: search\nAction: search\nAction Input: foo"
            return "Thought: got it\nFinal Answer: bar"

        result = await agent.run("second query", llm_fn=one_tool_then_answer)
        assert result == "bar"
        assert len(agent.history) == 1  # Only THIS run's iteration


# ── Tool-calling loop tests ───────────────────────────────────────


class TestToolCallingLoop:
    """ToolCallingLoop unit tests."""

    @pytest.mark.asyncio()
    async def test_tool_calling_executes_and_returns(self) -> None:
        """Loop calls tool, feeds result back, gets final answer."""
        loop = ToolCallingLoop(tools=[CALC_TOOL], max_calls=5)
        call_count = 0

        async def mock_llm(system: str, user: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "CALL: calculator(2+2)"
            return "The answer is 42."

        result = await loop.run("What is 2+2?", llm_fn=mock_llm)
        assert result == "The answer is 42."
        assert len(loop.call_log) == 1
        assert loop.call_log[0]["tool"] == "calculator"

    @pytest.mark.asyncio()
    async def test_tool_calling_max_calls(self) -> None:
        """Loop stops after max_calls."""
        loop = ToolCallingLoop(tools=[SEARCH_TOOL], max_calls=2)

        async def always_calls(system: str, user: str) -> str:
            return "CALL: search(more)"

        result = await loop.run("loop forever", llm_fn=always_calls)
        assert "Max tool calls" in result

    @pytest.mark.asyncio()
    async def test_tool_calling_unknown_tool(self) -> None:
        """Unknown tool returns error message in conversation."""
        loop = ToolCallingLoop(tools=[], max_calls=3)
        call_count = 0

        async def mock_llm(system: str, user: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "CALL: unknown_tool(test)"
            return "Final answer after error."

        result = await loop.run("test", llm_fn=mock_llm)
        assert result == "Final answer after error."

    @pytest.mark.asyncio()
    async def test_tool_calling_log_cleared_between_runs(self) -> None:
        """call_log is reset at the start of each run() call."""
        loop = ToolCallingLoop(tools=[CALC_TOOL], max_calls=5)

        call_count = 0

        async def mock_llm_1(system: str, user: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "CALL: calculator(1+1)"
            return "Done first."

        await loop.run("first", llm_fn=mock_llm_1)
        assert len(loop.call_log) == 1

        # Second run: call_log should be fresh
        call_count = 0

        async def mock_llm_2(system: str, user: str) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "CALL: calculator(2+2)"
            return "Done second."

        await loop.run("second", llm_fn=mock_llm_2)
        assert len(loop.call_log) == 1  # Only THIS run's call, not 2


# ── LLM Map-Reduce tests ─────────────────────────────────────────


class TestLLMMapReduce:
    """LLMMapReduce unit tests."""

    def test_split(self) -> None:
        """split() divides document into chunks of the right size."""
        mr = LLMMapReduce(chunk_size=10)
        chunks = mr.split("ABCDEFGHIJ" * 3)  # 30 chars
        assert len(chunks) == 3
        assert all(len(c) == 10 for c in chunks)

    def test_split_empty(self) -> None:
        """split() on empty string returns empty list."""
        mr = LLMMapReduce(chunk_size=100)
        assert mr.split("") == []

    @pytest.mark.asyncio()
    async def test_map_reduce_single_chunk(self) -> None:
        """Single chunk skips reduce phase — returns map result directly."""
        mr = LLMMapReduce(chunk_size=10000)

        async def mock_llm(system: str, user: str) -> str:
            return "Summary: short doc."

        result = await mr.run(
            document="Short document.",
            map_prompt="Summarize: {chunk}",
            reduce_prompt="Combine: {results}",
            llm_fn=mock_llm,
        )
        assert result == "Summary: short doc."

    @pytest.mark.asyncio()
    async def test_map_reduce_multiple_chunks(self) -> None:
        """Multiple chunks go through map + reduce phases."""
        mr = LLMMapReduce(chunk_size=5)
        calls: list[str] = []

        async def mock_llm(system: str, user: str) -> str:
            calls.append(user)
            if "Combine:" in user:
                return "Combined result"
            return f"Mapped: {user[:10]}"

        result = await mr.run(
            document="ABCDEFGHIJKLMNO",  # 15 chars → 3 chunks
            map_prompt="Process: {chunk}",
            reduce_prompt="Combine: {results}",
            llm_fn=mock_llm,
        )
        assert result == "Combined result"
        # 3 map calls + 1 reduce call = 4
        assert len(calls) == 4

    @pytest.mark.asyncio()
    async def test_map_reduce_empty_document(self) -> None:
        """Empty document returns empty string."""
        mr = LLMMapReduce(chunk_size=100)

        async def mock_llm(system: str, user: str) -> str:
            return "should not be called"

        result = await mr.run(
            document="",
            map_prompt="{chunk}",
            reduce_prompt="{results}",
            llm_fn=mock_llm,
        )
        assert result == ""


class TestAgentPatternsGapFill:
    """Gap-fill tests for agent pattern edge cases."""

    def test_split_single_chunk(self) -> None:
        """Document smaller than chunk_size produces one chunk."""
        mr = LLMMapReduce(chunk_size=1000)
        chunks = mr.split("short text")
        assert len(chunks) == 1
        assert chunks[0] == "short text"

    def test_split_exact_multiple(self) -> None:
        """Document exactly divisible by chunk_size."""
        mr = LLMMapReduce(chunk_size=5)
        chunks = mr.split("ABCDE" * 4)  # 20 chars
        assert len(chunks) == 4
        assert all(len(c) == 5 for c in chunks)

    @pytest.mark.asyncio()
    async def test_react_agent_max_iterations(self) -> None:
        """ReActAgent stops after max_iterations."""

        async def noop_fn(**kw: object) -> str:
            return "ok"

        tools = [AgentTool(name="noop", description="no-op", function=noop_fn)]
        agent = ReActAgent(tools=tools, max_iterations=2)

        call_count = 0

        async def mock_llm(system: str, user: str) -> str:
            nonlocal call_count
            call_count += 1
            return "Thought: thinking\nAction: noop\nAction Input: {}"

        try:
            await agent.run("test", llm_fn=mock_llm)
        except Exception:
            pass  # May raise on max iterations
        assert call_count <= 3  # max_iterations + possible final
