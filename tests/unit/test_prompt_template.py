"""Tests for PromptTemplate — versioned prompt management."""

from __future__ import annotations

import pytest

from gravtory.ai.prompts import PromptTemplate


class TestPromptTemplate:
    """PromptTemplate unit tests."""

    def test_render(self) -> None:
        """render() substitutes variables into the template."""
        pt = PromptTemplate(name="test", template="Hello {name}, your order is {order_id}.")
        result = pt.render(name="Alice", order_id="ORD-42")
        assert result == "Hello Alice, your order is ORD-42."

    def test_to_messages_without_system(self) -> None:
        """to_messages() returns a single user message when no system_prompt."""
        pt = PromptTemplate(name="analyze", template="Analyze: {data}")
        messages = pt.to_messages(data="revenue data")
        assert len(messages) == 1
        assert messages[0]["role"] == "user"
        assert messages[0]["content"] == "Analyze: revenue data"

    def test_to_messages_with_system(self) -> None:
        """to_messages() prepends system message when system_prompt is set."""
        pt = PromptTemplate(
            name="analyze",
            template="Analyze: {data}",
            system_prompt="You are a data analyst.",
        )
        messages = pt.to_messages(data="sales figures")
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[0]["content"] == "You are a data analyst."
        assert messages[1]["role"] == "user"
        assert "sales figures" in messages[1]["content"]

    def test_missing_variable_raises_key_error(self) -> None:
        """render() raises KeyError when a required variable is missing."""
        pt = PromptTemplate(name="test", template="Hello {name}")
        with pytest.raises(KeyError):
            pt.render()

    def test_version_and_metadata(self) -> None:
        """PromptTemplate stores version, model, max_tokens, metadata."""
        pt = PromptTemplate(
            name="summarize",
            template="Summarize: {text}",
            version=3,
            model="gpt-4",
            max_tokens=500,
            metadata={"author": "team-a"},
        )
        assert pt.version == 3
        assert pt.model == "gpt-4"
        assert pt.max_tokens == 500
        assert pt.metadata == {"author": "team-a"}

    def test_defaults(self) -> None:
        """PromptTemplate has sensible defaults."""
        pt = PromptTemplate(name="x", template="y")
        assert pt.version == 1
        assert pt.model is None
        assert pt.max_tokens is None
        assert pt.system_prompt is None
        assert pt.metadata == {}


class TestPromptTemplateGapFill:
    """Gap-fill tests for prompt template edge cases."""

    def test_render_with_special_characters(self) -> None:
        pt = PromptTemplate(name="special", template="Query: {q}")
        result = pt.render(q='SELECT * FROM "table" WHERE x > 0')
        assert 'SELECT * FROM "table"' in result

    def test_render_with_multiline(self) -> None:
        pt = PromptTemplate(name="multi", template="Input:\n{text}\nEnd")
        result = pt.render(text="line1\nline2")
        assert "line1\nline2" in result

    def test_to_messages_with_system(self) -> None:
        pt = PromptTemplate(
            name="sys",
            template="Hello {name}",
            system_prompt="You are a helpful assistant.",
        )
        msgs = pt.to_messages(name="Alice")
        assert any(m["role"] == "system" for m in msgs)
        assert any("Hello Alice" in m["content"] for m in msgs)
