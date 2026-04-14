"""Unit tests for TokenCounter, cost estimation, and gap-fill tests."""

from __future__ import annotations

from gravtory.ai.tokens import MODEL_COSTS, TokenCounter


class TestTokenCounter:
    """TokenCounter unit tests."""

    def test_count_returns_positive_int(self) -> None:
        """Token count returns a positive integer for non-empty text."""
        tc = TokenCounter()
        text = "A" * 40
        count = tc.count(text, "gpt-4")
        assert isinstance(count, int)
        assert count > 0

    def test_count_empty_string(self) -> None:
        """Empty string returns 0 tokens."""
        tc = TokenCounter()
        assert tc.count("", "gpt-4") == 0

    def test_count_short_string_minimum_one(self) -> None:
        """Very short strings return at least 1 token."""
        tc = TokenCounter()
        assert tc.count("Hi", "gpt-4") >= 1

    def test_estimate_cost_known_model(self) -> None:
        """Cost estimation uses MODEL_COSTS for known models."""
        tc = TokenCounter()
        cost = tc.estimate_cost(1000, 500, "gpt-4")
        # input: 1000/1000 * 0.03 = 0.03
        # output: 500/1000 * 0.03 * 2 = 0.03
        # total: 0.06
        expected = (1000 / 1000) * 0.03 + (500 / 1000) * 0.03 * 2
        assert abs(cost - expected) < 1e-6

    def test_estimate_cost_unknown_model(self) -> None:
        """Unknown models default to 0.0 cost."""
        tc = TokenCounter()
        cost = tc.estimate_cost(1000, 500, "unknown-model-xyz")
        assert cost == 0.0

    def test_model_costs_table(self) -> None:
        """MODEL_COSTS contains expected models."""
        assert "gpt-4" in MODEL_COSTS
        assert "gpt-3.5-turbo" in MODEL_COSTS
        assert "claude-3-opus" in MODEL_COSTS
        assert all(v > 0 for v in MODEL_COSTS.values())


class TestTokenCounterGapFill:
    """Gap-fill tests for token counting edge cases."""

    def test_count_long_text(self) -> None:
        """Long text produces proportionally more tokens."""
        tc = TokenCounter()
        short = tc.count("Hello", "gpt-4")
        long = tc.count("Hello " * 1000, "gpt-4")
        assert long > short

    def test_count_unicode_text(self) -> None:
        """Unicode text is counted (chars/4 fallback)."""
        tc = TokenCounter()
        count = tc.count("日本語テスト", "gpt-4")
        assert count >= 1

    def test_estimate_cost_zero_tokens(self) -> None:
        """Zero input/output tokens produce zero cost."""
        tc = TokenCounter()
        assert tc.estimate_cost(0, 0, "gpt-4") == 0.0

    def test_estimate_cost_only_input(self) -> None:
        """Cost with only input tokens (no output)."""
        tc = TokenCounter()
        cost = tc.estimate_cost(1000, 0, "gpt-4")
        assert cost > 0.0

    def test_estimate_cost_only_output(self) -> None:
        """Cost with only output tokens (no input)."""
        tc = TokenCounter()
        cost = tc.estimate_cost(0, 1000, "gpt-4")
        assert cost > 0.0

    def test_model_costs_all_positive(self) -> None:
        """All model costs are positive floats."""
        for model, cost in MODEL_COSTS.items():
            assert isinstance(cost, float), f"{model} cost is not float"
            assert cost > 0, f"{model} cost is not positive"
