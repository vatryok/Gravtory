"""Tests for @llm_step decorator — LLM-powered workflow steps."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gravtory.ai.llm_step import (
    CostLimitExceededError,
    LLMConfig,
    _ResponseCache,
    _token_counter,
    get_usage_tracker,
    llm_step,
)
from gravtory.core.errors import ConfigurationError
from gravtory.decorators.step import step

if TYPE_CHECKING:
    from gravtory.core.types import StepDefinition


class TestLLMStep:
    """@llm_step decorator tests."""

    def _make_decorated(
        self,
        *,
        model: str = "gpt-4",
        fallback_models: list[str] | None = None,
        cache: bool = False,
        cost_limit: float | None = None,
    ) -> tuple[object, StepDefinition]:
        """Helper: create a @step + @llm_step decorated async function."""

        @llm_step(
            model=model,
            fallback_models=fallback_models,
            cache=cache,
            cost_limit=cost_limit,
        )
        @step(1)
        async def my_llm_fn(data: str, _llm_model: str = "") -> str:
            return f"response from {_llm_model}: {data}"

        step_def: StepDefinition = my_llm_fn.__gravtory_step__
        return my_llm_fn, step_def

    def test_llm_step_sets_retry_policy(self) -> None:
        """@llm_step sets retries=5, exponential backoff with jitter."""
        _fn, sd = self._make_decorated()
        assert sd.retries == 5
        assert sd.backoff == "exponential"
        assert sd.backoff_base == 2.0
        assert sd.jitter is True

    def test_llm_step_preserves_custom_retries(self) -> None:
        """If user already set retries, @llm_step doesn't overwrite."""

        @llm_step(model="gpt-4")
        @step(1, retries=10, backoff="linear")
        async def fn(data: str, _llm_model: str = "") -> str:
            return data

        sd: StepDefinition = fn.__gravtory_step__
        assert sd.retries == 10
        assert sd.backoff == "linear"

    def test_llm_step_requires_step_decorator(self) -> None:
        """@llm_step without @step raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="outside @step"):

            @llm_step(model="gpt-4")
            async def bare_fn(data: str) -> str:
                return data

    def test_llm_config_attached(self) -> None:
        """LLMConfig is attached to the function."""
        fn, _sd = self._make_decorated(
            model="gpt-3.5-turbo",
            fallback_models=["gpt-4"],
            cache=True,
            cost_limit=0.50,
        )
        cfg: LLMConfig = fn.__gravtory_llm__  # type: ignore[attr-defined]
        assert cfg.model == "gpt-3.5-turbo"
        assert cfg.fallback_models == ["gpt-4"]
        assert cfg.cache is True
        assert cfg.cost_limit == 0.50

    @pytest.mark.asyncio()
    async def test_model_fallback(self) -> None:
        """On failure, @llm_step tries fallback models."""
        call_log: list[str] = []

        @llm_step(model="bad-model", fallback_models=["gpt-3.5-turbo"])
        @step(1)
        async def fn(data: str, _llm_model: str = "") -> str:
            call_log.append(_llm_model)
            if _llm_model == "bad-model":
                msg = "model not found"
                raise RuntimeError(msg)
            return f"ok: {_llm_model}"

        sd: StepDefinition = fn.__gravtory_step__
        result = await sd.function("hello")  # type: ignore[misc]
        assert result == "ok: gpt-3.5-turbo"
        assert call_log == ["bad-model", "gpt-3.5-turbo"]

    @pytest.mark.asyncio()
    async def test_cost_limit_enforcement(self) -> None:
        """When estimated cost exceeds limit, CostLimitExceededError is raised."""

        @llm_step(model="gpt-4", cost_limit=0.0)  # $0 limit = always exceeded
        @step(1)
        async def fn(data: str, _llm_model: str = "") -> str:
            return data

        sd: StepDefinition = fn.__gravtory_step__
        with pytest.raises(CostLimitExceededError):
            await sd.function("some long input text that has tokens")  # type: ignore[misc]

    @pytest.mark.asyncio()
    async def test_cache_hit(self) -> None:
        """With cache=True, same args return cached response without re-calling."""
        call_count = 0

        @llm_step(model="gpt-4", cache=True)
        @step(1)
        async def fn(data: str, _llm_model: str = "") -> str:
            nonlocal call_count
            call_count += 1
            return f"result-{call_count}"

        sd: StepDefinition = fn.__gravtory_step__
        r1 = await sd.function("same-input")  # type: ignore[misc]
        r2 = await sd.function("same-input")  # type: ignore[misc]
        assert r1 == r2
        assert call_count == 1  # Only called once; second was cache hit

    @pytest.mark.asyncio()
    async def test_graceful_without_tiktoken(self) -> None:
        """Token counting works even without tiktoken (estimation fallback)."""
        # The test environment doesn't have tiktoken installed,
        # so this exercises the fallback path.
        count = _token_counter.count("Hello, this is a test sentence.", "gpt-4")
        assert count > 0

    def test_response_cache(self) -> None:
        """_ResponseCache stores and retrieves values."""
        cache = _ResponseCache()
        k = cache.key("gpt-4", ("hello",), {})
        assert cache.get(k) is None
        cache.set(k, "world")
        assert cache.get(k) == "world"

    @pytest.mark.asyncio()
    async def test_cost_limit_tries_cheaper_fallback(self) -> None:
        """CostLimitExceededError on primary model falls through to cheaper fallback."""
        # gpt-4 costs $0.03/1K tokens; gpt-4o-mini costs $0.00015/1K tokens
        # For input "x" + kwargs, ~7 tokens → gpt-4 est ~$0.00021, gpt-4o-mini ~$0.000001
        # Set limit between the two so gpt-4 exceeds it but gpt-4o-mini doesn't.

        @llm_step(
            model="gpt-4",
            fallback_models=["gpt-4o-mini"],
            cost_limit=0.0001,  # gpt-4 ~$0.00021 exceeds, gpt-4o-mini ~$0.000001 doesn't
        )
        @step(1)
        async def fn(data: str, _llm_model: str = "") -> str:
            return f"ok: {_llm_model}"

        sd: StepDefinition = fn.__gravtory_step__
        result = await sd.function("x")  # type: ignore[misc]
        # Should have fallen back to the cheaper model
        assert result == "ok: gpt-4o-mini"

    def test_usage_tracker_singleton(self) -> None:
        """get_usage_tracker returns the module-level tracker."""
        tracker = get_usage_tracker()
        assert tracker is not None


class TestLLMStepGapFill:
    """Gap-fill tests for llm_step edge cases."""

    def test_response_cache_key_deterministic(self) -> None:
        cache = _ResponseCache()
        k1 = cache.key("gpt-4", ("hello",), {"temp": 0.5})
        k2 = cache.key("gpt-4", ("hello",), {"temp": 0.5})
        assert k1 == k2

    def test_response_cache_different_keys(self) -> None:
        cache = _ResponseCache()
        k1 = cache.key("gpt-4", ("hello",), {})
        k2 = cache.key("gpt-4", ("world",), {})
        assert k1 != k2

    def test_response_cache_miss_returns_none(self) -> None:
        cache = _ResponseCache()
        assert cache.get("nonexistent-key") is None
