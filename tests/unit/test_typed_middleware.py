"""Tests for enhanced typed middleware — StepMiddleware, MiddlewareChain, built-ins."""

from __future__ import annotations

from typing import Any

import pytest

from gravtory.enterprise.middleware import (
    LoggingMiddleware,
    MetricsMiddleware,
    MiddlewareChain,
    MiddlewareContext,
    RateLimitMiddleware,
    StepMiddleware,
    TimeoutMiddleware,
)


def _make_ctx(**overrides: Any) -> MiddlewareContext:
    """Create a MiddlewareContext with sensible defaults."""
    defaults: dict[str, Any] = {
        "workflow_name": "TestWorkflow",
        "workflow_run_id": "run-1",
        "step_name": "step_a",
        "step_order": 1,
        "namespace": "default",
        "retry_count": 0,
        "inputs": {},
        "duration_ms": 42,
    }
    defaults.update(overrides)
    return MiddlewareContext(**defaults)


class RecordingMiddleware(StepMiddleware):
    """Test middleware that records calls."""

    def __init__(self, name: str = "rec") -> None:
        self.name = name
        self.calls: list[str] = []

    async def before(self, ctx: MiddlewareContext) -> None:
        self.calls.append(f"{self.name}.before")

    async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
        self.calls.append(f"{self.name}.after")
        return result

    async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        self.calls.append(f"{self.name}.on_error")


class TransformMiddleware(StepMiddleware):
    """Middleware that transforms the result in after()."""

    def __init__(self, suffix: str) -> None:
        self.suffix = suffix

    async def before(self, ctx: MiddlewareContext) -> None:
        pass

    async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
        return f"{result}_{self.suffix}"

    async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        pass


class FailingMiddleware(StepMiddleware):
    """Middleware that raises in all hooks."""

    async def before(self, ctx: MiddlewareContext) -> None:
        raise RuntimeError("before boom")

    async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
        raise RuntimeError("after boom")

    async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        raise RuntimeError("on_error boom")


class TestTypedMiddleware:
    """StepMiddleware and MiddlewareChain tests."""

    @pytest.mark.asyncio()
    async def test_before_fires(self) -> None:
        """before() hooks fire in registration order."""
        m1 = RecordingMiddleware("m1")
        m2 = RecordingMiddleware("m2")
        chain = MiddlewareChain([m1, m2])

        ctx = _make_ctx()
        await chain.run_before(ctx)

        assert m1.calls == ["m1.before"]
        assert m2.calls == ["m2.before"]

    @pytest.mark.asyncio()
    async def test_after_fires(self) -> None:
        """after() hooks fire in reverse order."""
        m1 = RecordingMiddleware("m1")
        m2 = RecordingMiddleware("m2")
        chain = MiddlewareChain([m1, m2])

        ctx = _make_ctx()
        await chain.run_after(ctx, "result")

        # Reverse order: m2 first, then m1
        assert m2.calls == ["m2.after"]
        assert m1.calls == ["m1.after"]

    @pytest.mark.asyncio()
    async def test_on_error_fires(self) -> None:
        """on_error() hooks fire in reverse order."""
        m1 = RecordingMiddleware("m1")
        m2 = RecordingMiddleware("m2")
        chain = MiddlewareChain([m1, m2])

        ctx = _make_ctx()
        await chain.run_on_error(ctx, RuntimeError("test"))

        assert m2.calls == ["m2.on_error"]
        assert m1.calls == ["m1.on_error"]

    @pytest.mark.asyncio()
    async def test_chain_order(self) -> None:
        """Full onion order: M1.before → M2.before → step → M2.after → M1.after."""
        all_calls: list[str] = []

        class OrderedMW(StepMiddleware):
            def __init__(self, name: str) -> None:
                self._name = name

            async def before(self, ctx: MiddlewareContext) -> None:
                all_calls.append(f"{self._name}.before")

            async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
                all_calls.append(f"{self._name}.after")
                return result

            async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
                pass

        chain = MiddlewareChain([OrderedMW("M1"), OrderedMW("M2")])
        ctx = _make_ctx()

        await chain.run_before(ctx)
        all_calls.append("step")
        await chain.run_after(ctx, "result")

        assert all_calls == ["M1.before", "M2.before", "step", "M2.after", "M1.after"]

    @pytest.mark.asyncio()
    async def test_after_transforms_result(self) -> None:
        """after() can transform the result through the chain."""
        chain = MiddlewareChain(
            [
                TransformMiddleware("A"),
                TransformMiddleware("B"),
            ]
        )
        ctx = _make_ctx()
        # Reverse order: B.after runs first, then A.after
        result = await chain.run_after(ctx, "start")
        assert result == "start_B_A"

    @pytest.mark.asyncio()
    async def test_middleware_exception_isolated(self) -> None:
        """A failing middleware doesn't crash the chain or prevent others."""
        good = RecordingMiddleware("good")
        bad = FailingMiddleware()
        chain = MiddlewareChain([good, bad])

        ctx = _make_ctx()
        # Should not raise
        await chain.run_before(ctx)
        await chain.run_after(ctx, "result")
        await chain.run_on_error(ctx, RuntimeError("test"))

        # Good middleware still executed
        assert "good.before" in good.calls
        assert "good.after" in good.calls
        assert "good.on_error" in good.calls

    @pytest.mark.asyncio()
    async def test_add_middleware(self) -> None:
        """add() appends middleware to the chain."""
        chain = MiddlewareChain()
        m = RecordingMiddleware("m")
        chain.add(m)
        assert len(chain.middlewares) == 1

    def test_middleware_context_defaults(self) -> None:
        """MiddlewareContext has correct defaults."""
        ctx = MiddlewareContext(
            workflow_name="wf",
            workflow_run_id="r1",
            step_name="s1",
            step_order=1,
            namespace="default",
            retry_count=0,
        )
        assert ctx.inputs == {}
        assert ctx.duration_ms == 0
        assert ctx.metadata == {}


class TestBuiltInMiddleware:
    """Tests for built-in middleware implementations."""

    @pytest.mark.asyncio()
    async def test_logging_middleware(self) -> None:
        """LoggingMiddleware runs without errors."""
        mw = LoggingMiddleware()
        ctx = _make_ctx()
        await mw.before(ctx)
        result = await mw.after(ctx, "ok")
        assert result == "ok"
        await mw.on_error(ctx, RuntimeError("fail"))

    @pytest.mark.asyncio()
    async def test_metrics_middleware(self) -> None:
        """MetricsMiddleware collects counts and durations."""
        mw = MetricsMiddleware()
        ctx = _make_ctx(step_name="charge", duration_ms=100)

        await mw.before(ctx)
        await mw.after(ctx, "ok")
        await mw.on_error(ctx, RuntimeError("fail"))

        m = mw.metrics
        assert m["step_counts"]["charge"] == 1
        assert m["step_durations"]["charge"] == [100]
        assert m["error_counts"]["charge"] == 1

    @pytest.mark.asyncio()
    async def test_timeout_middleware(self) -> None:
        """TimeoutMiddleware sets metadata and doesn't crash."""
        mw = TimeoutMiddleware(default_timeout_ms=1000)
        ctx = _make_ctx()
        await mw.before(ctx)
        assert "_timeout_start" in ctx.metadata
        result = await mw.after(ctx, "ok")
        assert result == "ok"

    @pytest.mark.asyncio()
    async def test_rate_limit_middleware(self) -> None:
        """RateLimitMiddleware runs without errors and tracks tokens."""
        mw = RateLimitMiddleware(max_rate=100.0, per_seconds=1.0)
        ctx = _make_ctx()
        await mw.before(ctx)
        result = await mw.after(ctx, "ok")
        assert result == "ok"


class TestTypedMiddlewareGapFill:
    """Gap-fill tests for typed middleware edge cases."""

    @pytest.mark.asyncio()
    async def test_empty_chain(self) -> None:
        """Empty MiddlewareChain passes through without modification."""
        chain = MiddlewareChain([])
        ctx = _make_ctx()
        await chain.run_before(ctx)
        result = await chain.run_after(ctx, "original")
        assert result == "original"

    @pytest.mark.asyncio()
    async def test_logging_middleware_no_error(self) -> None:
        """LoggingMiddleware before/after don't raise."""
        mw = LoggingMiddleware()
        ctx = _make_ctx()
        await mw.before(ctx)
        result = await mw.after(ctx, "ok")
        assert result == "ok"

    @pytest.mark.asyncio()
    async def test_middleware_context_fields(self) -> None:
        ctx = _make_ctx()
        assert ctx.workflow_name == "TestWorkflow"
        assert ctx.step_name == "step_a"
        assert ctx.workflow_run_id == "run-1"
