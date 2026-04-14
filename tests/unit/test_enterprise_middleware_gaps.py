"""Tests for enterprise.middleware — MiddlewareChain and built-in middleware."""

from __future__ import annotations

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


def _make_ctx(**overrides) -> MiddlewareContext:
    defaults = dict(
        workflow_name="TestWF",
        workflow_run_id="run-1",
        step_name="step_a",
        step_order=1,
        namespace="default",
        retry_count=0,
    )
    defaults.update(overrides)
    return MiddlewareContext(**defaults)


class TestMiddlewareContext:
    def test_defaults(self) -> None:
        ctx = _make_ctx()
        assert ctx.duration_ms == 0
        assert ctx.inputs == {}
        assert ctx.metadata == {}


class TestLoggingMiddleware:
    @pytest.mark.asyncio
    async def test_before(self) -> None:
        mw = LoggingMiddleware()
        ctx = _make_ctx()
        await mw.before(ctx)  # should not raise

    @pytest.mark.asyncio
    async def test_after(self) -> None:
        mw = LoggingMiddleware()
        ctx = _make_ctx(duration_ms=42)
        result = await mw.after(ctx, {"output": True})
        assert result == {"output": True}

    @pytest.mark.asyncio
    async def test_on_error(self) -> None:
        mw = LoggingMiddleware()
        ctx = _make_ctx()
        await mw.on_error(ctx, RuntimeError("boom"))


class TestMetricsMiddleware:
    @pytest.mark.asyncio
    async def test_metrics_collection(self) -> None:
        mw = MetricsMiddleware()
        ctx = _make_ctx()

        await mw.before(ctx)
        assert mw.metrics["step_counts"]["step_a"] == 1

        ctx.duration_ms = 100
        result = await mw.after(ctx, "ok")
        assert result == "ok"
        assert mw.metrics["step_durations"]["step_a"] == [100]

    @pytest.mark.asyncio
    async def test_error_counts(self) -> None:
        mw = MetricsMiddleware()
        ctx = _make_ctx()
        await mw.on_error(ctx, ValueError("fail"))
        assert mw.metrics["error_counts"]["step_a"] == 1

    @pytest.mark.asyncio
    async def test_multiple_invocations(self) -> None:
        mw = MetricsMiddleware()
        for _ in range(3):
            ctx = _make_ctx()
            await mw.before(ctx)
        assert mw.metrics["step_counts"]["step_a"] == 3


class TestTimeoutMiddleware:
    @pytest.mark.asyncio
    async def test_normal_execution(self) -> None:
        mw = TimeoutMiddleware(default_timeout_ms=10000)
        ctx = _make_ctx()
        await mw.before(ctx)
        assert "_timeout_start" in ctx.metadata
        result = await mw.after(ctx, "done")
        assert result == "done"

    @pytest.mark.asyncio
    async def test_on_error_noop(self) -> None:
        mw = TimeoutMiddleware()
        ctx = _make_ctx()
        await mw.on_error(ctx, TimeoutError("slow"))

    @pytest.mark.asyncio
    async def test_timeout_exceeded_logs_warning(self) -> None:
        import time

        mw = TimeoutMiddleware(default_timeout_ms=1)  # 1ms timeout
        ctx = _make_ctx()
        await mw.before(ctx)
        time.sleep(0.01)  # sleep 10ms to exceed 1ms timeout
        result = await mw.after(ctx, "late")
        assert result == "late"  # still returns, just logs warning


class TestRateLimitMiddleware:
    @pytest.mark.asyncio
    async def test_allows_within_rate(self) -> None:
        mw = RateLimitMiddleware(max_rate=100.0, per_seconds=1.0)
        ctx = _make_ctx()
        await mw.before(ctx)  # should not delay

    @pytest.mark.asyncio
    async def test_after_passthrough(self) -> None:
        mw = RateLimitMiddleware()
        ctx = _make_ctx()
        result = await mw.after(ctx, "value")
        assert result == "value"

    @pytest.mark.asyncio
    async def test_on_error_noop(self) -> None:
        mw = RateLimitMiddleware()
        ctx = _make_ctx()
        await mw.on_error(ctx, RuntimeError("err"))

    @pytest.mark.asyncio
    async def test_rate_limit_delays_when_exhausted(self) -> None:
        mw = RateLimitMiddleware(max_rate=1.0, per_seconds=1.0)
        ctx1 = _make_ctx()
        await mw.before(ctx1)  # consume the 1 token
        # Next call should trigger delay
        ctx2 = _make_ctx()
        await mw.before(ctx2)


class TestMiddlewareChain:
    @pytest.mark.asyncio
    async def test_empty_chain(self) -> None:
        chain = MiddlewareChain()
        ctx = _make_ctx()
        await chain.run_before(ctx)
        result = await chain.run_after(ctx, "val")
        assert result == "val"
        await chain.run_on_error(ctx, RuntimeError("err"))

    @pytest.mark.asyncio
    async def test_add_middleware(self) -> None:
        chain = MiddlewareChain()
        mw = LoggingMiddleware()
        chain.add(mw)
        assert len(chain.middlewares) == 1

    @pytest.mark.asyncio
    async def test_before_order(self) -> None:
        order: list[str] = []

        class MW1(StepMiddleware):
            async def before(self, ctx: MiddlewareContext) -> None:
                order.append("mw1")

            async def after(self, ctx: MiddlewareContext, result) -> object:
                return result

            async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
                pass

        class MW2(StepMiddleware):
            async def before(self, ctx: MiddlewareContext) -> None:
                order.append("mw2")

            async def after(self, ctx: MiddlewareContext, result) -> object:
                return result

            async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
                pass

        chain = MiddlewareChain([MW1(), MW2()])
        await chain.run_before(_make_ctx())
        assert order == ["mw1", "mw2"]

    @pytest.mark.asyncio
    async def test_after_reverse_order(self) -> None:
        order: list[str] = []

        class MW1(StepMiddleware):
            async def before(self, ctx: MiddlewareContext) -> None:
                pass

            async def after(self, ctx: MiddlewareContext, result) -> object:
                order.append("mw1")
                return result

            async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
                pass

        class MW2(StepMiddleware):
            async def before(self, ctx: MiddlewareContext) -> None:
                pass

            async def after(self, ctx: MiddlewareContext, result) -> object:
                order.append("mw2")
                return result

            async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
                pass

        chain = MiddlewareChain([MW1(), MW2()])
        await chain.run_after(_make_ctx(), "val")
        assert order == ["mw2", "mw1"]  # reverse order

    @pytest.mark.asyncio
    async def test_on_error_reverse_order(self) -> None:
        order: list[str] = []

        class MW1(StepMiddleware):
            async def before(self, ctx: MiddlewareContext) -> None:
                pass

            async def after(self, ctx: MiddlewareContext, result) -> object:
                return result

            async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
                order.append("mw1")

        class MW2(StepMiddleware):
            async def before(self, ctx: MiddlewareContext) -> None:
                pass

            async def after(self, ctx: MiddlewareContext, result) -> object:
                return result

            async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
                order.append("mw2")

        chain = MiddlewareChain([MW1(), MW2()])
        await chain.run_on_error(_make_ctx(), RuntimeError("err"))
        assert order == ["mw2", "mw1"]

    @pytest.mark.asyncio
    async def test_failing_middleware_isolated(self) -> None:
        class FailingMW(StepMiddleware):
            async def before(self, ctx: MiddlewareContext) -> None:
                raise RuntimeError("broken")

            async def after(self, ctx: MiddlewareContext, result) -> object:
                raise RuntimeError("broken")

            async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
                raise RuntimeError("broken")

        chain = MiddlewareChain([FailingMW(), LoggingMiddleware()])
        ctx = _make_ctx()
        # None of these should raise
        await chain.run_before(ctx)
        result = await chain.run_after(ctx, "value")
        assert result == "value"
        await chain.run_on_error(ctx, RuntimeError("test"))
