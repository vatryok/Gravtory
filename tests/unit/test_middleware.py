"""Tests for MiddlewareRegistry — @before_step, @after_step, @on_failure hooks."""

from __future__ import annotations

from typing import Any

import pytest

from gravtory.decorators.middleware import MiddlewareRegistry


class TestMiddlewareRegistry:
    @pytest.mark.asyncio
    async def test_before_step_fires(self) -> None:
        registry = MiddlewareRegistry()
        calls: list[str] = []

        @registry.before_step
        async def hook(**kwargs: Any) -> None:
            calls.append(f"before:{kwargs['step_name']}")

        await registry.run_before("Wf", "charge", "run-1", {})
        assert calls == ["before:charge"]

    @pytest.mark.asyncio
    async def test_after_step_fires(self) -> None:
        registry = MiddlewareRegistry()
        calls: list[str] = []

        @registry.after_step
        async def hook(**kwargs: Any) -> None:
            calls.append(f"after:{kwargs['step_name']}:{kwargs['duration_ms']}")

        await registry.run_after("Wf", "charge", "run-1", output="ok", duration_ms=42.0)
        assert calls == ["after:charge:42.0"]

    @pytest.mark.asyncio
    async def test_on_failure_fires(self) -> None:
        registry = MiddlewareRegistry()
        calls: list[str] = []

        @registry.on_failure
        async def hook(**kwargs: Any) -> None:
            calls.append(f"fail:{kwargs['step_name']}:{kwargs['error']}")

        err = ValueError("boom")
        await registry.run_on_failure("Wf", "charge", "run-1", err)
        assert calls == ["fail:charge:boom"]

    @pytest.mark.asyncio
    async def test_middleware_order(self) -> None:
        """Hooks fire in registration order."""
        registry = MiddlewareRegistry()
        order: list[int] = []

        @registry.before_step
        async def first(**kwargs: Any) -> None:
            order.append(1)

        @registry.before_step
        async def second(**kwargs: Any) -> None:
            order.append(2)

        @registry.before_step
        async def third(**kwargs: Any) -> None:
            order.append(3)

        await registry.run_before("Wf", "s", "r", {})
        assert order == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_middleware_exception_isolated(self) -> None:
        """A broken hook must not prevent other hooks from firing."""
        registry = MiddlewareRegistry()
        calls: list[str] = []

        @registry.before_step
        async def bad_hook(**kwargs: Any) -> None:
            raise RuntimeError("hook exploded")

        @registry.before_step
        async def good_hook(**kwargs: Any) -> None:
            calls.append("ok")

        await registry.run_before("Wf", "s", "r", {})
        assert calls == ["ok"]

    @pytest.mark.asyncio
    async def test_decorator_returns_original_function(self) -> None:
        registry = MiddlewareRegistry()

        @registry.before_step
        async def my_hook(**kwargs: Any) -> None:
            pass

        assert my_hook.__name__ == "my_hook"

    @pytest.mark.asyncio
    async def test_no_hooks_registered(self) -> None:
        """Calling run_* with no hooks should not raise."""
        registry = MiddlewareRegistry()
        await registry.run_before("Wf", "s", "r", {})
        await registry.run_after("Wf", "s", "r", None, 0.0)
        await registry.run_on_failure("Wf", "s", "r", ValueError("x"))


class TestMiddlewareGapFill:
    """Gap-fill tests for middleware edge cases."""

    @pytest.mark.asyncio
    async def test_all_hook_types_fire_for_same_step(self) -> None:
        """Before, after, and on_failure hooks all fire independently."""
        registry = MiddlewareRegistry()
        calls: list[str] = []

        @registry.before_step
        async def before_hook(**kwargs: Any) -> None:
            calls.append("before")

        @registry.after_step
        async def after_hook(**kwargs: Any) -> None:
            calls.append("after")

        @registry.on_failure
        async def fail_hook(**kwargs: Any) -> None:
            calls.append("fail")

        await registry.run_before("Wf", "s", "r", {})
        await registry.run_after("Wf", "s", "r", "output", 10.0)
        await registry.run_on_failure("Wf", "s", "r", RuntimeError("x"))
        assert calls == ["before", "after", "fail"]

    @pytest.mark.asyncio
    async def test_hook_receives_workflow_name(self) -> None:
        registry = MiddlewareRegistry()
        received: dict[str, Any] = {}

        @registry.before_step
        async def hook(**kwargs: Any) -> None:
            received.update(kwargs)

        await registry.run_before("MyWorkflow", "my_step", "run-42", {"key": "val"})
        assert received["workflow_name"] == "MyWorkflow"
        assert received["step_name"] == "my_step"
        assert received["run_id"] == "run-42"

    @pytest.mark.asyncio
    async def test_many_hooks_all_fire(self) -> None:
        registry = MiddlewareRegistry()
        counter = {"n": 0}

        for _ in range(10):

            @registry.before_step
            async def hook(**kwargs: Any) -> None:
                counter["n"] += 1

        await registry.run_before("Wf", "s", "r", {})
        assert counter["n"] == 10
