# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Enhanced typed middleware system with onion-model chaining.

Provides :class:`StepMiddleware` (abstract base) and :class:`MiddlewareChain`
for executing middleware in correct order: before in registration order,
after in reverse order — like onion layers.

Built-in middleware:
  - :class:`LoggingMiddleware` — structured logging for all steps
  - :class:`MetricsMiddleware` — in-memory metrics collection
  - :class:`TimeoutMiddleware` — enforce step-level time limits
  - :class:`RateLimitMiddleware` — token-bucket rate limiting
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("gravtory.enterprise.middleware")


@dataclass
class MiddlewareContext:
    """Context passed to middleware hooks.

    Populated before step execution; ``duration_ms`` is set after.
    The ``metadata`` dict can be used by middleware to pass data
    between ``before`` and ``after`` hooks.
    """

    workflow_name: str
    workflow_run_id: str
    step_name: str
    step_order: int
    namespace: str
    retry_count: int
    inputs: dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class StepMiddleware(ABC):
    """Base class for typed step middleware.

    Subclass and implement ``before``, ``after``, and ``on_error``::

        class MyMiddleware(StepMiddleware):
            async def before(self, ctx: MiddlewareContext) -> None:
                logger.info("Starting %s", ctx.step_name)

            async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
                logger.info("Done %s in %dms", ctx.step_name, ctx.duration_ms)
                return result

            async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
                logger.error("Failed: %s", error)
    """

    @abstractmethod
    async def before(self, ctx: MiddlewareContext) -> None:
        """Called before step execution."""
        ...

    @abstractmethod
    async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
        """Called after step execution. May transform the result."""
        ...

    @abstractmethod
    async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        """Called when step execution fails."""
        ...


class MiddlewareChain:
    """Executes a sequence of middleware in onion order.

    ``before`` hooks run in registration order (first registered → first called).
    ``after`` hooks run in reverse order (last registered → first called).
    ``on_error`` hooks run in reverse order.

    Middleware exceptions are isolated — a failing hook never prevents
    other hooks from running or crashes the step.
    """

    def __init__(self, middlewares: list[StepMiddleware] | None = None) -> None:
        self._middlewares: list[StepMiddleware] = list(middlewares or [])

    def add(self, middleware: StepMiddleware) -> None:
        """Append a middleware to the chain."""
        self._middlewares.append(middleware)

    @property
    def middlewares(self) -> list[StepMiddleware]:
        return list(self._middlewares)

    async def run_before(self, ctx: MiddlewareContext) -> None:
        """Run all ``before`` hooks in registration order."""
        for mw in self._middlewares:
            try:
                await mw.before(ctx)
            except Exception:
                logger.exception(
                    "Middleware %s.before failed for step %s",
                    type(mw).__name__,
                    ctx.step_name,
                )

    async def run_after(self, ctx: MiddlewareContext, result: Any) -> Any:
        """Run all ``after`` hooks in reverse order.

        Each hook may transform the result. The transformed result
        is passed to the next hook.
        """
        current = result
        for mw in reversed(self._middlewares):
            try:
                current = await mw.after(ctx, current)
            except Exception:
                logger.exception(
                    "Middleware %s.after failed for step %s",
                    type(mw).__name__,
                    ctx.step_name,
                )
        return current

    async def run_on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        """Run all ``on_error`` hooks in reverse order."""
        for mw in reversed(self._middlewares):
            try:
                await mw.on_error(ctx, error)
            except Exception:
                logger.exception(
                    "Middleware %s.on_error failed for step %s",
                    type(mw).__name__,
                    ctx.step_name,
                )


# ── Built-in middleware ──────────────────────────────────────────────


class LoggingMiddleware(StepMiddleware):
    """Structured logging for all step executions."""

    def __init__(self, log_level: int = logging.INFO) -> None:
        self._level = log_level
        self._logger = logging.getLogger("gravtory.middleware.logging")

    async def before(self, ctx: MiddlewareContext) -> None:
        self._logger.log(
            self._level,
            "STEP_START workflow=%s run=%s step=%s order=%d retry=%d",
            ctx.workflow_name,
            ctx.workflow_run_id,
            ctx.step_name,
            ctx.step_order,
            ctx.retry_count,
        )

    async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
        self._logger.log(
            self._level,
            "STEP_DONE workflow=%s run=%s step=%s duration_ms=%d",
            ctx.workflow_name,
            ctx.workflow_run_id,
            ctx.step_name,
            ctx.duration_ms,
        )
        return result

    async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        self._logger.error(
            "STEP_FAIL workflow=%s run=%s step=%s error=%s",
            ctx.workflow_name,
            ctx.workflow_run_id,
            ctx.step_name,
            error,
        )


class MetricsMiddleware(StepMiddleware):
    """In-memory metrics collection for step executions.

    Collects counts and durations per step name, accessible via
    the ``metrics`` property.
    """

    def __init__(self) -> None:
        self._step_counts: dict[str, int] = {}
        self._step_durations: dict[str, list[int]] = {}
        self._error_counts: dict[str, int] = {}

    @property
    def metrics(self) -> dict[str, Any]:
        """Return collected metrics snapshot."""
        return {
            "step_counts": dict(self._step_counts),
            "step_durations": {k: list(v) for k, v in self._step_durations.items()},
            "error_counts": dict(self._error_counts),
        }

    async def before(self, ctx: MiddlewareContext) -> None:
        self._step_counts[ctx.step_name] = self._step_counts.get(ctx.step_name, 0) + 1

    async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
        self._step_durations.setdefault(ctx.step_name, []).append(ctx.duration_ms)
        return result

    async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        self._error_counts[ctx.step_name] = self._error_counts.get(ctx.step_name, 0) + 1


class TimeoutMiddleware(StepMiddleware):
    """Enforce step-level time limits.

    Records start time in ``before`` and checks elapsed time in ``after``.
    If the step exceeds the timeout, logs a warning (the actual timeout
    enforcement should be done by the execution engine).
    """

    def __init__(self, default_timeout_ms: int = 30_000) -> None:
        self._default_timeout_ms = default_timeout_ms

    async def before(self, ctx: MiddlewareContext) -> None:
        ctx.metadata["_timeout_start"] = time.monotonic()
        ctx.metadata["_timeout_limit_ms"] = self._default_timeout_ms

    async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
        start = ctx.metadata.get("_timeout_start")
        if start is not None:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            limit = ctx.metadata.get("_timeout_limit_ms", self._default_timeout_ms)
            if elapsed_ms > limit:
                logger.warning(
                    "Step %s exceeded timeout: %dms > %dms",
                    ctx.step_name,
                    elapsed_ms,
                    limit,
                )
        return result

    async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        pass  # Timeout errors are handled by the engine


class RateLimitMiddleware(StepMiddleware):
    """Token-bucket rate limiting for step execution.

    Limits the rate of step executions per key (default: step name).
    When the rate is exceeded, the step is delayed via ``asyncio.sleep``.
    """

    def __init__(
        self,
        max_rate: float = 10.0,
        per_seconds: float = 1.0,
    ) -> None:
        self._max_rate = max_rate
        self._per_seconds = per_seconds
        self._tokens: dict[str, float] = {}
        self._last_refill: dict[str, float] = {}

    async def before(self, ctx: MiddlewareContext) -> None:
        key = ctx.step_name
        now = time.monotonic()

        if key not in self._tokens:
            self._tokens[key] = self._max_rate
            self._last_refill[key] = now

        # Refill tokens
        elapsed = now - self._last_refill[key]
        refill = elapsed * (self._max_rate / self._per_seconds)
        self._tokens[key] = min(self._max_rate, self._tokens[key] + refill)
        self._last_refill[key] = now

        if self._tokens[key] < 1.0:
            wait = (1.0 - self._tokens[key]) * (self._per_seconds / self._max_rate)
            logger.debug("Rate limiting step %s — waiting %.3fs", key, wait)
            await asyncio.sleep(wait)
            self._tokens[key] = 0.0
        else:
            self._tokens[key] -= 1.0

    async def after(self, ctx: MiddlewareContext, result: Any) -> Any:
        return result

    async def on_error(self, ctx: MiddlewareContext, error: Exception) -> None:
        pass
