# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Middleware hooks — cross-cutting concerns for step execution.

Provides a :class:`MiddlewareRegistry` and three registration helpers
(:func:`before_step`, :func:`after_step`, :func:`on_failure`) that let
users inject logic before/after every step or on step failure.

Middleware exceptions are isolated — a broken hook never crashes the
step or prevents other hooks from running.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("gravtory.decorators.middleware")

# Type alias for async middleware functions
# Middleware hooks accept keyword args and return None.
# Using Callable[..., Any] since hooks may be sync or async.
MiddlewareFunc = Callable[..., Any]


class MiddlewareRegistry:
    """Stores and invokes middleware hooks.

    Usage::

        registry = MiddlewareRegistry()

        @registry.before_step
        async def log_start(workflow_name, step_name, run_id, inputs):
            logging.getLogger("gravtory").info("Starting %s", step_name)

        @registry.after_step
        async def log_end(workflow_name, step_name, run_id, output, duration_ms):
            logging.getLogger("gravtory").info("Completed %s", step_name)

        @registry.on_failure
        async def alert(workflow_name, step_name, run_id, error):
            await notify_ops(error)
    """

    def __init__(self) -> None:
        self._before_hooks: list[Callable[..., Any]] = []
        self._after_hooks: list[Callable[..., Any]] = []
        self._failure_hooks: list[Callable[..., Any]] = []

    # ------------------------------------------------------------------
    # Registration decorators
    # ------------------------------------------------------------------

    def before_step(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Register a function to run BEFORE every step."""
        self._before_hooks.append(func)
        return func

    def after_step(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Register a function to run AFTER every step (success only)."""
        self._after_hooks.append(func)
        return func

    def on_failure(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Register a function to run when a step fails."""
        self._failure_hooks.append(func)
        return func

    # ------------------------------------------------------------------
    # Invocation (called by the engine)
    # ------------------------------------------------------------------

    async def run_before(
        self,
        workflow_name: str,
        step_name: str,
        run_id: str,
        inputs: dict[str, Any],
    ) -> None:
        """Invoke all ``@before_step`` hooks in registration order."""
        for hook in self._before_hooks:
            try:
                await hook(
                    workflow_name=workflow_name,
                    step_name=step_name,
                    run_id=run_id,
                    inputs=inputs,
                )
            except Exception:
                logger.exception(
                    "before_step hook %s failed",
                    hook.__name__,
                )

    async def run_after(
        self,
        workflow_name: str,
        step_name: str,
        run_id: str,
        output: Any,
        duration_ms: float,
    ) -> None:
        """Invoke all ``@after_step`` hooks in registration order."""
        for hook in self._after_hooks:
            try:
                await hook(
                    workflow_name=workflow_name,
                    step_name=step_name,
                    run_id=run_id,
                    output=output,
                    duration_ms=duration_ms,
                )
            except Exception:
                logger.exception(
                    "after_step hook %s failed",
                    hook.__name__,
                )

    async def run_on_failure(
        self,
        workflow_name: str,
        step_name: str,
        run_id: str,
        error: Exception,
    ) -> None:
        """Invoke all ``@on_failure`` hooks in registration order."""
        for hook in self._failure_hooks:
            try:
                await hook(
                    workflow_name=workflow_name,
                    step_name=step_name,
                    run_id=run_id,
                    error=error,
                )
            except Exception:
                logger.exception(
                    "on_failure hook %s failed",
                    hook.__name__,
                )
