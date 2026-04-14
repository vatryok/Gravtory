# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""@step decorator — marks a method/function as a workflow step."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, get_type_hints

from gravtory.core.types import StepDefinition

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import timedelta


def step(
    order: int,
    *,
    name: str | None = None,
    depends_on: int | list[int] | None = None,
    timeout: timedelta | None = None,
    retries: int = 0,
    backoff: str | None = None,
    backoff_base: float = 1.0,
    backoff_max: float = 300.0,
    backoff_multiplier: float = 2.0,
    jitter: bool = False,
    retry_on: list[type[Exception]] | None = None,
    abort_on: list[type[Exception]] | None = None,
    compensate: str | None = None,
    condition: Callable[..., bool] | None = None,
    rate_limit: str | None = None,
    priority: int = 0,
) -> Callable[..., Any]:
    """Decorator that marks a method as a workflow step.

    Usage:
        @step(1)
        async def charge(self, order_id: str) -> dict: ...

        @step(2, depends_on=1, retries=3, backoff="exponential")
        async def ship(self, order_id: str) -> None: ...

    What it does:
      1. Creates a StepDefinition with all provided parameters
      2. Attaches it to the function as __gravtory_step__ attribute
      3. Resolves depends_on to always be a list[int]
      4. Extracts input/output types from function signature
      5. Returns the ORIGINAL function (not a wrapper — no runtime overhead)
    """
    if not isinstance(order, int) or order < 1:
        raise ValueError(f"@step(order) must be a positive integer >= 1, got {order!r}")

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # Normalize depends_on
        deps: list[int] = []
        if depends_on is not None:
            deps = [depends_on] if isinstance(depends_on, int) else list(depends_on)

        # Extract types from function signature
        try:
            hints = get_type_hints(func)
        except Exception:
            hints = {}

        output_type = hints.get("return")
        if output_type is type(None):
            output_type = None

        input_types: dict[str, type] = {}
        sig = inspect.signature(func)
        for param_name, param in sig.parameters.items():
            if param_name == "self":
                continue
            if param.annotation != inspect.Parameter.empty:
                input_types[param_name] = param.annotation

        # Create StepDefinition
        step_def = StepDefinition(
            order=order,
            name=name or func.__name__,
            depends_on=deps,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            backoff_multiplier=backoff_multiplier,
            jitter=jitter,
            retry_on=retry_on or [],
            abort_on=abort_on or [],
            compensate=compensate,
            condition=condition,
            priority=priority,
            rate_limit=rate_limit,
            input_types=input_types,
            output_type=output_type,
            function=func,
        )

        # Pick up signal config from @wait_for_signal (applied before @step)
        signal_cfg = getattr(func, "__gravtory_signal_config__", None)
        if signal_cfg is not None:
            step_def.signal_config = signal_cfg

        # Attach to function (non-invasive)
        func.__gravtory_step__ = step_def  # type: ignore[attr-defined]
        return func

    return decorator
