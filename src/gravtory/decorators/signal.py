# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""@wait_for_signal decorator — makes a step wait for an external signal."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from gravtory.core.types import SignalConfig

if TYPE_CHECKING:
    from collections.abc import Callable


def wait_for_signal(
    signal_name: str,
    timeout: timedelta = timedelta(days=7),
) -> Callable[..., Any]:
    """Decorator that makes a step wait for an external signal before executing.

    Applied **below** ``@step`` (closer to the function)::

        @step(2, depends_on=1)
        @wait_for_signal("approval", timeout=timedelta(hours=24))
        async def wait_for_approval(self, signal_data: dict) -> bool:
            return signal_data["approved"]

    Python applies decorators bottom-up, so ``@wait_for_signal`` runs
    first and stores a ``__gravtory_signal_config__`` attribute on the
    function. When ``@step`` runs next, it detects this attribute and
    copies the config into the :class:`StepDefinition`.

    At execution time, the engine calls ``signal_handler.wait()``
    before invoking the step function, and injects the received data
    as the ``signal_data`` keyword argument.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        config = SignalConfig(name=signal_name, timeout=timeout)

        # If @step already ran (stacking order reversed), apply directly
        step_def = getattr(func, "__gravtory_step__", None)
        if step_def is not None:
            step_def.signal_config = config
        else:
            # Store for @step to pick up later (normal stacking order)
            func.__gravtory_signal_config__ = config  # type: ignore[attr-defined]

        return func

    return decorator
