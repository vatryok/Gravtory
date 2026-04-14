# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Mock utilities for testing Gravtory workflows.

Provides :class:`MockStep`, :class:`FailNTimes`, and :class:`DelayedMock`
for replacing step functions during tests.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class MockStep:
    """Step mock with call tracking.

    Usage::

        mock = MockStep(return_value={"id": "123"})
        runner.mock_step("charge", side_effect=mock)

        # After test:
        assert mock.call_count == 1
        assert mock.last_call_args == {"order_id": "abc"}
    """

    return_value: Any = None
    side_effect: Callable[..., Any] | None = None
    raises: type[Exception] | None = None
    call_count: int = field(default=0, init=False)
    call_history: list[dict[str, Any]] = field(default_factory=list, init=False)
    last_call_args: dict[str, Any] | None = field(default=None, init=False)

    async def __call__(self, **kwargs: Any) -> Any:
        self.call_count += 1
        self.call_history.append(kwargs)
        self.last_call_args = kwargs
        if self.raises is not None:
            raise self.raises()
        if self.side_effect is not None:
            if asyncio.iscoroutinefunction(self.side_effect):
                return await self.side_effect(**kwargs)
            return self.side_effect(**kwargs)
        return self.return_value

    def reset(self) -> None:
        """Reset call tracking."""
        self.call_count = 0
        self.call_history.clear()
        self.last_call_args = None


class FailNTimes:
    """Mock that fails *failures* times then returns *success_value*.

    Usage::

        runner.mock_step("flaky", side_effect=FailNTimes(
            failures=2, exception=ConnectionError, success_value="ok"
        ))
    """

    def __init__(
        self,
        failures: int,
        exception: type[Exception] = RuntimeError,
        success_value: Any = None,
    ) -> None:
        self._failures = failures
        self._exception = exception
        self._success_value = success_value
        self._attempt = 0

    async def __call__(self, **kwargs: Any) -> Any:
        self._attempt += 1
        if self._attempt <= self._failures:
            raise self._exception(f"Simulated failure #{self._attempt}")
        return self._success_value

    @property
    def attempt(self) -> int:
        """Number of attempts made so far."""
        return self._attempt

    def reset(self) -> None:
        """Reset attempt counter."""
        self._attempt = 0


class DelayedMock:
    """Mock that simulates slow execution.

    Usage::

        runner.mock_step("slow_api", side_effect=DelayedMock(
            delay=0.5, return_value="done"
        ))
    """

    def __init__(
        self,
        delay: float,
        return_value: Any = None,
    ) -> None:
        self._delay = delay
        self._return_value = return_value

    async def __call__(self, **kwargs: Any) -> Any:
        await asyncio.sleep(self._delay)
        return self._return_value
