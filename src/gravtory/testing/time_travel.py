# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Time-travel utilities for testing schedules and timeouts.

Provides :class:`TimeTraveler` — a context manager that overrides the
``gravtory.testing.time_travel.now()`` function so that Gravtory components
can be tested without waiting real time.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

_original_now: datetime | None = None
_override: datetime | None = None


def now() -> datetime:
    """Return the current UTC time, respecting any active :class:`TimeTraveler`."""
    if _override is not None:
        return _override
    return datetime.now(tz=timezone.utc)


class TimeTraveler:
    """Context manager that overrides the perceived current time.

    Usage::

        async with TimeTraveler(start=datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc)):
            # now() returns 2025-01-01 09:00 UTC
            tt.advance(hours=1)
            # now() returns 2025-01-01 10:00 UTC

    Can also be used as a sync context manager.
    """

    def __init__(self, start: datetime | None = None) -> None:
        self._start = start or datetime.now(tz=timezone.utc)
        self._current = self._start

    # -- Sync context manager --

    def __enter__(self) -> TimeTraveler:
        global _override
        _override = self._current
        return self

    def __exit__(self, *args: Any) -> None:
        global _override
        _override = None

    # -- Async context manager --

    async def __aenter__(self) -> TimeTraveler:
        return self.__enter__()

    async def __aexit__(self, *args: Any) -> None:
        self.__exit__()

    # -- Time manipulation --

    def advance(self, **kwargs: Any) -> datetime:
        """Advance time by :class:`timedelta` keyword args.

        Returns the new current time.
        """
        global _override
        self._current += timedelta(**kwargs)
        _override = self._current
        return self._current

    def set(self, dt: datetime) -> None:
        """Set time to a specific :class:`datetime`."""
        global _override
        self._current = dt
        _override = self._current

    @property
    def now(self) -> datetime:
        """Return the currently overridden time."""
        return self._current
