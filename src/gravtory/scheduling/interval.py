# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Interval scheduling — run every N seconds/minutes/hours/days."""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from gravtory.core.errors import ConfigurationError

_UNIT_MAP: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
}

_INTERVAL_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhd])$", re.IGNORECASE)


def parse_interval(s: str) -> timedelta:
    """Parse a human-readable interval string into a timedelta.

    Supported formats:
      "30s"  → 30 seconds
      "5m"   → 5 minutes
      "2h"   → 2 hours
      "1d"   → 1 day
      "1.5h" → 1 hour 30 minutes

    Raises:
        ConfigurationError: If the string cannot be parsed.
    """
    match = _INTERVAL_RE.match(s.strip())
    if not match:
        raise ConfigurationError(
            f"Invalid interval string '{s}'. "
            f"Expected format: '<number><unit>' where unit is s/m/h/d. "
            f"Examples: '30s', '5m', '2h', '1d'"
        )
    value = float(match.group(1))
    unit = match.group(2).lower()
    seconds = value * _UNIT_MAP[unit]
    if seconds <= 0:
        raise ConfigurationError(f"Interval must be positive, got {seconds}s from '{s}'")
    return timedelta(seconds=seconds)


class IntervalSchedule:
    """Simple fixed-interval schedule."""

    def __init__(self, seconds: float | None = None, interval: timedelta | None = None) -> None:
        if interval is not None:
            self._interval = interval
        elif seconds is not None:
            self._interval = timedelta(seconds=seconds)
        else:
            raise ConfigurationError("IntervalSchedule requires 'seconds' or 'interval'")

        if self._interval.total_seconds() <= 0:
            raise ConfigurationError(
                f"Interval must be positive, got {self._interval.total_seconds()}s"
            )

    @property
    def interval(self) -> timedelta:
        return self._interval

    @property
    def total_seconds(self) -> float:
        return self._interval.total_seconds()

    def next_fire_time(self, after: datetime) -> datetime:
        """Compute the next fire time after the given datetime."""
        return after + self._interval
