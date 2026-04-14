# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Cron expression parser — 5-field and extended 6-field support.

5-field: minute hour day-of-month month day-of-week
6-field: second minute hour day-of-month month day-of-week

Syntax:
  *         every value
  N         specific value
  N-M       range (inclusive)
  N-M/S     range with step
  */S       every S values
  N,M,O     list of values
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from gravtory.core.errors import ConfigurationError

# Field boundaries: (min_val, max_val)
_FIELD_BOUNDS_5 = [
    (0, 59),  # minute
    (0, 23),  # hour
    (1, 31),  # day-of-month
    (1, 12),  # month
    (0, 6),  # day-of-week (0=Sunday)
]

_FIELD_BOUNDS_6 = [
    (0, 59),  # second
    (0, 59),  # minute
    (0, 23),  # hour
    (1, 31),  # day-of-month
    (1, 12),  # month
    (0, 6),  # day-of-week (0=Sunday)
]


def _parse_field(token: str, lo: int, hi: int) -> set[int]:
    """Parse a single cron field token into a set of valid integers."""
    result: set[int] = set()

    for part in token.split(","):
        part = part.strip()
        if not part:
            raise ConfigurationError(f"Empty cron field part in '{token}'")

        if "/" in part:
            range_part, step_str = part.split("/", 1)
            try:
                step = int(step_str)
            except ValueError as err:
                raise ConfigurationError(
                    f"Invalid step value '{step_str}' in cron field '{token}'"
                ) from err
            if step <= 0:
                raise ConfigurationError(f"Step must be positive, got {step} in '{token}'")

            if range_part == "*":
                start, end = lo, hi
            elif "-" in range_part:
                a, b = range_part.split("-", 1)
                start, end = int(a), int(b)
            else:
                start = int(range_part)
                end = hi

            for v in range(start, end + 1, step):
                if lo <= v <= hi:
                    result.add(v)

        elif part == "*":
            result.update(range(lo, hi + 1))

        elif "-" in part:
            a, b = part.split("-", 1)
            try:
                start, end = int(a), int(b)
            except ValueError as err:
                raise ConfigurationError(f"Invalid range '{part}' in cron field '{token}'") from err
            if start > end:
                raise ConfigurationError(f"Invalid range {start}-{end} in cron field '{token}'")
            for v in range(start, end + 1):
                if lo <= v <= hi:
                    result.add(v)

        else:
            try:
                val = int(part)
            except ValueError as err:
                raise ConfigurationError(f"Invalid value '{part}' in cron field '{token}'") from err
            if val < lo or val > hi:
                raise ConfigurationError(
                    f"Value {val} out of range [{lo}-{hi}] in cron field '{token}'"
                )
            result.add(val)

    if not result:
        raise ConfigurationError(f"Cron field '{token}' produced no valid values")

    return result


class CronExpression:
    """Parsed cron expression with next-fire-time computation."""

    def __init__(self, expression: str, tz: str | ZoneInfo | None = None) -> None:
        self._expression = expression
        self._is_6_field = False
        self._fields = self._parse(expression)
        self._tz: ZoneInfo | None = ZoneInfo(tz) if isinstance(tz, str) else tz

    @property
    def expression(self) -> str:
        return self._expression

    def _parse(self, expr: str) -> list[set[int]]:
        """Parse cron expression into list of sets of valid values per field."""
        parts = expr.strip().split()
        if len(parts) == 5:
            bounds = _FIELD_BOUNDS_5
            self._is_6_field = False
        elif len(parts) == 6:
            bounds = _FIELD_BOUNDS_6
            self._is_6_field = True
        else:
            raise ConfigurationError(
                f"Cron expression must have 5 or 6 fields, got {len(parts)}: '{expr}'"
            )

        fields: list[set[int]] = []
        for token, (lo, hi) in zip(parts, bounds, strict=True):
            fields.append(_parse_field(token, lo, hi))
        return fields

    def matches(self, dt: datetime) -> bool:
        """Check if a datetime matches this cron expression."""
        return self._matches_internal(dt)

    def next_fire_time(self, after: datetime) -> datetime:
        """Compute the next datetime matching this cron expression after the given time.

        When a timezone is configured, matching is performed in local time
        so that DST transitions are handled correctly. The returned datetime
        is always timezone-aware.

        Raises ConfigurationError if no valid time found within 4 years.
        """
        if after.tzinfo is None:
            after = after.replace(tzinfo=timezone.utc)

        # Convert to local time for matching when timezone is set
        if self._tz is not None:
            after = after.astimezone(self._tz)

        if self._is_6_field:
            candidate = after + timedelta(seconds=1)
            candidate = candidate.replace(microsecond=0)
        else:
            candidate = after + timedelta(minutes=1)
            candidate = candidate.replace(second=0, microsecond=0)

        max_dt = after + timedelta(days=366 * 4)

        while candidate <= max_dt:
            if self._matches_internal(candidate):
                return candidate

            # Advance to next potential match
            candidate = self._advance(candidate)

        raise ConfigurationError(
            f"No valid fire time found within 4 years for cron expression '{self._expression}'"
        )

    def _matches_internal(self, dt: datetime) -> bool:
        """Internal match check using the parsed fields."""
        cron_dow = (dt.weekday() + 1) % 7  # Convert to cron weekday

        if self._is_6_field:
            sec, minute, hour, dom, month = (
                dt.second,
                dt.minute,
                dt.hour,
                dt.day,
                dt.month,
            )
            if month not in self._fields[4]:
                return False
            if hour not in self._fields[2]:
                return False
            if minute not in self._fields[1]:
                return False
            if sec not in self._fields[0]:
                return False
            # Day check
            dow_all = self._fields[5] == set(range(0, 7))
            dom_all = self._fields[3] == set(range(1, 32))
            if dow_all and dom_all:
                return True
            if not dow_all and not dom_all:
                return dom in self._fields[3] or cron_dow in self._fields[5]
            if not dom_all:
                return dom in self._fields[3]
            return cron_dow in self._fields[5]
        else:
            minute, hour, dom, month = dt.minute, dt.hour, dt.day, dt.month
            if month not in self._fields[3]:
                return False
            if hour not in self._fields[1]:
                return False
            if minute not in self._fields[0]:
                return False
            # Day check
            dow_all = self._fields[4] == set(range(0, 7))
            dom_all = self._fields[2] == set(range(1, 32))
            if dow_all and dom_all:
                return True
            if not dow_all and not dom_all:
                return dom in self._fields[2] or cron_dow in self._fields[4]
            if not dom_all:
                return dom in self._fields[2]
            return cron_dow in self._fields[4]

    def _advance(self, dt: datetime) -> datetime:
        """Advance datetime to the next potential candidate.

        Uses field-aware skipping for efficiency.
        """
        if self._is_6_field:
            # Check month
            if dt.month not in self._fields[4]:
                nxt = self._next_in_set(dt.month, self._fields[4])
                if nxt is not None and nxt > dt.month:
                    return dt.replace(month=nxt, day=1, hour=0, minute=0, second=0)
                return dt.replace(
                    year=dt.year + 1, month=min(self._fields[4]), day=1, hour=0, minute=0, second=0
                )
            # Check day
            if not self._day_matches(dt):
                return self._next_matching_day(dt)
            # Check hour
            if dt.hour not in self._fields[2]:
                nxt = self._next_in_set(dt.hour, self._fields[2])
                if nxt is not None and nxt > dt.hour:
                    return dt.replace(
                        hour=nxt, minute=min(self._fields[1]), second=min(self._fields[0])
                    )
                return self._next_matching_day(dt + timedelta(days=1))
            # Check minute
            if dt.minute not in self._fields[1]:
                nxt = self._next_in_set(dt.minute, self._fields[1])
                if nxt is not None and nxt > dt.minute:
                    return dt.replace(minute=nxt, second=min(self._fields[0]))
                nxt_h = self._next_in_set(dt.hour + 1, self._fields[2])
                if nxt_h is not None:
                    return dt.replace(
                        hour=nxt_h, minute=min(self._fields[1]), second=min(self._fields[0])
                    )
                return self._next_matching_day(dt + timedelta(days=1))
            # Check second
            nxt_s = self._next_in_set(dt.second + 1, self._fields[0])
            if nxt_s is not None:
                return dt.replace(second=nxt_s)
            nxt_m = self._next_in_set(dt.minute + 1, self._fields[1])
            if nxt_m is not None:
                return dt.replace(minute=nxt_m, second=min(self._fields[0]))
            nxt_h = self._next_in_set(dt.hour + 1, self._fields[2])
            if nxt_h is not None:
                return dt.replace(
                    hour=nxt_h, minute=min(self._fields[1]), second=min(self._fields[0])
                )
            return self._next_matching_day(dt + timedelta(days=1))
        else:
            # 5-field: no seconds
            if dt.month not in self._fields[3]:
                nxt = self._next_in_set(dt.month, self._fields[3])
                if nxt is not None and nxt > dt.month:
                    return dt.replace(month=nxt, day=1, hour=0, minute=0, second=0)
                return dt.replace(
                    year=dt.year + 1, month=min(self._fields[3]), day=1, hour=0, minute=0, second=0
                )
            if not self._day_matches(dt):
                return self._next_matching_day(dt)
            if dt.hour not in self._fields[1]:
                nxt = self._next_in_set(dt.hour, self._fields[1])
                if nxt is not None and nxt > dt.hour:
                    return dt.replace(hour=nxt, minute=min(self._fields[0]), second=0)
                return self._next_matching_day(dt + timedelta(days=1))
            nxt_m = self._next_in_set(dt.minute + 1, self._fields[0])
            if nxt_m is not None:
                return dt.replace(minute=nxt_m, second=0)
            nxt_h = self._next_in_set(dt.hour + 1, self._fields[1])
            if nxt_h is not None:
                return dt.replace(hour=nxt_h, minute=min(self._fields[0]), second=0)
            return self._next_matching_day(dt + timedelta(days=1))

    def _day_matches(self, dt: datetime) -> bool:
        """Check if the day of the given dt matches the day fields."""
        cron_dow = (dt.weekday() + 1) % 7
        if self._is_6_field:
            dow_all = self._fields[5] == set(range(0, 7))
            dom_all = self._fields[3] == set(range(1, 32))
            dom_set, dow_set = self._fields[3], self._fields[5]
        else:
            dow_all = self._fields[4] == set(range(0, 7))
            dom_all = self._fields[2] == set(range(1, 32))
            dom_set, dow_set = self._fields[2], self._fields[4]

        if dow_all and dom_all:
            return True
        if not dow_all and not dom_all:
            return dt.day in dom_set or cron_dow in dow_set
        if not dom_all:
            return dt.day in dom_set
        return cron_dow in dow_set

    def _next_matching_day(self, dt: datetime) -> datetime:
        """Advance to the start of the next day that matches day constraints."""
        if self._is_6_field:
            candidate = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            if candidate <= dt:
                candidate += timedelta(days=1)
            month_field = self._fields[4]
        else:
            candidate = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            if candidate <= dt:
                candidate += timedelta(days=1)
            month_field = self._fields[3]

        max_dt = dt + timedelta(days=366 * 4)
        while candidate <= max_dt:
            if candidate.month in month_field and self._day_matches(candidate):
                # Set to first valid hour/minute/second
                if self._is_6_field:
                    return candidate.replace(
                        hour=min(self._fields[2]),
                        minute=min(self._fields[1]),
                        second=min(self._fields[0]),
                    )
                return candidate.replace(
                    hour=min(self._fields[1]),
                    minute=min(self._fields[0]),
                    second=0,
                )
            # Skip to next month if current month doesn't match
            if candidate.month not in month_field:
                nxt = self._next_in_set(candidate.month, month_field)
                if nxt is not None and nxt > candidate.month:
                    candidate = candidate.replace(month=nxt, day=1)
                else:
                    candidate = candidate.replace(
                        year=candidate.year + 1, month=min(month_field), day=1
                    )
                continue
            candidate += timedelta(days=1)

        raise ConfigurationError(
            f"No valid fire time found within 4 years for cron '{self._expression}'"
        )

    @staticmethod
    def _next_in_set(val: int, valid: set[int]) -> int | None:
        """Find the smallest value in the set >= val, or None."""
        for v in sorted(valid):
            if v >= val:
                return v
        return None
