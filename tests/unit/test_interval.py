"""Tests for IntervalSchedule and parse_interval."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gravtory.core.errors import ConfigurationError
from gravtory.scheduling.interval import IntervalSchedule, parse_interval


class TestParseInterval:
    def test_seconds(self) -> None:
        assert parse_interval("30s") == timedelta(seconds=30)

    def test_minutes(self) -> None:
        assert parse_interval("5m") == timedelta(minutes=5)

    def test_hours(self) -> None:
        assert parse_interval("2h") == timedelta(hours=2)

    def test_days(self) -> None:
        assert parse_interval("1d") == timedelta(days=1)

    def test_fractional(self) -> None:
        assert parse_interval("1.5h") == timedelta(hours=1.5)

    def test_invalid_string(self) -> None:
        with pytest.raises(ConfigurationError):
            parse_interval("abc")

    def test_invalid_unit(self) -> None:
        with pytest.raises(ConfigurationError):
            parse_interval("5w")


class TestIntervalSchedule:
    def test_from_seconds(self) -> None:
        sched = IntervalSchedule(seconds=60)
        assert sched.total_seconds == 60.0

    def test_from_timedelta(self) -> None:
        sched = IntervalSchedule(interval=timedelta(minutes=5))
        assert sched.total_seconds == 300.0

    def test_next_fire_time(self) -> None:
        sched = IntervalSchedule(seconds=3600)
        after = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        nxt = sched.next_fire_time(after)
        assert nxt == datetime(2025, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

    def test_very_short_interval(self) -> None:
        sched = IntervalSchedule(seconds=1)
        after = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        nxt = sched.next_fire_time(after)
        assert nxt == datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)

    def test_very_long_interval(self) -> None:
        sched = IntervalSchedule(seconds=86400)
        after = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        nxt = sched.next_fire_time(after)
        assert nxt == datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    def test_no_params_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            IntervalSchedule()

    def test_negative_interval_raises(self) -> None:
        with pytest.raises(ConfigurationError):
            IntervalSchedule(seconds=-5)


class TestIntervalGapFill:
    """Gap-fill tests for interval schedule edge cases."""

    def test_parse_interval_zero_raises(self) -> None:
        """Zero-value interval is rejected as non-positive."""
        with pytest.raises(ConfigurationError):
            parse_interval("0s")

    def test_parse_interval_large_value(self) -> None:
        assert parse_interval("365d") == timedelta(days=365)

    def test_interval_schedule_from_parse(self) -> None:
        """IntervalSchedule can be constructed from parsed timedelta."""
        td = parse_interval("10m")
        sched = IntervalSchedule(interval=td)
        assert sched.total_seconds == 600.0

    def test_next_fire_consecutive(self) -> None:
        """Consecutive fire times are spaced by the interval."""
        sched = IntervalSchedule(seconds=60)
        t = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        t1 = sched.next_fire_time(t)
        t2 = sched.next_fire_time(t1)
        assert (t2 - t1).total_seconds() == 60.0
