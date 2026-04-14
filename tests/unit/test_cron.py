"""Tests for CronExpression parser."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from gravtory.core.errors import ConfigurationError
from gravtory.scheduling.cron import CronExpression


class TestCronParsing:
    def test_every_minute(self) -> None:
        cron = CronExpression("* * * * *")
        after = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt == datetime(2025, 1, 1, 12, 1, 0, tzinfo=timezone.utc)

    def test_every_5_minutes(self) -> None:
        cron = CronExpression("*/5 * * * *")
        after = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.minute % 5 == 0
        assert nxt > after

    def test_specific_time_930(self) -> None:
        cron = CronExpression("30 9 * * *")
        after = datetime(2025, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.hour == 9
        assert nxt.minute == 30

    def test_weekdays_only(self) -> None:
        cron = CronExpression("0 9 * * 1-5")
        # 2025-01-04 is Saturday
        after = datetime(2025, 1, 4, 10, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        # Next weekday is Monday Jan 6
        assert nxt.weekday() < 5  # Monday-Friday
        assert nxt.hour == 9
        assert nxt.minute == 0

    def test_monthly_first(self) -> None:
        cron = CronExpression("0 0 1 * *")
        after = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.day == 1
        assert nxt.month == 2

    def test_range_with_step(self) -> None:
        cron = CronExpression("0 */2 * * *")
        after = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.hour % 2 == 0
        assert nxt.minute == 0

    def test_list_values(self) -> None:
        cron = CronExpression("0 9,12,18 * * *")
        after = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.hour == 12
        assert nxt.minute == 0

    def test_6_field_seconds(self) -> None:
        cron = CronExpression("*/10 * * * * *")
        after = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.second % 10 == 0
        assert nxt > after

    def test_invalid_expression_too_few_fields(self) -> None:
        with pytest.raises(ConfigurationError):
            CronExpression("* * *")

    def test_invalid_expression_bad_value(self) -> None:
        with pytest.raises(ConfigurationError):
            CronExpression("abc * * * *")

    def test_next_fire_time_always_future(self) -> None:
        cron = CronExpression("* * * * *")
        after = datetime(2025, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt > after

    def test_month_boundary(self) -> None:
        cron = CronExpression("0 0 * * *")
        after = datetime(2025, 1, 31, 23, 59, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.month == 2
        assert nxt.day == 1

    def test_year_boundary(self) -> None:
        cron = CronExpression("0 0 1 1 *")
        after = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.year == 2026
        assert nxt.month == 1
        assert nxt.day == 1

    def test_matches_simple(self) -> None:
        cron = CronExpression("30 9 * * *")
        dt = datetime(2025, 1, 1, 9, 30, 0, tzinfo=timezone.utc)
        assert cron.matches(dt)

    def test_matches_no_match(self) -> None:
        cron = CronExpression("30 9 * * *")
        dt = datetime(2025, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        assert not cron.matches(dt)

    def test_half_hour_every_2_hours(self) -> None:
        cron = CronExpression("30 */2 * * *")
        after = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.minute == 30
        assert nxt.hour % 2 == 0


class TestCronGapFill:
    """Gap-fill tests for cron edge cases."""

    def test_str_contains_expression(self) -> None:
        cron = CronExpression("30 9 * * *")
        assert str(cron.expression) == "30 9 * * *"

    def test_next_fire_consecutive(self) -> None:
        """Calling next_fire_time repeatedly produces monotonically increasing times."""
        cron = CronExpression("*/5 * * * *")
        t = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        times = []
        for _ in range(10):
            t = cron.next_fire_time(t)
            times.append(t)
        for i in range(1, len(times)):
            assert times[i] > times[i - 1]

    def test_sunday_dow_7(self) -> None:
        """Day-of-week 0 is Sunday, 7 also valid as Sunday in many cron impls."""
        cron = CronExpression("0 0 * * 0")
        after = datetime(2025, 1, 6, 0, 0, 0, tzinfo=timezone.utc)  # Monday
        nxt = cron.next_fire_time(after)
        assert nxt.weekday() == 6  # Python Sunday=6

    def test_specific_day_of_month(self) -> None:
        """15th of every month at midnight."""
        cron = CronExpression("0 0 15 * *")
        after = datetime(2025, 3, 16, 0, 0, 0, tzinfo=timezone.utc)
        nxt = cron.next_fire_time(after)
        assert nxt.day == 15
        assert nxt.month == 4

    def test_invalid_too_many_fields(self) -> None:
        with pytest.raises(ConfigurationError):
            CronExpression("* * * * * * *")

    def test_matches_with_seconds(self) -> None:
        """6-field cron matches at second precision."""
        cron = CronExpression("30 * * * * *")
        dt = datetime(2025, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
        assert cron.matches(dt)
