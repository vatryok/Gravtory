"""Gap-fill tests for scheduling.cron — CronExpression edge cases."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from gravtory.core.errors import ConfigurationError
from gravtory.scheduling.cron import CronExpression, _parse_field


class TestParseField:
    def test_wildcard(self) -> None:
        assert _parse_field("*", 0, 59) == set(range(0, 60))

    def test_single_value(self) -> None:
        assert _parse_field("5", 0, 59) == {5}

    def test_range(self) -> None:
        assert _parse_field("1-5", 0, 59) == {1, 2, 3, 4, 5}

    def test_step(self) -> None:
        assert _parse_field("*/15", 0, 59) == {0, 15, 30, 45}

    def test_range_with_step(self) -> None:
        assert _parse_field("1-10/3", 0, 59) == {1, 4, 7, 10}

    def test_list(self) -> None:
        assert _parse_field("1,5,10", 0, 59) == {1, 5, 10}

    def test_single_value_with_step(self) -> None:
        result = _parse_field("5/10", 0, 59)
        assert 5 in result
        assert 15 in result

    def test_empty_part_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="Empty"):
            _parse_field(",", 0, 59)

    def test_invalid_step_value(self) -> None:
        with pytest.raises(ConfigurationError, match="Invalid step"):
            _parse_field("*/abc", 0, 59)

    def test_zero_step_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="positive"):
            _parse_field("*/0", 0, 59)

    def test_invalid_range(self) -> None:
        with pytest.raises(ConfigurationError, match="Invalid range"):
            _parse_field("abc-def", 0, 59)

    def test_reversed_range_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="Invalid range"):
            _parse_field("10-5", 0, 59)

    def test_invalid_value(self) -> None:
        with pytest.raises(ConfigurationError, match="Invalid value"):
            _parse_field("abc", 0, 59)

    def test_out_of_range_value(self) -> None:
        with pytest.raises(ConfigurationError, match="out of range"):
            _parse_field("60", 0, 59)


class TestCronExpression5Field:
    def test_every_minute(self) -> None:
        c = CronExpression("* * * * *")
        assert c.expression == "* * * * *"
        dt = datetime(2025, 1, 1, 12, 30, tzinfo=timezone.utc)
        assert c.matches(dt) is True

    def test_specific_time(self) -> None:
        c = CronExpression("30 12 * * *")
        assert c.matches(datetime(2025, 1, 1, 12, 30, tzinfo=timezone.utc)) is True
        assert c.matches(datetime(2025, 1, 1, 12, 31, tzinfo=timezone.utc)) is False

    def test_specific_day_of_week(self) -> None:
        # 1 = Monday in cron (Sunday=0)
        c = CronExpression("0 9 * * 1")
        # 2025-01-06 is a Monday
        assert c.matches(datetime(2025, 1, 6, 9, 0, tzinfo=timezone.utc)) is True
        assert c.matches(datetime(2025, 1, 7, 9, 0, tzinfo=timezone.utc)) is False

    def test_specific_dom(self) -> None:
        c = CronExpression("0 0 15 * *")
        assert c.matches(datetime(2025, 3, 15, 0, 0, tzinfo=timezone.utc)) is True
        assert c.matches(datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)) is False

    def test_both_dom_and_dow(self) -> None:
        # dom=15 OR dow=1 (Monday)
        c = CronExpression("0 0 15 * 1")
        assert c.matches(datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)) is True
        # 2025-01-06 is Monday
        assert c.matches(datetime(2025, 1, 6, 0, 0, tzinfo=timezone.utc)) is True

    def test_next_fire_time(self) -> None:
        c = CronExpression("0 12 * * *")
        after = datetime(2025, 1, 1, 11, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.hour == 12
        assert nxt.minute == 0

    def test_next_fire_time_wraps_day(self) -> None:
        c = CronExpression("0 8 * * *")
        after = datetime(2025, 1, 1, 20, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        # Next 8:00 after 20:00 on Jan 1 is Jan 2
        assert nxt > after
        assert nxt.hour == 8
        assert nxt.minute == 0

    def test_next_fire_time_wraps_month(self) -> None:
        c = CronExpression("0 0 1 * *")
        after = datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.month == 2
        assert nxt.day == 1

    def test_next_fire_time_naive_datetime(self) -> None:
        c = CronExpression("0 12 * * *")
        after = datetime(2025, 1, 1, 11, 0)  # naive
        nxt = c.next_fire_time(after)
        assert nxt.tzinfo is not None

    def test_invalid_field_count(self) -> None:
        with pytest.raises(ConfigurationError, match="5 or 6 fields"):
            CronExpression("* * *")

    def test_next_fire_wraps_year(self) -> None:
        c = CronExpression("0 0 1 1 *")  # Jan 1st only
        after = datetime(2025, 1, 2, 0, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.year == 2026
        assert nxt.month == 1
        assert nxt.day == 1


class TestCronExpression6Field:
    def test_every_second(self) -> None:
        c = CronExpression("* * * * * *")
        dt = datetime(2025, 1, 1, 12, 30, 45, tzinfo=timezone.utc)
        assert c.matches(dt) is True

    def test_specific_second(self) -> None:
        c = CronExpression("30 0 12 * * *")
        assert c.matches(datetime(2025, 1, 1, 12, 0, 30, tzinfo=timezone.utc)) is True
        assert c.matches(datetime(2025, 1, 1, 12, 0, 31, tzinfo=timezone.utc)) is False

    def test_next_fire_time_6field(self) -> None:
        c = CronExpression("0 0 12 * * *")
        after = datetime(2025, 1, 1, 11, 59, 59, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.hour == 12
        assert nxt.minute == 0
        assert nxt.second == 0

    def test_specific_month_6field(self) -> None:
        c = CronExpression("0 0 0 1 6 *")  # June 1st midnight
        assert c.matches(datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)) is True
        assert c.matches(datetime(2025, 7, 1, 0, 0, 0, tzinfo=timezone.utc)) is False

    def test_both_dom_and_dow_6field(self) -> None:
        c = CronExpression("0 0 0 15 * 1")  # 15th OR Monday
        assert c.matches(datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)) is True


class TestCronNextInSet:
    def test_basic(self) -> None:
        assert CronExpression._next_in_set(5, {1, 3, 5, 7}) == 5

    def test_next_larger(self) -> None:
        assert CronExpression._next_in_set(4, {1, 3, 5, 7}) == 5

    def test_none_when_all_smaller(self) -> None:
        assert CronExpression._next_in_set(10, {1, 3, 5, 7}) is None
