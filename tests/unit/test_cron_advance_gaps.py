"""Tests for scheduling.cron — _advance, _next_matching_day, _day_matches deep paths."""

from __future__ import annotations

from datetime import datetime, timezone

from gravtory.scheduling.cron import CronExpression


class TestAdvance5Field:
    """Exercise _advance for 5-field expressions."""

    def test_advance_skips_wrong_month(self) -> None:
        # "0 0 1 3 *" = midnight on March 1st only
        c = CronExpression("0 0 1 3 *")
        after = datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.month == 3
        assert nxt.day == 1

    def test_advance_wraps_year_for_month(self) -> None:
        # "0 0 1 2 *" = midnight Feb 1st
        c = CronExpression("0 0 1 2 *")
        after = datetime(2025, 3, 1, 0, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.year == 2026
        assert nxt.month == 2

    def test_advance_skips_wrong_day(self) -> None:
        # "0 12 15 * *" = noon on 15th of every month
        c = CronExpression("0 12 15 * *")
        after = datetime(2025, 1, 16, 0, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.month == 2
        assert nxt.day == 15
        assert nxt.hour == 12

    def test_advance_skips_wrong_hour(self) -> None:
        # "30 8 * * *" = 8:30 every day
        c = CronExpression("30 8 * * *")
        after = datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.hour == 8
        assert nxt.minute == 30
        assert nxt > after

    def test_advance_next_hour(self) -> None:
        # "0 8,12,18 * * *" = at 8:00, 12:00, 18:00
        c = CronExpression("0 8,12,18 * * *")
        after = datetime(2025, 1, 1, 8, 30, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.hour == 12
        assert nxt.minute == 0

    def test_advance_next_minute(self) -> None:
        # "15,45 * * * *" = at :15 and :45
        c = CronExpression("15,45 * * * *")
        after = datetime(2025, 1, 1, 10, 20, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.hour == 10
        assert nxt.minute == 45

    def test_day_of_week_constraint(self) -> None:
        # "0 9 * * 1" = 9am on Mondays (cron DOW: 1=Monday)
        c = CronExpression("0 9 * * 1")
        after = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)  # Wednesday
        nxt = c.next_fire_time(after)
        assert nxt.weekday() == 0  # Python Monday=0
        assert nxt.hour == 9

    def test_both_dom_and_dow_constraint(self) -> None:
        # "0 0 15 * 5" = midnight on 15th OR Fridays (cron DOW: 5=Friday)
        c = CronExpression("0 0 15 * 5")
        after = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)  # Wednesday
        nxt = c.next_fire_time(after)
        # Should be either Jan 3 (Friday) or Jan 15, whichever is first
        assert nxt.day == 3 or nxt.day == 15


class TestAdvance6Field:
    """Exercise _advance for 6-field (second-granularity) expressions."""

    def test_6field_basic(self) -> None:
        # "30 0 12 * * *" = 12:00:30 every day
        c = CronExpression("30 0 12 * * *")
        after = datetime(2025, 1, 1, 12, 0, 29, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.second == 30
        assert nxt.hour == 12

    def test_6field_skip_month(self) -> None:
        # "0 0 0 1 6 *" = midnight June 1st
        c = CronExpression("0 0 0 1 6 *")
        after = datetime(2025, 7, 1, 0, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.year == 2026
        assert nxt.month == 6

    def test_6field_skip_hour(self) -> None:
        # "0 0 10,14 * * *" = at 10:00:00 and 14:00:00
        c = CronExpression("0 0 10,14 * * *")
        after = datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.hour == 14

    def test_6field_skip_minute(self) -> None:
        # "0 30 12 * * *" = 12:30:00
        c = CronExpression("0 30 12 * * *")
        after = datetime(2025, 1, 1, 12, 15, 0, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.minute == 30

    def test_6field_advance_second(self) -> None:
        # "15,45 * * * * *" = at :15 and :45 seconds
        c = CronExpression("15,45 * * * * *")
        after = datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc)
        nxt = c.next_fire_time(after)
        assert nxt.second == 45

    def test_6field_day_of_week(self) -> None:
        # "0 0 9 * * 1" = 9:00 on Mondays
        c = CronExpression("0 0 9 * * 1")
        after = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)  # Wednesday
        nxt = c.next_fire_time(after)
        assert nxt.weekday() == 0  # Monday


class TestDayMatches:
    def test_both_wildcard(self) -> None:
        c = CronExpression("* * * * *")
        dt = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert c._day_matches(dt) is True

    def test_dom_only(self) -> None:
        c = CronExpression("0 0 15 * *")
        dt15 = datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)
        dt16 = datetime(2025, 1, 16, 0, 0, tzinfo=timezone.utc)
        assert c._day_matches(dt15) is True
        assert c._day_matches(dt16) is False

    def test_dow_only(self) -> None:
        # "0 0 * * 1" = Mondays
        c = CronExpression("0 0 * * 1")
        monday = datetime(2025, 1, 6, 0, 0, tzinfo=timezone.utc)  # Monday
        tuesday = datetime(2025, 1, 7, 0, 0, tzinfo=timezone.utc)
        assert c._day_matches(monday) is True
        assert c._day_matches(tuesday) is False


class TestNextMatchingDay:
    def test_next_matching_day_advances(self) -> None:
        c = CronExpression("0 0 15 * *")
        dt = datetime(2025, 1, 16, 12, 0, tzinfo=timezone.utc)
        nxt = c._next_matching_day(dt)
        assert nxt.day == 15
        assert nxt.month == 2

    def test_next_matching_day_skips_month(self) -> None:
        # "0 0 1 6 *" = June 1st
        c = CronExpression("0 0 1 6 *")
        dt = datetime(2025, 7, 15, 0, 0, tzinfo=timezone.utc)
        nxt = c._next_matching_day(dt)
        assert nxt.year == 2026
        assert nxt.month == 6
        assert nxt.day == 1


class TestNextInSet:
    def test_found(self) -> None:
        assert CronExpression._next_in_set(5, {1, 3, 5, 7}) == 5

    def test_found_larger(self) -> None:
        assert CronExpression._next_in_set(4, {1, 3, 5, 7}) == 5

    def test_not_found(self) -> None:
        assert CronExpression._next_in_set(8, {1, 3, 5, 7}) is None
