"""Property-based tests for cron expression parsing.

Tests that valid cron expressions always produce a valid next-run datetime,
and that the next-run is always in the future relative to the reference time.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from gravtory.scheduling.cron import CronExpression

pytestmark = pytest.mark.property


# ── Strategies ───────────────────────────────────────────────────

minutes = st.sampled_from(["*", "0", "15", "30", "45", "*/5", "*/15"])
hours = st.sampled_from(["*", "0", "6", "12", "18", "*/2", "*/6"])
days = st.sampled_from(["*", "1", "15", "28", "*/5"])
months = st.sampled_from(["*", "1", "6", "12", "*/3"])
weekdays = st.sampled_from(["*", "0", "1-5", "6"])


@st.composite
def cron_expressions(draw: st.DrawFn) -> str:
    """Generate valid cron expression strings."""
    m = draw(minutes)
    h = draw(hours)
    d = draw(days)
    mo = draw(months)
    wd = draw(weekdays)
    return f"{m} {h} {d} {mo} {wd}"


reference_times = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2030, 12, 31),
    timezones=st.just(timezone.utc),
)


# ── Tests ────────────────────────────────────────────────────────


class TestCronNextRun:
    @given(expr=cron_expressions(), ref=reference_times)
    @settings(max_examples=200)
    def test_next_fire_time_is_after_reference(self, expr: str, ref: datetime) -> None:
        """next_fire_time() always returns a time >= reference time."""
        cron = CronExpression(expr)
        nxt = cron.next_fire_time(ref)
        assert nxt >= ref, f"next_fire_time {nxt} is before reference {ref}"

    @given(expr=cron_expressions(), ref=reference_times)
    @settings(max_examples=200)
    def test_next_fire_time_returns_datetime(self, expr: str, ref: datetime) -> None:
        """next_fire_time() always returns a datetime object."""
        cron = CronExpression(expr)
        nxt = cron.next_fire_time(ref)
        assert isinstance(nxt, datetime)

    @given(expr=cron_expressions(), ref=reference_times)
    @settings(max_examples=100)
    def test_next_fire_time_within_reasonable_bound(self, expr: str, ref: datetime) -> None:
        """next_fire_time() is within 400 days of the reference (no infinite loops)."""
        cron = CronExpression(expr)
        nxt = cron.next_fire_time(ref)
        delta = nxt - ref
        assert delta.days <= 400, f"next_fire_time is {delta.days} days in the future"
