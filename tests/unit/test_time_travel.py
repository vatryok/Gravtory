"""Tests for TimeTraveler (Section 11.3)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from gravtory.testing.time_travel import TimeTraveler, now


class TestTimeTraveler:
    def test_override_time(self) -> None:
        target = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        with TimeTraveler(start=target) as tt:
            assert now() == target
            assert tt.now == target
        # After exit, now() should return real time (not the override)
        assert now() != target

    def test_advance_time(self) -> None:
        target = datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
        with TimeTraveler(start=target) as tt:
            new_time = tt.advance(hours=2, minutes=30)
            assert new_time == datetime(2025, 1, 1, 11, 30, 0, tzinfo=timezone.utc)
            assert now() == new_time

    def test_set_time(self) -> None:
        with TimeTraveler() as tt:
            new_dt = datetime(2030, 12, 25, 0, 0, 0, tzinfo=timezone.utc)
            tt.set(new_dt)
            assert now() == new_dt
            assert tt.now == new_dt

    def test_restore_original_time(self) -> None:
        before = now()
        target = datetime(2000, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        with TimeTraveler(start=target):
            assert now() == target
        after = now()
        # After exiting context, now() should be close to real time (not 2000)
        assert after.year >= before.year

    @pytest.mark.asyncio
    async def test_async_context_manager(self) -> None:
        target = datetime(2025, 3, 1, 8, 0, 0, tzinfo=timezone.utc)
        async with TimeTraveler(start=target) as tt:
            assert now() == target
            tt.advance(days=1)
            assert now().day == 2
        # Restored
        assert now().year >= 2024


class TestTimeTravelGapFill:
    """Gap-fill tests for time travel edge cases."""

    def test_advance_hours(self) -> None:
        target = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        with TimeTraveler(start=target) as tt:
            tt.advance(hours=5)
            assert now().hour == 5

    def test_advance_negative(self) -> None:
        target = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        with TimeTraveler(start=target) as tt:
            tt.advance(hours=-2)
            assert now().hour == 10

    def test_multiple_advances(self) -> None:
        target = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        with TimeTraveler(start=target) as tt:
            tt.advance(minutes=30)
            tt.advance(minutes=30)
            assert now().hour == 1
            assert now().minute == 0
