"""Tests for scheduling.scheduler re-export module."""

from __future__ import annotations

from gravtory.scheduling.engine import Scheduler as EngineScheduler
from gravtory.scheduling.scheduler import Scheduler


class TestSchedulerReExport:
    def test_scheduler_is_same_class(self) -> None:
        assert Scheduler is EngineScheduler

    def test_all_exports(self) -> None:
        from gravtory.scheduling import scheduler

        assert "Scheduler" in scheduler.__all__
