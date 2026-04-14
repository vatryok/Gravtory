"""Tests for @schedule decorator."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gravtory.core.errors import ConfigurationError
from gravtory.core.types import ScheduleType
from gravtory.decorators.schedule import schedule
from gravtory.decorators.step import step
from gravtory.decorators.workflow import WorkflowProxy, workflow


class TestScheduleDecorator:
    def test_cron_schedule(self) -> None:
        @schedule(cron="0 9 * * 1-5")
        @workflow(id="daily-{date}")
        class DailyReport:
            @step(1)
            async def run(self) -> str:
                return "ok"

        assert isinstance(DailyReport, WorkflowProxy)
        assert hasattr(DailyReport, "_schedule")
        sched = DailyReport._schedule
        assert sched.schedule_type == ScheduleType.CRON
        assert sched.schedule_config == "0 9 * * 1-5"

    def test_interval_timedelta(self) -> None:
        @schedule(interval=timedelta(minutes=5))
        @workflow(id="interval-{ts}")
        class IntervalWf:
            @step(1)
            async def run(self) -> str:
                return "ok"

        sched = IntervalWf._schedule  # type: ignore[attr-defined]
        assert sched.schedule_type == ScheduleType.INTERVAL
        assert sched.schedule_config == "300.0"

    def test_interval_float(self) -> None:
        @schedule(interval=120.0)
        @workflow(id="interval-float-{ts}")
        class IntervalFloatWf:
            @step(1)
            async def run(self) -> str:
                return "ok"

        sched = IntervalFloatWf._schedule  # type: ignore[attr-defined]
        assert sched.schedule_type == ScheduleType.INTERVAL
        assert sched.schedule_config == "120.0"

    def test_every_string(self) -> None:
        @schedule(every="30m")
        @workflow(id="every-{ts}")
        class EveryWf:
            @step(1)
            async def run(self) -> str:
                return "ok"

        sched = EveryWf._schedule  # type: ignore[attr-defined]
        assert sched.schedule_type == ScheduleType.INTERVAL
        assert sched.schedule_config == "1800.0"

    def test_event_trigger(self) -> None:
        @schedule(on_event="order_completed")
        @workflow(id="event-{ts}")
        class EventWf:
            @step(1)
            async def run(self) -> str:
                return "ok"

        sched = EventWf._schedule  # type: ignore[attr-defined]
        assert sched.schedule_type == ScheduleType.EVENT
        assert sched.schedule_config == "order_completed"

    def test_after_workflow(self) -> None:
        @schedule(after="ParentWorkflow")
        @workflow(id="after-{ts}")
        class AfterWf:
            @step(1)
            async def run(self) -> str:
                return "ok"

        sched = AfterWf._schedule  # type: ignore[attr-defined]
        assert sched.schedule_type == ScheduleType.EVENT
        assert sched.schedule_config == "workflow:ParentWorkflow"

    def test_one_time_at(self) -> None:
        target = datetime(2030, 6, 15, 9, 0, 0, tzinfo=timezone.utc)

        @schedule(at=target)
        @workflow(id="onetime-{ts}")
        class OneTimeWf:
            @step(1)
            async def run(self) -> str:
                return "ok"

        sched = OneTimeWf._schedule  # type: ignore[attr-defined]
        assert sched.schedule_type == ScheduleType.ONE_TIME
        assert target.isoformat() in sched.schedule_config

    def test_no_params_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="@schedule requires"):

            @schedule()
            @workflow(id="bad-{ts}")
            class BadWf:
                @step(1)
                async def run(self) -> str:
                    return "ok"

    def test_multiple_params_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="only one"):

            @schedule(cron="* * * * *", every="5m")
            @workflow(id="bad2-{ts}")
            class BadWf2:
                @step(1)
                async def run(self) -> str:
                    return "ok"

    def test_schedule_on_bare_class(self) -> None:
        @schedule(cron="0 0 * * *")
        class BareClass:
            pass

        assert hasattr(BareClass, "__gravtory_schedule__")
        meta = BareClass.__gravtory_schedule__
        assert meta["type"] == ScheduleType.CRON
        assert meta["config"] == "0 0 * * *"
        assert meta["enabled"] is True


class TestScheduleDecoratorGapFill:
    """Gap-fill tests for @schedule decorator edge cases."""

    def test_disabled_schedule(self) -> None:
        @schedule(cron="0 0 * * *", enabled=False)
        @workflow(id="disabled-{ts}")
        class DisabledWf:
            @step(1)
            async def run(self) -> str:
                return "ok"

        sched = DisabledWf._schedule
        assert sched.enabled is False

    def test_every_hours(self) -> None:
        @schedule(every="2h")
        @workflow(id="hourly-{ts}")
        class HourlyWf:
            @step(1)
            async def run(self) -> str:
                return "ok"

        sched = HourlyWf._schedule
        assert sched.schedule_type == ScheduleType.INTERVAL
        assert sched.schedule_config == "7200.0"
