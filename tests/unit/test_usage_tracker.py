"""Tests for UsageTracker — LLM usage recording and reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gravtory.ai.tokens import LLMUsage, UsageTracker


class TestUsageTracker:
    """UsageTracker unit tests."""

    @pytest.fixture()
    def tracker(self) -> UsageTracker:
        return UsageTracker()

    @pytest.mark.asyncio()
    async def test_record_usage(self, tracker: UsageTracker) -> None:
        """record() stores a usage entry."""
        usage = LLMUsage(
            model="gpt-4",
            input_tokens=100,
            output_tokens=50,
            cost_usd=0.005,
            step_name="analyze",
            workflow_run_id="run-1",
        )
        await tracker.record(usage)
        report = await tracker.get_workflow_usage("run-1")
        assert report["calls"] == 1
        assert report["total_input_tokens"] == 100
        assert report["total_output_tokens"] == 50

    @pytest.mark.asyncio()
    async def test_workflow_usage_report(self, tracker: UsageTracker) -> None:
        """get_workflow_usage aggregates by step and model."""
        await tracker.record(
            LLMUsage(
                model="gpt-4",
                input_tokens=100,
                output_tokens=50,
                cost_usd=0.01,
                step_name="step-a",
                workflow_run_id="run-1",
            )
        )
        await tracker.record(
            LLMUsage(
                model="gpt-3.5-turbo",
                input_tokens=200,
                output_tokens=100,
                cost_usd=0.002,
                step_name="step-b",
                workflow_run_id="run-1",
            )
        )

        report = await tracker.get_workflow_usage("run-1")
        assert report["calls"] == 2
        assert report["total_input_tokens"] == 300
        assert report["total_output_tokens"] == 150
        assert report["total_cost_usd"] == 0.012
        assert "step-a" in report["by_step"]
        assert "step-b" in report["by_step"]
        assert "gpt-4" in report["by_model"]
        assert "gpt-3.5-turbo" in report["by_model"]

    @pytest.mark.asyncio()
    async def test_daily_usage(self, tracker: UsageTracker) -> None:
        """get_daily_usage filters by calendar day."""
        now = datetime.now(tz=timezone.utc)
        await tracker.record(
            LLMUsage(
                model="gpt-4",
                input_tokens=500,
                output_tokens=200,
                cost_usd=0.05,
                step_name="s1",
                workflow_run_id="r1",
                timestamp=now,
            )
        )

        report = await tracker.get_daily_usage(now.date())
        assert report["calls"] == 1
        assert report["total_input_tokens"] == 500

        # Different day should be empty
        yesterday = (now - timedelta(days=1)).date()
        report2 = await tracker.get_daily_usage(yesterday)
        assert report2["calls"] == 0

    @pytest.mark.asyncio()
    async def test_cost_report(self, tracker: UsageTracker) -> None:
        """get_cost_report generates a time-range cost report."""
        now = datetime.now(tz=timezone.utc)
        await tracker.record(
            LLMUsage(
                model="gpt-4",
                input_tokens=1000,
                output_tokens=500,
                cost_usd=0.10,
                step_name="s1",
                workflow_run_id="r1",
                timestamp=now,
            )
        )
        await tracker.record(
            LLMUsage(
                model="gpt-3.5-turbo",
                input_tokens=2000,
                output_tokens=1000,
                cost_usd=0.02,
                step_name="s2",
                workflow_run_id="r2",
                timestamp=now,
            )
        )

        report = await tracker.get_cost_report(
            since=now - timedelta(hours=1),
            until=now + timedelta(hours=1),
        )
        assert report["calls"] == 2
        assert report["total_cost_usd"] == 0.12
        assert "gpt-4" in report["by_model"]

    @pytest.mark.asyncio()
    async def test_empty_workflow_report(self, tracker: UsageTracker) -> None:
        """get_workflow_usage for unknown run returns zeros."""
        report = await tracker.get_workflow_usage("nonexistent")
        assert report["calls"] == 0
        assert report["total_input_tokens"] == 0
        assert report["total_cost_usd"] == 0.0


class TestUsageTrackerGapFill:
    """Gap-fill tests for usage tracker edge cases."""

    @pytest.mark.asyncio()
    async def test_record_many_usages(self) -> None:
        tracker = UsageTracker()
        now = datetime.now(tz=timezone.utc)
        for i in range(20):
            await tracker.record(
                LLMUsage(
                    workflow_run_id=f"run-{i % 3}",
                    step_name="s1",
                    model="gpt-4",
                    input_tokens=100,
                    output_tokens=50,
                    cost_usd=0.01,
                    timestamp=now,
                )
            )
        report = await tracker.get_cost_report(
            since=now - timedelta(seconds=5),
            until=now + timedelta(seconds=5),
        )
        assert report["calls"] == 20

    @pytest.mark.asyncio()
    async def test_cost_report_time_filter(self) -> None:
        tracker = UsageTracker()
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        recent = datetime.now(tz=timezone.utc)
        await tracker.record(
            LLMUsage(
                workflow_run_id="r1",
                step_name="s1",
                model="gpt-4",
                input_tokens=100,
                output_tokens=50,
                cost_usd=0.05,
                timestamp=old,
            )
        )
        await tracker.record(
            LLMUsage(
                workflow_run_id="r2",
                step_name="s1",
                model="gpt-4",
                input_tokens=100,
                output_tokens=50,
                cost_usd=0.05,
                timestamp=recent,
            )
        )
        report = await tracker.get_cost_report(
            since=recent - timedelta(seconds=5),
            until=recent + timedelta(seconds=5),
        )
        assert report["calls"] == 1
