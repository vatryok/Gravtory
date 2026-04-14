"""Tests for MetricsCollector — Prometheus metrics integration."""

from __future__ import annotations

from gravtory.observability.metrics import _HAS_PROMETHEUS, MetricsCollector

# Module-level singleton — avoids prometheus duplicate registration errors.
_collector: MetricsCollector | None = None


def _get_collector() -> MetricsCollector:
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
    return _collector


class TestMetricsCollectorNoOp:
    """All recording methods are safe no-ops when prometheus is absent."""

    def test_no_op_without_prometheus(self) -> None:
        collector = _get_collector()
        # Should never raise, regardless of whether prometheus is installed
        collector.record_workflow_started("Order", "default")
        collector.record_workflow_completed("Order", "default", duration=1.5)
        collector.record_workflow_failed("Order", "default")
        collector.record_step_completed("Order", "charge", duration=0.042)
        collector.record_step_failed("Order", "charge")
        collector.record_retry("Order", "charge")
        collector.record_compensation("Order", "completed")
        collector.record_signal("approval", "sent")
        collector.record_checkpoint_size("json", "none", 1024)
        collector.set_active_workflows("Order", "default", 5)
        collector.set_pending_steps(10)
        collector.set_dlq_size(3)

    def test_enabled_matches_availability(self) -> None:
        collector = _get_collector()
        assert collector.enabled is _HAS_PROMETHEUS

    def test_workflow_counter_increments(self) -> None:
        collector = _get_collector()
        collector.record_workflow_started("Order", "default")
        collector.record_workflow_completed("Order", "default", duration=1.5)

    def test_step_duration_histogram(self) -> None:
        collector = _get_collector()
        collector.record_step_completed("Order", "charge", duration=0.042)
        collector.record_step_completed("Order", "charge", duration=0.15)

    def test_step_failed_counter(self) -> None:
        collector = _get_collector()
        collector.record_step_failed("Order", "charge")

    def test_retry_counter(self) -> None:
        collector = _get_collector()
        collector.record_retry("Order", "charge")

    def test_compensation_counter(self) -> None:
        collector = _get_collector()
        collector.record_compensation("Order", "completed")

    def test_signal_counter(self) -> None:
        collector = _get_collector()
        collector.record_signal("approval", "sent")
        collector.record_signal("approval", "received")

    def test_checkpoint_size_histogram(self) -> None:
        collector = _get_collector()
        collector.record_checkpoint_size("json", "none", 1024)

    def test_active_workflows_gauge(self) -> None:
        collector = _get_collector()
        collector.set_active_workflows("Order", "default", 5)

    def test_pending_steps_gauge(self) -> None:
        collector = _get_collector()
        collector.set_pending_steps(10)

    def test_dlq_size_gauge(self) -> None:
        collector = _get_collector()
        collector.set_dlq_size(3)

    def test_workflow_failed_counter(self) -> None:
        collector = _get_collector()
        collector.record_workflow_started("Order", "default")
        collector.record_workflow_failed("Order", "default")


class TestMetricsGapFill:
    """Gap-fill tests for metrics collector edge cases."""

    def test_multiple_workflow_names(self) -> None:
        """Recording metrics for different workflows doesn't raise."""
        c = _get_collector()
        for name in ("OrderWf", "ShipWf", "RefundWf"):
            c.record_workflow_started(name, "default")
            c.record_workflow_completed(name, "default", duration=1.0)

    def test_zero_duration(self) -> None:
        c = _get_collector()
        c.record_workflow_completed("Wf", "default", duration=0.0)
        c.record_step_completed("Wf", "s", duration=0.0)

    def test_very_large_values(self) -> None:
        c = _get_collector()
        c.set_active_workflows("Wf", "default", 1_000_000)
        c.set_pending_steps(999_999)
        c.set_dlq_size(500_000)
        c.record_checkpoint_size("json", "gzip", 100_000_000)
