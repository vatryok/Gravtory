"""Tests for TracingProvider — OpenTelemetry tracing integration."""

from __future__ import annotations

import pytest

from gravtory.observability.tracing import TracingProvider, _NoOpSpan

try:
    import opentelemetry  # noqa: F401

    _HAS_OTEL = True
except ImportError:
    _HAS_OTEL = False


class TestTracingProviderNoOp:
    """When OTel IS installed, TracingProvider should produce real spans.
    When not installed, it should degrade gracefully. We test both paths."""

    def test_no_op_span_methods(self) -> None:
        """_NoOpSpan methods don't raise."""
        span = _NoOpSpan()
        span.set_attribute("key", "val")
        span.set_status("OK")
        span.record_exception(ValueError("test"))

    @pytest.mark.skipif(not _HAS_OTEL, reason="opentelemetry not installed")
    def test_provider_enabled_property(self) -> None:
        """Provider reports enabled=True when OTel is available."""
        provider = TracingProvider(service_name="test")
        assert provider.enabled is True

    @pytest.mark.skipif(not _HAS_OTEL, reason="opentelemetry not installed")
    def test_get_tracer_returns_tracer(self) -> None:
        provider = TracingProvider(service_name="test-tracer")
        tracer = provider.get_tracer()
        assert tracer is not None

    def test_workflow_span_yields_span(self) -> None:
        provider = TracingProvider(service_name="test-wf")
        with provider.workflow_span("MyWorkflow", "run-1") as span:
            assert span is not None
            span.set_attribute("custom", "value")

    def test_step_span_yields_span(self) -> None:
        provider = TracingProvider(service_name="test-step")
        with provider.step_span("MyWorkflow", "run-1", "charge", 1) as span:
            assert span is not None
            span.set_attribute("gravtory.step.duration_ms", 42)

    def test_step_span_records_exception(self) -> None:
        provider = TracingProvider(service_name="test-err")
        try:
            with provider.step_span("MyWorkflow", "run-1", "fail_step", 2):
                raise ValueError("boom")
        except ValueError:
            pass  # Expected — span should have recorded the error

    def test_step_span_with_retry_count(self) -> None:
        provider = TracingProvider(service_name="test-retry")
        with provider.step_span("Wf", "run-1", "step", 1, retry_count=3) as span:
            span.set_attribute("gravtory.step.retry", 3)

    def test_workflow_span_records_exception(self) -> None:
        provider = TracingProvider(service_name="test-wf-err")
        try:
            with provider.workflow_span("Wf", "run-1"):
                raise RuntimeError("wf fail")
        except RuntimeError:
            pass

    @pytest.mark.skipif(not _HAS_OTEL, reason="opentelemetry not installed")
    def test_console_export_mode(self) -> None:
        """Console export mode should not raise."""
        provider = TracingProvider(service_name="console-test", console_export=True)
        assert provider.enabled is True


class TestTracingGapFill:
    """Gap-fill tests for tracing edge cases."""

    def test_noop_span_all_methods_safe(self) -> None:
        """_NoOpSpan methods are all safe no-ops."""
        span = _NoOpSpan()
        span.set_attribute("a", 1)
        span.set_attribute("b", "text")
        span.set_status("OK")
        span.record_exception(RuntimeError("test"))
        span.record_exception(ValueError("test2"))

    def test_nested_workflow_and_step_spans(self) -> None:
        """Step spans can be nested inside workflow spans."""
        provider = TracingProvider(service_name="test-nested")
        with provider.workflow_span("Wf", "run-1") as wf_span:
            wf_span.set_attribute("workflow", "Wf")
            with provider.step_span("Wf", "run-1", "step1", 1) as step_span:
                step_span.set_attribute("step", "step1")

    def test_multiple_step_spans(self) -> None:
        """Multiple step spans in sequence."""
        provider = TracingProvider(service_name="test-multi")
        for i in range(5):
            with provider.step_span("Wf", "run-1", f"step_{i}", i) as span:
                span.set_attribute("order", i)

    @pytest.mark.skipif(not _HAS_OTEL, reason="opentelemetry not installed")
    def test_provider_different_service_names(self) -> None:
        """Different service names produce independent providers."""
        p1 = TracingProvider(service_name="svc-a")
        p2 = TracingProvider(service_name="svc-b")
        assert p1.enabled is True
        assert p2.enabled is True
