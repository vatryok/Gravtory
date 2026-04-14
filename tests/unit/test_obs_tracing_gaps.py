"""Tests for observability.tracing — TracingProvider no-op paths."""

from __future__ import annotations

from gravtory.observability.tracing import TracingProvider, _NoOpSpan


class TestNoOpSpan:
    def test_set_attribute(self) -> None:
        span = _NoOpSpan()
        span.set_attribute("key", "value")

    def test_set_status(self) -> None:
        span = _NoOpSpan()
        span.set_status("OK")

    def test_record_exception(self) -> None:
        span = _NoOpSpan()
        span.record_exception(ValueError("test"))


class TestTracingProvider:
    def test_enabled_with_otel(self) -> None:
        tp = TracingProvider(service_name="test-svc", console_export=True)
        assert tp.enabled is True

    def test_get_tracer(self) -> None:
        tp = TracingProvider(service_name="test-svc2")
        tracer = tp.get_tracer()
        assert tracer is not None

    def test_workflow_span(self) -> None:
        tp = TracingProvider(service_name="test-svc3")
        with tp.workflow_span("MyWorkflow", "run-123") as span:
            span.set_attribute("test", "value")

    def test_step_span(self) -> None:
        tp = TracingProvider(service_name="test-svc4")
        with tp.step_span("MyWorkflow", "run-123", "charge", 1, retry_count=2) as span:
            span.set_attribute("test", "value")

    def test_workflow_span_exception(self) -> None:
        import pytest

        tp = TracingProvider(service_name="test-svc5")
        with pytest.raises(ValueError, match="boom"):
            with tp.workflow_span("MyWorkflow", "run-err") as span:
                raise ValueError("boom")

    def test_step_span_exception(self) -> None:
        import pytest

        tp = TracingProvider(service_name="test-svc6")
        with pytest.raises(RuntimeError, match="step failed"):
            with tp.step_span("MyWorkflow", "run-err", "step1", 1) as span:
                raise RuntimeError("step failed")


class TestTracingProviderDisabled:
    def test_disabled_workflow_span(self) -> None:
        tp = TracingProvider(service_name="disabled-svc")
        tp._tracer = None  # force disabled
        assert tp.enabled is False
        with tp.workflow_span("WF", "run-1") as span:
            assert isinstance(span, _NoOpSpan)

    def test_disabled_step_span(self) -> None:
        tp = TracingProvider(service_name="disabled-svc2")
        tp._tracer = None
        with tp.step_span("WF", "run-1", "step", 1) as span:
            assert isinstance(span, _NoOpSpan)
