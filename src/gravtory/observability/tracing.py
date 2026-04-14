# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""OpenTelemetry tracing integration.

Provides :class:`TracingProvider` for distributed tracing of workflow and
step execution.  When ``opentelemetry-api`` / ``opentelemetry-sdk`` are not
installed, all operations degrade to silent no-ops so users are never
forced to install OTel just to run Gravtory.
"""

from __future__ import annotations

import contextlib
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

logger = logging.getLogger("gravtory.observability.tracing")

# ---------------------------------------------------------------------------
# Optional OTel imports — everything falls back to no-ops if missing
# ---------------------------------------------------------------------------
try:
    import opentelemetry  # noqa: F401
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider as _SDKTracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.trace import SpanKind as _SpanKind
    from opentelemetry.trace import StatusCode, Tracer

    _HAS_OTEL = True
except ImportError:  # pragma: no cover
    _HAS_OTEL = False


# ---------------------------------------------------------------------------
# No-op fallbacks
# ---------------------------------------------------------------------------
class _NoOpSpan:
    """Minimal stand-in for ``opentelemetry.trace.Span``."""

    def set_attribute(self, key: str, value: object) -> None:
        pass

    def set_status(self, *args: object, **kwargs: object) -> None:
        pass

    def record_exception(self, exception: BaseException) -> None:
        pass


class TracingProvider:
    """Distributed tracing for Gravtory workflows.

    When the ``otel`` extra is installed (``pip install gravtory[otel]``),
    creates real OpenTelemetry spans.  Otherwise every method is a no-op.

    Usage::

        tracing = TracingProvider(service_name="my-app")

        with tracing.workflow_span("OrderWorkflow", "run-123") as ws:
            with tracing.step_span("OrderWorkflow", "run-123", "charge", 1) as ss:
                ...
    """

    def __init__(
        self,
        service_name: str = "gravtory",
        endpoint: str | None = None,
        console_export: bool = False,
    ) -> None:
        self._service_name = service_name
        self._tracer: Any = None  # Real Tracer or None

        if not _HAS_OTEL:
            logger.debug("OpenTelemetry not installed — tracing disabled")
            return

        resource = Resource.create({"service.name": service_name})
        provider = _SDKTracerProvider(resource=resource)

        if endpoint is not None:
            # OTLP exporter — import lazily so grpc/http deps are optional
            with contextlib.suppress(ImportError):
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                    OTLPSpanExporter,
                )

                provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))

        if console_export:
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

        trace.set_tracer_provider(provider)
        self._tracer = trace.get_tracer("gravtory", schema_url=None)
        logger.info("OpenTelemetry tracing initialised (service=%s)", service_name)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        """Whether real OTel tracing is active."""
        return self._tracer is not None

    def get_tracer(self) -> Any:
        """Return the underlying ``Tracer`` (or *None* if OTel is absent)."""
        return self._tracer

    # ------------------------------------------------------------------
    # Context managers
    # ------------------------------------------------------------------

    @contextmanager
    def workflow_span(
        self,
        workflow_name: str,
        workflow_run_id: str,
    ) -> Generator[Any, None, None]:
        """Create a parent span for an entire workflow execution."""
        if self._tracer is None:
            yield _NoOpSpan()
            return

        tracer: Tracer = self._tracer
        with tracer.start_as_current_span(
            workflow_name,
            kind=_SpanKind.INTERNAL,
        ) as span:
            span.set_attribute("gravtory.workflow.name", workflow_name)
            span.set_attribute("gravtory.workflow.run_id", workflow_run_id)
            try:
                yield span
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                span.record_exception(exc)
                raise

    @contextmanager
    def step_span(
        self,
        workflow_name: str,
        workflow_run_id: str,
        step_name: str,
        step_order: int,
        retry_count: int = 0,
    ) -> Generator[Any, None, None]:
        """Create a span for a single step execution.

        Attributes set on the span:
          - ``gravtory.workflow.name``
          - ``gravtory.workflow.run_id``
          - ``gravtory.step.name``
          - ``gravtory.step.order``
          - ``gravtory.step.retry``
        """
        if self._tracer is None:
            yield _NoOpSpan()
            return

        tracer: Tracer = self._tracer
        with tracer.start_as_current_span(
            f"{workflow_name}/{step_name}",
            kind=_SpanKind.INTERNAL,
        ) as span:
            span.set_attribute("gravtory.workflow.name", workflow_name)
            span.set_attribute("gravtory.workflow.run_id", workflow_run_id)
            span.set_attribute("gravtory.step.name", step_name)
            span.set_attribute("gravtory.step.order", step_order)
            span.set_attribute("gravtory.step.retry", retry_count)
            try:
                yield span
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                span.record_exception(exc)
                raise
