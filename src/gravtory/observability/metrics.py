# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Prometheus metrics integration.

Provides :class:`MetricsCollector` exposing counters, histograms, and gauges
for workflow/step execution.  When ``prometheus-client`` is not installed,
all recording methods are silent no-ops.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("gravtory.observability.metrics")

# ---------------------------------------------------------------------------
# Optional prometheus_client import
# ---------------------------------------------------------------------------
try:
    import prometheus_client  # noqa: F401
    from prometheus_client import Counter, Gauge, Histogram
    from prometheus_client import start_http_server as _start_http_server

    _HAS_PROMETHEUS = True
except ImportError:  # pragma: no cover
    _HAS_PROMETHEUS = False


class MetricsCollector:
    """Prometheus metrics for Gravtory.

    When ``prometheus-client`` is installed (``pip install gravtory[prometheus]``),
    creates real metrics and optionally starts an HTTP server.  Otherwise every
    method is a no-op.

    Usage::

        metrics = MetricsCollector(port=9090)
        metrics.record_workflow_started("OrderWorkflow", "default")
        metrics.record_step_completed("OrderWorkflow", "charge", 0.042)
    """

    def __init__(self, port: int | None = None) -> None:
        self._enabled = _HAS_PROMETHEUS

        if not self._enabled:
            logger.debug("prometheus-client not installed — metrics disabled")
            return

        # -- Counters --
        self._workflows_total: Any = Counter(
            "gravtory_workflows_total",
            "Total workflow runs",
            ["workflow_name", "status", "namespace"],
        )
        self._steps_total: Any = Counter(
            "gravtory_steps_total",
            "Total step executions",
            ["workflow_name", "step_name", "status"],
        )
        self._retries_total: Any = Counter(
            "gravtory_retries_total",
            "Total retry attempts",
            ["workflow_name", "step_name"],
        )
        self._compensations_total: Any = Counter(
            "gravtory_compensations_total",
            "Total saga compensations",
            ["workflow_name", "status"],
        )
        self._signals_total: Any = Counter(
            "gravtory_signals_total",
            "Total signals sent/received",
            ["signal_name", "action"],
        )
        self._dlq_total: Any = Counter(
            "gravtory_dlq_total",
            "Total DLQ entries",
            ["workflow_name"],
        )

        # -- Histograms --
        self._step_duration: Any = Histogram(
            "gravtory_step_duration_seconds",
            "Step execution duration",
            ["workflow_name", "step_name"],
        )
        self._workflow_duration: Any = Histogram(
            "gravtory_workflow_duration_seconds",
            "Workflow total duration",
            ["workflow_name"],
        )
        self._checkpoint_size: Any = Histogram(
            "gravtory_checkpoint_size_bytes",
            "Checkpoint payload sizes",
            ["serializer", "compression"],
            buckets=(256, 1024, 4096, 16384, 65536, 262144, 1048576),
        )

        # -- Gauges --
        self._active_workflows: Any = Gauge(
            "gravtory_active_workflows",
            "Currently running workflows",
            ["workflow_name", "namespace"],
        )
        self._active_workers: Any = Gauge(
            "gravtory_active_workers",
            "Currently active workers",
            ["node_id"],
        )
        self._pending_steps: Any = Gauge(
            "gravtory_pending_steps_count",
            "Number of pending steps in queue",
        )
        self._dlq_size: Any = Gauge(
            "gravtory_dlq_size",
            "Current DLQ size",
        )
        self._scheduler_leader: Any = Gauge(
            "gravtory_scheduler_is_leader",
            "Whether this node is the scheduler leader",
            ["node_id"],
        )

        if port is not None:
            _start_http_server(port)
            logger.info("Prometheus metrics server started on port %d", port)

    # ------------------------------------------------------------------
    # Public property
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ------------------------------------------------------------------
    # Recording methods
    # ------------------------------------------------------------------

    def record_workflow_started(
        self,
        workflow_name: str,
        namespace: str = "default",
    ) -> None:
        if not self._enabled:
            return
        self._workflows_total.labels(
            workflow_name=workflow_name,
            status="started",
            namespace=namespace,
        ).inc()
        self._active_workflows.labels(
            workflow_name=workflow_name,
            namespace=namespace,
        ).inc()

    def record_workflow_completed(
        self,
        workflow_name: str,
        namespace: str = "default",
        duration: float = 0.0,
    ) -> None:
        if not self._enabled:
            return
        self._workflows_total.labels(
            workflow_name=workflow_name,
            status="completed",
            namespace=namespace,
        ).inc()
        self._workflow_duration.labels(workflow_name=workflow_name).observe(duration)
        self._active_workflows.labels(
            workflow_name=workflow_name,
            namespace=namespace,
        ).dec()

    def record_workflow_failed(
        self,
        workflow_name: str,
        namespace: str = "default",
    ) -> None:
        if not self._enabled:
            return
        self._workflows_total.labels(
            workflow_name=workflow_name,
            status="failed",
            namespace=namespace,
        ).inc()
        self._active_workflows.labels(
            workflow_name=workflow_name,
            namespace=namespace,
        ).dec()

    def record_step_completed(
        self,
        workflow_name: str,
        step_name: str,
        duration: float = 0.0,
    ) -> None:
        if not self._enabled:
            return
        self._steps_total.labels(
            workflow_name=workflow_name,
            step_name=step_name,
            status="completed",
        ).inc()
        self._step_duration.labels(
            workflow_name=workflow_name,
            step_name=step_name,
        ).observe(duration)

    def record_step_failed(self, workflow_name: str, step_name: str) -> None:
        if not self._enabled:
            return
        self._steps_total.labels(
            workflow_name=workflow_name,
            step_name=step_name,
            status="failed",
        ).inc()

    def record_retry(self, workflow_name: str, step_name: str) -> None:
        if not self._enabled:
            return
        self._retries_total.labels(
            workflow_name=workflow_name,
            step_name=step_name,
        ).inc()

    def record_compensation(self, workflow_name: str, status: str) -> None:
        if not self._enabled:
            return
        self._compensations_total.labels(
            workflow_name=workflow_name,
            status=status,
        ).inc()

    def record_signal(self, signal_name: str, action: str) -> None:
        if not self._enabled:
            return
        self._signals_total.labels(signal_name=signal_name, action=action).inc()

    def record_checkpoint_size(
        self,
        serializer: str,
        compression: str,
        size_bytes: int,
    ) -> None:
        if not self._enabled:
            return
        self._checkpoint_size.labels(
            serializer=serializer,
            compression=compression,
        ).observe(size_bytes)

    def set_active_workflows(
        self,
        workflow_name: str,
        namespace: str,
        count: int,
    ) -> None:
        if not self._enabled:
            return
        self._active_workflows.labels(
            workflow_name=workflow_name,
            namespace=namespace,
        ).set(count)

    def set_pending_steps(self, count: int) -> None:
        if not self._enabled:
            return
        self._pending_steps.set(count)

    def set_dlq_size(self, count: int) -> None:
        if not self._enabled:
            return
        self._dlq_size.set(count)
