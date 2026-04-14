# Observability

Gravtory provides built-in observability through OpenTelemetry tracing, Prometheus metrics, structured logging, and a web dashboard.

## OpenTelemetry Tracing

Every workflow execution creates spans for each step:

```python
grav = Gravtory(
    "postgresql://localhost/mydb",
    otel_endpoint="http://jaeger:4317",
)
```

Traces include:
- Workflow execution span (root)
- Individual step spans (children)
- Retry attempt spans
- Compensation spans (for sagas)

Each span carries attributes: `workflow.name`, `workflow.run_id`, `step.order`, `step.name`, `step.status`.

## Prometheus Metrics

```python
grav = Gravtory(
    "postgresql://localhost/mydb",
    metrics_port=9090,  # Expose /metrics endpoint
)
```

Available metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `gravtory_workflow_total` | Counter | Total workflow executions |
| `gravtory_workflow_duration_seconds` | Histogram | Execution duration |
| `gravtory_step_total` | Counter | Total step executions |
| `gravtory_step_duration_seconds` | Histogram | Step duration |
| `gravtory_retry_total` | Counter | Retry attempts |
| `gravtory_dlq_entries` | Gauge | Current DLQ size |
| `gravtory_active_workers` | Gauge | Active worker count |

## Structured Logging

Gravtory emits structured logs for all operations:

```python
grav = Gravtory(
    "postgresql://localhost/mydb",
    log_level="INFO",
)
```

Log events include workflow start/complete/fail, step execution, retries, compensations, and scheduler actions.

## Built-in Dashboard

```python
grav = Gravtory(
    "postgresql://localhost/mydb",
    dashboard=True,       # Starts web UI on port 7777
    dashboard_port=7777,
)
```

The dashboard provides:
- **Workflow list** — filter by status, search by ID
- **Workflow detail** — step-by-step execution timeline
- **DLQ viewer** — inspect and retry failed workflows
- **Schedule viewer** — see upcoming scheduled runs
- **Worker status** — active workers and their current tasks
- **Signal sender** — send signals to waiting workflows

## Alerting

Configure alerts for workflow failures:

```python
from gravtory.observability.alerts import AlertManager, SlackAlert

alerts = AlertManager()
alerts.add(SlackAlert(
    webhook_url="https://hooks.slack.com/...",
    on=["workflow.failed", "dlq.threshold"],
))

grav = Gravtory("postgresql://localhost/mydb", alerts=alerts)
```

## Introspection API

Programmatically inspect workflow state:

```python
state = await grav.inspect("order-ord_123")
print(state.status)              # "completed"
print(state.steps[1].output)     # {"charge_id": "ch_xyz"}
print(state.steps[1].duration_ms)  # 142
print(state.steps[1].started_at)
print(state.steps[1].completed_at)
```
