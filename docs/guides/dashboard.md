# Dashboard

Gravtory includes a built-in web dashboard for monitoring workflows, inspecting state, and managing operations — no separate UI deployment needed.

## Starting the Dashboard

```python
grav = Gravtory(
    "postgresql://localhost/mydb",
    dashboard=True,
    dashboard_port=7777,
)
await grav.start()
# Dashboard available at http://localhost:7777
```

Or via CLI:

```bash
gravtory dashboard --port 7777
```

## Views

### Workflow List

- Filter by status: pending, running, completed, failed, compensated
- Search by workflow ID or name
- Sort by creation time, duration, or status
- Pagination for large result sets

### Workflow Detail

- Step-by-step execution timeline with durations
- Step inputs and outputs (expandable JSON)
- Error messages and stack traces for failed steps
- Retry history and compensation records

### Dead Letter Queue

- List all DLQ entries with failure reason
- One-click retry for individual entries
- Bulk retry and purge operations

### Schedules

- List all registered schedules
- Next fire time for each schedule
- Enable/disable schedules
- Trigger a schedule manually

### Workers

- Active worker count and status
- Current tasks being executed
- Worker heartbeat timestamps
- Node distribution (for multi-machine setups)

### Signals

- List workflows waiting for signals
- Send signals directly from the UI
- Signal history and delivery status

## Authentication

```python
grav = Gravtory(
    "postgresql://localhost/mydb",
    dashboard=True,
    dashboard_auth_token="your-secret-token",
)
```

Access the dashboard with the token in the `Authorization` header or as a query parameter.

## REST API

The dashboard exposes a REST API for programmatic access:

```bash
# List workflow runs
curl http://localhost:7777/api/runs?status=failed

# Inspect a run
curl http://localhost:7777/api/runs/order-ord_123

# Send a signal
curl -X POST http://localhost:7777/api/signals \
  -H "Content-Type: application/json" \
  -d '{"run_id": "expense-42", "name": "approval", "data": {"approved": true}}'
```
