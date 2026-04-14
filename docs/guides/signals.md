# Signals

Signals allow external systems to send data to running workflows. This enables **human-in-the-loop** patterns, approval gates, and event-driven workflow progression.

## How Signals Work

1. A workflow step declares it's waiting for a signal using `@wait_for_signal`
2. The workflow pauses at that step
3. An external system (API, CLI, dashboard) sends a signal with data
4. The workflow resumes with the signal data as input

## Defining a Signal Step

```python
from gravtory import Gravtory, step
from gravtory.decorators.signal import wait_for_signal
from datetime import timedelta

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="expense-{id}")
class ExpenseWorkflow:

    @step(1)
    async def submit(self, id: str, amount: float) -> dict:
        await slack.send(f"Approve expense #{id} (${amount})?")
        return {"id": id, "amount": amount}

    @step(2, depends_on=1)
    @wait_for_signal("approval", timeout=timedelta(days=7))
    async def await_approval(self, signal: dict) -> bool:
        return signal["approved"]

    @step(3, depends_on=2, condition=lambda ctx: ctx.output(2))
    async def reimburse(self, id: str) -> None:
        await accounting.pay(id)
```

## Sending Signals

### From Python Code

```python
await grav.signal("expense-42", "approval", {"approved": True})
```

### From the CLI

```bash
gravtory signal expense-42 approval '{"approved": true}'
```

### From the Dashboard

The dashboard provides a UI for sending signals to waiting workflows.

## Signal Timeout

If a signal isn't received within the timeout period, the step raises a `SignalTimeoutError`. You can handle this with retry or condition logic:

```python
@step(2, depends_on=1)
@wait_for_signal("approval", timeout=timedelta(hours=24))
async def await_approval(self, signal: dict) -> bool:
    return signal.get("approved", False)
```

## Signal Transport

Gravtory supports different signal delivery mechanisms depending on your backend:

| Backend | Transport | Latency |
|---------|-----------|---------|
| PostgreSQL | `LISTEN/NOTIFY` | ~10ms |
| SQLite | Polling | ~1s |
| MySQL | Polling | ~1s |
| MongoDB | Change Streams | ~100ms |
| Redis | Pub/Sub | ~5ms |

## Use Cases

- **Approval gates** — human review before proceeding
- **External events** — webhook triggers, payment confirmations
- **Manual intervention** — operator sends retry/skip signal
- **Data enrichment** — external service provides additional data
