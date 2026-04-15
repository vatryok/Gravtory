<div align="center">

# Gravtory

**Temporal-level power. Zero infrastructure. Just your database.**

The Python library for crash-proof workflows, distributed execution, sagas, scheduling, and observability — with no separate server, no message broker, no Redis.

[![PyPI](https://img.shields.io/pypi/v/gravtory.svg)](https://pypi.org/project/gravtory/)
[![Python](https://img.shields.io/pypi/pyversions/gravtory.svg)](https://pypi.org/project/gravtory/)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![Tests](https://github.com/vatryok/gravtory/actions/workflows/ci.yml/badge.svg)](https://github.com/vatryok/gravtory/actions)
[![Coverage](https://img.shields.io/codecov/c/github/vatryok/gravtory)](https://codecov.io/gh/vatryok/gravtory)

[Quick Start](#quick-start) · [Examples](examples/)

</div>

---

## Why Gravtory?

Every production app eventually needs **workflows that don't break**. A payment that must complete. An order that must ship. A pipeline that must finish. When your process crashes between steps, you need guarantees.

Today, you either:
- Use **Celery** and accept that tasks can be lost
- Deploy **Temporal** (2-day setup, 3 services, brutal learning curve)
- Pay for **Prefect Cloud** or manage an Airflow cluster
- Build it yourself with retry loops and prayer

**Gravtory gives you a third option**: Temporal-level reliability as a Python library, using the database you already have.

```python
pip install gravtory[postgres]
```

That's the infrastructure.

---

## Quick Start

### 1. Define a workflow

```python
from gravtory import Gravtory, workflow, step

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="order-{order_id}")
class OrderWorkflow:

    @step(1)
    async def charge_card(self, order_id: str) -> dict:
        return await stripe.charge(order_id)

    @step(2, depends_on=1)
    async def reserve_inventory(self, order_id: str) -> dict:
        return await inventory.reserve(order_id)

    @step(3, depends_on=2)
    async def send_notification(self, order_id: str) -> None:
        await email.send(order_id)
```

### 2. Run it

```python
await grav.start()
result = await grav.run(OrderWorkflow, order_id="ord_abc123")
```

### 3. It survives anything

```
First run:     step 1 [OK] -> step 2 [OK] -> step 3 [OK]    (completes normally)
Crash at step 2: step 1 [OK] -> step 2 [CRASH]              (process dies)
Auto-resume:   step 1 [SKIP] -> step 2 [OK] -> step 3 [OK]    (step 1 NOT re-executed)
```

**Step 1 (charge card) is NEVER re-executed.** Its output was atomically checkpointed to your database. On restart, Gravtory loads the checkpoint and continues from where it left off.

---

## Features

<table>
<tr><td>

### Core
- **Crash-safe execution** — steps checkpointed atomically
- **Exact resume** — restart from the precise failed step
- **Idempotency** — no step ever runs twice
- **5 backends** — PostgreSQL, SQLite, MySQL, MongoDB, Redis
- **Zero infrastructure** — uses YOUR database

</td><td>

### Patterns
- **Saga compensation** — automatic rollback on failure
- **Parallel fan-out/in** — process 1000 items concurrently
- **Conditional branching** — if/else in workflows
- **Sub-workflows** — composable, nested workflows
- **Circuit breaker** — protect external services

</td></tr>
<tr><td>

### Distribution
- **Multi-worker** — scale to N processes
- **Multi-machine** — distribute across machines via DB
- **Priority queues** — urgent work goes first
- **Rate limiting** — control API call rates
- **Graceful shutdown** — no work lost during deploys

</td><td>

### Operations
- **Cron scheduling** — built-in, no external tool
- **Signals** — send data to running workflows
- **Human-in-the-loop** — approval gates
- **OpenTelemetry** — traces and metrics
- **Built-in dashboard** — no separate UI to deploy

</td></tr>
<tr><td>

### Developer Experience
- **Type-safe** — Pydantic models for step I/O
- **Testing framework** — in-memory, no DB needed
- **CLI tool** — manage workflows from terminal
- **Rich errors** — context + suggestions

</td><td>

### AI/ML Native
- **LLM step** — checkpointed AI calls
- **Streaming** — SSE-compatible streamed outputs
- **Token tracking** — usage and cost per workflow
- **Model fallback** — automatic failover
- **Agent loops** — durable tool-calling agents

</td></tr>
<tr><td>

### Enterprise
- **Audit logging** — track all workflow operations
- **Key rotation** — rotate encryption keys safely
- **DLQ management** — inspect, retry, purge failed work
- **Workflow versioning** — migrate between versions
- **Admin operations** — cancel, retry, purge workflows

</td><td>

### Security
- **AES-256-GCM encryption** — checkpoint data at rest
- **Restricted pickle** — allowlist-based unpickling
- **CORS allowlist** — dashboard origin control
- **Bearer auth** — dashboard API authentication
- **Input validation** — Pydantic schema enforcement

</td></tr>
</table>

---

## Comparison

| | Celery | Temporal | Prefect | DBOS | **Gravtory** |
|---|---|---|---|---|---|
| Infrastructure | Redis/RMQ | Server+DB+Workers | Server | None | **None** |
| Setup time | ~30 min | ~2 days | ~2 hours | ~10 min | **~3 min** |
| Library vs Service | Lib+Broker | Service | Service | Library | **Library** |
| Crash-safe | [FAIL] | [PASS] | Partial | [PASS] | **[PASS]** |
| Distributed workers | [PASS] | [PASS] | [PASS] | [FAIL] | **[PASS]** |
| Saga compensation | [FAIL] | [PASS] | [FAIL] | [FAIL] | **[PASS]** |
| Signals | [FAIL] | [PASS] | [FAIL] | [FAIL] | **[PASS]** |
| Scheduling | Celery Beat | Schedules | [PASS] | [PASS] | **[PASS]** |
| Dashboard | Flower | [PASS] | [PASS] | [FAIL] | **[PASS]** |
| Type-safe | [FAIL] | [FAIL] | [FAIL] | [FAIL] | **[PASS]** |
| Testing framework | [FAIL] | [PASS] | [FAIL] | [FAIL] | **[PASS]** |
| AI/LLM native | [FAIL] | [FAIL] | [FAIL] | [FAIL] | **[PASS]** |
| Backends | Redis/RMQ | PG/Cassandra | PG | PG only | **5 DBs** |
| License | BSD | MIT | Apache | MIT | **AGPL** |

---

## Patterns

### Saga with Automatic Compensation

```python
@grav.workflow(id="transfer-{id}")
@saga
class TransferWorkflow:

    @step(1, compensate="refund")
    async def debit(self, amount: Decimal) -> dict:
        return await bank.debit(self.source, amount)

    @step(2, depends_on=1, compensate="reverse")
    async def credit(self, amount: Decimal) -> dict:
        return await bank.credit(self.dest, amount)

    async def refund(self, output: dict):
        await bank.credit(self.source, output["amount"])

    async def reverse(self, output: dict):
        await bank.reverse(output["transaction_id"])

# If credit fails → refund runs automatically (crash-safe!)
```

### Parallel Processing

```python
@grav.workflow(id="batch-{id}")
class BatchWorkflow:

    @step(1)
    async def get_items(self, id: str) -> list[str]:
        return await db.get_item_ids(id)

    @step(2, depends_on=1)
    @parallel(max_concurrency=20)
    async def process(self, item_id: str) -> dict:
        return await compute(item_id)
    # Each item is individually checkpointed.
    # On crash: only unfinished items re-execute.

    @step(3, depends_on=2)
    async def summarize(self, results: list[dict]) -> dict:
        return {"processed": len(results)}
```

### Human-in-the-Loop

```python
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

# From your API or Slack bot:
await grav.signal("expense-42", "approval", {"approved": True})
```

### Scheduled Workflows

```python
@grav.workflow(id="daily-report")
@grav.schedule(cron="0 9 * * *", tz="US/Eastern")
class DailyReport:

    @step(1)
    async def generate(self) -> dict:
        return await analytics.report()

    @step(2, depends_on=1)
    async def send(self, report: dict) -> None:
        await email.send_report(report)
```

### Retry with Backoff

```python
@step(1, retries=5, backoff="exponential", backoff_base=2.0, retry_on=[httpx.TimeoutError])
async def call_external_api(self, url: str) -> dict:
    return await httpx.get(url).json()
# Retries at: 2s, 4s, 8s, 16s, 32s (with jitter)
```

---

## Distribution (No Message Broker)

```python
# Scale from 1 to N workers — just change a number
grav = Gravtory("postgresql://localhost/mydb", workers=8)

# Scale across machines — same code, different machines
# Machine A:
grav = Gravtory("postgresql://shared-db/workflows", workers=8, node_id="a")
# Machine B:
grav = Gravtory("postgresql://shared-db/workflows", workers=8, node_id="b")

# Workers coordinate through the database.
# No Redis. No RabbitMQ. No Kafka.
```

---

## Observability

```python
grav = Gravtory(
    "postgresql://localhost/mydb",
    dashboard=True,              # Web UI at :7777
    otel_endpoint="jaeger:4317", # OpenTelemetry traces
    metrics_port=9090,           # Prometheus metrics
)

# Introspection API
state = await grav.inspect("order-ord_123")
print(state.status)            # "completed"
print(state.steps[1].output)   # {"charge_id": "ch_xyz"}
print(state.steps[1].duration_ms)  # 142

# Middleware
@grav.on_failure
async def alert(ctx):
    await slack.send(f"Workflow {ctx.workflow_run_id} failed: {ctx.error}")
```

---

## Testing (No Database Required)

```python
from gravtory.testing import WorkflowTestRunner

async def test_order_workflow():
    runner = WorkflowTestRunner()  # In-memory!
    runner.mock(OrderWorkflow.charge_card, return_value={"charge_id": "test"})
    runner.mock(OrderWorkflow.reserve_inventory, return_value={"ok": True})
    runner.mock(OrderWorkflow.send_notification, return_value=None)

    result = await runner.run(OrderWorkflow, order_id="test_123")
    assert result.status == "completed"

    # Simulate crash and verify resume
    runner.simulate_crash_after(step=1)
    result = await runner.run(OrderWorkflow, order_id="test_456")
    result = await runner.resume("order-test_456")
    assert result.steps[1].was_replayed  # Not re-executed!
```

---

## CLI

```bash
gravtory list --status=failed
gravtory inspect order-ord_123
gravtory retry order-ord_123
gravtory signal expense-42 approval '{"approved": true}'
gravtory dlq list
gravtory dashboard
gravtory workers start --count=4
```

---

## Installation

```bash
# Core + PostgreSQL (recommended)
pip install gravtory[postgres]

# Core + SQLite (development)
pip install gravtory[sqlite]

# Core + MySQL
pip install gravtory[mysql]

# Core + MongoDB
pip install gravtory[mongodb]

# Everything
pip install gravtory[all]
```

**Requirements**: Python 3.10+

**Type checking**: Gravtory ships with a `py.typed` marker ([PEP 561](https://peps.python.org/pep-0561/)). Full type annotations work out of the box with mypy, pyright, and other type checkers — no stubs package needed.

---

## Backends

| Backend | Best For | Distribution | Signals |
|---|---|---|---|
| **PostgreSQL** | Production | `SKIP LOCKED` | `LISTEN/NOTIFY` |
| **SQLite** | Development, testing | File locks | Polling |
| **MySQL 8+** | Enterprise | `SKIP LOCKED` | Polling |
| **MongoDB** | Document-heavy | `findOneAndUpdate` | Change Streams |
| **Redis** | High-throughput | Lua scripts | Pub/Sub |

---

## Framework Integration

### FastAPI

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from gravtory import Gravtory

grav = Gravtory("postgresql://localhost/mydb")

@asynccontextmanager
async def lifespan(app):
    await grav.start()
    yield
    await grav.shutdown()

app = FastAPI(lifespan=lifespan)

@app.post("/orders/{order_id}")
async def create_order(order_id: str):
    run_id = await grav.run(OrderWorkflow, order_id=order_id, background=True)
    return {"run_id": run_id}
```

### Django (coming soon)

Django integration is planned for a future release. Track progress
in [GitHub Issues](https://github.com/vatryok/gravtory/issues).

---

## Coming from Celery?

```python
# Before (Celery)
@app.task(bind=True, max_retries=3)
def charge_card(self, order_id):
    try:
        result = stripe.charge(order_id)
    except Exception as exc:
        raise self.retry(exc=exc)
    return result

# If this crashes: task may be lost. Result may be lost.
# If charge succeeds but ack fails: card charged twice.

# After (Gravtory)
@step(1, retries=3, backoff="exponential")
async def charge_card(self, order_id: str) -> dict:
    return await stripe.charge(order_id)

# If this crashes: checkpoint guarantees at-least-once with idempotent replay.
# On resume: if charge completed, it's loaded from DB. Never re-executed.
```

---

## License

AGPL — open source, copyleft. Keeps the ecosystem open.

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## Support the Project

Backing the ongoing development efforts is appreciated. [**Support Gravtory on Ko-Fi**](https://ko-fi.com/gravtory)