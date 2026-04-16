<div align="center">

# Gravtory

**Crash-proof Python workflows with zero infrastructure.**

Durable execution, distributed workers, sagas, scheduling, and observability,
backed by the database you already run.

[![Python](https://img.shields.io/pypi/pyversions/gravtory.svg)](https://pypi.org/project/gravtory/)
[![License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![CI](https://github.com/vatryok/Gravtory/actions/workflows/ci.yml/badge.svg)](https://github.com/vatryok/Gravtory/actions)
[![Coverage](https://img.shields.io/codecov/c/github/vatryok/Gravtory)](https://codecov.io/gh/vatryok/Gravtory)

[Quick Start](#quick-start) · [Features](#features) · [Patterns](#patterns) · [Installation](#installation) · [Examples](examples/)

</div>

## Why Gravtory?

Every production application eventually needs workflows that cannot fail halfway.
A payment that must complete. An order that must ship. A data pipeline that must
finish. When a process crashes between steps, you need guarantees.

The current options each carry significant trade-offs:

- **Celery** accepts that tasks can be lost on crash.
- **Temporal** requires a multi-service deployment and a steep learning curve.
- **Prefect Cloud** and **Airflow** demand hosted infrastructure or managed clusters.
- **Hand-rolled retry loops** provide no real durability.

Gravtory offers an alternative: Temporal-grade reliability delivered as a Python
library, using the database you already operate.

```bash
pip install gravtory[postgres]
```

That is the entire infrastructure requirement.


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
Normal run:   step 1 [OK]   -> step 2 [OK]   -> step 3 [OK]     completes
Crash:        step 1 [OK]   -> step 2 [CRASH]                    process dies
Auto-resume:  step 1 [SKIP] -> step 2 [OK]   -> step 3 [OK]     resumes exactly
```

Step 1 (charge card) is **never re-executed**. Its output was atomically
checkpointed to the database. On restart, Gravtory loads that checkpoint and
continues from the precise point of failure.


## Features

<table>
<tr><td>

### Core
- **Crash-safe execution** -- steps checkpointed atomically
- **Exact resume** -- restart from the precise failed step
- **Idempotency** -- no step ever runs twice
- **5 backends** -- PostgreSQL, SQLite, MySQL, MongoDB, Redis
- **Zero infrastructure** -- uses your existing database

</td><td>

### Patterns
- **Saga compensation** -- automatic rollback on failure
- **Parallel fan-out/in** -- process 1000 items concurrently
- **Conditional branching** -- if/else within workflows
- **Sub-workflows** -- composable, nested execution
- **Circuit breaker** -- protect external services

</td></tr>
<tr><td>

### Distribution
- **Multi-worker** -- scale to N processes
- **Multi-machine** -- distribute across hosts via the DB
- **Priority queues** -- urgent work goes first
- **Rate limiting** -- control outbound API call rates
- **Graceful shutdown** -- no work lost during deploys

</td><td>

### Operations
- **Cron scheduling** -- built-in, no external tool
- **Signals** -- send data to running workflows
- **Human-in-the-loop** -- approval gates
- **OpenTelemetry** -- distributed traces and metrics
- **Built-in dashboard** -- no separate UI to deploy

</td></tr>
<tr><td>

### Developer Experience
- **Type-safe** -- Pydantic models for step I/O
- **Testing framework** -- in-memory, no database needed
- **CLI tool** -- manage workflows from the terminal
- **Rich errors** -- context and actionable suggestions

</td><td>

### AI/ML Native
- **LLM step** -- checkpointed AI calls
- **Streaming** -- SSE-compatible streamed outputs
- **Token tracking** -- usage and cost per workflow
- **Model fallback** -- automatic failover between providers
- **Agent loops** -- durable tool-calling agents

</td></tr>
<tr><td>

### Enterprise
- **Audit logging** -- track every workflow operation
- **Key rotation** -- rotate encryption keys without downtime
- **DLQ management** -- inspect, retry, purge failed work
- **Workflow versioning** -- migrate between versions safely
- **Admin operations** -- cancel, retry, purge workflows

</td><td>

### Security
- **AES-256-GCM encryption** -- checkpoint data at rest
- **Restricted pickle** -- allowlist-based deserialization
- **CORS allowlist** -- dashboard origin control
- **Bearer auth** -- dashboard API authentication
- **Input validation** -- Pydantic schema enforcement

</td></tr>
</table>


## Comparison

| | Celery | Temporal | Prefect | DBOS | **Gravtory** |
|---|---|---|---|---|---|
| Infrastructure | Redis / RabbitMQ | Server + DB + Workers | Server | None | **None** |
| Setup time | ~30 min | ~2 days | ~2 hours | ~10 min | **~3 min** |
| Library vs Service | Library + Broker | Service | Service | Library | **Library** |
| Crash-safe | No | Yes | Partial | Yes | **Yes** |
| Distributed workers | Yes | Yes | Yes | No | **Yes** |
| Saga compensation | No | Yes | No | No | **Yes** |
| Signals | No | Yes | No | No | **Yes** |
| Scheduling | Celery Beat | Built-in | Yes | Yes | **Yes** |
| Dashboard | Flower | Yes | Yes | No | **Yes** |
| Type-safe | No | No | No | No | **Yes** |
| Testing framework | No | Yes | No | No | **Yes** |
| AI/LLM native | No | No | No | No | **Yes** |
| Backends | Redis / RabbitMQ | PG / Cassandra | PG | PG only | **5 databases** |
| License | BSD | MIT | Apache | MIT | **AGPL** |


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

# If credit fails, refund runs automatically -- crash-safe.
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


## Distribution

No message broker required. Workers coordinate through the database.

```python
# Scale from 1 to N workers on a single machine
grav = Gravtory("postgresql://localhost/mydb", workers=8)

# Scale across machines -- same code, different hosts
# Machine A:
grav = Gravtory("postgresql://shared-db/workflows", workers=8, node_id="a")
# Machine B:
grav = Gravtory("postgresql://shared-db/workflows", workers=8, node_id="b")
```


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

# Failure hooks
@grav.on_failure
async def alert(ctx):
    await slack.send(f"Workflow {ctx.workflow_run_id} failed: {ctx.error}")
```


## Testing

No database required. The built-in test runner operates entirely in memory.

```python
from gravtory.testing import WorkflowTestRunner

async def test_order_workflow():
    runner = WorkflowTestRunner()  # In-memory
    runner.mock(OrderWorkflow.charge_card, return_value={"charge_id": "test"})
    runner.mock(OrderWorkflow.reserve_inventory, return_value={"ok": True})
    runner.mock(OrderWorkflow.send_notification, return_value=None)

    result = await runner.run(OrderWorkflow, order_id="test_123")
    assert result.status == "completed"

    # Simulate crash and verify resume
    runner.simulate_crash_after(step=1)
    result = await runner.run(OrderWorkflow, order_id="test_456")
    result = await runner.resume("order-test_456")
    assert result.steps[1].was_replayed  # Not re-executed
```


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


## Installation

```bash
# Core + PostgreSQL (recommended for production)
pip install gravtory[postgres]

# Core + SQLite (local development)
pip install gravtory[sqlite]

# Core + MySQL
pip install gravtory[mysql]

# Core + MongoDB
pip install gravtory[mongodb]

# All backends and optional extras
pip install gravtory[all]
```

**Requirements:** Python 3.10+

Gravtory ships with a `py.typed` marker
([PEP 561](https://peps.python.org/pep-0561/)). Full type annotations work out
of the box with mypy, pyright, and other type checkers.


## Backends

| Backend | Best For | Distribution | Signals |
|---|---|---|---|
| **PostgreSQL** | Production | `SKIP LOCKED` | `LISTEN/NOTIFY` |
| **SQLite** | Development, testing | File locks | Polling |
| **MySQL 8+** | Enterprise | `SKIP LOCKED` | Polling |
| **MongoDB** | Document-heavy workloads | `findOneAndUpdate` | Change Streams |
| **Redis** | High-throughput | Lua scripts | Pub/Sub |


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

### Django

Django integration is planned for a future release. Progress is tracked in
[GitHub Issues](https://github.com/vatryok/Gravtory/issues).


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


## License

Gravtory is released under the
[GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0-or-later).

For commercial licensing inquiries, contact **vatryok@protonmail.com**.


## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) for
development setup, coding standards, and submission guidelines.


## Support

If you find Gravtory useful, consider supporting its continued development on
[Ko-Fi](https://ko-fi.com/gravtory).
