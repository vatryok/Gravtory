# Migrating from DBOS

This guide helps teams migrate from [DBOS Transact](https://docs.dbos.dev/) to Gravtory.

## Key Differences

| Feature | DBOS | Gravtory |
|---------|------|----------|
| **Language** | TypeScript / Python | Python only |
| **Database** | PostgreSQL only | PostgreSQL, SQLite, MySQL, MongoDB, Redis |
| **Hosting** | DBOS Cloud or self-hosted | Self-hosted only (your database) |
| **Decorators** | `@DBOS.workflow`, `@DBOS.step` | `@workflow`, `@step` |
| **Durability** | Automatic | Automatic |
| **Scheduling** | Built-in | Built-in (cron, interval, one-time, event) |
| **Signals** | `setEvent` / `getEvent` | `send_signal` / `@wait_for_signal` |
| **Sagas** | `@DBOS.workflow` with compensations | `@saga` decorator with `compensate=` on steps |

## Migration Steps

### 1. Replace Decorators

**DBOS:**
```python
from dbos import DBOS

@DBOS.workflow()
def my_workflow():
    result = step_one()
    return step_two(result)

@DBOS.step()
def step_one():
    return "hello"
```

**Gravtory:**
```python
from gravtory import workflow, step

@workflow(id="my-workflow-{uuid}")
class MyWorkflow:

    @step(order=1)
    async def step_one(self, ctx):
        return "hello"

    @step(order=2, depends_on=[1])
    async def step_two(self, prev: str) -> str:
        return f"{prev} world"
```

### 2. Replace Event/Signal Handling

**DBOS:**
```python
DBOS.set_event(workflow_id, "approval", data)
result = DBOS.get_event(workflow_id, "approval", timeout=300)
```

**Gravtory:**
```python
# Sender
await grav.signal(run_id, "approval", data)

# Receiver (in workflow)
@step(order=2)
@wait_for_signal("approval", timeout=timedelta(minutes=5))
async def wait_approval(self, signal: dict) -> bool:
    return signal["approved"]
```

### 3. Replace Database Connection

**DBOS:**
```python
DBOS(database_url="postgresql://...")
```

**Gravtory:**
```python
from gravtory import Gravtory

grav = Gravtory("postgresql://...")
# or: Gravtory("sqlite:///local.db")
# or: Gravtory("mysql://...")
```

### 4. Replace Scheduling

**DBOS:**
```python
@DBOS.scheduled("*/5 * * * *")
@DBOS.workflow()
def scheduled_job():
    ...
```

**Gravtory:**
```python
from gravtory import workflow, step, schedule

@workflow(id="scheduled-job-{date}")
@schedule(cron="*/5 * * * *")
class ScheduledJob:

    @step(order=1)
    async def run(self, ctx):
        ...
```

## What You Gain

- **Multi-database support** — not locked into PostgreSQL
- **Saga support** — automatic compensation on failure
- **Parallel execution** — `@parallel` decorator for fan-out/fan-in
- **Dead letter queue** — failed steps are preserved for inspection
- **Dashboard** — built-in web UI for monitoring
- **No cloud dependency** — runs entirely on your infrastructure
