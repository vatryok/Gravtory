# PostgreSQL Backend

The recommended backend for production. Uses `SKIP LOCKED` for distributed work claiming and `LISTEN/NOTIFY` for real-time signal delivery.

## Installation

```bash
pip install gravtory[postgres]
```

## Connection

```python
from gravtory import Gravtory

grav = Gravtory("postgresql://user:password@localhost:5432/mydb")
await grav.start()  # Creates tables automatically
```

## Features

| Feature | Implementation |
|---------|---------------|
| Work claiming | `SELECT ... FOR UPDATE SKIP LOCKED` |
| Signal delivery | `LISTEN/NOTIFY` (~10ms latency) |
| Leader election | Advisory locks (`pg_advisory_lock`) |
| Concurrency | Full MVCC isolation |
| Schema | Auto-created on `start()` |

## Tables Created

Gravtory creates these tables in your database (prefixed with `gravtory_`):

- `gravtory_workflow_runs` — workflow execution state
- `gravtory_step_outputs` — checkpointed step outputs
- `gravtory_pending_steps` — steps ready for execution
- `gravtory_signals` — pending signals
- `gravtory_schedules` — cron/interval schedules
- `gravtory_dlq` — dead letter queue entries
- `gravtory_locks` — distributed locks
- `gravtory_workers` — worker heartbeats
- `gravtory_compensations` — saga compensation records

## Connection Pooling

For production, configure connection pooling:

```python
grav = Gravtory(
    "postgresql://user:pass@localhost/mydb",
    pool_size=10,
    max_overflow=20,
)
```

## Environment Variables

```bash
export GRAVTORY_BACKEND="postgresql://user:pass@localhost/mydb"
```

```python
import os
grav = Gravtory(os.environ["GRAVTORY_BACKEND"])
```
