# MongoDB Backend

Document-oriented backend using `findOneAndUpdate` for atomic work claiming and Change Streams for real-time signal delivery.

## Installation

```bash
pip install gravtory[mongodb]
```

## Connection

```python
from gravtory import Gravtory

grav = Gravtory("mongodb://localhost:27017/mydb")
await grav.start()
```

## Features

| Feature | Implementation |
|---------|---------------|
| Work claiming | `findOneAndUpdate` with atomic transitions |
| Signal delivery | Change Streams (~100ms latency) |
| Leader election | Unique index + TTL |
| Concurrency | Document-level atomicity |
| Schema | Collections auto-created on `start()` |

## Collections Created

- `gravtory_workflow_runs`
- `gravtory_step_outputs`
- `gravtory_signals`
- `gravtory_schedules`
- `gravtory_dlq`
- `gravtory_locks`
- `gravtory_workers`

## Requirements

- MongoDB 4.0+ (required for Change Streams with replica set)
- For Change Streams: must run as a replica set (even single-node)

## When to Use

- Document-heavy payloads (large JSON step outputs)
- Existing MongoDB infrastructure
- Flexible schema requirements
