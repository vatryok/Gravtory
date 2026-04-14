# Redis Backend

High-throughput backend using Lua scripts for atomic work claiming and Pub/Sub for real-time signal delivery.

## Installation

```bash
pip install gravtory[redis]
```

## Connection

```python
from gravtory import Gravtory

grav = Gravtory("redis://localhost:6379/0")
await grav.start()
```

## Features

| Feature | Implementation |
|---------|---------------|
| Work claiming | Lua scripts (atomic) |
| Signal delivery | Pub/Sub (~5ms latency) |
| Leader election | `SET NX` with TTL |
| Concurrency | Single-threaded + Lua atomicity |
| Schema | Keys auto-created on `start()` |

## Key Prefix

All Redis keys are prefixed with `gravtory:` to avoid collisions:

- `gravtory:workflow:{run_id}`
- `gravtory:step:{run_id}:{order}`
- `gravtory:signal:{run_id}:{name}`
- `gravtory:schedule:{name}`
- `gravtory:dlq:{entry_id}`

## When to Use

- High-throughput workloads (thousands of workflows/second)
- Low-latency signal delivery requirements
- Existing Redis infrastructure
- Ephemeral workflows where durability is less critical

## Considerations

- **Persistence** — configure Redis with AOF or RDB for durability
- **Memory** — all data is in memory; monitor usage for large payloads
- **Eviction** — ensure `maxmemory-policy` is set to `noeviction`
