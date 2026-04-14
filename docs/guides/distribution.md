# Distribution

Gravtory scales from a single process to multiple machines — all coordinated through your database. No message broker required.

## Single Process (Default)

```python
grav = Gravtory("postgresql://localhost/mydb")
await grav.start()
```

All workflows run in the current process using asyncio concurrency.

## Multi-Worker (Single Machine)

```python
grav = Gravtory("postgresql://localhost/mydb", workers=8)
await grav.start()
```

Spawns 8 worker processes that pick up and execute workflows concurrently. Workers use database-level locking (`SELECT ... FOR UPDATE SKIP LOCKED` on PostgreSQL/MySQL) to avoid duplicate execution.

## Multi-Machine (Distributed)

```python
# Machine A
grav = Gravtory("postgresql://shared-db/workflows", workers=8, node_id="node-a")

# Machine B
grav = Gravtory("postgresql://shared-db/workflows", workers=8, node_id="node-b")
```

Same code, same database, different machines. Workers coordinate through the database — no Redis, no RabbitMQ, no Kafka.

## Priority Queues

```python
grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="urgent-{id}", priority=10)
class UrgentWorkflow:
    ...

@grav.workflow(id="batch-{id}", priority=1)
class BatchWorkflow:
    ...
```

Higher priority workflows are picked up first by workers.

## Rate Limiting

Control the rate of workflow execution to protect external services:

```python
from gravtory.workers.rate_limit import RateLimiter

grav = Gravtory(
    "postgresql://localhost/mydb",
    rate_limiter=RateLimiter(max_per_second=10),
)
```

## Graceful Shutdown

On SIGTERM/SIGINT, Gravtory:

1. Stops accepting new workflows
2. Waits for in-progress steps to complete
3. Checkpoints current state
4. Shuts down cleanly

No work is lost during deploys.

```python
await grav.shutdown(timeout=30)  # Wait up to 30s for in-progress work
```

## Backend Support

| Feature | PostgreSQL | SQLite | MySQL | MongoDB | Redis |
|---------|-----------|--------|-------|---------|-------|
| Multi-worker | `SKIP LOCKED` | File locks | `SKIP LOCKED` | `findOneAndUpdate` | Lua scripts |
| Multi-machine | [PASS] | [FAIL] | [PASS] | [PASS] | [PASS] |
| Leader election | Advisory locks | N/A | `GET_LOCK` | Unique index | `SET NX` |
