# Migrating from Temporal

Gravtory provides similar durability guarantees to Temporal but as a Python library — no separate server, no Docker compose, no gRPC.

## Key Differences

| Aspect | Temporal | Gravtory |
|--------|----------|----------|
| Architecture | Server + Workers (gRPC) | Library (pip install) |
| Setup time | ~2 days | ~3 minutes |
| Language | Go server, Python SDK | Pure Python |
| Infrastructure | Temporal Server + Cassandra/PG | Your existing database |
| Signals | [PASS] | [PASS] |
| Sagas | [PASS] (via patterns) | [PASS] (built-in `@saga`) |
| Versioning | Deterministic replay | Checkpoint-based |
| Testing | Sandbox environment | In-memory runner |

## Conceptual Mapping

| Temporal Concept | Gravtory Equivalent |
|-----------------|---------------------|
| Workflow | `@workflow` class |
| Activity | `@step` method |
| Signal | `@wait_for_signal` + `grav.signal()` |
| Query | `grav.inspect()` |
| Child Workflow | Nested `grav.run()` in a step |
| Schedule | `@schedule(cron=...)` |
| Worker | `Gravtory(workers=N)` |
| Task Queue | Priority-based workflow routing |

## Migration Example

### Before (Temporal)

```python
# workflow.py
from temporalio import workflow
from temporalio.common import RetryPolicy

@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> dict:
        result = await workflow.execute_activity(
            charge_card,
            order_id,
            retry_policy=RetryPolicy(maximum_attempts=3),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            send_notification,
            order_id,
            start_to_close_timeout=timedelta(seconds=10),
        )
        return result
```

### After (Gravtory)

```python
from gravtory import Gravtory, step

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="order-{order_id}")
class OrderWorkflow:

    @step(1, retries=3, timeout=30)
    async def charge_card(self, order_id: str) -> dict:
        return await stripe.charge(order_id)

    @step(2, depends_on=1, timeout=10)
    async def send_notification(self, order_id: str) -> None:
        await email.send(order_id)
```

## What You Gain

- **No server** — eliminate Temporal Server, its database, and Docker compose
- **Simpler deployment** — `pip install gravtory[postgres]` is all you need
- **Familiar patterns** — Python decorators instead of gRPC SDK
- **Same guarantees** — crash safety, exactly-once, compensation

## What You Trade

- **Deterministic replay** — Temporal replays entire workflow history; Gravtory uses step-level checkpoints
- **Language support** — Temporal supports Go, Java, TypeScript, PHP; Gravtory is Python-only
- **Ecosystem maturity** — Temporal has a larger community and more battle-testing at extreme scale
