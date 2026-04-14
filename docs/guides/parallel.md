# Parallel Execution

Gravtory supports parallel step execution for fan-out/fan-in patterns with bounded concurrency and per-item checkpointing.

## Automatic Parallelism

Steps with no mutual dependencies run in parallel automatically:

```python
@grav.workflow(id="report-{id}")
class ReportWorkflow:

    @step(1)
    async def fetch_sales(self, id: str) -> dict:
        return await sales_api.get(id)

    @step(2)
    async def fetch_inventory(self, id: str) -> dict:
        return await inventory_api.get(id)

    @step(3, depends_on=[1, 2])
    async def generate_report(self, id: str) -> dict:
        sales = self.context.output(1)
        inventory = self.context.output(2)
        return {"sales": sales, "inventory": inventory}
```

Steps 1 and 2 run concurrently. Step 3 waits for both.

## Fan-Out / Fan-In with @parallel

Process collections of items with bounded concurrency:

```python
from gravtory.decorators.parallel import parallel

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

## Per-Item Checkpointing

Each item processed by a `@parallel` step is checkpointed individually. On crash:

1. Already-processed items are loaded from the database
2. Only remaining items are re-processed
3. No duplicate processing

## Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_concurrency` | `int` | `10` | Maximum parallel items |

## Resume Behavior

```
First run:   item 1 [OK] -> item 2 [OK] -> item 3 [CRASH] -> item 4 [PENDING]
Resume:      item 1 [SKIP] -> item 2 [SKIP] -> item 3 [OK] -> item 4 [OK]
```

Items 1 and 2 are NOT re-processed — their results are loaded from checkpoints.
