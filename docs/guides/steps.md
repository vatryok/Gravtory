# Steps

Steps are the atomic units of work in a Gravtory workflow. Each step's output is checkpointed to the database, guaranteeing exactly-once execution even across crashes.

## Defining Steps

Use the `@step` decorator on workflow methods:

```python
@step(1)
async def validate(self, order_id: str) -> dict:
    return {"validated": True}

@step(2, depends_on=1)
async def charge(self, order_id: str) -> dict:
    return {"charged": True}
```

The first argument is the **step order** (unique integer within the workflow).

## Dependencies

Use `depends_on` to declare that a step requires another step to complete first:

```python
@step(3, depends_on=2)       # Depends on step 2
@step(4, depends_on=[2, 3])  # Depends on steps 2 AND 3
```

Gravtory builds a DAG (directed acyclic graph) from dependencies and executes steps in topological order. Steps with no mutual dependencies can run in parallel.

## Step Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `order` | `int` | required | Unique step number |
| `depends_on` | `int \| list[int]` | `[]` | Step dependencies |
| `retries` | `int` | `0` | Max retry attempts |
| `backoff` | `str` | `"constant"` | Backoff strategy |
| `backoff_base` | `float` | `1.0` | Base delay in seconds |
| `retry_on` | `list[type]` | `[Exception]` | Exceptions to retry on |
| `abort_on` | `list[type]` | `[]` | Exceptions that skip retries |
| `timeout` | `float` | `None` | Step timeout in seconds |
| `compensate` | `str` | `None` | Compensation handler name |
| `condition` | `callable` | `None` | Condition function for conditional execution |

## Retry Configuration

```python
@step(1, retries=5, backoff="exponential", backoff_base=2.0)
async def call_api(self, url: str) -> dict:
    return await httpx.get(url).json()
# Retries at: 2s, 4s, 8s, 16s, 32s (with jitter)
```

Backoff strategies: `"constant"`, `"linear"`, `"exponential"`.

## Conditional Steps

Steps can be conditionally skipped based on previous step outputs:

```python
@step(3, depends_on=2, condition=lambda ctx: ctx.output(2)["approved"])
async def process_payment(self, amount: float) -> dict:
    return {"paid": True}
```

The `condition` receives a `StepContext` with access to previous step outputs via `ctx.output(step_order)`.

## Compensation Handlers

For saga workflows, steps can declare a compensation handler that runs on failure:

```python
@step(1, compensate="undo_charge")
async def charge(self, amount: float) -> dict:
    return {"charge_id": "ch_123"}

async def undo_charge(self, output: dict):
    await refund(output["charge_id"])
```

See the [Sagas guide](sagas.md) for details.
