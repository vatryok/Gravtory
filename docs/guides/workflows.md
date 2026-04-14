# Workflows

Workflows are the core building block of Gravtory. A workflow is a sequence of **steps** that execute in a defined order, with each step's output atomically checkpointed to your database.

## Class-Based Workflows

The most common pattern — define a class with `@workflow` and methods with `@step`:

```python
from gravtory import Gravtory, workflow, step

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="order-{order_id}")
class OrderWorkflow:

    @step(1)
    async def validate(self, order_id: str) -> dict:
        return {"validated": True, "order_id": order_id}

    @step(2, depends_on=1)
    async def charge(self, order_id: str) -> dict:
        return {"charged": True}

    @step(3, depends_on=2)
    async def ship(self, order_id: str) -> None:
        print(f"Shipping {order_id}")
```

## Workflow ID Templates

The `id` parameter is a template string. Keyword arguments passed to `grav.run()` are substituted:

```python
@grav.workflow(id="invoice-{customer_id}-{month}")
class InvoiceWorkflow:
    ...

# Produces run ID: "invoice-cust_123-2025-01"
await grav.run(InvoiceWorkflow, customer_id="cust_123", month="2025-01")
```

**Idempotency**: If a workflow with the same ID already completed, `grav.run()` returns the cached result without re-executing.

## Running Workflows

```python
await grav.start()

# Foreground (blocks until complete)
result = await grav.run(OrderWorkflow, order_id="abc123")
print(result.status)  # WorkflowStatus.COMPLETED

# Background (returns run_id immediately)
run_id = await grav.run(OrderWorkflow, background=True, order_id="abc123")
```

## Inspecting Workflows

```python
run = await grav.inspect("order-abc123")
print(run.status)
print(run.workflow_name)
print(run.created_at)

# List all runs
runs = await grav.list(status="completed", limit=10)
```

## Configuration Options

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `str` | ID template with `{param}` placeholders |
| `saga` | `bool` | Enable saga compensation (default: `False`) |
| `version` | `int` | Workflow version for migration (default: `1`) |
| `timeout` | `timedelta` | Maximum execution time |

## Sub-Workflows

Call one workflow from another by running it within a step:

```python
@grav.workflow(id="parent-{id}")
class ParentWorkflow:

    @step(1)
    async def run_child(self, id: str) -> dict:
        result = await grav.run(ChildWorkflow, child_id=id)
        return {"child_status": result.status.value}
```

## Error Handling

When a step raises an exception:

1. The workflow status is set to `FAILED`
2. The error message is stored in `run.error_message`
3. If saga is enabled, compensation handlers run in reverse order
4. The failed workflow is added to the Dead Letter Queue (DLQ)
