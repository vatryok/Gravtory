# Conditional Branching

Gravtory supports conditional step execution, enabling if/else patterns within workflows.

## Basic Conditions

Use the `condition` parameter on `@step` to conditionally execute a step:

```python
from gravtory import Gravtory, step

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="order-{id}")
class OrderWorkflow:

    @step(1)
    async def check_inventory(self, id: str) -> dict:
        in_stock = await inventory.check(id)
        return {"in_stock": in_stock, "id": id}

    @step(2, depends_on=1, condition=lambda ctx: ctx.output(1)["in_stock"])
    async def ship_immediately(self, id: str) -> dict:
        return await shipping.express(id)

    @step(3, depends_on=1, condition=lambda ctx: not ctx.output(1)["in_stock"])
    async def backorder(self, id: str) -> dict:
        return await shipping.backorder(id)
```

## StepContext

The `condition` function receives a `StepContext` object with access to previous step outputs:

```python
def my_condition(ctx):
    step_1_output = ctx.output(1)  # Get output of step 1
    return step_1_output["approved"]
```

### Available Methods

| Method | Description |
|--------|-------------|
| `ctx.output(step_order)` | Get the output of a completed step |
| `ctx.input_data` | Access the original workflow input data |

## Complex Conditions

```python
def high_value_order(ctx):
    order = ctx.output(1)
    return order["amount"] > 1000 and order["customer_tier"] == "premium"

@step(3, depends_on=1, condition=high_value_order)
async def premium_handling(self, id: str) -> dict:
    return await premium_service.process(id)
```

## Skipped Steps

When a condition evaluates to `False`, the step is **skipped** — not failed. Downstream steps that depend on a skipped step will also be skipped unless they have alternative dependencies.

## If/Else Pattern

```python
@step(2, depends_on=1, condition=lambda ctx: ctx.output(1)["approved"])
async def process_approved(self, data: dict) -> dict:
    return {"action": "approved"}

@step(3, depends_on=1, condition=lambda ctx: not ctx.output(1)["approved"])
async def process_rejected(self, data: dict) -> dict:
    return {"action": "rejected"}

@step(4, depends_on=[2, 3])
async def finalize(self, data: dict) -> None:
    # Runs after whichever branch executed
    ...
```
