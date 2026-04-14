# Testing

Gravtory includes a complete testing framework so you can test workflows without a database.

## WorkflowTestRunner

The `WorkflowTestRunner` provides an in-memory execution environment:

```python
from gravtory.testing import WorkflowTestRunner

async def test_order_workflow():
    runner = WorkflowTestRunner()

    result = await runner.run(OrderWorkflow, order_id="test_123")
    assert result.status == "completed"
    assert result.steps[1].output == {"validated": True}
```

## Mocking Steps

Replace step implementations with mock return values:

```python
runner = WorkflowTestRunner()
runner.mock(OrderWorkflow.charge_card, return_value={"charge_id": "test"})
runner.mock(OrderWorkflow.reserve_inventory, return_value={"ok": True})

result = await runner.run(OrderWorkflow, order_id="test_123")
assert result.status == "completed"
```

## Simulating Crashes

Test that workflows resume correctly after crashes:

```python
runner = WorkflowTestRunner()
runner.simulate_crash_after(step=1)

# First run — crashes after step 1
result = await runner.run(OrderWorkflow, order_id="test_456")
assert result.status == "failed"

# Resume — step 1 is NOT re-executed
result = await runner.resume("order-test_456")
assert result.status == "completed"
assert result.steps[1].was_replayed  # Loaded from checkpoint
```

## Testing Saga Compensation

```python
runner = WorkflowTestRunner()
runner.mock(TransferWorkflow.credit, side_effect=Exception("Bank error"))

result = await runner.run(TransferWorkflow, id="t_123", amount=100.0)
assert result.status == "compensated"
assert result.compensations_run == ["undo_debit"]
```

## Time Travel

Test time-dependent logic (scheduling, timeouts) without waiting:

```python
from gravtory.testing import TimeTraveler

async def test_schedule_trigger():
    traveler = TimeTraveler()

    async with traveler.freeze("2025-01-15T09:00:00Z"):
        # Time is frozen at 9 AM
        assert traveler.now().hour == 9

    async with traveler.shift(hours=24):
        # Time advanced by 24 hours
        ...
```

## Workflow Introspection

Inspect workflow progress during tests:

```python
from gravtory.testing import WorkflowInspector

inspector = WorkflowInspector(runner)
progress = inspector.get_progress("order-test_123")
print(progress.completed_steps)  # [1, 2]
print(progress.pending_steps)    # [3]
print(progress.percentage)       # 66.7
```

## Best Practices

- **Test each step independently** — mock dependencies, test return values
- **Test the full workflow** — verify DAG execution order
- **Test crash recovery** — simulate failures at each step
- **Test compensations** — verify saga rollback behavior
- **Test conditions** — verify conditional branching logic
- **Use in-memory backend** — fast tests, no database cleanup needed
