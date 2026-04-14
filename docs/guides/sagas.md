# Sagas

Sagas provide **automatic rollback** when a multi-step workflow fails partway through. If step 3 fails, Gravtory runs compensation handlers for steps 2 and 1 in reverse order.

## Why Sagas?

Consider a money transfer: debit account A, then credit account B. If the credit fails, the debit must be reversed. Sagas automate this pattern.

## Defining a Saga Workflow

```python
from gravtory import Gravtory, workflow, step
from gravtory.decorators.saga import saga

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="transfer-{id}")
@saga
class TransferWorkflow:

    @step(1, compensate="undo_debit")
    async def debit(self, id: str, amount: float) -> dict:
        result = await bank.debit(self.source, amount)
        return {"transaction_id": result["id"], "amount": amount}

    @step(2, depends_on=1, compensate="undo_credit")
    async def credit(self, id: str, amount: float) -> dict:
        return await bank.credit(self.dest, amount)

    # Compensation handlers
    async def undo_debit(self, output: dict):
        await bank.credit(self.source, output["amount"])

    async def undo_credit(self, output: dict):
        await bank.reverse(output["transaction_id"])
```

## How Compensation Works

1. Steps execute normally: step 1 → step 2 → step 3
2. If step 3 raises an exception, Gravtory triggers the saga coordinator
3. Compensation runs in **reverse order**: undo step 2 → undo step 1
4. Each compensation handler receives the **output** of its corresponding step
5. The workflow status transitions to `COMPENSATED` or `COMPENSATION_FAILED`

## Compensation Handler Signature

```python
async def undo_step_name(self, output: dict):
    # output is whatever the original step returned
    ...
```

Handlers can be sync or async. They receive the output of the completed step they're compensating.

## Error During Compensation

If a compensation handler itself fails:

- The error is logged
- The failed compensation is added to the DLQ
- Remaining compensations continue executing
- The workflow status becomes `COMPENSATION_FAILED`

## Best Practices

- **Make compensations idempotent** — they may be retried
- **Keep compensation logic simple** — don't call other workflows
- **Log compensation actions** — for audit trails
- **Test compensations** — use `WorkflowTestRunner` to simulate failures
