# Migrating from Celery

Gravtory replaces Celery for workflows that need crash safety, exactly-once execution, and saga compensation — without a message broker.

## Key Differences

| Aspect | Celery | Gravtory |
|--------|--------|----------|
| Infrastructure | Redis/RabbitMQ required | Your existing database |
| Crash safety | Tasks can be lost | Atomic checkpointing |
| Exactly-once | At-most-once or at-least-once | Exactly-once (via checkpoints) |
| Compensation | Manual | Automatic sagas |
| State inspection | Limited (Flower) | Built-in dashboard + API |
| Testing | Requires broker | In-memory, no DB needed |

## Migration Example

### Before (Celery)

```python
from celery import Celery

app = Celery('tasks', broker='redis://localhost:6379/0')

@app.task(bind=True, max_retries=3)
def charge_card(self, order_id):
    try:
        result = stripe.charge(order_id)
    except Exception as exc:
        raise self.retry(exc=exc, countdown=2**self.request.retries)
    return result

@app.task
def send_email(order_id, charge_result):
    email.send(order_id, charge_result)

# Chain tasks
from celery import chain
chain(charge_card.s("ord_123"), send_email.s("ord_123"))()
```

### After (Gravtory)

```python
from gravtory import Gravtory, step

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="order-{order_id}")
class OrderWorkflow:

    @step(1, retries=3, backoff="exponential", backoff_base=2.0)
    async def charge_card(self, order_id: str) -> dict:
        return await stripe.charge(order_id)

    @step(2, depends_on=1)
    async def send_email(self, order_id: str) -> None:
        charge = self.context.output(1)
        await email.send(order_id, charge)

# Run
await grav.start()
await grav.run(OrderWorkflow, order_id="ord_123")
```

## What You Gain

- **No broker** — remove Redis/RabbitMQ from your stack
- **Crash safety** — if `charge_card` succeeds but the process dies before `send_email`, the charge is NOT repeated on restart
- **Saga rollback** — add `compensate="refund"` to automatically reverse charges on failure
- **Full visibility** — inspect every step's input, output, duration, and status

## Gradual Migration

You don't have to migrate everything at once. Gravtory and Celery can coexist:

1. Start with new workflows in Gravtory
2. Migrate critical workflows (payments, orders) first
3. Move remaining tasks over time
4. Decommission the broker when ready
