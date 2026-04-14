# Quick Start

Get a crash-proof workflow running in under 5 minutes.

## 1. Install

```bash
pip install gravtory
```

For PostgreSQL (recommended for production):

```bash
pip install gravtory[postgresql]
```

## 2. Your First Workflow

Create a file called `app.py`:

```python
import asyncio
from gravtory import Gravtory, step

# SQLite — zero setup, great for getting started
grav = Gravtory(":memory:")  # or "sqlite:///workflows.db" for persistence

@grav.workflow(id="hello-{name}")
class HelloWorkflow:

    @step(1)
    async def greet(self, name: str) -> str:
        return f"Hello, {name}!"

    @step(2, depends_on=1)
    async def log(self) -> None:
        greeting = self.context.output(1)
        print(greeting)

async def main():
    async with grav:
        run = await grav.run(HelloWorkflow, name="World")
        print(f"Workflow {run.id} completed: {run.status.value}")

asyncio.run(main())
```

```bash
python app.py
# Output:
# Hello, World!
# Workflow hello-World completed: completed
```

## 3. What Happens on Crash?

If your process crashes after step 1 completes, Gravtory will:

1. Detect the incomplete workflow on restart
2. Load step 1's output from the database (not re-execute it)
3. Continue from step 2

This is the core value proposition — **your side effects never repeat**.

## 4. Add Retries

Steps can retry automatically with configurable backoff:

```python
@step(1, retries=3, backoff="exponential", backoff_base=2.0)
async def call_api(self, url: str) -> dict:
    return await httpx.get(url).json()
```

If `call_api` fails, Gravtory retries up to 3 times with 2s, 4s, 8s delays.

## 5. Add Saga Compensation

Automatically reverse completed steps when a later step fails:

```python
from gravtory import saga

@grav.workflow(id="order-{order_id}")
class OrderWorkflow:

    @step(1)
    @saga(compensate="refund_charge")
    async def charge_card(self, order_id: str) -> dict:
        return await stripe.charge(order_id)

    async def refund_charge(self, charge_result: dict) -> None:
        await stripe.refund(charge_result["charge_id"])

    @step(2, depends_on=1)
    async def ship_order(self, order_id: str) -> None:
        await warehouse.ship(order_id)
```

If `ship_order` fails, Gravtory automatically calls `refund_charge`.

## 6. Run with Workers (Production)

For production, run workers that poll for pending workflows:

```python
# worker.py
import asyncio
from gravtory import Gravtory

grav = Gravtory("postgresql://localhost/mydb")

# Register your workflows
from app import HelloWorkflow, OrderWorkflow

async def main():
    async with grav:
        await grav.run_worker()  # polls forever

asyncio.run(main())
```

Or use the CLI:

```bash
gravtory worker start --dsn postgresql://localhost/mydb
```

## 7. Use the CLI

```bash
# List workflows
gravtory workflow list

# Inspect a run
gravtory workflow status hello-World

# Retry a failed run
gravtory workflow retry hello-World

# Manage dead letter queue
gravtory dlq list
gravtory dlq retry <entry-id>
```

Enable shell completion for discoverability:

```bash
gravtory completion bash >> ~/.bashrc
```

## Next Steps

- [Configuration](configuration.md) — backends, serialization, encryption
- [Concepts](concepts.md) — DAGs, signals, parallel steps
- [Guides](../guides/) — scheduling, observability, testing
- [Migration from Celery](../migration/celery.md)
