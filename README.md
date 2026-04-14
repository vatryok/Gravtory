<div align="center">

# Gravtory

**Temporal-level reliability. Zero infrastructure. Just your database.**

[![PyPI](https://img.shields.io/pypi/v/gravtory.svg)](https://pypi.org/project/gravtory/)
[![Python Requirements](https://img.shields.io/pypi/pyversions/gravtory.svg)](https://pypi.org/project/gravtory/)
[![License: AGPL 3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![CI Status](https://github.com/vatryok/gravtory/actions/workflows/ci.yml/badge.svg)](https://github.com/vatryok/gravtory/actions)
[![Coverage](https://img.shields.io/codecov/c/github/vatryok/gravtory)](https://codecov.io/gh/vatryok/gravtory)

*The modern Python framework for crash-proof workflows, distributed execution, sagas, and observables.*<br/>
*No separate servers. No message brokers. No Redis.*

[Getting Started](#quick-start) •
[Documentation](docs/) •
[Examples](examples/) •
[Contributing](#contributing)

</div>


## The Problem

Every production application eventually needs workflows that do not break. A payment that must complete. An order that must ship. An onboarding pipeline that must finish. When your process crashes between tasks, you need guarantees.

Traditional solutions force architectural trade-offs:
- **Celery / RQ**: Tasks are frequently lost in the void during crashes.
- **Temporal**: Exceptional resilience, but requires deploying three separate services, a secondary database, and involves a steep learning curve.
- **Prefect / Airflow**: Hosted orchestration platforms that abstract execution too far from the core application code.

## The Solution

**Gravtory** provides Temporal-tier reliability as a pure Python library, utilizing the database infrastructure you already have in place.

```bash
pip install "gravtory[postgres]"
```
That is your entire infrastructure setup.

---

## Quick Start

### 1. Define Your Workflow
Use straightforward decorators to build highly resilient chains.

```python
from gravtory import Gravtory, workflow, step
import stripe, inventory, email

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="order-{order_id}")
class OrderWorkflow:

    @step(1)
    async def charge_card(self, order_id: str) -> dict:
        return await stripe.charge(order_id)

    @step(2, depends_on=1)
    async def reserve_inventory(self, order_id: str) -> dict:
        return await inventory.reserve(order_id)

    @step(3, depends_on=2)
    async def send_notification(self, order_id: str) -> None:
        await email.send(order_id)
```

### 2. Run Execution
```python
await grav.start()
result = await grav.run(OrderWorkflow, order_id="ord_abc123")
```

### 3. Fault Tolerance
If your application server crashes during `reserve_inventory`:
- **Step 1 is never re-executed**. Its output was securely and atomically checkpointed directly to your database.
- The pipeline effortlessly resumes precisely where it left off once the server restarts.

> [!TIP]
> Run multiple processes or deploy across multiple machines. Gravtory coordinates concurrency entirely through your database.


## Technical Features

### Core Reliability
- **Atomic Checkpointing**: Process state is saved atomically as discrete tasks complete.
- **Idempotency Guarantee**: A successfully completed step will never run twice.
- **Flexible Backends**: Drop-in support for PostgreSQL, MySQL, SQLite, MongoDB, and Redis.

### Orchestration Patterns
- **Sagas & Rollbacks**: Automatic compensation functions handle distributed rollbacks if a subsequent step fails.
- **Parallel Execution**: Process thousands of concurrent fan-out steps safely.
- **Conditional Branching**: Smart DAG-based routing dynamically based on previous task outputs.
- **Human-in-the-Loop**: Pause workflows indefinitely until an external signal (e.g., a dashboard approval) triggers continuation.

### Observability & Operations
- **Built-in Dashboard**: Visualize workflows, debug failures, and view internal logs via a built-in web UI.
- **OpenTelemetry Native**: First-class support for OpenTelemetry traces and Prometheus metrics.
- **Cron Scheduling**: Execute reliable background jobs without external dependencies.
- **Dead Letter Queues**: Safely isolate, inspect, and replay irrecoverable task failures.

### AI Native Capabilities
- First-class support for LLM pipelines and agent-loops.
- Native streaming outputs and granular token tracking.
- Fallback mechanics to ensure continuous operation for non-deterministic steps.


## Architecture Comparison

| Feature | Gravtory | Temporal | Celery | Prefect |
| :--- | :---: | :---: | :---: | :---: |
| **New Infrastructure** | **None** | Server + DB + Workers | Redis / RMQ | Server |
| **Setup Time** | **< 3 min** | Days | Hours | Hours |
| **Architecture** | **Library** | Service | Lib + Broker | Service |
| **Crash Safety** | **Yes** | Yes | No | Partial |
| **Stateful Sagas** | **Native** | Yes | No | No |
| **Type Safe API** | **Fully** | No | No | No |


## Ecosystem & Tooling

### CLI Management
Gravtory ships with a powerful CLI out of the box for monitoring and control:
```bash
# Monitor failed workflows
gravtory list --status=failed

# Pause workflow execution and wait for specific events
gravtory signal expense-42 approval '{"approved": true}'

# Launch the visual dashboard locally
gravtory dashboard
```

### Testing Capabilities
You do not need databases for unit testing. Gravtory ships with an in-memory testing runner:

```python
from gravtory.testing import WorkflowTestRunner

async def test_order_workflow():
    runner = WorkflowTestRunner()
    runner.mock(OrderWorkflow.charge_card, return_value={"id": "test"})
    
    result = await runner.run(OrderWorkflow, order_id="test_123")
    assert result.status == "completed"
```



## Installation

Choose your preferred database backend:

```bash
pip install "gravtory[postgres]"  # Recommended for Production
pip install "gravtory[sqlite]"    # Optimal for local development
pip install "gravtory[mysql]"     # MySQL / MariaDB support
pip install "gravtory[mongodb]"   # Document DB
pip install "gravtory[all]"       # Complete installation
```

*Requires Python 3.10+*


## Contributing

We welcome structural improvements and bug fixes. Please examine our [Contribution Guidelines](CONTRIBUTING.md) to initialize your local development environment and submit Pull Requests.


## License & Support

**Gravtory** is open-source and dual-licensed.
- Free for all use cases under the **AGPL-3.0**.
- Commercial licensing is available for closed-source corporate ecosystems.

For corporate inquiries or support: vatryok@protonmail.com

Support ongoing open-source development through [Ko-Fi](https://ko-fi.com/gravtory)