# Configuration

Gravtory is configured primarily through the `Gravtory` engine constructor
and `pyproject.toml` / environment variables for deployment.

## Engine Configuration

```python
from gravtory import Gravtory

grav = Gravtory(
    "postgresql://user:pass@localhost:5432/mydb",
    namespace="production",          # isolate runs by environment
    table_prefix="gravtory_",        # prefix for all DB tables
    serializer="json",               # "json", "msgpack", or "pickle"
    compression=None,                # None, "gzip", "lz4", or "zstd"
    encryption_key=None,             # AES-256-GCM key for at-rest encryption
    workers=4,                       # concurrent step execution (0 = no workers)
    scheduler=False,                 # enable the built-in scheduler
    dashboard=False,                 # start the web dashboard
    dashboard_port=7777,             # dashboard HTTP port
)
```

## Backend DSN Formats

| Backend | DSN Format |
|---------|-----------|
| **PostgreSQL** | `postgresql://user:pass@host:5432/dbname` |
| **SQLite** | `sqlite:///path/to/file.db` or `sqlite://:memory:` |
| **MySQL** | `mysql://user:pass@host:3306/dbname` |
| **MongoDB** | `mongodb://user:pass@host:27017/dbname` |
| **Redis** | `redis://host:6379/0` |

## Environment Variables

Gravtory reads these environment variables as fallbacks:

| Variable | Description | Default |
|----------|-------------|---------|
| `GRAVTORY_BACKEND` | Database connection string | `sqlite:///gravtory.db` |
| `GRAVTORY_NAMESPACE` | Run namespace | `"default"` |
| `GRAVTORY_TABLE_PREFIX` | Table/collection prefix | `"gravtory_"` |
| `GRAVTORY_LOG_LEVEL` | Logging level | `"INFO"` |
| `GRAVTORY_ENCRYPTION_KEY` | AES-256-GCM encryption key | (none) |

## Workflow Configuration

Per-workflow settings are passed via the `@workflow` decorator:

```python
from datetime import timedelta
from gravtory import workflow, step

@workflow(
    id="order-{order_id}",
    version=2,
    deadline=timedelta(hours=1),      # max workflow duration
    priority=5,                       # higher = claimed first
    namespace="orders",               # namespace override
)
class OrderWorkflow:

    @step(
        order=1,
        timeout=timedelta(seconds=30),
        retries=3,
        backoff="exponential",
        backoff_base=5.0,
    )
    async def charge_payment(self, order_id: str) -> dict:
        ...
```

## Retry Configuration

```python
from gravtory import RetryPolicy, BackoffPolicy

@step(
    order=1,
    retries=5,
    retry_policy=RetryPolicy(
        backoff=BackoffPolicy.EXPONENTIAL,
        base_delay=1.0,
        max_delay=60.0,
        jitter=True,
    ),
)
async def flaky_step(self, ctx):
    ...
```

## Logging

Gravtory uses Python's standard `logging` module under the `gravtory` logger:

```python
import logging

logging.getLogger("gravtory").setLevel(logging.DEBUG)
```

Sub-loggers: `gravtory.engine`, `gravtory.scheduler`, `gravtory.distributed`,
`gravtory.workers.health`, `gravtory.scheduling.leader`.
