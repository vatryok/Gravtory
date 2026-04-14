# SQLite Backend

The easiest backend to get started with. Zero configuration — just point to a file path.

## Installation

SQLite support is included by default (uses Python's built-in `sqlite3` module):

```bash
pip install gravtory
```

## Connection

```python
from gravtory import Gravtory

# File-based (persists across restarts)
grav = Gravtory("sqlite:///path/to/gravtory.db")

# In-memory (for testing, no persistence)
grav = Gravtory("sqlite:///:memory:")

await grav.start()
```

## Features

| Feature | Implementation |
|---------|---------------|
| Work claiming | File-level locks |
| Signal delivery | Polling (~1s latency) |
| Leader election | N/A (single process) |
| Concurrency | WAL mode for read concurrency |
| Schema | Auto-created on `start()` |

## Limitations

- **Single machine only** — file locks don't work across machines
- **Limited concurrency** — SQLite serializes writes
- **Polling signals** — no push-based notification

## When to Use

- Local development
- Testing and CI
- Single-process deployments
- Prototyping before moving to PostgreSQL

## Migration to PostgreSQL

When ready for production, change the connection string:

```python
# Development
grav = Gravtory("sqlite:///dev.db")

# Production
grav = Gravtory("postgresql://user:pass@prod-db/myapp")
```

No code changes needed — the API is identical across all backends.
