# Backend Overview

Gravtory stores all workflow state — checkpoints, step outputs, schedules, signals, DLQ entries — in your existing database. You choose the backend that fits your stack.

## Supported Backends

| Backend | Best For | Distribution | Signals | Install Extra |
|---------|----------|-------------|---------|---------------|
| **PostgreSQL** | Production | `SKIP LOCKED` | `LISTEN/NOTIFY` | `gravtory[postgres]` |
| **SQLite** | Development, testing | File locks | Polling | `gravtory[sqlite]` |
| **MySQL 8+** | Enterprise | `SKIP LOCKED` | Polling | `gravtory[mysql]` |
| **MongoDB** | Document-heavy | `findOneAndUpdate` | Change Streams | `gravtory[mongodb]` |
| **Redis** | High-throughput | Lua scripts | Pub/Sub | `gravtory[redis]` |

## Choosing a Backend

- **Starting out?** Use SQLite — zero config, great for development
- **Production?** PostgreSQL is the recommended choice
- **Already on MySQL/MongoDB/Redis?** Use what you have — no need to add another database

## Connection Strings

```python
# PostgreSQL
grav = Gravtory("postgresql://user:pass@localhost:5432/mydb")

# SQLite (file)
grav = Gravtory("sqlite:///path/to/gravtory.db")

# SQLite (in-memory, for testing)
grav = Gravtory("sqlite:///:memory:")

# MySQL
grav = Gravtory("mysql://user:pass@localhost:3306/mydb")

# MongoDB
grav = Gravtory("mongodb://localhost:27017/mydb")

# Redis
grav = Gravtory("redis://localhost:6379/0")
```

## Auto-Detection

Gravtory auto-detects the backend from the connection string prefix. You can also specify it explicitly:

```python
from gravtory.backends.sqlite import SQLiteBackend

backend = SQLiteBackend("sqlite:///gravtory.db")
grav = Gravtory(backend=backend)
```

## Schema Initialization

All backends auto-create their tables/collections on first use:

```python
await grav.start()  # Creates tables if they don't exist
```

Or via the CLI:

```bash
gravtory init --backend postgresql://localhost/mydb
```

## In-Memory Backend

For testing, Gravtory provides an in-memory backend with no persistence:

```python
from gravtory.backends.memory import InMemoryBackend

grav = Gravtory(backend=InMemoryBackend())
```
