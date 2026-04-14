# MySQL Backend

Enterprise-ready backend using MySQL 8+ with `SKIP LOCKED` for distributed work claiming.

## Installation

```bash
pip install gravtory[mysql]
```

## Connection

```python
from gravtory import Gravtory

grav = Gravtory("mysql://user:password@localhost:3306/mydb")
await grav.start()
```

## Features

| Feature | Implementation |
|---------|---------------|
| Work claiming | `SELECT ... FOR UPDATE SKIP LOCKED` (MySQL 8+) |
| Signal delivery | Polling (~1s latency) |
| Leader election | `GET_LOCK()` / `RELEASE_LOCK()` |
| Concurrency | InnoDB row-level locks |
| Schema | Auto-created on `start()` |

## Requirements

- MySQL 8.0+ (required for `SKIP LOCKED`)
- InnoDB storage engine (default in MySQL 8)

## Configuration

```python
grav = Gravtory(
    "mysql://user:pass@localhost:3306/mydb",
    pool_size=10,
)
```
