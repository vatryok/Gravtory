# Installation

## Quick Install

```bash
# Core + PostgreSQL (recommended)
pip install gravtory[postgres]

# Core + SQLite (development)
pip install gravtory[sqlite]

# Everything
pip install gravtory[all]
```

## Requirements

- Python 3.10+
- One of: PostgreSQL, SQLite, MySQL 8+, MongoDB, Redis

## Backend-Specific Dependencies

| Backend | Extra | Package |
|---------|-------|---------|
| PostgreSQL | `postgres` | asyncpg |
| SQLite | `sqlite` | aiosqlite |
| MySQL | `mysql` | aiomysql |
| MongoDB | `mongodb` | motor |
| Redis | `redis` | redis[hiredis] |
