# Gravtory

**Crash-proof Python workflows with zero infrastructure.**

Gravtory is a Python library that gives you Temporal-level reliability using only your existing database. No separate server, no message broker, no Redis.

## Why Gravtory?

Every production app eventually needs **workflows that don't break**. Gravtory provides:

- **Atomic checkpointing** — step outputs saved in a single DB transaction
- **Exact resume** — restart from the precise failed step, never re-execute completed ones
- **5 database backends** — PostgreSQL, SQLite, MySQL, MongoDB, Redis
- **Zero infrastructure** — `pip install gravtory[postgres]` is all you need

## Quick Links

- [Installation](getting-started/installation.md)
- [Quick Start](getting-started/quickstart.md)
- [Core Concepts](getting-started/concepts.md)
- [API Reference](api/types.md)
