# ADR 002: Backend Abstraction Layer

## Status

Accepted

## Date

2026-04-06

## Context

Gravtory must support multiple databases (PostgreSQL, SQLite, MySQL, MongoDB, Redis) with a single API. Each database has fundamentally different query languages, concurrency models, and transaction semantics. We need an abstraction that:

1. Hides database-specific details from the execution engine.
2. Allows adding new backends without modifying core code.
3. Preserves database-native performance features (e.g., `SKIP LOCKED` in PostgreSQL).

## Decision

We use an **abstract base class** (`Backend`) that defines ~30 async methods grouped by domain (workflow runs, step outputs, signals, scheduling, locks, DLQ, workers). Each concrete backend implements all methods using database-native operations.

Key design choices:
- **No ORM**: Direct SQL/queries for maximum control and performance.
- **Parameterized queries only**: All user-supplied values passed as parameters, never interpolated into SQL strings.
- **Schema module**: Shared SQL schema templates with dialect-specific tokens (e.g., `SERIAL` vs `AUTOINCREMENT`).
- **Connection string auto-detection**: `create_backend("postgresql://...")` automatically selects the correct backend class.
- **`_ensure_connected()` pattern**: Every method validates the connection is alive before operating, raising `BackendConnectionError` on failure.

## Consequences

- **Positive**: Clean separation — execution engine has zero knowledge of SQL or database specifics.
- **Positive**: Adding a new backend (e.g., CockroachDB) only requires implementing the `Backend` ABC.
- **Positive**: Each backend can use database-native features (`LISTEN/NOTIFY`, Lua scripts, change streams).
- **Negative**: ~30 methods per backend means significant implementation effort for each new backend.
- **Negative**: Behavior differences between backends (e.g., transaction isolation levels) can cause subtle bugs if not tested per-backend.

## Alternatives Considered

1. **SQLAlchemy ORM**: Would simplify SQL backends but doesn't help with MongoDB/Redis. Also adds a heavy dependency and limits database-specific optimizations. Rejected.
2. **Single SQL backend + adapters**: Only support SQL databases, adapt dialect differences. Would exclude MongoDB and Redis. Rejected.
3. **Event sourcing abstraction**: Store events instead of state. More complex, overkill for the checkpoint use case. Rejected for v0.1.
