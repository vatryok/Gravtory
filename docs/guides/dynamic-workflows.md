# Dynamic Workflow Persistence

Gravtory supports **dynamic workflow definitions** — workflows that are
registered, updated, or removed at runtime and persisted across restarts.

## Overview

Static workflows are defined in Python code with `@workflow` / `@step`
decorators and registered at startup.  Dynamic workflows extend this model
by storing the workflow definition JSON in the backend so that:

- New workflows can be deployed without restarting the engine.
- Workflow definitions survive process restarts.
- Multiple engine instances share the same definition set.

## Backend API

Every backend implements three methods:

| Method | Description |
|--------|-------------|
| `save_workflow_definition(name, version, definition_json)` | Upsert a definition. |
| `load_workflow_definitions() → list[tuple[name, version, json]]` | Load all persisted definitions. |
| `delete_workflow_definition(name, version)` | Remove a single version. |

### Example

```python
import json
from gravtory import Gravtory

async with Gravtory(backend="sqlite:///app.db") as grav:
    # Save a workflow definition
    definition = {
        "name": "etl-pipeline",
        "version": 1,
        "steps": [
            {"order": 1, "name": "extract", "function": "etl.extract"},
            {"order": 2, "name": "transform", "depends_on": [1]},
            {"order": 3, "name": "load", "depends_on": [2]},
        ],
    }
    await grav.backend.save_workflow_definition(
        "etl-pipeline", 1, json.dumps(definition)
    )

    # Load all definitions
    defs = await grav.backend.load_workflow_definitions()
    for name, version, json_str in defs:
        print(f"{name} v{version}")

    # Delete a definition
    await grav.backend.delete_workflow_definition("etl-pipeline", 1)
```

## Versioning

Multiple versions of the same workflow can coexist.  The `(name, version)`
pair is the primary key.  Use `save_workflow_definition` with the same key
to update (upsert semantics).

## Backend-Specific Notes

### Redis

Redis stores definitions as hashes with an optional TTL.  Configure via
the `wfdef_ttl` constructor parameter (seconds, default: no expiry):

```python
from gravtory.backends.redis import RedisBackend

backend = RedisBackend("redis://localhost", wfdef_ttl=3600)  # 1-hour TTL
```

### MongoDB

MongoDB creates a unique compound index on `(name, version)` during
`initialize()` for efficient lookups and upsert safety.

### PostgreSQL / MySQL

Uses `ON CONFLICT DO UPDATE` / `ON DUPLICATE KEY UPDATE` for atomic upserts.

### SQLite

Uses `INSERT OR REPLACE` via the shared schema definition.

## Circuit Breaker State Persistence

The circuit breaker pattern is also persisted across restarts:

| Method | Description |
|--------|-------------|
| `save_circuit_state(name, state_json)` | Persist circuit breaker state. |
| `load_circuit_state(name) → str \| None` | Load persisted state, or `None`. |

This enables cross-worker circuit breaker sharing — when one worker trips
a breaker, all workers see it immediately via the shared backend.

### Redis TTL

Circuit breaker state in Redis defaults to a 24-hour TTL (`circuit_ttl=86400`).
Set to `None` for no expiry:

```python
backend = RedisBackend("redis://localhost", circuit_ttl=None)
```

## Schema Migration

The `workflow_definitions` and `circuit_breakers` tables were added in
**schema v2**.  Existing v1 databases are automatically migrated when
`SchemaMigrator.check_and_migrate()` runs (called during startup).

See [MIGRATION.md](../../MIGRATION.md) for details.
