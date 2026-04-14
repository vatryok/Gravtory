# Migration Guide

## Upgrading from v0.1.0 to v1.0.0

### Breaking Changes

#### Pickle Serializer Requires Allowlist (B-001)

The `PickleSerializer` no longer accepts arbitrary classes by default. You **must** provide an explicit `allowed_classes` list or use `unsafe_pickle=True` (strongly discouraged in production).

```python
# Before (insecure — no longer works without explicit opt-in)
grav = Gravtory("sqlite:///db", serializer="pickle")

# After — provide allowlist via CheckpointEngine
from gravtory.core.checkpoint import CheckpointEngine
checkpoint = CheckpointEngine(serializer="pickle")  # raises ConfigurationError

# Safe usage:
from gravtory.serialization.pickle import PickleSerializer
ser = PickleSerializer(allowed_classes=[MyDataClass, MyResult])
```

#### Dashboard Authentication Required (S-001)

The dashboard now requires an `auth_token` when not in development mode. If you don't provide one, a random token is generated and logged at startup.

```python
# Set via environment variable
# GRAVTORY_DASHBOARD_TOKEN=your-secret-token

# Or pass explicitly
grav = Gravtory("postgresql://...", dashboard=True, dashboard_token="your-token")
```

#### Docker Compose Requires .env File (I-003)

Database credentials are no longer hardcoded. Create a `.env` file from `.env.example`:

```bash
cp .env.example .env
# Edit .env with your credentials
docker compose up
```

### Database Schema Migration

Gravtory v1.0.0 includes a schema migration framework. On first start with an existing database:

1. The `schema_version` table is created automatically.
2. Pending migrations are applied sequentially.
3. Each migration is recorded with its version number.

**No manual SQL is required** for upgrading from v0.1.0 schemas.

If you need to verify the current schema version:

```sql
-- PostgreSQL / SQLite
SELECT version FROM gravtory_schema_version ORDER BY applied_at DESC LIMIT 1;
```

### Backend API Changes

If you have custom `Backend` implementations, you must add:

- `update_step_output(run_id, step_order, output_data)` — update existing checkpoint data
- `count_dlq(*, namespace)` — efficient DLQ entry count

### Worker Pool Changes

`WorkerPool` now accepts `registry_setup_fn` — a callback that populates the `WorkflowRegistry` in forked child processes. Without it, workers will have an empty registry and cannot execute workflows.

```python
def setup_registry(registry):
    registry.register(my_workflow_definition)

pool = WorkerPool(4, "postgresql://...", registry_setup_fn=setup_registry)
```

## Version Compatibility Matrix

| Gravtory Version | Schema Version | Python | PostgreSQL | SQLite |
|-----------------|----------------|--------|------------|--------|
| 1.0.0           | 1              | ≥3.10  | ≥14        | ≥3.35  |
| 0.1.0           | 1              | ≥3.10  | ≥14        | ≥3.35  |
