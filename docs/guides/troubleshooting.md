# Troubleshooting Guide

Common issues and solutions when working with Gravtory.

---

## Installation Issues

### `ModuleNotFoundError: No module named 'asyncpg'`

**Cause**: You're using a PostgreSQL connection string but didn't install the PostgreSQL extra.

**Fix**:
```bash
pip install gravtory[postgres]
```

Same pattern for other backends:
```bash
pip install gravtory[mysql]      # MySQL
pip install gravtory[mongodb]    # MongoDB
pip install gravtory[redis]      # Redis
pip install gravtory[encryption] # AES-256-GCM encryption
```

### `pip install` fails with version conflicts

**Cause**: Dev dependency versions may conflict with your environment.

**Fix**: Install only the runtime package without dev extras:
```bash
pip install gravtory            # Core only
pip install gravtory[postgres]  # Core + PostgreSQL
```

---

## Connection Issues

### `BackendConnectionError: Cannot connect to PostgreSQL backend`

**Cause**: Database is not running, connection string is wrong, or network issue.

**Checklist**:
1. Verify the database is running: `pg_isready -h localhost -p 5432`
2. Check your connection string format: `postgresql://user:password@host:port/dbname`
3. Ensure the database exists: `createdb gravtory`
4. Check firewall/network rules if connecting to a remote host

### `BackendConnectionError: Not connected. Call initialize() first.`

**Cause**: You're calling backend methods before `Gravtory.start()` or `backend.initialize()`.

**Fix**:
```python
grav = Gravtory("postgresql://localhost/mydb")
await grav.start()  # Must call this first!
result = await grav.run(MyWorkflow, order_id="abc")
```

Or use the context manager:
```python
async with Gravtory("postgresql://localhost/mydb") as grav:
    result = await grav.run(MyWorkflow, order_id="abc")
```

---

## Workflow Issues

### Workflow runs but step outputs are lost on resume

**Cause**: If you're using a custom backend without going through the Gravtory engine, the CheckpointEngine may not be connected.

**Fix**: Always use `Gravtory(...)` to create your engine — it automatically sets up the checkpoint pipeline (serialize → compress → encrypt).

### `WorkflowNotFoundError: Workflow 'X' is not registered`

**Cause**: The workflow class was not registered before calling `grav.run()`.

**Fix**: Use `@grav.workflow(id="...")` to decorate your workflow class, and call `grav.start()` before running:
```python
grav = Gravtory("sqlite:///gravtory.db")

@grav.workflow(id="my-workflow-{id}")
class MyWorkflow:
    @step(1)
    async def do_work(self, id: str) -> dict:
        return {"done": True}

await grav.start()  # Registers all pending workflows
await grav.run(MyWorkflow, id="123")
```

### `ConfigurationError: Workflow ID template 'X' requires parameter Y`

**Cause**: Your workflow ID template has placeholders that weren't provided in `grav.run()` kwargs.

**Fix**: Ensure all template variables are passed:
```python
@grav.workflow(id="order-{order_id}-{region}")
class OrderWorkflow: ...

# Must provide both order_id AND region:
await grav.run(OrderWorkflow, order_id="abc", region="us-east")
```

### Saga compensations not running

**Cause**: The `@saga` decorator or `saga=True` flag may not be set.

**Fix**: Enable saga mode on the workflow:
```python
@grav.workflow(id="transfer-{id}", saga=True)
class TransferWorkflow:
    @step(1, compensate="refund")
    async def charge(self, amount): ...

    async def refund(self, output): ...
```

Or use the `@saga` decorator:
```python
from gravtory import saga

@grav.workflow(id="transfer-{id}")
@saga
class TransferWorkflow: ...
```

---

## Performance Issues

### Startup is slow with many incomplete workflows

**Cause**: `Gravtory.start()` recovers all incomplete workflows concurrently, but with many workflows this can take time.

**Fix**: The recovery uses bounded concurrency (default 10). For faster startup, you can reduce the number of recovered workflows by cleaning up old runs:
```bash
gravtory workflows list --status=failed
gravtory workflows cancel <run-id>
```

### Steps are executing slowly

**Checklist**:
1. **Check retries**: Steps with `retries=5, backoff="exponential"` may wait up to 32 seconds between attempts.
2. **Check rate limiting**: Steps with `rate_limit` configured will throttle execution.
3. **Check database latency**: Each step checkpoint requires a DB write. Use a local database for development.
4. **Enable dashboard**: Set `dashboard=True` to monitor step durations in real-time.

---

## CLI Issues

### `gravtory` command not found

**Cause**: The package wasn't installed with the CLI entry point, or the virtual environment isn't activated.

**Fix**:
```bash
pip install -e .          # Install in editable mode
gravtory --help           # Should work now
```

### `gravtory list` shows no results

**Cause**: The CLI defaults to `sqlite:///gravtory.db`. If your workflows use a different backend, specify it:
```bash
gravtory -b postgresql://localhost/mydb workflows list
# Or set the environment variable:
export GRAVTORY_BACKEND=postgresql://localhost/mydb
gravtory workflows list
```

---

## Dashboard Issues

### Dashboard page is blank / connection refused

**Cause**: Dashboard may not be started, or you're connecting to the wrong port.

**Fix**:
```python
# In code:
grav = Gravtory("sqlite:///gravtory.db", dashboard=True, dashboard_port=7777)
await grav.start()
# Dashboard available at http://127.0.0.1:7777

# Or via CLI:
gravtory dashboard --port 7777
```

### CORS errors in browser console

**Cause**: The dashboard CORS middleware only allows explicitly configured origins.

**Fix**: Pass `allowed_origins` when creating the Dashboard:
```python
from gravtory.dashboard.server import Dashboard
dash = Dashboard(backend, registry, allowed_origins=["http://localhost:3000"])
```

---

## Testing Issues

### Tests pass with InMemoryBackend but fail with real database

**Cause**: The execution engine now serializes step outputs through the CheckpointEngine before saving. If your tests bypass the engine and call backend methods directly, behavior may differ.

**Fix**: Use `WorkflowTestRunner` which handles serialization correctly:
```python
from gravtory.testing import WorkflowTestRunner

runner = WorkflowTestRunner()
result = await runner.run(MyWorkflow, order_id="test-1")
assert result.status == WorkflowStatus.COMPLETED
```

---

## Getting Help

- **GitHub Issues**: [github.com/vatryok/gravtory/issues](https://github.com/vatryok/gravtory/issues)
- **Security Issues**: Email vatryok@protonmail.com (do NOT open public issues)
- **Documentation**: [Gravtory Docs](https://gravtory.dev)
