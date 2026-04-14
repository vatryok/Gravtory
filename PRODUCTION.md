# Production Deployment Guide

## Recommended Backend

**PostgreSQL ≥14** is the recommended backend for production. It provides:

- True concurrent access with row-level locking (`SELECT FOR UPDATE SKIP LOCKED`)
- ACID transactions for checkpoint consistency
- Connection pooling via `asyncpg`
- Proven scalability for workflow orchestration workloads

**SQLite** is for development and testing only. It uses a single connection and is not suitable for multi-worker deployments.

## Connection Pooling

PostgreSQL backend uses `asyncpg` connection pool. Configure via the connection string:

```python
grav = Gravtory(
    "postgresql://user:pass@host:5432/gravtory",
    workers=8,
)
```

For high-throughput deployments, tune pool size at the `asyncpg` level (Gravtory uses `asyncpg.create_pool` defaults: `min_size=10`, `max_size=10`).

## Worker Scaling

### Single-Node (LocalWorker)

```python
grav = Gravtory("postgresql://...", workers=8)
```

Each worker runs as an async task within the same process. Suitable for I/O-bound workflows.

### Multi-Node (WorkerPool)

```python
from gravtory.workers.pool import WorkerPool

def setup_registry(registry):
    registry.register(my_workflow)

pool = WorkerPool(
    count=4,
    backend_url="postgresql://...",
    registry_setup_fn=setup_registry,
)
await pool.start()
```

Each worker runs in a separate process. The supervisor restarts crashed workers with exponential backoff (max 5 restarts).

### Kubernetes

Use the provided manifests in `deploy/kubernetes/`:

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/secret.yaml
kubectl apply -f deploy/kubernetes/configmap.yaml
kubectl apply -f deploy/kubernetes/deployment.yaml
kubectl apply -f deploy/kubernetes/service.yaml
kubectl apply -f deploy/kubernetes/hpa.yaml
kubectl apply -f deploy/kubernetes/pdb.yaml
kubectl apply -f deploy/kubernetes/networkpolicy.yaml
```

The HPA scales engine pods based on CPU (70%) and memory (80%) utilization.

## Monitoring & Alerting

### Prometheus Metrics

Enable metrics by setting `metrics_port`:

```python
grav = Gravtory("postgresql://...", metrics_port=9090)
```

Key metrics:
- `gravtory_workflows_total` — workflow execution counts by status
- `gravtory_step_duration_seconds` — step execution latency
- `gravtory_active_workers` — currently active workers
- `gravtory_dlq_size` — dead letter queue depth
- `gravtory_pending_steps_count` — pending step queue depth

### OpenTelemetry Tracing

```python
grav = Gravtory("postgresql://...", otel_endpoint="http://otel-collector:4317")
```

### Structured Logging

```python
from gravtory.observability.logging import configure_logging
configure_logging(level="INFO", fmt="json")
```

## Graceful Shutdown

Gravtory handles `SIGTERM` and `SIGINT` signals automatically:

1. Stops accepting new workflows
2. Drains active tasks (30s timeout by default)
3. Cancels remaining tasks after timeout
4. Closes database connections

In Kubernetes, set `terminationGracePeriodSeconds: 30` (already configured in the provided manifests).

## Backup & Restore

### PostgreSQL

```bash
# Backup
pg_dump -U gravtory gravtory > gravtory_backup.sql

# Restore
psql -U gravtory gravtory < gravtory_backup.sql
```

Critical tables to back up:
- `gravtory_workflow_runs` — workflow execution state
- `gravtory_step_outputs` — checkpoint data
- `gravtory_schema_version` — migration tracking

## Security Checklist

- [ ] Set `GRAVTORY_DASHBOARD_TOKEN` environment variable
- [ ] Set `GRAVTORY_ENCRYPTION_KEY` for checkpoint encryption
- [ ] Use PostgreSQL with TLS (`sslmode=require`)
- [ ] Apply Kubernetes NetworkPolicy
- [ ] Do **not** use pickle serializer unless absolutely necessary
- [ ] Review `.env` file permissions (should be `600`)
- [ ] Pin container image to specific version tag
