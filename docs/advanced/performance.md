# Performance

Guidelines for optimizing Gravtory performance in production.

## Serialization

The default JSON serializer is safe and readable. For better performance with large payloads:

```python
from gravtory.serialization.pickle import PickleSerializer

grav = Gravtory(
    "postgresql://localhost/mydb",
    serializer=PickleSerializer(),  # ~3x faster for complex objects
)
```

### Compression

Enable compression for large step outputs:

```python
from gravtory.serialization.compression import GzipCompressor

grav = Gravtory(
    "postgresql://localhost/mydb",
    compressor=GzipCompressor(),  # Reduces storage by 60-90%
)
```

## Database Tuning

### PostgreSQL

- **Connection pooling**: Set `pool_size=10, max_overflow=20` for production
- **Indexes**: Gravtory creates indexes automatically; ensure they exist on `workflow_runs(status)` and `pending_steps(run_id)`
- **WAL level**: Default is fine for single-machine; set `wal_level=logical` for replication
- **Vacuum**: Schedule regular `VACUUM ANALYZE` on Gravtory tables

### SQLite

- **WAL mode**: Enabled automatically for read concurrency
- **Journal size**: Set `PRAGMA journal_size_limit=67108864` for large workloads
- **Not for production**: Use PostgreSQL or MySQL for production deployments

## Worker Tuning

### Concurrency

```python
grav = Gravtory(
    "postgresql://localhost/mydb",
    workers=8,  # Match CPU cores for CPU-bound workflows
)
```

For I/O-bound workflows (API calls, database queries), use more workers than CPU cores.

### Rate Limiting

Prevent overwhelming external services:

```python
from gravtory.workers.rate_limit import RateLimiter

grav = Gravtory(
    "postgresql://localhost/mydb",
    rate_limiter=RateLimiter(max_per_second=50),
)
```

## Benchmarks

Run the included benchmark suite:

```bash
pytest tests/benchmarks/ --benchmark-min-rounds=10
```

### Measured Performance (single machine)

| Operation | Median Latency | Throughput |
|-----------|---------------|-----------|
| JSON serialize (100B) | 2.7μs | 63K/s |
| JSON serialize (1KB) | 12.8μs | 16.6K/s |
| JSON serialize (100KB) | 648μs | 276/s |
| JSON roundtrip (1KB) | 9.8μs | 12.8K/s |
| DAG topological sort (5 steps) | 2.3μs | 51.9K/s |
| DAG topological sort (50 steps) | 70.3μs | 1.9K/s |
| Cron next fire time | 5.7μs | 155K/s |
| Cron parse expression | 3.6μs | 52.7K/s |
| Workflow ID template | 780ns | 117K/s |
| Gzip compress (1KB) | 16.8μs | 10.5K/s |
| Gzip roundtrip (1KB) | 29.1μs | 6.9K/s |

Run benchmarks yourself: `python benchmarks/run_benchmarks.py`

## Monitoring

Use the built-in metrics to identify bottlenecks:

```python
grav = Gravtory(
    "postgresql://localhost/mydb",
    metrics_port=9090,
)
```

Key metrics to watch:
- `gravtory_step_duration_seconds` — slow steps indicate optimization targets
- `gravtory_retry_total` — high retry rates indicate flaky dependencies
- `gravtory_dlq_entries` — growing DLQ indicates unresolved failures
