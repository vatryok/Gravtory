# Retry & Backoff

Gravtory provides configurable retry policies with multiple backoff strategies, jitter, and circuit breaker integration.

## Basic Retry

```python
from gravtory import step

@step(1, retries=3)
async def call_api(self, url: str) -> dict:
    return await httpx.get(url).json()
```

On failure, the step retries up to 3 times with a 1-second constant delay.

## Backoff Strategies

### Constant (Default)

```python
@step(1, retries=5, backoff="constant", backoff_base=2.0)
async def fetch(self, url: str) -> dict:
    ...
# Retries at: 2s, 2s, 2s, 2s, 2s
```

### Linear

```python
@step(1, retries=5, backoff="linear", backoff_base=1.0)
async def fetch(self, url: str) -> dict:
    ...
# Retries at: 1s, 2s, 3s, 4s, 5s
```

### Exponential

```python
@step(1, retries=5, backoff="exponential", backoff_base=2.0)
async def fetch(self, url: str) -> dict:
    ...
# Retries at: 2s, 4s, 8s, 16s, 32s
```

## Jitter

All backoff strategies include automatic jitter (±25%) to prevent thundering herd problems when multiple workflows retry simultaneously.

## Selective Retry

### retry_on — Only retry specific exceptions

```python
import httpx

@step(1, retries=3, retry_on=[httpx.TimeoutException, httpx.ConnectError])
async def call_api(self, url: str) -> dict:
    return await httpx.get(url).json()
# Only retries on timeout/connection errors. Other exceptions fail immediately.
```

### abort_on — Never retry specific exceptions

```python
@step(1, retries=3, abort_on=[ValueError, PermissionError])
async def process(self, data: dict) -> dict:
    ...
# Retries on all exceptions EXCEPT ValueError and PermissionError.
```

## Circuit Breaker

Protect external services from being overwhelmed by repeated failures:

```python
from gravtory.retry.circuit_breaker import CircuitBreaker

breaker = CircuitBreaker(
    failure_threshold=5,    # Open after 5 consecutive failures
    recovery_timeout=30.0,  # Try again after 30 seconds
    half_open_max=2,        # Allow 2 test requests in half-open state
)

@step(1, retries=3, circuit_breaker=breaker)
async def call_external(self, data: dict) -> dict:
    return await external_api.post(data)
```

Circuit breaker states:
- **Closed** — requests flow normally
- **Open** — requests fail immediately (no network call)
- **Half-open** — limited test requests to check recovery

## Dead Letter Queue

When all retries are exhausted, the failed step is added to the DLQ for manual inspection:

```python
# List DLQ entries
entries = await grav.dlq.list()

# Retry a DLQ entry
await grav.dlq.retry(entry_id)

# Purge old entries
await grav.dlq.purge(older_than=timedelta(days=30))
```
