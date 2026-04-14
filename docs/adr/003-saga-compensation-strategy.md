# ADR 003: Saga Compensation Strategy

## Status

Accepted

## Date

2026-04-06

## Context

When a workflow step fails, previously completed steps may need to be "undone" (e.g., refund a charge, release inventory). This is the Saga pattern. We need to decide:

1. How compensation handlers are defined and associated with steps.
2. What order compensations run in.
3. What happens when a compensation itself fails.

## Decision

### Definition
Compensation handlers are defined as methods on the workflow class, referenced by name via the `compensate=` parameter on `@step`:

```python
@step(1, compensate="refund")
async def charge(self, amount): ...

async def refund(self, output):
    await bank.refund(output["charge_id"])
```

### Execution Order
Compensations run in **reverse step order** (last completed step first). This matches the standard Saga pattern — undo the most recent action first.

### Failure Handling: Best-Effort
If a compensation handler fails:
1. The error is logged.
2. The failed compensation is added to the Dead Letter Queue (DLQ).
3. **Remaining compensations still execute** (best-effort continuation).
4. Final workflow status: `compensation_failed` if any failed, `compensated` if all succeeded.

### Activation
Saga mode is enabled via `@saga` decorator or `saga=True` on `@workflow()`.

## Consequences

- **Positive**: Best-effort means one broken compensation doesn't prevent others from running.
- **Positive**: DLQ captures failed compensations for manual retry.
- **Positive**: Reverse order is intuitive and matches industry standard.
- **Negative**: No automatic retry of failed compensations (manual via DLQ).
- **Negative**: Compensation handlers must be idempotent — they may run more than once if retried from DLQ.

## Alternatives Considered

1. **Fail-fast**: Stop on first compensation failure. Simpler but leaves partially compensated state. Rejected.
2. **Automatic compensation retry**: Retry failed compensations with backoff. Adds complexity and may not be safe for all handlers. Deferred to v0.2.
3. **Choreography-based saga**: Each step publishes events, compensations triggered by event handlers. More decoupled but harder to reason about ordering. Rejected for simplicity.
