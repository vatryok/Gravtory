# Technical Debt Register

Tracked items from the v1.0.0 audit. Each entry has a severity, owner area, and
recommended approach. Items are resolved by PR reference.

---

## A-005: Dual Compensation Paths

**Severity**: Medium
**Area**: `core/execution.py`, `core/saga.py`
**Status**: Open

### Problem

Compensation logic exists in two places:

1. `ExecutionEngine._trigger_compensations` (automatic, on step failure)
2. `SagaCoordinator.trigger` (explicit saga invocation)

Both iterate completed steps in reverse and call compensation handlers, but with
slightly different error handling and DLQ integration. This duplication risks
drift and makes compensation behaviour harder to reason about.

### Recommended Approach

1. Extract a shared `run_compensations(backend, registry, run_id, definition,
   completed_steps)` coroutine into `core/compensation.py`.
2. Have both `ExecutionEngine._trigger_compensations` and
   `SagaCoordinator.trigger` delegate to this shared function.
3. The shared function should:
   - Accept a callback for DLQ/compensation persistence (saga needs
     `save_compensation`, engine only needs `add_to_dlq`).
   - Return a `CompensationResult` dataclass with per-step outcomes.
4. Add integration tests that verify identical behaviour from both paths.

### Risk

- State machine transitions differ slightly between the two paths.
- `SagaCoordinator` persists individual `Compensation` records; the engine does
  not. The shared function must support both modes.
- Regression risk is medium; thorough test coverage is required before merging.

### References

- Audit report Section 5, item A-005
- `src/gravtory/core/execution.py:856-941`
- `src/gravtory/core/saga.py:70-164`
