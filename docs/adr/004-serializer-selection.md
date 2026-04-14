# ADR 004: Default Serializer Selection

## Status

Accepted

## Date

2026-04-06

## Context

Gravtory serializes step outputs before checkpointing to the database. The serializer choice affects security, performance, compatibility, and type fidelity. Three serializers are supported: JSON, MsgPack, and Pickle.

## Decision

**JSON is the default serializer.** Users must explicitly opt in to alternatives.

| Serializer | Default | Security | Speed | Type Fidelity | Cross-Language |
|-----------|---------|----------|-------|---------------|----------------|
| JSON | **Yes** | Safe | Good | Extended* | Yes |
| MsgPack | No | Safe | Fast | Binary types | Yes |
| Pickle | No | **Dangerous** | Fast | Full Python | No |

*Extended: Custom `GravtoryJSONEncoder` handles datetime, UUID, Decimal, bytes, sets, Pydantic models, dataclasses, and enums via tagged round-trip encoding (`__grav_type__` markers).

### Pickle Safety

Pickle is gated behind a runtime warning. Instantiating `PickleSerializer` without an `allowed_classes` allowlist emits a `UserWarning` and a security log message. A `RestrictedUnpickler` is used when an allowlist is provided, rejecting any class not explicitly permitted.

## Consequences

- **Positive**: JSON-by-default eliminates remote code execution risk from untrusted checkpoint data.
- **Positive**: Tagged encoding preserves Python-specific types (datetime, Decimal, etc.) through JSON round-trips.
- **Positive**: MsgPack available for performance-sensitive workloads without security trade-offs.
- **Negative**: JSON is slower than pickle for large/complex objects.
- **Negative**: Custom classes require Pydantic models or dataclasses for automatic serialization. Raw objects need pickle.

## Alternatives Considered

1. **Pickle as default**: Maximum compatibility but unacceptable security risk. Rejected.
2. **MsgPack as default**: Faster than JSON but requires an optional dependency. Rejected for zero-dependency-by-default goal.
3. **No default — require explicit choice**: Forces users to make a decision. Rejected for DX — sensible defaults are better.
