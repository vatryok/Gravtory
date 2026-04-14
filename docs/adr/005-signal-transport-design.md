# ADR 005: Signal Transport Design

## Status

Accepted

## Date

2026-04-06

## Context

Gravtory signals enable external systems to send data to running workflows (e.g., human approvals, webhook events). The challenge is delivering signals with low latency across different database backends, some of which support push notifications (PostgreSQL LISTEN/NOTIFY) and some that don't (SQLite, MySQL).

## Decision

We use a **dual-path signal delivery architecture**:

1. **Database persistence** (all backends): Every signal is stored in the DB via `backend.send_signal()`. This guarantees durability — signals survive crashes.

2. **Transport layer** (push or poll): A pluggable `SignalTransport` handles real-time delivery:
   - `PostgreSQLSignalTransport`: Uses `LISTEN/NOTIFY` for zero-polling push delivery.
   - `PollingSignalTransport`: Polls the DB at a configurable interval (default 1s). Used for all other backends.

3. **Race-safe waiting**: `SignalHandler.wait()` races a local in-process `asyncio.Future` against the transport. The local future handles signals sent within the same process (fast path). The transport handles cross-process delivery.

4. **Auto-detection**: `SignalHandler._detect_transport()` automatically selects the best transport for the given backend.

## Consequences

- **Positive**: PostgreSQL users get sub-millisecond signal delivery via LISTEN/NOTIFY.
- **Positive**: All backends work out of the box with polling fallback.
- **Positive**: In-process signals resolve instantly without any DB round-trip.
- **Negative**: Polling backends have up to 1-second latency (configurable).
- **Negative**: LISTEN/NOTIFY requires a dedicated connection for the listener.

## Alternatives Considered

1. **Polling only**: Simpler but adds unnecessary latency for PostgreSQL. Rejected.
2. **External message broker (Redis Pub/Sub, RabbitMQ)**: Would add infrastructure requirements, contradicting the "zero infrastructure" promise. Rejected.
3. **WebSocket transport**: Would require a WebSocket server. Deferred to v0.2.
