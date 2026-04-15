# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-04-14

### Added
- Core durable execution engine with atomic step checkpointing
- Workflow and step decorators (`@workflow`, `@step`) with DAG-based dependency resolution
- Crash-safe execution with exact resume from checkpoints
- Idempotent workflow execution — no step ever runs twice
- SQLite backend for development and testing
- PostgreSQL backend for production deployments
- MySQL backend for enterprise environments
- MongoDB backend for document-heavy workloads
- Redis backend for high-throughput scenarios
- Saga compensation with automatic rollback on failure
- Retry policies: constant, linear, and exponential backoff with jitter
- Circuit breaker for external service protection
- Dead Letter Queue (DLQ) for failed workflow inspection and replay
- Parallel step execution with configurable concurrency limits
- Conditional branching in workflows
- Signal handling for human-in-the-loop and external event patterns
- Cron and interval scheduling with timezone support
- Distributed worker coordination via database (no message broker required)
- Priority queues and rate limiting
- OpenTelemetry tracing and Prometheus metrics integration
- Built-in web dashboard for workflow monitoring
- CLI tool (`gravtory`) for workflow management and operations
- Testing framework with `WorkflowTestRunner`, mocks, and time-travel utilities
- Workflow introspection API
- AI/ML native support: LLM steps, streaming, token tracking, model fallback, and agent loops
- JSON serializer (default) and Pickle serializer with allowlist-based `RestrictedUnpickler`
- Gzip, LZ4, and Zstandard compression support
- AES-256-GCM checkpoint encryption with key rotation via `KeyManager`
- Token-based dashboard authentication and CORS origin control
- Pydantic schema enforcement for step inputs and outputs
- Audit logging for workflow operations
- Workflow versioning and migration support
- Admin operations: cancel, retry, and purge workflows
- FastAPI integration with lifespan management
- `py.typed` marker (PEP 561) for full type checker compatibility
- Comprehensive test suite: unit, integration, E2E, property-based, and mutation tests

## [0.1.0] - 2026-04-10

### Added
- Initial proof-of-concept release
- Core execution engine with step checkpointing
- SQLite and in-memory backends
- Basic workflow and step decorators

[1.0.0]: https://github.com/vatryok/gravtory/releases/tag/v1.0.0
[0.1.0]: https://github.com/vatryok/gravtory/releases/tag/v0.1.0
