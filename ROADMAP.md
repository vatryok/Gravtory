# Gravtory Roadmap

## v1.0.0 (Current Release — April 2026)

- Core workflow execution engine
- Atomic checkpointing with crash recovery
- 5 database backends (PostgreSQL, SQLite, MySQL, MongoDB, Redis)
- Saga compensation with reverse-order rollback
- Parallel execution with per-item checkpointing
- Conditional branching
- Human-in-the-loop signals
- Cron and interval scheduling
- Built-in dashboard and CLI
- OpenTelemetry tracing and Prometheus metrics
- AI/ML workflow support (LLM steps, streaming, model fallback)
- AES-256-GCM encryption at rest (PBKDF2 600,000 iterations)
- Pluggable serialization (JSON, Pickle, MsgPack)
- Gzip and LZ4 compression
- Middleware system
- Dead Letter Queue with retry
- Dashboard token authentication
- Stable API guarantee

## v1.1.0 (Target: Q3 2026)

- Workflow versioning with live migration
- Task stealing for distributed workers
- Custom backend interface for user-provided databases
- gRPC transport option for signals
- WebSocket transport for dashboard
- Improved worker auto-scaling
- Batch workflow execution API

## v1.2.0 (Target: Q4 2026)

- Temporal-compatible API adapter
- Kubernetes operator for auto-scaling workers
- GraphQL dashboard API
- Multi-region support
- Event sourcing mode
- Workflow templates and marketplace

## v2.0.0 (Target: Q2 2027)

- Long-term support (LTS) commitment
- Enterprise support offering
- Hosted Gravtory Cloud (SaaS)
- SOC 2 compliance documentation
- Advanced analytics and reporting

---

*This roadmap is subject to change based on community feedback and priorities.*
*Open a [feature request](https://github.com/vatryok/gravtory/issues/new?template=feature_request.md) to influence the roadmap.*
