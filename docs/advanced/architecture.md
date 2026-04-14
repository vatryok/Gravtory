# Architecture

An overview of Gravtory's internal architecture and design decisions.

## High-Level Architecture

```
┌─────────────────────────────────────────────────┐
│                  User Code                       │
│  @workflow, @step, @saga, @schedule, @signal    │
└──────────────┬──────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────┐
│              Gravtory Engine                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │
│  │ Registry │ │ Executor │ │ Saga Coordinator │ │
│  └──────────┘ └──────────┘ └──────────────────┘ │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │
│  │ DAG      │ │Checkpoint│ │ Signal Handler   │ │
│  └──────────┘ └──────────┘ └──────────────────┘ │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │
│  │Scheduler │ │ Workers  │ │ Middleware        │ │
│  └──────────┘ └──────────┘ └──────────────────┘ │
└──────────────┬──────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────┐
│              Backend (Abstract)                   │
│  PostgreSQL │ SQLite │ MySQL │ MongoDB │ Redis   │
└─────────────────────────────────────────────────┘
```

## Core Components

### WorkflowRegistry

Stores all registered workflow definitions. When `@workflow` is applied, the class is scanned for `@step`-decorated methods and a `WorkflowDefinition` is created.

### DAG (Directed Acyclic Graph)

Built from step dependencies. Validates:
- No circular dependencies
- All dependency references valid
- No orphan steps

Provides topological sort for execution order and identifies steps that can run in parallel.

### ExecutionEngine

The core execution loop:

1. Initialize or load workflow run from backend
2. Build DAG from workflow definition
3. Execute steps in topological order
4. For each step: resolve inputs → execute → checkpoint output
5. On failure: trigger saga compensation or add to DLQ

### CheckpointEngine

Atomically saves step outputs to the backend within a database transaction. On resume, loads completed step outputs and skips re-execution.

### SagaCoordinator

Manages compensation on workflow failure:

1. Receives failure notification
2. Iterates completed steps in reverse order
3. Calls compensation handler for each step
4. Records compensation results

### Serialization Pipeline

```
Python object → Serializer → Compressor → Encryptor → bytes → Database
Database → bytes → Decryptor → Decompressor → Deserializer → Python object
```

Default: JSON serializer, no compression, no encryption.

## Design Principles

- **Zero infrastructure** — everything runs in-process, state in your DB
- **Crash-safe by default** — atomic checkpointing, idempotent execution
- **Backend-agnostic** — abstract `Backend` interface, 5 implementations
- **Decorator-driven** — minimal boilerplate, Python-native API
- **Composable** — middleware, serializers, compressors, encryptors are all pluggable
