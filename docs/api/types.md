# Types API Reference

Core dataclasses and enums used throughout Gravtory. All types are importable
directly from the top-level `gravtory` package.

## Workflow Types

- **`WorkflowRun`** — represents a single execution of a workflow
- **`WorkflowDefinition`** — the registered structure of a workflow (steps, config)
- **`WorkflowConfig`** — per-workflow settings (priority, timeout, concurrency)
- **`WorkflowStatus`** — enum: `pending`, `running`, `completed`, `failed`, `cancelled`

## Step Types

- **`StepDefinition`** — metadata for a single step (order, retries, timeout)
- **`StepOutput`** — checkpoint record for a completed step
- **`StepResult`** — execution result returned by the engine
- **`PendingStep`** — a step waiting to be claimed by a worker
- **`StepStatus`** — enum: `pending`, `running`, `completed`, `failed`

## Other Types

- **`Signal`** / **`SignalWait`** / **`SignalConfig`** — inter-workflow communication
- **`Schedule`** / **`ScheduleType`** — cron, interval, one-time, event triggers
- **`Compensation`** — saga rollback record
- **`DLQEntry`** — dead letter queue entry
- **`Lock`** — distributed lock state
- **`WorkerInfo`** / **`WorkerStatus`** — worker registration and health

---

::: gravtory.core.types
    options:
      show_root_heading: false
      members_order: source
