# Core Concepts

## Workflows

A **workflow** is a sequence of steps that execute in a defined order. Each workflow has a unique ID template and one or more steps.

## Steps

A **step** is a single unit of work within a workflow. Steps are:

- **Ordered** — each step has a numeric order
- **Checkpointed** — outputs are atomically saved to the database
- **Idempotent** — a completed step is never re-executed

## Checkpointing

When a step completes, its output is **atomically persisted** to the database. If the process crashes, the output is still there. On resume, Gravtory loads it instead of re-running the step.

## Backends

A **backend** is the database adapter that Gravtory uses. All state — workflow runs, step outputs, signals, locks — lives in your database.

## Sagas

A **saga** is a workflow with compensation handlers. If a step fails, previously completed steps are rolled back in reverse order by executing their compensation functions.

## Signals

A **signal** sends data to a running workflow from outside. This enables human-in-the-loop patterns and inter-workflow communication.
