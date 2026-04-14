# ETL Pipeline Example

A data pipeline demonstrating parallel processing with per-item checkpointing and conditional branching.

## What It Demonstrates

- Parallel step execution with bounded concurrency
- Conditional branching (validate results)
- DAG dependencies (fan-out / fan-in)
- Crash-safe per-item checkpointing

## How It Works

1. **Extract** — fetch data from a source (simulated API)
2. **Transform** — process each record (parallel, max 5 concurrent)
3. **Load** — write transformed data to destination
4. **Validate** — check row counts match

If the process crashes during the transform step, only unprocessed records are re-transformed on resume.

## Run

```bash
pip install gravtory
python main.py
```

Uses SQLite by default — no external database needed.
