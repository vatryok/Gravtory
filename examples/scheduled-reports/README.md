# Scheduled Reports Example

A daily report generation workflow demonstrating cron scheduling, multi-step data processing, and email delivery.

## What It Demonstrates

- `@schedule` decorator with cron expressions
- Multi-step report generation pipeline
- Interval-based scheduling alternative
- Missed run catch-up

## How It Works

1. **Query data** — fetch daily statistics from a data source
2. **Generate charts** — create visualizations from the data
3. **Compile report** — assemble HTML report with charts
4. **Send email** — deliver the report to recipients

Scheduled to run at 8 AM on weekdays. Missed runs (e.g., if the server was down) are caught up automatically.

## Run

```bash
pip install gravtory
python main.py
```

Uses SQLite by default — no external database needed. This demo runs the workflow once immediately instead of waiting for the cron trigger.
