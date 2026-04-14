# CLI Usage Guide

Gravtory provides a comprehensive CLI for managing workflows, workers, schedules, and the dead letter queue.

## Global Options

```bash
gravtory --backend <CONNECTION_STRING> <command>
# Or set via environment variable:
export GRAVTORY_BACKEND="sqlite:///gravtory.db"
```

## Commands

### Initialize Database

```bash
gravtory init
```

Creates all required tables in the configured backend.

### Check Status

```bash
gravtory status
gravtory status --json
```

### Version

```bash
gravtory version
```

---

## Workflow Management

### List Workflows

```bash
gravtory workflows list
gravtory workflows list --status running --limit 50
gravtory workflows list --workflow my_pipeline --json
```

Options:
- `--status / -s` — Filter by status (`pending`, `running`, `completed`, `failed`, `cancelled`)
- `--workflow / -w` — Filter by workflow name
- `--limit / -l` — Max results (default: 20)
- `--json` — Output as JSON

### Inspect a Workflow Run

```bash
gravtory workflows inspect <RUN_ID>
gravtory workflows inspect <RUN_ID> --json
```

Shows workflow metadata, status, steps, and timing.

### Retry a Failed Workflow

```bash
gravtory workflows retry <RUN_ID>
```

Re-queues a failed workflow for execution. Resumes from the last successful checkpoint.

### Cancel a Running Workflow

```bash
gravtory workflows cancel <RUN_ID>
```

### Count Workflows

```bash
gravtory workflows count
gravtory workflows count --status failed
```

---

## Step Management

### List Steps for a Run

```bash
gravtory steps list <RUN_ID>
gravtory steps list <RUN_ID> --json
```

---

## Signal Management

### Send a Signal

```bash
gravtory signal send <RUN_ID> <SIGNAL_NAME>
gravtory signal send <RUN_ID> approve --data '{"approved": true}'
```

---

## Dead Letter Queue (DLQ)

### List DLQ Entries

```bash
gravtory dlq list
gravtory dlq list --limit 50 --json
```

### Retry a DLQ Entry

```bash
gravtory dlq retry <ENTRY_ID>
```

### Purge All DLQ Entries

```bash
gravtory dlq purge
```

---

## Worker Management

### List Active Workers

```bash
gravtory workers list
gravtory workers list --json
```

---

## Schedule Management

### List Schedules

```bash
gravtory schedules list
gravtory schedules list --json
```

### Toggle a Schedule

```bash
gravtory schedules toggle <SCHEDULE_ID>
```

---

## Dashboard

### Start the Web Dashboard

```bash
gravtory dashboard
gravtory dashboard --port 8080 --host 0.0.0.0
```

Options:
- `--port / -p` — Port (default: 7777)
- `--host / -h` — Host (default: 127.0.0.1)

---

## Development

### Run a Script with Gravtory Context

```bash
gravtory dev <SCRIPT_PATH>
```

Runs a Python script with a pre-configured Gravtory engine for local development and testing.

---

## Common Workflows

### 1. Local Development Setup

```bash
export GRAVTORY_BACKEND="sqlite:///dev.db"
gravtory init
gravtory dashboard
# Open http://localhost:7777
```

### 2. Monitor Production

```bash
export GRAVTORY_BACKEND="postgresql://user:pass@host/gravtory"
gravtory status --json
gravtory workflows list --status failed
gravtory dlq list
```

### 3. Handle Failed Workflows

```bash
# Inspect the failure
gravtory workflows inspect <RUN_ID>

# Check DLQ for details
gravtory dlq list

# Retry from last checkpoint
gravtory workflows retry <RUN_ID>

# Or retry from DLQ
gravtory dlq retry <ENTRY_ID>
```
