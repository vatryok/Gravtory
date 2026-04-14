# CLI

Gravtory includes a command-line interface for managing workflows, inspecting state, and operating the system.

## Installation

The CLI is included with the main package:

```bash
pip install gravtory
```

## Configuration

Set the backend via environment variable or CLI flag:

```bash
export GRAVTORY_BACKEND="postgresql://localhost/mydb"
gravtory status

# Or pass it directly
gravtory --backend postgresql://localhost/mydb status
```

## Commands

### `gravtory version`

Print the installed version:

```bash
$ gravtory version
gravtory 0.1.0
```

### `gravtory init`

Initialize the database schema:

```bash
$ gravtory init
Database initialized successfully.
```

### `gravtory status`

Check system status (backend connectivity, worker count, schedule count):

```bash
$ gravtory status
Backend: postgresql://localhost/mydb [OK]
Workers: 4 active
Schedules: 3 registered
DLQ: 0 entries
```

### `gravtory workflows`

List registered workflow definitions:

```bash
$ gravtory workflows
OrderWorkflow    3 steps  saga=true
DailyReport      2 steps  cron="0 9 * * *"
BatchProcessor   3 steps  parallel=true
```

### `gravtory list`

List workflow runs with optional filters:

```bash
$ gravtory list
$ gravtory list --status=failed
$ gravtory list --status=completed --limit=10
$ gravtory list --json  # JSON output
```

### `gravtory inspect <run_id>`

Show detailed information about a specific workflow run:

```bash
$ gravtory inspect order-ord_123
Run ID:    order-ord_123
Workflow:  OrderWorkflow
Status:    completed
Created:   2025-01-15T09:00:00Z
Duration:  1.42s

Steps:
  1. charge_card     [OK]  142ms  {"charge_id": "ch_xyz"}
  2. reserve_inv     [OK]   89ms  {"reserved": true}
  3. send_email      [OK]   31ms  null
```

### `gravtory signal <run_id> <signal_name> <data>`

Send a signal to a waiting workflow:

```bash
$ gravtory signal expense-42 approval '{"approved": true}'
Signal sent to expense-42
```

### `gravtory dlq list`

List Dead Letter Queue entries:

```bash
$ gravtory dlq list
```

### `gravtory dlq retry <entry_id>`

Retry a DLQ entry:

```bash
$ gravtory dlq retry dlq_abc123
```

## Output Formats

Most commands support `--json` for machine-readable output:

```bash
$ gravtory list --json | jq '.[] | .run_id'
```
