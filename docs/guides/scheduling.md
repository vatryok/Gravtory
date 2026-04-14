# Scheduling

Gravtory has built-in scheduling — no external cron daemon or Celery Beat required. Schedules are stored in your database and survive restarts.

## Cron Schedules

```python
from gravtory import Gravtory, step
from gravtory.decorators.schedule import schedule

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="daily-report")
@schedule(cron="0 9 * * *", timezone="US/Eastern")
class DailyReport:

    @step(1)
    async def generate(self) -> dict:
        return await analytics.daily_report()

    @step(2, depends_on=1)
    async def send(self, report: dict) -> None:
        await email.send_report(report)
```

### Cron Expression Format

Standard 5-field cron: `minute hour day_of_month month day_of_week`

| Field | Values | Special Characters |
|-------|--------|--------------------|
| Minute | 0-59 | `*`, `,`, `-`, `/` |
| Hour | 0-23 | `*`, `,`, `-`, `/` |
| Day of month | 1-31 | `*`, `,`, `-`, `/` |
| Month | 1-12 | `*`, `,`, `-`, `/` |
| Day of week | 0-6 (Sun=0) | `*`, `,`, `-`, `/` |

Examples:
- `*/5 * * * *` — every 5 minutes
- `0 9 * * 1-5` — 9 AM on weekdays
- `0 0 1 * *` — midnight on the 1st of each month

## Interval Schedules

```python
from datetime import timedelta

@grav.workflow(id="health-check")
@schedule(interval=timedelta(minutes=5))
class HealthCheck:

    @step(1)
    async def check(self) -> dict:
        return {"status": "healthy"}
```

## Missed Run Handling

If the scheduler was offline and missed scheduled runs, Gravtory can catch up:

```python
@schedule(cron="0 * * * *", catchup=True, max_catchup=5)
class HourlyJob:
    ...
```

- `catchup=True` — execute missed runs on startup
- `max_catchup=5` — limit catch-up to 5 missed runs

## Leader Election

In distributed deployments, only one worker runs the scheduler. Gravtory uses database-based leader election to prevent duplicate schedule triggers.

## Listing Schedules

```python
schedules = await grav.list_schedules()
for s in schedules:
    print(f"{s.workflow_name}: next run at {s.next_run_at}")
```

```bash
gravtory schedules list
```
