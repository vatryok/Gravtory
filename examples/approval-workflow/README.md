# Approval Workflow Example

An expense approval workflow with human-in-the-loop signals, demonstrating how external systems can interact with running workflows.

## What It Demonstrates

- Human-in-the-loop with signals
- Conditional branching (approved vs rejected)
- FastAPI integration for sending signals via HTTP
- Workflow pausing and resumption

## How It Works

1. **Submit expense** — records the expense request
2. **Wait for approval** — pauses until a signal is received
3. **Process** — if approved, processes the reimbursement; if rejected, notifies the submitter

## Run

```bash
pip install gravtory
python main.py
```

Uses SQLite by default — no external database needed.

## With FastAPI (optional)

```bash
pip install gravtory fastapi uvicorn
python api.py
```

Then send an approval signal via HTTP:

```bash
curl -X POST http://localhost:8000/approve/exp_001 \
  -H "Content-Type: application/json" \
  -d '{"approved": true, "reviewer": "manager@example.com"}'
```
