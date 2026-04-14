# Order Processing Example

A 3-step order workflow demonstrating crash-proof execution with retry, saga compensation, and automatic rollback.

## What It Demonstrates

- `@workflow` and `@step` decorators
- Automatic retry with exponential backoff
- Saga compensation (automatic refund on shipping failure)
- Crash-safe checkpointing

## How It Works

1. **Charge payment** — charges the customer with retry (simulated)
2. **Reserve inventory** — reserves items with compensation handler
3. **Send confirmation** — sends email notification

If any step fails after previous steps completed, compensation handlers automatically run in reverse order to undo completed work.

## Run

```bash
pip install gravtory
python main.py
```

Uses SQLite by default — no external database needed.

## Expected Output

```
[order-ord_001] Step 1: Charging payment of $99.99...
[order-ord_001] Step 1: Payment charged (txn_abc123)
[order-ord_001] Step 2: Reserving inventory for item_42...
[order-ord_001] Step 2: Inventory reserved (rsv_xyz789)
[order-ord_001] Step 3: Sending confirmation to user@example.com...
[order-ord_001] Step 3: Confirmation sent!
[order-ord_001] Workflow completed successfully
```
