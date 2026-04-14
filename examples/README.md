# Gravtory Examples

Practical examples demonstrating common Gravtory patterns.

## Examples

### [ai-agent/](ai-agent/)
Durable AI agent with checkpointed LLM calls, tool use, and model fallback.

### [approval-workflow/](approval-workflow/)
Human-in-the-loop expense approval using signals and conditional branching.

### [etl-pipeline/](etl-pipeline/)
Extract-Transform-Load pipeline with parallel processing and crash recovery.

### [order-processing/](order-processing/)
E-commerce order workflow with saga compensation for payment rollback.

### [scheduled-reports/](scheduled-reports/)
Cron-scheduled daily report generation with email delivery.

## Running Examples

```bash
# Install gravtory with all extras
pip install gravtory[all]

# Run an example
cd examples/order-processing
python main.py
```

Each example directory contains its own `README.md` with specific instructions.
