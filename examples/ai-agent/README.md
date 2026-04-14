# AI Agent Example

A durable AI content generation workflow demonstrating crash-safe LLM calls, model fallback, and parallel processing.

## What It Demonstrates

- Checkpointed LLM calls (crash won't re-call the API)
- Model fallback chain
- Parallel content generation
- Multi-step agent pattern

## How It Works

1. **Research** — gather information on a topic (simulated LLM call)
2. **Outline** — generate a content outline from research
3. **Write sections** — write each section (parallel, simulated)
4. **Review** — final review and editing pass

Each LLM call is checkpointed. If the process crashes after research completes, the research is NOT re-run — its output is loaded from the database.

## Run

```bash
pip install gravtory
python main.py
```

Uses SQLite by default — no external database needed. LLM calls are simulated for this demo.
