# AI/ML Workflows

Gravtory is AI/ML-native with built-in support for LLM steps, streaming, token tracking, model fallback, and durable agent patterns.

## LLM Steps

Checkpointed LLM calls that survive crashes:

```python
from gravtory import Gravtory, step
from gravtory.ai.llm_step import llm_step

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="summarize-{doc_id}")
class SummarizeWorkflow:

    @step(1)
    async def fetch_document(self, doc_id: str) -> dict:
        return await docs_api.get(doc_id)

    @step(2, depends_on=1)
    @llm_step(model="gpt-4", temperature=0.3)
    async def summarize(self, document: dict) -> str:
        return f"Summarize this document: {document['text']}"
```

If the process crashes after the LLM response is received, the response is loaded from the checkpoint — no duplicate API calls.

## Model Fallback

Automatic failover between models:

```python
@step(1)
@llm_step(
    model="gpt-4",
    fallback_models=["gpt-3.5-turbo", "claude-3-sonnet"],
)
async def analyze(self, data: dict) -> str:
    return f"Analyze: {data}"
# If gpt-4 fails → tries gpt-3.5-turbo → tries claude-3-sonnet
```

## Streaming

Stream LLM responses with checkpoint support:

```python
from gravtory.ai.streaming import streaming_step

@step(1)
@streaming_step(model="gpt-4")
async def generate(self, prompt: str):
    async for chunk in llm.stream(prompt):
        yield chunk
# Chunks are buffered and checkpointed as a complete response
```

## Token & Cost Tracking

Track token usage and costs across workflows:

```python
from gravtory.ai.tokens import UsageTracker

tracker = UsageTracker()

# After workflow execution
report = await tracker.report(workflow_run_id="summarize-doc_123")
print(report.total_input_tokens)   # 1500
print(report.total_output_tokens)  # 300
print(report.total_cost_usd)       # 0.054

# Aggregate report
monthly = await tracker.report(
    start=datetime(2025, 1, 1),
    end=datetime(2025, 2, 1),
)
```

## Agent Patterns

Build durable tool-calling agents with crash-safe execution:

```python
@grav.workflow(id="agent-{task_id}")
class ResearchAgent:

    @step(1)
    @llm_step(model="gpt-4")
    async def plan(self, task_id: str) -> dict:
        return {"plan": "Search for topic, summarize findings"}

    @step(2, depends_on=1)
    async def execute_tools(self, task_id: str) -> dict:
        plan = self.context.output(1)
        results = await search_api.query(plan["plan"])
        return {"results": results}

    @step(3, depends_on=2)
    @llm_step(model="gpt-4")
    async def synthesize(self, task_id: str) -> str:
        results = self.context.output(2)
        return f"Synthesize these findings: {results}"
```

Each agent step is individually checkpointed. If the agent crashes during synthesis, the search results are NOT re-fetched.

## Best Practices

- **Checkpoint before LLM calls** — save expensive intermediate results
- **Use model fallback** — protect against provider outages
- **Track costs** — monitor per-workflow spending
- **Set timeouts** — LLM calls can hang; use step timeouts
- **Cache responses** — enable response caching for repeated prompts
