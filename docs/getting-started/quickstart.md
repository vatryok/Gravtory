# Quick Start

## Your First Workflow

```python
from gravtory import Gravtory, workflow, step

grav = Gravtory("postgresql://localhost/mydb")

@grav.workflow(id="hello-{name}")
class HelloWorkflow:

    @step(1)
    async def greet(self, name: str) -> str:
        return f"Hello, {name}!"

    @step(2, depends_on=1)
    async def log(self, greeting: str) -> None:
        print(greeting)

# Run it
async def main():
    await grav.start()
    result = await grav.run(HelloWorkflow, name="World")
    print(result)
```

## What Happens on Crash?

If your process crashes after step 1 completes, Gravtory will:

1. Detect the incomplete workflow on restart
2. Load step 1's output from the database (not re-execute it)
3. Continue from step 2
