# Mutation Testing

Gravtory uses [mutmut](https://github.com/boxed/mutmut) for mutation testing to verify test suite quality.

## Configuration

```toml
# pyproject.toml
[tool.mutmut]
paths_to_mutate = "src/gravtory/"
tests_dir = "tests/unit/"
```

## Running Mutation Tests

```bash
# Full mutation run (slow — may take 30+ minutes)
mutmut run

# Run against specific module
mutmut run --paths-to-mutate src/gravtory/core/checkpoint.py

# View results
mutmut results

# Show a specific surviving mutant
mutmut show <mutant_id>
```

## Baseline (v1.0.0)

| Module | Mutants | Killed | Survived | Score |
|--------|---------|--------|----------|-------|
| `core/checkpoint.py` | — | — | — | Target: >80% |
| `core/execution.py` | — | — | — | Target: >80% |
| `core/dag.py` | — | — | — | Target: >80% |
| `core/saga.py` | — | — | — | Target: >80% |
| `retry/circuit_breaker.py` | — | — | — | Target: >80% |
| `enterprise/dlq_manager.py` | — | — | — | Target: >80% |

> **Note:** Baseline numbers should be filled in after the first full mutation run.
> Run `mutmut run && mutmut results` and update this table.

## Targets

- **Core modules** (`core/`): >80% mutation kill rate
- **Backend modules** (`backends/`): >70% mutation kill rate
- **Overall**: >75% mutation kill rate

## CI Integration

Mutation testing is not part of the default CI pipeline due to execution time. Run it manually before major releases:

```bash
# In CI or locally
pip install -e ".[dev]"
mutmut run --CI
```

## Analyzing Surviving Mutants

Surviving mutants indicate gaps in the test suite. Common categories:

1. **Boundary conditions** — off-by-one errors not caught
2. **Default values** — tests don't exercise non-default paths
3. **Error messages** — string changes that don't affect behavior
4. **Logging** — mutations in log messages (acceptable survivors)

Focus remediation on categories 1 and 2. Categories 3 and 4 are typically acceptable.
