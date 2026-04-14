# Contributing to Gravtory

Thank you for your interest in contributing to Gravtory! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.10+
- Git

### Getting Started

```bash
git clone https://github.com/vatryok/gravtory.git
cd gravtory

# One-command setup (creates venv, installs all deps, runs smoke test)
./scripts/dev-setup.sh

# Or manually:
make dev          # install in editable mode with all extras
make test         # run unit tests
make lint         # ruff lint + format check
make typecheck    # mypy strict
```

## Development Workflow

1. **Branch** from `main`: `git checkout -b feature/your-feature`
2. **Code** -- follow existing patterns, add type hints everywhere
3. **Test** -- every feature needs unit tests; backend features need integration tests
4. **Check** -- `make lint && make typecheck && make coverage`
5. **PR** -- fill out the template, ensure CI is green

## Code Style

| Tool | Config |
|------|--------|
| **Formatter** | Ruff (`ruff format`) |
| **Linter** | Ruff (`ruff check`) |
| **Type checker** | mypy strict mode |
| **Line length** | 100 characters |
| **Python** | 3.10+ syntax (`X | Y` unions) |

## Test Structure

```
tests/
  unit/           # Fast, no I/O, mocked deps
  integration/    # Real database, single process
  e2e/            # Multi-process, crash simulation
  property/       # Hypothesis property-based tests
  benchmarks/     # pytest-benchmark performance tests
```

## Architecture

```
src/gravtory/
  core/           # Engine, types, errors, registry, checkpoint, DAG, retry, saga
  backends/       # Abstract base + concrete backends (postgres, sqlite, etc.)
  workers/        # Worker pool, task claiming
  scheduling/     # Cron, interval, event triggers
  signals/        # Inter-workflow communication
  serialization/  # JSON, msgpack, compression
  observability/  # OpenTelemetry, Prometheus, middleware
  testing/        # In-memory test runner
  cli/            # Command-line interface
  dashboard/      # Web UI
```

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation only
- `test:` adding or fixing tests
- `refactor:` code change that neither fixes a bug nor adds a feature

## Questions?

- Open a [GitHub Discussion](https://github.com/vatryok/gravtory/discussions)

## License

By contributing, you agree that your contributions will be licensed under the AGPL-3.0 license.
