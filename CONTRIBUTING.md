# Contributing to Gravtory

Thank you for your interest in contributing to Gravtory! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.10+
- Git

### Getting Started

```bash
# Clone the repository
git clone https://github.com/gravtory/gravtory.git
cd gravtory

# Install in development mode with all extras
make dev

# Verify everything works
make test
make lint
make typecheck
```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Write code following existing patterns
- Add tests for new functionality
- Ensure type hints are complete

### 3. Run Checks

```bash
# Format code
make format

# Lint
make lint

# Type check
make typecheck

# Run tests
make test

# Run tests with coverage
make coverage
```

### 4. Submit a Pull Request

- Fill out the PR template
- Ensure CI passes
- Request review

## Code Style

- **Formatter**: Ruff (auto-formatted)
- **Linter**: Ruff
- **Type checker**: mypy (strict mode)
- **Line length**: 100 characters
- **Python**: 3.10+ syntax (use `X | Y` unions, not `Union[X, Y]`)

## Testing Guidelines

- Every new feature needs unit tests
- Integration tests for backend interactions
- Use `pytest-asyncio` for async tests
- Target 95%+ code coverage

### Test Structure

```
tests/
  unit/           # Fast, no I/O, mocked dependencies
  integration/    # Real database, single process
  e2e/            # Multi-process, crash simulation
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
  contrib/        # Framework integrations (Django, FastAPI)
  dashboard/      # Web UI
```

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add PostgreSQL backend
fix: handle connection timeout in worker pool
docs: update quickstart guide
test: add saga compensation tests
refactor: simplify checkpoint engine
```

## Questions?

- Open a [GitHub Discussion](https://github.com/gravtory/gravtory/discussions)
- Join our [Discord](https://discord.gg/gravtory)

## License

By contributing, you agree that your contributions will be licensed under the AGPL-3.0 license.
