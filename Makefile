.PHONY: install dev lint format typecheck test test-unit test-integration test-e2e test-property test-mutation coverage security benchmark docs docs-build clean build release setup docker docker-up docker-down

# ── Developer Workflow ────────────────────────────────────────────

install:
	pip install -e .

dev:
	pip install -e ".[dev,all]"
	pre-commit install

setup:
	bash scripts/dev-setup.sh

# ── Code Quality ─────────────────────────────────────────────────

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

format:
	ruff check --fix src/ tests/
	ruff format src/ tests/

typecheck:
	mypy src/gravtory/

# ── Testing ──────────────────────────────────────────────────────

test:
	pytest tests/ -v --benchmark-disable

test-unit:
	pytest tests/unit/ -v --benchmark-disable

test-integration:
	pytest tests/integration/ -v -m integration --benchmark-disable

test-e2e:
	pytest tests/e2e/ -v -m e2e --benchmark-disable

test-property:
	pytest tests/property/ -v -m property --benchmark-disable

test-mutation:
	mutmut run

coverage:
	pytest tests/ --cov=src/gravtory --cov-report=html --cov-report=term-missing --benchmark-disable

benchmark:
	python benchmarks/run_benchmarks.py

# ── Security ────────────────────────────────────────────────

security:
	pip-audit .
	bandit -r src/gravtory -ll -q

# ── Docker ──────────────────────────────────────────────────

docker:
	docker build -t gravtory .

docker-up:
	docker compose up -d

docker-down:
	docker compose down

# ── Documentation ────────────────────────────────────────────────

docs:
	mkdocs serve

docs-build:
	mkdocs build

# ── Build & Release ──────────────────────────────────────────────

build:
	bash scripts/build.sh

release:
	bash scripts/release.sh

# ── Cleanup ──────────────────────────────────────────────────────

clean:
	rm -rf dist/ build/ release/ *.egg-info .mypy_cache .ruff_cache .pytest_cache htmlcov/ .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
