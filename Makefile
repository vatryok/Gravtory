.PHONY: install dev lint format typecheck test test-unit test-integration test-e2e coverage docs clean

install:
	pip install -e .

dev:
	pip install -e ".[dev,all]"
	pre-commit install

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

format:
	ruff check --fix src/ tests/
	ruff format src/ tests/

typecheck:
	mypy src/gravtory/

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v -m integration

test-e2e:
	pytest tests/e2e/ -v -m e2e

coverage:
	pytest tests/ --cov=src/gravtory --cov-report=html --cov-report=term-missing

docs:
	mkdocs serve

docs-build:
	mkdocs build

clean:
	rm -rf dist/ build/ *.egg-info .mypy_cache .ruff_cache .pytest_cache htmlcov/ .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
