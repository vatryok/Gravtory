# Mutation Testing

## Current Status

mutmut v3 has compatibility issues with `src/` layout (editable install) projects —
it copies files to a `mutants/` directory which breaks import resolution.

## Workaround

Until mutmut fixes `src/` layout support, use the manual mutation baseline script:

```bash
python tests/mutation/baseline.py
```

This script applies a small set of representative mutations to core modules and
verifies that the test suite catches them (i.e., tests fail when code is mutated).

## Configuration

When mutmut compatibility is resolved, the configuration in `setup.cfg` is ready:

```ini
[mutmut]
paths_to_mutate=src/gravtory/core/
pytest_add_cli_args_test_selection=tests/unit/
```

## Target: >80% mutation score on core modules
