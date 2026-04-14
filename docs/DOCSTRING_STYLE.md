# Docstring Style Guide

Gravtory uses **Google-style docstrings** as the project standard.

## Format

```python
def function_name(arg1: str, arg2: int = 0) -> bool:
    """Short one-line summary.

    Longer description if needed. Can span multiple lines.

    Args:
        arg1: Description of arg1.
        arg2: Description of arg2. Defaults to 0.

    Returns:
        Description of the return value.

    Raises:
        ValueError: When arg1 is empty.
        TypeError: When arg2 is not an integer.
    """
```

## Rules

1. **First line**: Imperative mood, one sentence, ends with period.
2. **Blank line**: Between summary and body.
3. **Args section**: Required for all public functions with parameters.
4. **Returns section**: Required for all functions that return non-None.
5. **Raises section**: Required if the function raises exceptions.
6. **Type hints**: Always in the signature, never repeated in the docstring.
7. **Private methods**: Docstring optional but encouraged for complex logic.
8. **Classes**: Document `__init__` params in the class docstring, not `__init__`.

## Enforcement

- mkdocstrings renders Google-style docstrings automatically.
- Future: add `pydocstyle` or `ruff` `D` rules to enforce.
