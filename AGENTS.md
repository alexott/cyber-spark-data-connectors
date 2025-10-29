# AGENTS.md

This file provides guidance to LLM when working with code in this repository.

## Project Overview

This repository contains source code of different custom Python data sources (part of
Apache Spark API) related to Cybersecurity.  These data sources allows to read from or
write to different Cybersecurity solutions in batch and/or streaming manner.

### Documentation and examples of custom data sources using the same API

There is a number of publicly available examples that demonstrate how to implement custom Python data sources:

- https://github.com/databricks/tmm/tree/main/Lakeflow-OpenSkyNetwork
- https://github.com/allisonwang-db/pyspark-data-sources
- https://github.com/databricks-industry-solutions/python-data-sources
- https://github.com/dmatrix/spark-misc/tree/main/src/py/data_source
- https://github.com/huggingface/pyspark_huggingface
- https://www.databricks.com/blog/simplify-data-ingestion-new-python-data-source-api
- https://github.com/dmoore247/PythonDataSources
- https://github.com/dgomez04/pyspark-hubspot
- https://github.com/dgomez04/pyspark-faker

More information could be found in the documentation:

- https://docs.databricks.com/aws/en/pyspark/datasources
- https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html

## 🚨 SENIOR DEVELOPER GUIDELINES 🚨

**CRITICAL: This project follows SIMPLE, MAINTAINABLE patterns. DO NOT over-engineer!**

### Code Philosophy

1. **SIMPLE over CLEVER**: Write obvious code that any developer can understand
2. **EXPLICIT over IMPLICIT**: Prefer clear, direct implementations over abstractions
3. **FLAT over NESTED**: Avoid deep inheritance, complex factories, or excessive abstraction layers
4. **FOCUSED over GENERIC**: Write code for the specific use case, not hypothetical future needs

### Forbidden Patterns (DO NOT ADD THESE)

❌ **Abstract base classes** or complex inheritance hierarchies
❌ **Factory patterns** or dependency injection containers
❌ **Decorators for cross-cutting concerns** (logging, caching, performance monitoring)
❌ **Complex configuration classes** with nested structures
❌ **Async/await patterns** unless absolutely necessary
❌ **Connection pooling** or caching layers
❌ **Generic "framework" code** or reusable utilities
❌ **Complex error handling systems** or custom exceptions
❌ **Performance optimization** patterns (premature optimization)
❌ **Enterprise patterns** like singleton, observer, strategy, etc.

### Required Patterns (ALWAYS USE THESE)
✅ **Direct function calls** - no indirection or abstraction layers
✅ **Simple classes** with clear, single responsibilities
✅ **Environment variables** for configuration (no complex config objects)
✅ **Explicit imports** - import exactly what you need
✅ **Basic error handling** with try/catch and simple return dictionaries
✅ **Straightforward control flow** - avoid complex conditional logic
✅ **Standard library first** - only add dependencies when absolutely necessary

### Implementation Rules

1. **One concept per file**: Each module should have a single, clear purpose
2. **Functions over classes**: Prefer functions unless you need state management
3. **Direct SDK calls**: Call Databricks SDK directly, no wrapper layers
4. **Simple data structures**: Use dicts and lists, avoid custom data classes
5. **Basic testing**: Simple unit tests with basic mocking, no complex test frameworks
6. **Minimal dependencies**: Only add new dependencies if critically needed

### Code Review Questions

Before adding any code, ask yourself:
- "Is this the simplest way to solve this problem?"
- "Would a new developer understand this immediately?"
- "Am I adding abstraction for a real need or hypothetical flexibility?"
- "Can I solve this with standard library or existing dependencies?"
- "Does this follow the existing patterns in the codebase?"

## Development Commands

### Python Execution Rules

**CRITICAL: Always use `poetry run` instead of direct `python`:**
```bash
# ✅ CORRECT
poetry run python script.py

# ❌ WRONG  
python script.py
```

## Development Workflow

### Package Management

- **Python**: Use `poetry add/remove` for dependencies, never edit `pyproject.toml` manually
- Always check if dependencies already exist before adding new ones
- **Principle**: Only add dependencies if absolutely critical

## Testing

### Running Tests

#### Python Tests

```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest tests/test_basic_functionality.py

# Run single test
poetry run pytest tests/test_e2e_examples.py::test_specific_function

# Run tests by marker
poetry run pytest -m unit        # Unit tests only
poetry run pytest -m integration # Integration tests
poetry run pytest -m e2e         # End-to-end tests
poetry run pytest -m "not slow"  # Skip slow tests

# Run with coverage
poetry run pytest --cov=server --cov-report=html
```


## Simplified Testing

### Basic Testing Suite

The project includes a **focused** test suite with essential tests only:
- **Unit Tests**: Basic component testing with simple mocks. Use `pytest` package to write unit tests

### Running Tests

```bash
# Run all tests (simple and fast)
make test

# Or directly with uv
poetry run pytest tests/ -v
```

### Test Structure

Tests use minimal pytest configuration:

- **test files** covering core functionality
- **Basic markers**: unit, tools, integration
- **Simple fixtures**: Basic mocking utilities only
- **No coverage requirements**: Focus on functionality, not metrics

### Writing Tests

Follow the **simple pattern**:

```python
def test_your_feature(mock_env_vars):
    """Test your feature."""
    # Load tools
    load_tools(mcp_server)

    # Mock Databricks SDK calls
    with patch('server.tools.module.get_workspace_client') as mock_client:
        mock_client.return_value.some_api.method.return_value = expected_data

        # Test the tool
        tool = mcp_server._tools['tool_name']
        result = tool.func()

        # Basic assertions
        assert result['status'] == 'success'
```

**Testing principles:**

- Keep tests simple and focused
- Mock external dependencies (Databricks SDK, external APIs)
- Test success and error cases only
- No complex test infrastructure or frameworks

## Summary: What Makes This Project "Senior Developer Approved"

✅ **Readable**: Any developer can understand the code immediately
✅ **Maintainable**: Simple patterns that are easy to modify
✅ **Focused**: Each module has a single, clear responsibility
✅ **Direct**: No unnecessary abstractions or indirection
✅ **Practical**: Solves the specific problem without over-engineering

When in doubt, choose the **simpler** solution. Your future self (and your teammates) will thank you.

---

## Important Instruction Reminders

**For an agent when working on this project:**

1. **Do what has been asked; nothing more, nothing less**
2. **NEVER create files unless absolutely necessary for achieving the goal**
3. **ALWAYS prefer editing an existing file to creating a new one**
4. **NEVER proactively create documentation files (*.md) or README files**
5. **Follow the SIMPLE patterns established in this codebase**
6. **When in doubt, ask "Is this the simplest way?" before implementing**

This project is intentionally simplified. **Respect that simplicity.**
