# AGENTS.md

This file provides guidance to LLM when working with code in this repository.

## Project Overview

This repository contains source code of different custom Python data sources (part of
Apache Spark API) related to Cybersecurity.  These data sources allows to read from or
write to different Cybersecurity solutions in batch and/or streaming manner.

**Architecture**: Simple, flat structure with one data source per file. Each data source implements:
- `DataSource` base class with `name()`, `writer()`, and `streamWriter()` methods
- Separate writer classes for batch (`DataSourceWriter`) and streaming (`DataSourceStreamWriter`)
- Shared writer logic in a common base class (e.g., `SplunkHecWriter`)

### Documentation and examples of custom data sources using the same API

There is a number of publicly available examples that demonstrate how to implement custom Python data sources:

- https://github.com/databricks/tmm/tree/main/Lakeflow-OpenSkyNetwork
- https://github.com/allisonwang-db/pyspark-data-sources
- https://github.com/databricks-industry-solutions/python-data-sources
- https://github.com/dmatrix/spark-misc/tree/main/src/py/data_source
- https://github.com/huggingface/pyspark_huggingface
- https://github.com/dmoore247/PythonDataSources
- https://github.com/dgomez04/pyspark-hubspot
- https://github.com/dgomez04/pyspark-faker
- https://github.com/skyler-myers-db/activemq_pyspark_connector
- https://github.com/jiteshsoni/ethereum-streaming-pipeline/blob/6e06cdea573780ba09a33a334f7f07539721b85e/ethereum_block_stream_chainstack.py
- https://www.canadiandataguy.com/p/stop-waiting-for-connectors-stream
- https://www.databricks.com/blog/simplify-data-ingestion-new-python-data-source-api

More information could be found in the documentation:

- https://docs.databricks.com/aws/en/pyspark/datasources
- https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html

## Architecture Patterns

### Data Source Implementation Pattern

Each data source follows this structure (see `cyber_connectors/Splunk.py` as reference):

1. **DataSource class**: Entry point, returns appropriate writers
   - Implements `name()` class method (returns format name like "splunk")
   - Implements `writer()` for batch operations
   - Implements `streamWriter()` for streaming operations

2. **Base Writer class**: Shared logic for batch and streaming
   - Extracts and validates options in `__init__`
   - Implements `write(iterator)` that processes rows and returns `SimpleCommitMessage`
   - Uses `get_http_session()` from `common.py` for HTTP operations with retries
   - Batches records before sending (configurable via `batch_size` option)

3. **Batch Writer class**: Inherits from base writer + `DataSourceWriter`
   - No additional methods needed

4. **Stream Writer class**: Inherits from base writer + `DataSourceStreamWriter`
   - Implements `commit()` (handles successful batch completion)
   - Implements `abort()` (handles failed batch)

### Common Utilities (`cyber_connectors/common.py`)

- `SimpleCommitMessage`: Dataclass for partition write results (partition_id, count)
- `DateTimeJsonEncoder`: JSON encoder that converts datetime/date to ISO format
- `get_http_session()`: Creates requests.Session with retry logic (5 retries by default, handles 429/5xx errors)

### Key Design Principles

1. **SIMPLE over CLEVER**: No abstract base classes, factory patterns, or complex inheritance
2. **EXPLICIT over IMPLICIT**: Direct implementations, no hidden abstractions
3. **FLAT over NESTED**: Single-level inheritance (DataSource → Writer → Batch/Stream)
4. **Imports inside methods**: For partition-level execution, import libraries within `write()` methods
5. **Row-by-row processing**: Iterate rows, batch them, send when buffer full

## Adding a New Data Source

Follow this checklist (use existing sources as templates):

1. Create new file `cyber_connectors/YourSource.py`
2. Implement `YourSourceDataSource(DataSource)` with `name()`, `writer()`, `streamWriter()`
3. Implement base writer class with:
   - Options validation in `__init__`
   - `write(iterator)` method with batch logic
   - Use `get_http_session()` for HTTP calls
4. Implement batch and stream writer classes (minimal boilerplate)
5. Add exports to `cyber_connectors/__init__.py`
6. Create test file `tests/test_yoursource.py` with unit tests
7. Update README.md with usage examples and options

## 🚨 SENIOR DEVELOPER GUIDELINES 🚨

**CRITICAL: This project follows SIMPLE, MAINTAINABLE patterns. DO NOT over-engineer!**

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

### Setup
```bash
# Install dependencies (first time)
poetry install

# Activate environment
. $(poetry env info -p)/bin/activate
```

### Testing
```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest tests/test_splunk.py

# Run single test
poetry run pytest tests/test_splunk.py::TestSplunkDataSource::test_name

# Run with verbose output
poetry run pytest -v
```

### Building
```bash
# Build wheel package
poetry build

# Output will be in dist/ directory
```

### Code Quality
```bash
# Format and lint code (ruff)
poetry run ruff check cyber_connectors/
poetry run ruff format cyber_connectors/

# Type checking
poetry run mypy cyber_connectors/
```

## Testing Guidelines

- Tests use `pytest` with `pytest-spark` for Spark session fixtures
- Mock external HTTP calls using `unittest.mock.patch`
- Test writer initialization, option validation, and data processing logic
- See `tests/test_splunk.py` for comprehensive examples

**Test structure**:
- Use fixtures for common setup (`basic_options`, `sample_schema`)
- Test data source name registration
- Test writer instantiation (batch and streaming)
- Test option validation (required vs optional parameters)
- Mock HTTP responses to test write operations

## Important Notes

- **Python version**: 3.9-3.13 (defined in `pyproject.toml`)
- **Spark version**: 4.0.1+ required (PySpark DataSource API)
- **Dependencies**: Keep minimal - only add if critically needed
- **Never use direct `python` commands**: Always use `poetry run python`
- **Ruff configuration**: Line length 120, enforces docstrings, isort, flake8-bugbear
- **No premature optimization**: Focus on clarity over performance

## Data Source Registration

Users register data sources like this:
```python
from cyber_connectors import SplunkDataSource
spark.dataSource.register(SplunkDataSource)

# Then use with .format("splunk")
df.write.format("splunk").option("url", "...").save()
```

## Current Data Sources

1. **Splunk** (`splunk`): Write to Splunk HEC endpoint
   - Supports indexed fields, custom event columns, metadata
   - See `cyber_connectors/Splunk.py`

2. **Microsoft Sentinel** (`ms-sentinel` / `azure-monitor`): Write to Azure Monitor/Sentinel
   - Uses Azure service principal authentication
   - See `cyber_connectors/MsSentinel.py`

3. **REST API** (`rest`): Generic REST API writer
   - Supports POST/PUT with JSON payload
   - See `cyber_connectors/RestApi.py`

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
