# Unit Tests for Cyber Spark Data Connectors

This directory contains comprehensive unit tests for all implemented data sources in the cyber-spark-data-connectors package.

## Test Coverage

The test suite covers the following data sources:

### 1. Splunk Data Source (`test_splunk.py`)
- **TestSplunkDataSource**: Tests for the main data source class
  - Data source name validation
  - Stream and batch writer creation
  
- **TestSplunkHecWriter**: Tests for the Splunk HEC writer functionality
  - Initialization with required and optional parameters
  - Missing parameter validation
  - Basic write operations
  - Time column handling
  - Indexed fields support
  - Single event column mode
  - Batching behavior
  
- **TestSplunkHecStreamWriter**: Tests for stream-specific functionality
  - Commit and abort methods

### 2. REST API Data Source (`test_restapi.py`)
- **TestRestApiDataSource**: Tests for the main data source class
  - Data source name validation
  - Stream and batch writer creation
  
- **TestRestApiWriter**: Tests for the REST API writer functionality
  - Initialization with required and optional parameters
  - Missing/invalid parameter validation
  - POST and PUT method support
  - JSON serialization
  - Multiple row handling
  - HTTP headers configuration
  
- **TestRestApiStreamWriter**: Tests for stream-specific functionality
  - Commit and abort methods

### 3. Microsoft Sentinel / Azure Monitor Data Source (`test_mssentinel.py`)
- **TestAzureMonitorDataSource**: Tests for the Azure Monitor data source
  - Data source name validation
  - Stream and batch writer creation
  
- **TestMicrosoftSentinelDataSource**: Tests for the MS Sentinel data source
  - Data source name validation (alias for Azure Monitor)
  - Stream and batch writer creation
  
- **TestAzureMonitorWriter**: Tests for the Azure Monitor writer functionality
  - Initialization with all required Azure parameters
  - Missing parameter validation for each required field
  - Custom batch size configuration
  - Basic write operations
  - DateTime field conversion
  - Batching behavior
  - Azure credential creation
  
- **TestAzureMonitorStreamWriter**: Tests for stream-specific functionality
  - Commit and abort methods

### 4. Common Utilities (`test_common.py`)
- **TestSimpleCommitMessage**: Tests for the commit message dataclass
  - Creation and attribute access
  
- **TestDateTimeJsonEncoder**: Tests for JSON encoding utilities
  - DateTime and date object serialization
  - Microseconds handling
  - Regular types support
  - Mixed type handling
  
- **TestGetHttpSession**: Tests for HTTP session creation
  - Default session configuration
  - Custom headers
  - Retry configuration
  - Retry on POST option

## Running the Tests

### Run all tests
```bash
poetry run pytest tests/
```

### Run tests with verbose output
```bash
poetry run pytest tests/ -v
```

### Run tests with coverage
```bash
poetry run pytest tests/ --cov=cyber_connectors --cov-report=term-missing
```

### Run specific test file
```bash
poetry run pytest tests/test_splunk.py
poetry run pytest tests/test_restapi.py
poetry run pytest tests/test_mssentinel.py
poetry run pytest tests/test_common.py
```

### Run specific test class or method
```bash
poetry run pytest tests/test_splunk.py::TestSplunkDataSource
poetry run pytest tests/test_splunk.py::TestSplunkHecWriter::test_write_basic
```

## Test Coverage Summary

As of the latest run:
- **Overall Coverage**: 95%
- **MsSentinel.py**: 100%
- **RestApi.py**: 98%
- **Splunk.py**: 89%
- **common.py**: 97%
- **__init__.py**: 100%

Total: 66 tests, all passing

## Testing Approach

The tests use the following approach:
- **Unit Tests**: All tests are unit tests with mocked dependencies
- **Mocking**: External dependencies (HTTP sessions, Azure clients, PySpark TaskContext) are mocked
- **Fixtures**: pytest fixtures are used for common test data (options, schemas)
- **Assertions**: Comprehensive assertions to validate behavior
- **Edge Cases**: Tests cover both happy paths and error conditions

## Dependencies

The test suite requires:
- pytest
- pytest-cov (for coverage reports)
- pytest-spark (for Spark testing utilities)
- unittest.mock (standard library)

All dependencies are managed through Poetry and are installed automatically when running tests in the Poetry environment.

### pytest-spark Fixtures

The project uses `pytest-spark` which provides the following fixtures if needed for future tests:
- `spark_session` - A SparkSession instance (scope: session)
- `spark_context` - A SparkContext instance (scope: session)

**Note:** The current unit tests don't use these fixtures because they mock all Spark interactions for speed and isolation. However, these fixtures are available for integration tests if needed in the future.

Example usage (if needed):
```python
def test_with_real_spark(spark_session):
    """Example test using actual Spark session."""
    df = spark_session.range(10)
    assert df.count() == 10
```
