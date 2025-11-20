"""Unit tests for Azure Monitor Batch Reader."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from cyber_connectors.MsSentinel import (
    AzureMonitorBatchReader,
    AzureMonitorDataSource,
    MicrosoftSentinelDataSource,
)


class TestAzureMonitorDataSource:
    """Test data source registration and reader creation."""

    @pytest.fixture
    def basic_options(self):
        """Basic valid options for reader."""
        return {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "timespan": "P1D",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

    def test_azure_monitor_name(self):
        """Test Azure Monitor data source name."""
        assert AzureMonitorDataSource.name() == "azure-monitor"

    def test_ms_sentinel_name(self):
        """Test Microsoft Sentinel data source name."""
        assert MicrosoftSentinelDataSource.name() == "ms-sentinel"

    def test_reader_method_exists(self):
        """Test that reader method exists and returns reader."""
        ds = AzureMonitorDataSource(options={})
        schema = StructType([StructField("test", StringType(), True)])

        # Should not raise exception (actual reader creation will fail due to missing options)
        assert hasattr(ds, "reader")

    def test_datasource_has_schema_method(self, basic_options):
        """Test that data source has schema() method that infers schema."""
        # Mock the Azure Monitor query client
        from unittest.mock import Mock, patch
        from azure.monitor.query import LogsQueryStatus

        with patch("azure.monitor.query.LogsQueryClient") as mock_client_cls, patch(
            "azure.identity.ClientSecretCredential"
        ) as mock_credential:
            # Create mock response - columns are strings, data types inferred from rows
            mock_response = Mock()
            mock_response.status = LogsQueryStatus.SUCCESS
            mock_table = Mock()
            mock_table.columns = ["TestCol"]
            mock_table.rows = [["test value"]]
            mock_response.tables = [mock_table]

            mock_client = Mock()
            mock_client.query_workspace.return_value = mock_response
            mock_client_cls.return_value = mock_client

            ds = AzureMonitorDataSource(options=basic_options)

            # Verify schema method exists and returns a schema
            assert hasattr(ds, "schema")
            returned_schema = ds.schema()
            assert returned_schema is not None
            assert isinstance(returned_schema, StructType)

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_schema_inference(self, mock_credential, mock_client, basic_options):
        """Test that schema is inferred by DataSource from actual row data."""
        from datetime import datetime, timezone
        from azure.monitor.query import LogsQueryStatus

        # Create mock response for schema inference
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS

        # table.columns is always a list of strings (column names)
        # Schema is inferred from actual data types in rows
        mock_table = Mock()
        mock_table.columns = ["TimeGenerated", "OperationName", "Count"]
        mock_table.rows = [
            [
                datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),  # datetime -> TimestampType
                "Read",  # str -> StringType
                100,  # int -> LongType
            ]
        ]
        mock_response.tables = [mock_table]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create data source and call schema()
        ds = AzureMonitorDataSource(options=basic_options)
        schema = ds.schema()

        # Verify schema was inferred
        assert schema is not None
        assert len(schema.fields) == 3

        # Verify field names and types
        from pyspark.sql.types import LongType, StringType, TimestampType

        assert schema.fields[0].name == "TimeGenerated"
        assert isinstance(schema.fields[0].dataType, TimestampType)

        assert schema.fields[1].name == "OperationName"
        assert isinstance(schema.fields[1].dataType, StringType)

        assert schema.fields[2].name == "Count"
        assert isinstance(schema.fields[2].dataType, LongType)

        # Verify the query was modified to include "| take 1"
        call_kwargs = mock_client_instance.query_workspace.call_args[1]
        assert "| take 1" in call_kwargs["query"]

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_schema_inference_query_already_has_limit(self, mock_credential, mock_client, basic_options):
        """Test that schema inference doesn't add duplicate limit when query already has one."""
        from azure.monitor.query import LogsQueryStatus

        # Modify query to already have a limit
        basic_options["query"] = "AzureActivity | take 1"

        # Create mock response - columns are strings, data types inferred from rows
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["TestCol"]
        mock_table.rows = [["test"]]
        mock_response.tables = [mock_table]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create data source and call schema()
        ds = AzureMonitorDataSource(options=basic_options)
        ds.schema()

        # Verify the query was not modified (already has limit)
        call_kwargs = mock_client_instance.query_workspace.call_args[1]
        query_used = call_kwargs["query"]
        # Should only have one "| take 1"
        assert query_used.count("| take 1") == 1

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_schema_inference_no_tables(self, mock_credential, mock_client, basic_options):
        """Test that schema inference fails when query returns no tables."""
        from azure.monitor.query import LogsQueryStatus

        # Create mock response with no tables
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_response.tables = []

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create data source and attempt to call schema()
        ds = AzureMonitorDataSource(options=basic_options)

        with pytest.raises(Exception, match="Schema inference failed: query returned no tables"):
            ds.schema()

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_schema_inference_no_columns(self, mock_credential, mock_client, basic_options):
        """Test that schema inference fails when query returns no columns."""
        from azure.monitor.query import LogsQueryStatus

        # Create mock response with table but no columns
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = []
        mock_response.tables = [mock_table]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create data source and attempt to call schema()
        ds = AzureMonitorDataSource(options=basic_options)

        with pytest.raises(Exception, match="Schema inference failed: query returned no columns"):
            ds.schema()

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_schema_inference_query_failure(self, mock_credential, mock_client, basic_options):
        """Test that schema inference fails when query fails."""
        from azure.monitor.query import LogsQueryStatus

        # Create mock response with failure status
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.PARTIAL

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create data source and attempt to call schema()
        ds = AzureMonitorDataSource(options=basic_options)

        with pytest.raises(Exception, match="Query failed with status"):
            ds.schema()

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_schema_inference_type_mapping(self, mock_credential, mock_client, basic_options):
        """Test that schema inference correctly maps Python types to PySpark types."""
        from datetime import datetime, timezone
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, TimestampType

        # Create mock response with various Python data types
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS

        # Map Python types to expected PySpark types
        # (column_name, python_value, expected_spark_type)
        test_data = [
            ("BoolCol", True, BooleanType),
            ("DateCol", datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc), TimestampType),
            ("IntCol", 42, LongType),
            ("FloatCol", 3.14, DoubleType),
            ("StringCol", "test", StringType),
            ("NullCol", None, StringType),  # None defaults to StringType
        ]

        mock_table = Mock()
        mock_table.columns = [col_name for col_name, _, _ in test_data]
        mock_table.rows = [[value for _, value, _ in test_data]]
        mock_response.tables = [mock_table]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create data source and call schema()
        ds = AzureMonitorDataSource(options=basic_options)
        schema = ds.schema()

        # Verify schema was inferred with correct types
        assert len(schema.fields) == len(test_data)

        for i, (expected_name, _, expected_type_class) in enumerate(test_data):
            assert schema.fields[i].name == expected_name
            assert isinstance(schema.fields[i].dataType, expected_type_class)


class TestAzureMonitorBatchReader:
    """Test Azure Monitor Batch Reader implementation."""

    @pytest.fixture
    def basic_options(self):
        """Basic valid options for reader."""
        return {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "timespan": "P1D",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

    @pytest.fixture
    def basic_schema(self):
        """Basic schema for testing."""
        return StructType(
            [StructField("TimeGenerated", StringType(), True), StructField("OperationName", StringType(), True)]
        )

    def test_reader_initialization(self, basic_options, basic_schema):
        """Test reader initializes with valid options."""
        reader = AzureMonitorBatchReader(basic_options, basic_schema)

        assert reader.workspace_id == "test-workspace-id"
        assert reader.query == "AzureActivity | take 5"
        assert reader.tenant_id == "test-tenant"
        assert reader.client_id == "test-client"
        assert reader.client_secret == "test-secret"
        assert reader.num_partitions == 1
        # Verify that start_time and end_time were set (from timespan)
        assert isinstance(reader.start_time, datetime)
        assert isinstance(reader.end_time, datetime)
        assert reader.start_time < reader.end_time

    def test_reader_optional_parameters(self, basic_options, basic_schema):
        """Test reader handles optional parameters."""
        basic_options["num_partitions"] = "3"

        reader = AzureMonitorBatchReader(basic_options, basic_schema)

        assert reader.num_partitions == 3

    def test_reader_missing_workspace_id(self, basic_schema):
        """Test reader fails without workspace_id."""
        options = {
            "query": "AzureActivity | take 5",
            "timespan": "P1D",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        with pytest.raises(AssertionError, match="workspace_id is required"):
            AzureMonitorBatchReader(options, basic_schema)

    def test_reader_missing_query(self, basic_schema):
        """Test reader fails without query."""
        options = {
            "workspace_id": "test-workspace-id",
            "timespan": "P1D",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        with pytest.raises(AssertionError, match="query is required"):
            AzureMonitorBatchReader(options, basic_schema)

    def test_reader_missing_timespan(self, basic_schema):
        """Test reader fails without timespan (now requires either timespan or start_time)."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        with pytest.raises(Exception, match="Either 'timespan' or 'start_time' must be provided"):
            AzureMonitorBatchReader(options, basic_schema)

    def test_reader_missing_tenant_id(self, basic_schema):
        """Test reader fails without tenant_id."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "timespan": "P1D",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        with pytest.raises(AssertionError, match="tenant_id is required"):
            AzureMonitorBatchReader(options, basic_schema)

    def test_reader_missing_client_id(self, basic_schema):
        """Test reader fails without client_id."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "timespan": "P1D",
            "tenant_id": "test-tenant",
            "client_secret": "test-secret",
        }

        with pytest.raises(AssertionError, match="client_id is required"):
            AzureMonitorBatchReader(options, basic_schema)

    def test_reader_missing_client_secret(self, basic_schema):
        """Test reader fails without client_secret."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "timespan": "P1D",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
        }

        with pytest.raises(AssertionError, match="client_secret is required"):
            AzureMonitorBatchReader(options, basic_schema)

    def test_reader_missing_time_range(self, basic_schema):
        """Test reader fails without timespan or start_time."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        with pytest.raises(Exception, match="Either 'timespan' or 'start_time' must be provided"):
            AzureMonitorBatchReader(options, basic_schema)

    def test_reader_both_timespan_and_start_time(self, basic_schema):
        """Test that reader uses timespan when both are provided (timespan takes precedence)."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "timespan": "P1D",
            "start_time": "2024-01-01T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        # Should succeed - timespan takes precedence over start_time in module-level function
        reader = AzureMonitorBatchReader(options, basic_schema)
        assert reader.start_time is not None
        assert reader.end_time is not None

    def test_reader_with_start_time_only(self, basic_schema):
        """Test reader initializes with only start_time (end_time defaults to now)."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "start_time": "2024-01-01T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorBatchReader(options, basic_schema)
        assert isinstance(reader.start_time, datetime)
        assert isinstance(reader.end_time, datetime)
        assert reader.end_time is not None  # Should be set to current time
        assert reader.start_time < reader.end_time

    def test_reader_with_start_and_end_time(self, basic_schema):
        """Test reader initializes with both start_time and end_time."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-02T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorBatchReader(options, basic_schema)
        assert isinstance(reader.start_time, datetime)
        assert isinstance(reader.end_time, datetime)
        # Verify the times are approximately correct (allowing for timezone handling)
        assert reader.start_time.year == 2024
        assert reader.start_time.month == 1
        assert reader.start_time.day == 1
        assert reader.end_time.year == 2024
        assert reader.end_time.month == 1
        assert reader.end_time.day == 2

    def test_reader_with_end_time_only(self, basic_schema):
        """Test reader fails with only end_time (start_time is required)."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "end_time": "2024-01-02T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        # Should fail because neither timespan nor start_time is provided
        with pytest.raises(Exception, match="Either 'timespan' or 'start_time' must be provided"):
            AzureMonitorBatchReader(options, basic_schema)

    def test_timespan_parsing_days(self):
        """Test timespan parsing for days using module-level function."""
        from cyber_connectors.MsSentinel import _parse_time_range

        start, end = _parse_time_range(timespan="P1D")
        assert (end - start).days == 1

        start, end = _parse_time_range(timespan="P7D")
        assert (end - start).days == 7

    def test_timespan_parsing_hours(self):
        """Test timespan parsing for hours using module-level function."""
        from cyber_connectors.MsSentinel import _parse_time_range

        start, end = _parse_time_range(timespan="PT1H")
        assert (end - start).total_seconds() == 3600

        start, end = _parse_time_range(timespan="PT24H")
        assert (end - start).total_seconds() == 86400

    def test_timespan_parsing_minutes(self):
        """Test timespan parsing for minutes using module-level function."""
        from cyber_connectors.MsSentinel import _parse_time_range

        start, end = _parse_time_range(timespan="PT30M")
        assert (end - start).total_seconds() == 1800

    def test_timespan_parsing_seconds(self):
        """Test timespan parsing for seconds using module-level function."""
        from cyber_connectors.MsSentinel import _parse_time_range

        start, end = _parse_time_range(timespan="PT120S")
        assert (end - start).total_seconds() == 120

    def test_timespan_parsing_combined(self):
        """Test timespan parsing for combined duration using module-level function."""
        from cyber_connectors.MsSentinel import _parse_time_range

        # P1DT2H30M
        start, end = _parse_time_range(timespan="P1DT2H30M")
        total_seconds = (end - start).total_seconds()
        expected_seconds = 1 * 86400 + 2 * 3600 + 30 * 60
        assert total_seconds == expected_seconds

    def test_timespan_parsing_invalid(self):
        """Test timespan parsing with invalid format using module-level function."""
        from cyber_connectors.MsSentinel import _parse_time_range

        with pytest.raises(ValueError, match="Invalid timespan format"):
            _parse_time_range(timespan="invalid")

        with pytest.raises(ValueError, match="Invalid timespan format"):
            _parse_time_range(timespan="1D")

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_success_single_partition(self, mock_credential, mock_client, basic_options, basic_schema):
        """Test successful data reading with single partition."""
        from azure.monitor.query import LogsQueryStatus

        # Create mock response
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS

        # Create mock table with columns and rows
        # table.columns is always a list of strings (column names)
        mock_table = Mock()
        mock_table.columns = ["TimeGenerated", "OperationName"]
        mock_table.rows = [
            ["2024-01-01T00:00:00Z", "Read"],
            ["2024-01-01T01:00:00Z", "Write"],
            ["2024-01-01T02:00:00Z", "Delete"],
        ]
        mock_response.tables = [mock_table]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create reader and read
        reader = AzureMonitorBatchReader(basic_options, basic_schema)
        # Use the partitions method to get the partition
        partitions = reader.partitions()
        assert len(partitions) == 1
        rows = list(reader.read(partitions[0]))

        # Verify results
        assert len(rows) == 3
        assert rows[0].TimeGenerated == "2024-01-01T00:00:00Z"
        assert rows[0].OperationName == "Read"
        assert rows[1].TimeGenerated == "2024-01-01T01:00:00Z"
        assert rows[1].OperationName == "Write"
        assert rows[2].TimeGenerated == "2024-01-01T02:00:00Z"
        assert rows[2].OperationName == "Delete"

        # Verify query_workspace was called with correct parameters
        mock_client_instance.query_workspace.assert_called_once()
        call_kwargs = mock_client_instance.query_workspace.call_args[1]
        assert call_kwargs["workspace_id"] == "test-workspace-id"
        assert call_kwargs["query"] == "AzureActivity | take 5"

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_multiple_tables(self, mock_credential, mock_client, basic_options, basic_schema):
        """Test reading with multiple tables in response."""
        from azure.monitor.query import LogsQueryStatus

        # Create mock response with two tables
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS

        # First table
        mock_col1 = Mock()
        mock_col1.name = "Col1"
        mock_table1 = Mock()
        mock_table1.columns = [mock_col1]
        mock_table1.rows = [["value1"], ["value2"]]

        # Second table
        mock_col2 = Mock()
        mock_col2.name = "Col2"
        mock_table2 = Mock()
        mock_table2.columns = [mock_col2]
        mock_table2.rows = [["value3"]]

        mock_response.tables = [mock_table1, mock_table2]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create reader and read
        reader = AzureMonitorBatchReader(basic_options, basic_schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        # Should have rows from both tables
        assert len(rows) == 3

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_query_failure(self, mock_credential, mock_client, basic_options, basic_schema):
        """Test handling of query failure."""
        from azure.monitor.query import LogsQueryStatus

        # Create mock response with failure status
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.PARTIAL

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create reader and attempt to read
        reader = AzureMonitorBatchReader(basic_options, basic_schema)
        partitions = reader.partitions()

        with pytest.raises(Exception, match="Query failed with status"):
            list(reader.read(partitions[0]))

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_empty_results(self, mock_credential, mock_client, basic_options, basic_schema):
        """Test reading with empty results."""
        from azure.monitor.query import LogsQueryStatus

        # Create mock response with no rows
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_column = Mock()
        mock_column.name = "TestCol"
        mock_table = Mock()
        mock_table.columns = [mock_column]
        mock_table.rows = []
        mock_response.tables = [mock_table]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create reader and read
        reader = AzureMonitorBatchReader(basic_options, basic_schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        # Should have no rows
        assert len(rows) == 0

    def test_partitions_generation(self, basic_options, basic_schema):
        """Test that partitions method generates correct time ranges."""
        basic_options["num_partitions"] = "3"
        basic_options["start_time"] = "2024-01-01T00:00:00Z"
        basic_options["end_time"] = "2024-01-01T03:00:00Z"
        del basic_options["timespan"]  # Remove timespan to use start/end time

        reader = AzureMonitorBatchReader(basic_options, basic_schema)
        partitions = reader.partitions()

        # Should have 3 partitions
        assert len(partitions) == 3

        # Each partition should be approximately 1 hour
        # Partition 0: 00:00 - 01:00
        # Partition 1: 01:00 - 02:00
        # Partition 2: 02:00 - 03:00
        assert partitions[0].start_time == reader.start_time
        assert partitions[2].end_time == reader.end_time

        # Verify partitions are contiguous and non-overlapping
        assert partitions[0].end_time == partitions[1].start_time
        assert partitions[1].end_time == partitions[2].start_time

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_multiple_partitions(self, mock_credential, mock_client, basic_options, basic_schema):
        """Test reading with multiple partitions (each partition queries independently)."""
        from azure.monitor.query import LogsQueryStatus

        basic_options["num_partitions"] = "3"

        # Setup mock client to return different results based on timespan
        mock_client_instance = Mock()

        def query_side_effect(*args, **kwargs):
            # Each partition will get a different response
            mock_response = Mock()
            mock_response.status = LogsQueryStatus.SUCCESS
            mock_column = Mock()
            mock_column.name = "Value"
            mock_table = Mock()
            mock_table.columns = [mock_column]
            # Return 3 rows per partition
            mock_table.rows = [["row1"], ["row2"], ["row3"]]
            mock_response.tables = [mock_table]
            return mock_response

        mock_client_instance.query_workspace.side_effect = query_side_effect
        mock_client.return_value = mock_client_instance

        # Create reader
        reader = AzureMonitorBatchReader(basic_options, basic_schema)
        partitions = reader.partitions()

        # Verify we have 3 partitions
        assert len(partitions) == 3

        # Read from each partition independently
        rows0 = list(reader.read(partitions[0]))
        rows1 = list(reader.read(partitions[1]))
        rows2 = list(reader.read(partitions[2]))

        # Each partition should have returned 3 rows
        assert len(rows0) == 3
        assert len(rows1) == 3
        assert len(rows2) == 3

        # Verify query was called 3 times (once per partition)
        assert mock_client_instance.query_workspace.call_count == 3

        # Verify each query was called with different timespan ranges
        call_args_list = mock_client_instance.query_workspace.call_args_list
        timespan0 = call_args_list[0][1]["timespan"]
        timespan1 = call_args_list[1][1]["timespan"]
        timespan2 = call_args_list[2][1]["timespan"]

        # Verify timespans are tuples (start, end)
        assert isinstance(timespan0, tuple) and len(timespan0) == 2
        assert isinstance(timespan1, tuple) and len(timespan1) == 2
        assert isinstance(timespan2, tuple) and len(timespan2) == 2

        # Verify partitions are non-overlapping
        assert timespan0[0] == partitions[0].start_time
        assert timespan0[1] == partitions[0].end_time
        assert timespan1[0] == partitions[1].start_time
        assert timespan1[1] == partitions[1].end_time
        assert timespan2[0] == partitions[2].start_time
        assert timespan2[1] == partitions[2].end_time

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_with_start_and_end_time(self, mock_credential, mock_client, basic_schema):
        """Test reading with start_time and end_time instead of timespan."""
        from azure.monitor.query import LogsQueryStatus

        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-02T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        # Create mock response
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_column = Mock()
        mock_column.name = "TestCol"
        mock_table = Mock()
        mock_table.columns = [mock_column]
        mock_table.rows = [["value1"], ["value2"]]
        mock_response.tables = [mock_table]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create reader and read
        reader = AzureMonitorBatchReader(options, basic_schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        # Verify results
        assert len(rows) == 2

        # Verify query_workspace was called with tuple timespan (start, end)
        call_kwargs = mock_client_instance.query_workspace.call_args[1]
        assert "timespan" in call_kwargs
        timespan_arg = call_kwargs["timespan"]
        assert isinstance(timespan_arg, tuple)
        assert len(timespan_arg) == 2

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_with_start_time_only(self, mock_credential, mock_client, basic_schema):
        """Test reading with only start_time (end_time defaults to now)."""
        from azure.monitor.query import LogsQueryStatus

        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity | take 5",
            "start_time": "2024-01-01T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        # Create mock response
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_column = Mock()
        mock_column.name = "TestCol"
        mock_table = Mock()
        mock_table.columns = [mock_column]
        mock_table.rows = [["value1"]]
        mock_response.tables = [mock_table]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create reader and read
        reader = AzureMonitorBatchReader(options, basic_schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        # Verify results
        assert len(rows) == 1

        # Verify end_time was set automatically
        assert isinstance(reader.end_time, datetime)
        assert isinstance(reader.start_time, datetime)
        assert reader.start_time < reader.end_time

        # Verify query_workspace was called with tuple timespan
        call_kwargs = mock_client_instance.query_workspace.call_args[1]
        timespan_arg = call_kwargs["timespan"]
        assert isinstance(timespan_arg, tuple)
        assert len(timespan_arg) == 2

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_type_conversion_string_to_int(self, mock_credential, mock_client, basic_options):
        """Test that string values are converted to int when schema specifies LongType."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import LongType, StructField, StructType

        # Schema expects int
        schema = StructType([StructField("Count", LongType(), True)])

        # Create mock response with string value
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["Count"]
        mock_table.rows = [["123"], ["456"]]  # String values
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorBatchReader(basic_options, schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        # Verify conversion to int
        assert len(rows) == 2
        assert rows[0].Count == 123
        assert isinstance(rows[0].Count, int)
        assert rows[1].Count == 456
        assert isinstance(rows[1].Count, int)

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_type_conversion_string_to_bool(self, mock_credential, mock_client, basic_options):
        """Test that string values are converted to bool when schema specifies BooleanType."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import BooleanType, StructField, StructType

        schema = StructType([StructField("IsActive", BooleanType(), True)])

        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["IsActive"]
        mock_table.rows = [["true"], ["false"], ["1"], ["0"]]
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorBatchReader(basic_options, schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        assert len(rows) == 4
        assert rows[0].IsActive is True
        assert rows[1].IsActive is False
        assert rows[2].IsActive is True
        assert rows[3].IsActive is False

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_type_conversion_string_to_timestamp(self, mock_credential, mock_client, basic_options):
        """Test that string values are converted to datetime when schema specifies TimestampType."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import StructField, StructType, TimestampType

        schema = StructType([StructField("Timestamp", TimestampType(), True)])

        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["Timestamp"]
        mock_table.rows = [["2024-01-01T00:00:00Z"], ["2024-12-31T23:59:59Z"]]
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorBatchReader(basic_options, schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        assert len(rows) == 2
        assert isinstance(rows[0].Timestamp, datetime)
        assert isinstance(rows[1].Timestamp, datetime)

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_type_conversion_invalid_raises_error(self, mock_credential, mock_client, basic_options):
        """Test that invalid type conversions raise ValueError with descriptive message."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import LongType, StructField, StructType

        schema = StructType([StructField("Count", LongType(), True)])

        # Create mock response with non-convertible value
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["Count"]
        mock_table.rows = [["not-a-number"]]  # Cannot convert to int
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorBatchReader(basic_options, schema)
        partitions = reader.partitions()

        # Should raise ValueError with row/column info
        with pytest.raises(ValueError) as exc_info:
            list(reader.read(partitions[0]))

        assert "Row 0" in str(exc_info.value)
        assert "Count" in str(exc_info.value)

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_type_conversion_mixed_types(self, mock_credential, mock_client, basic_options):
        """Test converting multiple columns with different types."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("Name", StringType(), True),
                StructField("Count", LongType(), True),
                StructField("Score", DoubleType(), True),
                StructField("Active", BooleanType(), True),
            ]
        )

        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["Name", "Count", "Score", "Active"]
        mock_table.rows = [
            ["Alice", "100", "95.5", "true"],
            ["Bob", "200", "87.3", "false"],
        ]
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorBatchReader(basic_options, schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        assert len(rows) == 2
        # First row
        assert rows[0].Name == "Alice"
        assert rows[0].Count == 100
        assert isinstance(rows[0].Count, int)
        assert rows[0].Score == 95.5
        assert isinstance(rows[0].Score, float)
        assert rows[0].Active is True
        # Second row
        assert rows[1].Name == "Bob"
        assert rows[1].Count == 200
        assert rows[1].Score == 87.3
        assert rows[1].Active is False

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_type_conversion_preserves_none(self, mock_credential, mock_client, basic_options):
        """Test that None/NULL values are preserved regardless of schema type."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("Name", StringType(), True),
                StructField("Count", LongType(), True),
            ]
        )

        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["Name", "Count"]
        mock_table.rows = [
            ["Alice", None],
            [None, "123"],
        ]
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorBatchReader(basic_options, schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        assert len(rows) == 2
        assert rows[0].Name == "Alice"
        assert rows[0].Count is None
        assert rows[1].Name is None
        assert rows[1].Count == 123

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_type_conversion_missing_columns_set_to_null(self, mock_credential, mock_client, basic_options):
        """Test that columns in schema but not in results are set to NULL."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        # Schema expects Name, Count, and Extra columns
        schema = StructType(
            [
                StructField("Name", StringType(), True),
                StructField("Count", LongType(), True),
                StructField("Extra", StringType(), True),
            ]
        )

        # Query results only have Name and Count (missing Extra)
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["Name", "Count"]  # Extra is missing
        mock_table.rows = [
            ["Alice", "100"],
            ["Bob", "200"],
        ]
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorBatchReader(basic_options, schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        # Verify all schema columns are present, missing ones are None
        assert len(rows) == 2
        assert rows[0].Name == "Alice"
        assert rows[0].Count == 100
        assert rows[0].Extra is None  # Missing in query results
        assert rows[1].Name == "Bob"
        assert rows[1].Count == 200
        assert rows[1].Extra is None  # Missing in query results

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_type_conversion_extra_columns_ignored(self, mock_credential, mock_client, basic_options):
        """Test that columns in results but not in schema are ignored."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import StringType, StructField, StructType

        # Schema only expects Name
        schema = StructType([StructField("Name", StringType(), True)])

        # Query results have Name and Extra (Extra not in schema)
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["Name", "Extra"]
        mock_table.rows = [
            ["Alice", "value1"],
            ["Bob", "value2"],
        ]
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorBatchReader(basic_options, schema)
        partitions = reader.partitions()
        rows = list(reader.read(partitions[0]))

        # Verify only schema columns are present
        assert len(rows) == 2
        assert rows[0].Name == "Alice"
        assert not hasattr(rows[0], "Extra")  # Extra column ignored
        assert rows[1].Name == "Bob"
        assert not hasattr(rows[1], "Extra")  # Extra column ignored
