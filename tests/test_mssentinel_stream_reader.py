"""Unit tests for Azure Monitor Stream Reader."""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from cyber_connectors.MsSentinel import (
    AzureMonitorDataSource,
    AzureMonitorOffset,
    AzureMonitorStreamReader,
)


class TestAzureMonitorDataSourceStream:
    """Test data source stream reader registration."""

    @pytest.fixture
    def stream_options(self):
        """Basic valid options for stream reader."""
        return {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "2024-01-01T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

    def test_stream_reader_method_exists(self, stream_options):
        """Test that streamReader method exists and returns reader."""
        ds = AzureMonitorDataSource(options=stream_options)
        schema = StructType([StructField("test", StringType(), True)])

        # Should not raise exception (reader creation will validate options)
        reader = ds.streamReader(schema)
        assert reader is not None
        assert isinstance(reader, AzureMonitorStreamReader)


class TestAzureMonitorOffset:
    """Test AzureMonitorOffset serialization and deserialization."""

    def test_offset_initialization(self):
        """Test offset initializes with timestamp."""
        offset = AzureMonitorOffset("2024-01-01T00:00:00Z")
        assert offset.timestamp == "2024-01-01T00:00:00Z"

    def test_offset_json_serialization(self):
        """Test offset JSON serialization."""
        offset = AzureMonitorOffset("2024-01-01T00:00:00Z")
        json_str = offset.json()
        assert json_str is not None
        assert "2024-01-01T00:00:00Z" in json_str
        assert "timestamp" in json_str

    def test_offset_json_deserialization(self):
        """Test offset JSON deserialization."""
        offset = AzureMonitorOffset("2024-01-01T00:00:00Z")
        json_str = offset.json()
        restored = AzureMonitorOffset.from_json(json_str)
        assert restored.timestamp == offset.timestamp

    def test_offset_roundtrip(self):
        """Test offset roundtrip serialization/deserialization."""
        original_timestamp = "2024-12-31T23:59:59Z"
        offset1 = AzureMonitorOffset(original_timestamp)
        json_str = offset1.json()
        offset2 = AzureMonitorOffset.from_json(json_str)
        assert offset2.timestamp == original_timestamp


class TestAzureMonitorStreamReader:
    """Test Azure Monitor Stream Reader implementation."""

    @pytest.fixture
    def stream_options(self):
        """Basic valid options for stream reader."""
        return {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "2024-01-01T00:00:00Z",
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

    def test_stream_reader_initialization(self, stream_options, basic_schema):
        """Test stream reader initializes with valid options."""
        reader = AzureMonitorStreamReader(stream_options, basic_schema)

        assert reader.workspace_id == "test-workspace-id"
        assert reader.query == "AzureActivity"
        assert reader.start_time == "2024-01-01T00:00:00Z"
        assert reader.tenant_id == "test-tenant"
        assert reader.client_id == "test-client"
        assert reader.client_secret == "test-secret"
        assert reader.partition_duration == 3600  # default 1 hour

    def test_stream_reader_custom_partition_duration(self, stream_options, basic_schema):
        """Test stream reader with custom partition_duration."""
        stream_options["partition_duration"] = "1800"  # 30 minutes
        reader = AzureMonitorStreamReader(stream_options, basic_schema)
        assert reader.partition_duration == 1800

    def test_stream_reader_start_time_latest(self, basic_schema):
        """Test stream reader with start_time='latest' defaults to current time."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            # start_time not provided - should default to 'latest'
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        assert reader.start_time is not None
        # Should be in ISO format
        assert "T" in reader.start_time

    def test_stream_reader_start_time_latest_explicit(self, basic_schema):
        """Test stream reader with explicit start_time='latest'."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "latest",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        assert reader.start_time is not None
        assert "T" in reader.start_time

    @patch("cyber_connectors.MsSentinel._execute_logs_query")
    def test_stream_reader_start_time_earliest(self, mock_query, basic_schema):
        """Test stream reader with start_time='earliest' detects earliest timestamp."""
        from azure.monitor.query import LogsQueryStatus

        # Mock the query response
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["earliest"]
        mock_table.rows = [[datetime(2023, 6, 1, 12, 0, 0, tzinfo=timezone.utc)]]
        mock_response.tables = [mock_table]
        mock_query.return_value = mock_response

        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "earliest",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        assert reader.start_time == "2023-06-01T12:00:00+00:00"

        # Verify the query was called with correct parameters
        mock_query.assert_called_once()
        call_kwargs = mock_query.call_args[1]
        assert "AzureActivity | summarize earliest=min(TimeGenerated)" in call_kwargs["query"]
        assert call_kwargs["timespan"] is None  # No time restriction

    @patch("cyber_connectors.MsSentinel._execute_logs_query")
    def test_stream_reader_start_time_earliest_custom_column(self, mock_query, basic_schema):
        """Test stream reader with start_time='earliest' and custom timestamp_column."""
        from azure.monitor.query import LogsQueryStatus

        # Mock the query response
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["earliest"]
        mock_table.rows = [[datetime(2023, 7, 15, 8, 30, 0, tzinfo=timezone.utc)]]
        mock_response.tables = [mock_table]
        mock_query.return_value = mock_response

        options = {
            "workspace_id": "test-workspace-id",
            "query": "CustomTable",
            "start_time": "earliest",
            "timestamp_column": "EventTime",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        assert reader.start_time == "2023-07-15T08:30:00+00:00"
        assert reader.timestamp_column == "EventTime"

        # Verify the query uses custom timestamp column
        mock_query.assert_called_once()
        call_kwargs = mock_query.call_args[1]
        assert "CustomTable | summarize earliest=min(EventTime)" in call_kwargs["query"]

    @patch("cyber_connectors.MsSentinel._execute_logs_query")
    def test_stream_reader_start_time_earliest_empty_table(self, mock_query, basic_schema):
        """Test stream reader with start_time='earliest' fallback for empty table."""
        from azure.monitor.query import LogsQueryStatus

        # Mock empty response
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["earliest"]
        mock_table.rows = []
        mock_response.tables = [mock_table]
        mock_query.return_value = mock_response

        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "earliest",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        # Should fallback to current time
        assert reader.start_time is not None
        assert "T" in reader.start_time
        # Verify it's a recent timestamp
        start_dt = datetime.fromisoformat(reader.start_time.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        time_diff = (now - start_dt).total_seconds()
        assert time_diff < 60  # Less than 1 minute difference

    @patch("cyber_connectors.MsSentinel._execute_logs_query")
    def test_stream_reader_start_time_earliest_null_value(self, mock_query, basic_schema):
        """Test stream reader with start_time='earliest' fallback for NULL timestamp."""
        from azure.monitor.query import LogsQueryStatus

        # Mock response with NULL value
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["earliest"]
        mock_table.rows = [[None]]
        mock_response.tables = [mock_table]
        mock_query.return_value = mock_response

        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "earliest",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        # Should fallback to current time
        assert reader.start_time is not None
        assert "T" in reader.start_time

    @patch("cyber_connectors.MsSentinel._execute_logs_query")
    def test_stream_reader_start_time_earliest_query_failure(self, mock_query, basic_schema):
        """Test stream reader with start_time='earliest' fallback on query failure."""
        from azure.monitor.query import LogsQueryStatus

        # Mock failed query
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.FAILURE
        mock_query.return_value = mock_response

        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "earliest",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        # Should fallback to current time
        assert reader.start_time is not None
        assert "T" in reader.start_time

    @patch("cyber_connectors.MsSentinel._execute_logs_query")
    def test_stream_reader_start_time_earliest_string_value(self, mock_query, basic_schema):
        """Test stream reader with start_time='earliest' handles string timestamp values."""
        from azure.monitor.query import LogsQueryStatus

        # Mock response with string timestamp (some queries might return this)
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["earliest"]
        mock_table.rows = [["2023-08-20T14:45:00Z"]]
        mock_response.tables = [mock_table]
        mock_query.return_value = mock_response

        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "earliest",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        # Should parse and normalize the timestamp
        assert reader.start_time == "2023-08-20T14:45:00+00:00"

    def test_stream_reader_invalid_start_time(self, basic_schema):
        """Test stream reader fails with invalid start_time format."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "invalid-timestamp",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        with pytest.raises(ValueError, match="Invalid start_time format"):
            AzureMonitorStreamReader(options, basic_schema)

        # Also verify the error message mentions all valid formats
        try:
            AzureMonitorStreamReader(options, basic_schema)
        except ValueError as e:
            assert "latest" in str(e)
            assert "earliest" in str(e)

    def test_stream_reader_missing_workspace_id(self, basic_schema):
        """Test stream reader fails without workspace_id or resource_id."""
        options = {
            "query": "AzureActivity",
            "start_time": "2024-01-01T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        with pytest.raises(ValueError, match="Must specify either workspace_id or resource_id"):
            AzureMonitorStreamReader(options, basic_schema)

    def test_stream_reader_missing_query(self, basic_schema):
        """Test stream reader fails without query."""
        options = {
            "workspace_id": "test-workspace-id",
            "start_time": "2024-01-01T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
            "client_secret": "test-secret",
        }

        with pytest.raises(AssertionError, match="query is required"):
            AzureMonitorStreamReader(options, basic_schema)

    def test_stream_reader_missing_authentication(self, basic_schema):
        """Test stream reader fails without any authentication method."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "2024-01-01T00:00:00Z",
        }

        with pytest.raises(AssertionError, match="Authentication required"):
            AzureMonitorStreamReader(options, basic_schema)

    def test_stream_reader_partial_sp_credentials_fails(self, basic_schema):
        """Test stream reader fails with partial SP credentials."""
        # Only tenant_id and client_id provided
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "2024-01-01T00:00:00Z",
            "tenant_id": "test-tenant",
            "client_id": "test-client",
        }

        with pytest.raises(AssertionError, match="Authentication required"):
            AzureMonitorStreamReader(options, basic_schema)

    def test_stream_reader_with_databricks_credential(self, basic_schema):
        """Test stream reader with databricks_credential authentication."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "2024-01-01T00:00:00Z",
            "databricks_credential": "my-azure-credential",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        assert reader.databricks_credential == "my-azure-credential"
        assert reader.azure_default_credential is False

    def test_stream_reader_with_azure_default_credential(self, basic_schema):
        """Test stream reader with azure_default_credential authentication."""
        options = {
            "workspace_id": "test-workspace-id",
            "query": "AzureActivity",
            "start_time": "2024-01-01T00:00:00Z",
            "azure_default_credential": "true",
        }

        reader = AzureMonitorStreamReader(options, basic_schema)
        assert reader.databricks_credential is None
        assert reader.azure_default_credential is True

    def test_initial_offset(self, stream_options, basic_schema):
        """Test initial offset returns start_time minus 1 microsecond as JSON string.

        The offset is adjusted by -1 microsecond to compensate for the +1 microsecond
        added in partitions() method, preventing overlap between consecutive batches.
        """
        reader = AzureMonitorStreamReader(stream_options, basic_schema)
        offset_json = reader.initialOffset()

        # Should return JSON string
        assert isinstance(offset_json, str)
        assert "timestamp" in offset_json

        # Deserialize and verify - should be 1 microsecond before start_time
        offset = AzureMonitorOffset.from_json(offset_json)
        assert offset.timestamp == "2023-12-31T23:59:59.999999+00:00"

    def test_latest_offset(self, stream_options, basic_schema):
        """Test latest offset returns current time as JSON string."""
        reader = AzureMonitorStreamReader(stream_options, basic_schema)
        offset_json = reader.latestOffset()

        # Should return JSON string
        assert isinstance(offset_json, str)
        assert "timestamp" in offset_json

        # Deserialize and verify
        offset = AzureMonitorOffset.from_json(offset_json)
        assert offset.timestamp is not None
        # Should be in ISO format
        assert "T" in offset.timestamp
        # Should be recent (within last minute)
        offset_time = datetime.fromisoformat(offset.timestamp.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        time_diff = (now - offset_time).total_seconds()
        assert time_diff < 60  # Less than 1 minute difference

    def test_partitions_single_partition(self, stream_options, basic_schema):
        """Test partitions with time range smaller than partition_duration.

        Note: partitions() adds 1 microsecond to start_time to prevent overlap with
        previous batch's end time.
        """
        reader = AzureMonitorStreamReader(stream_options, basic_schema)

        start_offset = AzureMonitorOffset("2024-01-01T00:00:00Z").json()
        end_offset = AzureMonitorOffset("2024-01-01T00:30:00Z").json()  # 30 minutes (< 1 hour)

        partitions = reader.partitions(start_offset, end_offset)

        # Should have single partition
        # Start time is adjusted by +1 microsecond to prevent batch overlap
        assert len(partitions) == 1
        assert partitions[0].start_time.isoformat() == "2024-01-01T00:00:00.000001+00:00"
        assert partitions[0].end_time.isoformat() == "2024-01-01T00:30:00+00:00"

    def test_partitions_multiple_partitions(self, stream_options, basic_schema):
        """Test partitions splits time range into multiple partitions.

        Note: partitions() adds 1 microsecond to start_time to prevent overlap with
        previous batch's end time. Within the same batch, each partition also starts
        1 microsecond after the previous partition ends.
        """
        reader = AzureMonitorStreamReader(stream_options, basic_schema)

        start_offset = AzureMonitorOffset("2024-01-01T00:00:00Z").json()
        end_offset = AzureMonitorOffset("2024-01-01T03:00:00Z").json()  # 3 hours

        partitions = reader.partitions(start_offset, end_offset)

        # Should have 3 partitions (1 hour each)
        assert len(partitions) == 3

        # Verify partition time ranges
        # First partition: start adjusted by +1 microsecond (to prevent batch overlap)
        assert partitions[0].start_time.isoformat() == "2024-01-01T00:00:00.000001+00:00"
        assert partitions[0].end_time.isoformat() == "2024-01-01T01:00:00.000001+00:00"

        # Second partition: starts 1 microsecond after first ends (to avoid overlap within batch)
        # End time = start_time + 1 hour
        assert partitions[1].start_time.isoformat() == "2024-01-01T01:00:00.000002+00:00"
        assert partitions[1].end_time.isoformat() == "2024-01-01T02:00:00.000002+00:00"

        # Third partition: starts 1 microsecond after second ends
        # End time = min(start_time + 1 hour, end_offset) = end_offset
        assert partitions[2].start_time.isoformat() == "2024-01-01T02:00:00.000003+00:00"
        assert partitions[2].end_time.isoformat() == "2024-01-01T03:00:00+00:00"

    def test_partitions_custom_duration(self, stream_options, basic_schema):
        """Test partitions with custom partition_duration.

        Note: partitions() adds 1 microsecond to start_time to prevent overlap with
        previous batch's end time.
        """
        stream_options["partition_duration"] = "1800"  # 30 minutes
        reader = AzureMonitorStreamReader(stream_options, basic_schema)

        start_offset = AzureMonitorOffset("2024-01-01T00:00:00Z").json()
        end_offset = AzureMonitorOffset("2024-01-01T01:00:00Z").json()  # 1 hour

        partitions = reader.partitions(start_offset, end_offset)

        # Should have 2 partitions (30 minutes each)
        assert len(partitions) == 2

        # First partition: start adjusted by +1 microsecond (to prevent batch overlap)
        assert partitions[0].start_time.isoformat() == "2024-01-01T00:00:00.000001+00:00"
        assert partitions[0].end_time.isoformat() == "2024-01-01T00:30:00.000001+00:00"

        # Second partition starts 1 microsecond after first ends (to avoid overlap within batch)
        assert partitions[1].start_time.isoformat() == "2024-01-01T00:30:00.000002+00:00"
        assert partitions[1].end_time.isoformat() == "2024-01-01T01:00:00+00:00"

    def test_partitions_partial_last_partition(self, stream_options, basic_schema):
        """Test partitions handles partial last partition correctly.

        Note: partitions() adds 1 microsecond to start_time to prevent overlap with
        previous batch's end time.
        """
        reader = AzureMonitorStreamReader(stream_options, basic_schema)

        start_offset = AzureMonitorOffset("2024-01-01T00:00:00Z").json()
        end_offset = AzureMonitorOffset("2024-01-01T02:30:00Z").json()  # 2.5 hours

        partitions = reader.partitions(start_offset, end_offset)

        # Should have 3 partitions: 1h, 1h, 0.5h
        assert len(partitions) == 3

        # First partition: start adjusted by +1 microsecond, end = start + 1 hour
        assert partitions[0].end_time.isoformat() == "2024-01-01T01:00:00.000001+00:00"
        # Second partition ends at 2-hour boundary + 2 microseconds (starts 1 microsecond after first)
        assert partitions[1].end_time.isoformat() == "2024-01-01T02:00:00.000002+00:00"
        # Third partition (partial, starts 1 microsecond after second) ends at 2.5 hours (end_offset)
        assert partitions[2].end_time.isoformat() == "2024-01-01T02:30:00+00:00"

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_success(self, mock_credential, mock_client, stream_options, basic_schema):
        """Test successful streaming read."""
        from azure.monitor.query import LogsQueryStatus

        from cyber_connectors.MsSentinel import TimeRangePartition

        # Create mock response
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS

        mock_table = Mock()
        mock_table.columns = ["TimeGenerated", "OperationName"]
        mock_table.rows = [
            ["2024-01-01T00:15:00Z", "Read"],
            ["2024-01-01T00:30:00Z", "Write"],
        ]
        mock_response.tables = [mock_table]

        # Setup mock client
        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        # Create reader
        reader = AzureMonitorStreamReader(stream_options, basic_schema)

        # Create partition with time range
        partition = TimeRangePartition(
            start_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

        # Read data
        rows = list(reader.read(partition))

        # Verify results
        assert len(rows) == 2
        assert rows[0].TimeGenerated == "2024-01-01T00:15:00Z"
        assert rows[0].OperationName == "Read"
        assert rows[1].TimeGenerated == "2024-01-01T00:30:00Z"
        assert rows[1].OperationName == "Write"

        # Verify query was called with correct parameters
        mock_client_instance.query_workspace.assert_called_once()
        call_kwargs = mock_client_instance.query_workspace.call_args[1]
        assert call_kwargs["workspace_id"] == "test-workspace-id"
        assert call_kwargs["query"] == "AzureActivity"

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_empty_results(self, mock_credential, mock_client, stream_options, basic_schema):
        """Test streaming read with empty results."""
        from azure.monitor.query import LogsQueryStatus

        from cyber_connectors.MsSentinel import TimeRangePartition

        # Create mock response with no rows
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["TimeGenerated", "OperationName"]
        mock_table.rows = []
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorStreamReader(stream_options, basic_schema)
        partition = TimeRangePartition(
            start_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

        rows = list(reader.read(partition))
        assert len(rows) == 0

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_query_failure(self, mock_credential, mock_client, stream_options, basic_schema):
        """Test handling of query failure (PARTIAL status for non-size-limit reasons) in streaming."""
        from azure.monitor.query import LogsQueryStatus

        from cyber_connectors.MsSentinel import TimeRangePartition

        # Create mock response with PARTIAL status (non-size-limit error)
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.PARTIAL
        # Use a dict for partial_error to avoid Mock auto-creation issues
        mock_response.partial_error = {
            "code": "SomeOtherError",
            "message": "Query timeout occurred",
            "details": None,
            "innererror": None,
        }
        mock_response.tables = []

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorStreamReader(stream_options, basic_schema)
        partition = TimeRangePartition(
            start_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

        with pytest.raises(Exception, match="Query failed with status"):
            list(reader.read(partition))

    def test_commit_does_nothing(self, stream_options, basic_schema):
        """Test commit method (should do nothing as Spark handles checkpointing)."""
        reader = AzureMonitorStreamReader(stream_options, basic_schema)
        end_offset = AzureMonitorOffset("2024-01-01T01:00:00Z").json()

        # Should not raise exception
        reader.commit(end_offset)

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_with_type_conversion(self, mock_credential, mock_client, stream_options):
        """Test streaming read with type conversion from schema."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import LongType, StructField, StructType

        from cyber_connectors.MsSentinel import TimeRangePartition

        schema = StructType([StructField("Count", LongType(), True)])

        # Create mock response with string values that should be converted to int
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["Count"]
        mock_table.rows = [["123"], ["456"]]
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorStreamReader(stream_options, schema)
        partition = TimeRangePartition(
            start_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

        rows = list(reader.read(partition))

        # Verify type conversion
        assert len(rows) == 2
        assert rows[0].Count == 123
        assert isinstance(rows[0].Count, int)
        assert rows[1].Count == 456
        assert isinstance(rows[1].Count, int)

    @patch("azure.monitor.query.LogsQueryClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_read_with_missing_columns(self, mock_credential, mock_client, stream_options):
        """Test streaming read when query results are missing schema columns."""
        from azure.monitor.query import LogsQueryStatus
        from pyspark.sql.types import StringType, StructField, StructType

        from cyber_connectors.MsSentinel import TimeRangePartition

        # Schema expects Name and Extra columns
        schema = StructType([StructField("Name", StringType(), True), StructField("Extra", StringType(), True)])

        # Query results only have Name (missing Extra)
        mock_response = Mock()
        mock_response.status = LogsQueryStatus.SUCCESS
        mock_table = Mock()
        mock_table.columns = ["Name"]
        mock_table.rows = [["Alice"], ["Bob"]]
        mock_response.tables = [mock_table]

        mock_client_instance = Mock()
        mock_client_instance.query_workspace.return_value = mock_response
        mock_client.return_value = mock_client_instance

        reader = AzureMonitorStreamReader(stream_options, schema)
        partition = TimeRangePartition(
            start_time=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

        rows = list(reader.read(partition))

        # Verify missing column is set to None
        assert len(rows) == 2
        assert rows[0].Name == "Alice"
        assert rows[0].Extra is None
        assert rows[1].Name == "Bob"
        assert rows[1].Extra is None
