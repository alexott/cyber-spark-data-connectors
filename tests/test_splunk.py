"""Unit tests for Splunk data source."""

import json
import os
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType, TimestampType

from cyber_connectors.Splunk import (
    SplunkBatchReader,
    SplunkDataSource,
    SplunkHecBatchWriter,
    SplunkHecStreamWriter,
    SplunkOffset,
    SplunkStreamReader,
    SplunkTimeRangePartition,
    _convert_value_to_schema_type,
    _parse_time_range,
)


@pytest.fixture
def basic_options():
    """Basic required options for Splunk data source."""
    return {"url": "http://localhost:8088/services/collector/event", "token": "test-token-12345"}


@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )


class TestSplunkDataSource:
    """Test SplunkDataSource class."""

    def test_name(self):
        """Test that data source name is 'splunk'."""
        assert SplunkDataSource.name() == "splunk"

    def test_streamWriter(self, basic_options, sample_schema):
        """Test that streamWriter returns SplunkHecStreamWriter."""
        ds = SplunkDataSource(options=basic_options)
        writer = ds.streamWriter(sample_schema, overwrite=True)
        assert isinstance(writer, SplunkHecStreamWriter)

    def test_writer(self, basic_options, sample_schema):
        """Test that writer returns SplunkHecBatchWriter."""
        ds = SplunkDataSource(options=basic_options)
        writer = ds.writer(sample_schema, overwrite=True)
        assert isinstance(writer, SplunkHecBatchWriter)


class TestSplunkHecWriter:
    """Test SplunkHecWriter functionality."""

    def test_init_required_options(self, basic_options):
        """Test initialization with required options."""
        writer = SplunkHecBatchWriter(basic_options)
        assert writer.url == "http://localhost:8088/services/collector/event"
        assert writer.token == "test-token-12345"
        assert writer.batch_size == 50
        assert writer.source_type == "_json"

    def test_init_missing_url(self):
        """Test that missing URL raises assertion error."""
        with pytest.raises(AssertionError):
            SplunkHecBatchWriter({"token": "test-token"})

    def test_init_missing_token(self):
        """Test that missing token raises assertion error."""
        with pytest.raises(AssertionError):
            SplunkHecBatchWriter({"url": "http://localhost:8088"})

    def test_init_with_all_options(self):
        """Test initialization with all options."""
        options = {
            "url": "http://localhost:8088/services/collector/event",
            "token": "test-token",
            "time_column": "ts",
            "batch_size": "100",
            "index": "main",
            "source": "spark",
            "host": "test-host",
            "sourcetype": "custom",
            "single_event_column": "message",
            "indexed_fields": "field1,field2",
            "remove_indexed_fields": "true",
        }
        writer = SplunkHecBatchWriter(options)
        assert writer.time_col == "ts"
        assert writer.batch_size == 100
        assert writer.index == "main"
        assert writer.source == "spark"
        assert writer.host == "test-host"
        assert writer.source_type == "custom"
        assert writer.single_event_column == "message"
        assert writer.indexed_fields == ["field1", "field2"]
        assert writer.omit_indexed_fields is True

    def test_single_event_column_changes_sourcetype(self):
        """Test that single_event_column changes sourcetype to 'text' if default."""
        options = {"url": "http://localhost:8088", "token": "test-token", "single_event_column": "message"}
        writer = SplunkHecBatchWriter(options)
        assert writer.source_type == "text"

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.Splunk.get_http_session")
    def test_write_basic(self, mock_get_session, mock_task_context, basic_options):
        """Test basic write functionality."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"text":"Success","code":0}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        writer = SplunkHecBatchWriter(basic_options)
        rows = [Row(id=1, name="test")]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.partition_id == 0
        assert commit_msg.count == 1
        assert mock_session.post.called

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.Splunk.get_http_session")
    def test_write_with_time_column(self, mock_get_session, mock_task_context):
        """Test write with time_column option."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"text":"Success","code":0}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {"url": "http://localhost:8088", "token": "test-token", "time_column": "timestamp"}
        writer = SplunkHecBatchWriter(options)

        timestamp = datetime(2024, 1, 1, 12, 0, 0)
        rows = [Row(id=1, timestamp=timestamp)]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        call_args = mock_session.post.call_args
        data = call_args[1]["data"]
        payload = json.loads(data)
        assert payload["time"] == timestamp.timestamp()

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.Splunk.get_http_session")
    def test_write_with_indexed_fields(self, mock_get_session, mock_task_context):
        """Test write with indexed_fields option."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"text":"Success","code":0}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {"url": "http://localhost:8088", "token": "test-token", "indexed_fields": "field1,field2"}
        writer = SplunkHecBatchWriter(options)

        rows = [Row(id=1, field1="value1", field2="value2", field3="value3")]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        call_args = mock_session.post.call_args
        data = call_args[1]["data"]
        payload = json.loads(data)
        assert "fields" in payload
        assert payload["fields"]["field1"] == "value1"
        assert payload["fields"]["field2"] == "value2"
        assert "event" in payload

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.Splunk.get_http_session")
    def test_write_with_single_event_column(self, mock_get_session, mock_task_context):
        """Test write with single_event_column option."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"text":"Success","code":0}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {"url": "http://localhost:8088", "token": "test-token", "single_event_column": "message"}
        writer = SplunkHecBatchWriter(options)

        rows = [Row(id=1, message="This is a log message")]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        call_args = mock_session.post.call_args
        data = call_args[1]["data"]
        payload = json.loads(data)
        assert payload["event"] == "This is a log message"
        assert payload["sourcetype"] == "text"

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.Splunk.get_http_session")
    def test_write_batching(self, mock_get_session, mock_task_context):
        """Test that batching works correctly."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"text":"Success","code":0}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {"url": "http://localhost:8088", "token": "test-token", "batch_size": "2"}
        writer = SplunkHecBatchWriter(options)

        rows = [Row(id=i) for i in range(5)]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 5
        # Should be called 3 times: 2+2+1
        assert mock_session.post.call_count == 3


class TestSplunkHecStreamWriter:
    """Test SplunkHecStreamWriter functionality."""

    def test_commit(self, basic_options):
        """Test commit method."""
        writer = SplunkHecStreamWriter(basic_options)
        messages = [Mock(count=10), Mock(count=20)]
        writer.commit(messages, batchId=1)

    def test_abort(self, basic_options):
        """Test abort method."""
        writer = SplunkHecStreamWriter(basic_options)
        messages = [Mock(count=10)]
        writer.abort(messages, batchId=1)


# =======================
# Splunk Reader Tests
# =======================


@pytest.fixture
def read_options():
    """Basic options for Splunk read operations."""
    return {
        "splunkd_url": "https://splunk.example.com:8089",
        "splunkd_token": "test-splunkd-token-12345",
        "index": "main",
        "timespan": "P1D",
    }


@pytest.fixture
def read_schema():
    """Sample schema for read testing."""
    return StructType(
        [
            StructField("_time", TimestampType(), True),
            StructField("_indextime", TimestampType(), True),
            StructField("_raw", StringType(), True),
            StructField("host", StringType(), True),
            StructField("source", StringType(), True),
            StructField("sourcetype", StringType(), True),
        ]
    )


class TestHelperFunctions:
    """Test helper functions."""

    def test_parse_time_range_with_timespan_days(self):
        """Test parsing timespan with days."""
        start, end = _parse_time_range(timespan="P7D")
        assert (end - start).days == 7

    def test_parse_time_range_with_timespan_hours(self):
        """Test parsing timespan with hours."""
        start, end = _parse_time_range(timespan="PT6H")
        assert (end - start).total_seconds() == 6 * 3600

    def test_parse_time_range_with_start_and_end_time(self):
        """Test parsing with start_time and end_time."""
        start, end = _parse_time_range(start_time="2024-01-01T00:00:00Z", end_time="2024-01-02T00:00:00Z")
        assert start.year == 2024
        assert start.month == 1
        assert start.day == 1
        assert end.day == 2

    def test_parse_time_range_with_start_time_only(self):
        """Test parsing with start_time only (end defaults to now)."""
        start, end = _parse_time_range(start_time="2024-01-01T00:00:00Z")
        assert start.year == 2024
        assert start.month == 1
        assert start.day == 1
        assert end > start

    def test_parse_time_range_invalid_timespan(self):
        """Test invalid timespan format."""
        with pytest.raises(ValueError, match="Invalid timespan format"):
            _parse_time_range(timespan="INVALID")

    def test_parse_time_range_empty_timespan(self):
        """Test empty timespan (no components)."""
        with pytest.raises(ValueError, match="must specify days"):
            _parse_time_range(timespan="P")

    def test_parse_time_range_neither_provided(self):
        """Test error when neither timespan nor start_time provided."""
        with pytest.raises(Exception, match="Either 'timespan' or 'start_time' must be provided"):
            _parse_time_range()

    def test_convert_value_string_epoch_to_timestamp(self):
        """Test converting string epoch to timestamp."""
        value = "1609459200.123"  # 2021-01-01 00:00:00.123
        result = _convert_value_to_schema_type(value, TimestampType())
        assert isinstance(result, datetime)
        assert result.year == 2021
        assert result.month == 1

    def test_convert_value_float_epoch_to_timestamp(self):
        """Test converting float epoch to timestamp."""
        value = 1609459200.456
        result = _convert_value_to_schema_type(value, TimestampType())
        assert isinstance(result, datetime)
        assert result.year == 2021

    def test_convert_value_to_string(self):
        """Test converting value to string."""
        assert _convert_value_to_schema_type(123, StringType()) == "123"
        assert _convert_value_to_schema_type(True, StringType()) == "True"

    def test_convert_value_to_integer(self):
        """Test converting value to integer."""
        assert _convert_value_to_schema_type("123", IntegerType()) == 123
        assert _convert_value_to_schema_type(123.7, IntegerType()) == 123

    def test_convert_value_none(self):
        """Test converting None value."""
        assert _convert_value_to_schema_type(None, StringType()) is None
        assert _convert_value_to_schema_type(None, TimestampType()) is None


class TestSplunkOffset:
    """Test SplunkOffset class."""

    def test_offset_initialization(self):
        """Test offset initialization."""
        offset = SplunkOffset(1609459200, "abc123")
        assert offset.indextime_epoch == 1609459200
        assert offset.tie_breaker == "abc123"
        assert offset.version == 1

    def test_offset_json_serialization(self):
        """Test offset JSON serialization."""
        offset = SplunkOffset(1609459200, "abc123")
        json_str = offset.json()
        data = json.loads(json_str)
        assert data["indextime_epoch"] == 1609459200
        assert data["tie_breaker"] == "abc123"
        assert data["version"] == 1

    def test_offset_json_deserialization(self):
        """Test offset JSON deserialization."""
        json_str = '{"indextime_epoch": 1609459200, "tie_breaker": "abc123", "version": 1}'
        offset = SplunkOffset.from_json(json_str)
        assert offset.indextime_epoch == 1609459200
        assert offset.tie_breaker == "abc123"
        assert offset.version == 1

    def test_offset_roundtrip(self):
        """Test offset roundtrip serialization."""
        original = SplunkOffset(1609459200, "xyz789", version=1)
        json_str = original.json()
        restored = SplunkOffset.from_json(json_str)
        assert restored.indextime_epoch == original.indextime_epoch
        assert restored.tie_breaker == original.tie_breaker
        assert restored.version == original.version


class TestSplunkDataSourceReader:
    """Test SplunkDataSource reader methods."""

    def test_reader_method_exists(self, read_options, read_schema):
        """Test that reader method returns SplunkBatchReader."""
        ds = SplunkDataSource(options=read_options)
        reader = ds.reader(read_schema)
        assert isinstance(reader, SplunkBatchReader)

    def test_stream_reader_method_exists(self, read_options, read_schema):
        """Test that streamReader method returns SplunkStreamReader."""
        ds = SplunkDataSource(options=read_options)
        stream_reader = ds.streamReader(read_schema)
        assert isinstance(stream_reader, SplunkStreamReader)

    def test_default_schema(self, read_options):
        """Test default schema generation."""
        ds = SplunkDataSource(options=read_options)
        schema = ds.schema()
        assert schema is not None
        field_names = [field.name for field in schema.fields]
        assert "_time" in field_names
        assert "_indextime" in field_names
        assert "_raw" in field_names


class TestSplunkBatchReader:
    """Test SplunkBatchReader class."""

    def test_reader_initialization(self, read_options, read_schema):
        """Test batch reader initialization."""
        reader = SplunkBatchReader(read_options, read_schema)
        assert reader.splunkd_url == "https://splunk.example.com:8089"
        assert reader.splunkd_token == "test-splunkd-token-12345"
        assert reader.spl_query == "search index=main"
        assert reader.num_partitions == 1

    def test_reader_missing_splunkd_url(self, read_schema):
        """Test error when splunkd_url is missing."""
        options = {"splunkd_token": "test-token", "index": "main", "timespan": "P1D"}
        with pytest.raises(AssertionError, match="splunkd_url is required"):
            SplunkBatchReader(options, read_schema)

    def test_reader_missing_splunkd_token(self, read_schema):
        """Test error when splunkd_token is missing."""
        options = {"splunkd_url": "https://splunk.example.com:8089", "index": "main", "timespan": "P1D"}
        with pytest.raises(AssertionError, match="splunkd_token is required"):
            SplunkBatchReader(options, read_schema)

    def test_reader_with_env_token(self, read_schema):
        """Test reader with token from environment variable."""
        options = {"splunkd_url": "https://splunk.example.com:8089", "index": "main", "timespan": "P1D"}
        with patch.dict(os.environ, {"SPLUNK_AUTH_TOKEN": "env-token-12345"}):
            reader = SplunkBatchReader(options, read_schema)
            assert reader.splunkd_token == "env-token-12345"

    def test_reader_missing_query_and_index(self, read_schema):
        """Test error when both query and index are missing."""
        options = {"splunkd_url": "https://splunk.example.com:8089", "splunkd_token": "test-token", "timespan": "P1D"}
        with pytest.raises(ValueError, match="Either 'query' or 'index' must be provided"):
            SplunkBatchReader(options, read_schema)

    def test_reader_with_full_query(self, read_schema):
        """Test reader with full SPL query."""
        options = {
            "splunkd_url": "https://splunk.example.com:8089",
            "splunkd_token": "test-token",
            "query": "search index=security sourcetype=firewall | stats count by status",
            "timespan": "P1D",
        }
        reader = SplunkBatchReader(options, read_schema)
        assert reader.spl_query == "search index=security sourcetype=firewall | stats count by status"

    def test_reader_with_simple_params(self, read_schema):
        """Test reader with simple parameters."""
        options = {
            "splunkd_url": "https://splunk.example.com:8089",
            "splunkd_token": "test-token",
            "index": "security",
            "sourcetype": "firewall",
            "search_filter": "status=200",
            "timespan": "P1D",
        }
        reader = SplunkBatchReader(options, read_schema)
        assert reader.spl_query == "search index=security sourcetype=firewall status=200"

    def test_reader_invalid_index_name(self, read_schema):
        """Test error with invalid index name."""
        options = {
            "splunkd_url": "https://splunk.example.com:8089",
            "splunkd_token": "test-token",
            "index": "main; DROP TABLE",  # SQL injection attempt
            "timespan": "P1D",
        }
        with pytest.raises(ValueError, match="Invalid index name"):
            SplunkBatchReader(options, read_schema)

    def test_reader_search_filter_with_pipe(self, read_schema):
        """Test error when search_filter contains pipe."""
        options = {
            "splunkd_url": "https://splunk.example.com:8089",
            "splunkd_token": "test-token",
            "index": "main",
            "search_filter": "error | stats count",
            "timespan": "P1D",
        }
        with pytest.raises(ValueError, match="search_filter cannot contain"):
            SplunkBatchReader(options, read_schema)

    def test_partitions_generation_num_partitions(self, read_options, read_schema):
        """Test partition generation with num_partitions."""
        options = {**read_options, "num_partitions": "4"}
        reader = SplunkBatchReader(options, read_schema)
        partitions = reader.partitions()
        assert len(partitions) == 4
        # Check non-overlapping ranges
        for i in range(len(partitions) - 1):
            assert partitions[i].end_epoch == partitions[i + 1].start_epoch

    def test_partitions_generation_partition_duration(self, read_options, read_schema):
        """Test partition generation with partition_duration."""
        options = {
            **read_options,
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-01T06:00:00Z",  # 6 hours
            "partition_duration": "7200",  # 2 hours
        }
        del options["timespan"]
        reader = SplunkBatchReader(options, read_schema)
        partitions = reader.partitions()
        assert len(partitions) == 3  # 6 hours / 2 hours = 3 partitions

    @patch("requests.Session.post")
    def test_read_success(self, mock_post, read_options, read_schema):
        """Test successful read operation."""
        # Mock response with Splunk export format
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.iter_lines.return_value = [
            '{"result": {"_time": "1609459200.0", "_raw": "test event 1", "host": "web01"}}',
            '{"result": {"_time": "1609459201.0", "_raw": "test event 2", "host": "web02"}}',
        ]
        mock_post.return_value = mock_response

        reader = SplunkBatchReader(read_options, read_schema)
        partition = SplunkTimeRangePartition(1609459200.0, 1609459300.0)

        rows = list(reader.read(partition))
        assert len(rows) == 2
        assert rows[0]["_raw"] == "test event 1"
        assert rows[0]["host"] == "web01"
        assert rows[1]["_raw"] == "test event 2"

    @patch("requests.Session.post")
    def test_read_empty_results(self, mock_post, read_options, read_schema):
        """Test read with empty results."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.iter_lines.return_value = []
        mock_post.return_value = mock_response

        reader = SplunkBatchReader(read_options, read_schema)
        partition = SplunkTimeRangePartition(1609459200.0, 1609459300.0)

        rows = list(reader.read(partition))
        assert len(rows) == 0

    @patch("requests.Session.post")
    def test_read_with_type_conversion(self, mock_post, read_options, read_schema):
        """Test read with type conversion."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.iter_lines.return_value = [
            '{"result": {"_time": "1609459200.123", "_indextime": "1609459200.456", "_raw": "test", "host": "web01"}}'
        ]
        mock_post.return_value = mock_response

        reader = SplunkBatchReader(read_options, read_schema)
        partition = SplunkTimeRangePartition(1609459200.0, 1609459300.0)

        rows = list(reader.read(partition))
        assert len(rows) == 1
        # Check timestamp conversion
        assert isinstance(rows[0]["_time"], datetime)
        assert isinstance(rows[0]["_indextime"], datetime)
        assert rows[0]["_time"].year == 2021


class TestSplunkStreamReader:
    """Test SplunkStreamReader class."""

    def test_stream_reader_initialization(self, read_options, read_schema):
        """Test stream reader initialization."""
        options = {**read_options, "start_time": "2024-01-01T00:00:00Z"}
        del options["timespan"]
        reader = SplunkStreamReader(options, read_schema)
        assert reader.splunkd_url == "https://splunk.example.com:8089"
        assert reader.start_time > 0
        assert reader.partition_duration == 3600
        assert reader.safety_lag_seconds == 60

    def test_stream_reader_start_time_latest(self, read_options, read_schema):
        """Test stream reader with start_time=latest."""
        options = {**read_options, "start_time": "latest"}
        del options["timespan"]
        reader = SplunkStreamReader(options, read_schema)
        assert reader.start_time > 0

    def test_stream_reader_custom_partition_duration(self, read_options, read_schema):
        """Test stream reader with custom partition_duration."""
        options = {**read_options, "start_time": "latest", "partition_duration": "7200"}
        del options["timespan"]
        reader = SplunkStreamReader(options, read_schema)
        assert reader.partition_duration == 7200

    def test_stream_reader_query_validation_stats(self, read_options, read_schema):
        """Test query validation rejects transforming commands."""
        options = {
            **read_options,
            "query": "search index=main | stats count by status",
            "start_time": "latest",
        }
        del options["timespan"]
        del options["index"]
        with pytest.raises(ValueError, match="Streaming mode requires event-returning queries"):
            SplunkStreamReader(options, read_schema)

    def test_stream_reader_allow_transforming_queries(self, read_options, read_schema):
        """Test stream reader with allow_transforming_queries=true."""
        options = {
            **read_options,
            "query": "search index=main | stats count by status",
            "start_time": "latest",
            "allow_transforming_queries": "true",
        }
        del options["timespan"]
        del options["index"]
        reader = SplunkStreamReader(options, read_schema)
        assert reader.spl_query == "search index=main | stats count by status"

    def test_initial_offset(self, read_options, read_schema):
        """Test initialOffset method."""
        options = {**read_options, "start_time": "2024-01-01T00:00:00Z"}
        del options["timespan"]
        reader = SplunkStreamReader(options, read_schema)
        offset_json = reader.initialOffset()
        offset = SplunkOffset.from_json(offset_json)
        # Initial offset should be start_time - 1 second
        assert offset.indextime_epoch < reader.start_time

    def test_latest_offset(self, read_options, read_schema):
        """Test latestOffset method."""
        options = {**read_options, "start_time": "latest"}
        del options["timespan"]
        reader = SplunkStreamReader(options, read_schema)
        offset_json = reader.latestOffset()
        offset = SplunkOffset.from_json(offset_json)
        # Latest offset should be current time minus safety lag
        current_time = int(datetime.now(timezone.utc).timestamp())
        assert offset.indextime_epoch < current_time
        assert offset.indextime_epoch >= current_time - reader.safety_lag_seconds - 2

    def test_partitions_single_partition(self, read_options, read_schema):
        """Test partitions method with single partition."""
        options = {**read_options, "start_time": "latest"}
        del options["timespan"]
        reader = SplunkStreamReader(options, read_schema)

        start_offset = SplunkOffset(1609459200, "")
        end_offset = SplunkOffset(1609459800, "")  # 600 seconds later (< 1 hour default)

        partitions = reader.partitions(start_offset.json(), end_offset.json())
        assert len(partitions) == 1

    def test_partitions_multiple_partitions(self, read_options, read_schema):
        """Test partitions method with multiple partitions."""
        options = {**read_options, "start_time": "latest", "partition_duration": "3600"}
        del options["timespan"]
        reader = SplunkStreamReader(options, read_schema)

        start_offset = SplunkOffset(1609459200, "")
        end_offset = SplunkOffset(1609470000, "")  # 10800 seconds later (3 hours)

        partitions = reader.partitions(start_offset.json(), end_offset.json())
        assert len(partitions) == 3  # 3 hours / 1 hour = 3 partitions
