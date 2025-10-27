"""Unit tests for Splunk data source."""

import json
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql.types import Row, StructField, StructType, IntegerType, StringType, TimestampType

from cyber_connectors.Splunk import SplunkDataSource, SplunkHecBatchWriter, SplunkHecStreamWriter


@pytest.fixture
def basic_options():
    """Basic required options for Splunk data source."""
    return {
        "url": "http://localhost:8088/services/collector/event",
        "token": "test-token-12345"
    }


@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])


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
            "remove_indexed_fields": "true"
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
        options = {
            "url": "http://localhost:8088",
            "token": "test-token",
            "single_event_column": "message"
        }
        writer = SplunkHecBatchWriter(options)
        assert writer.source_type == "text"

    @patch('pyspark.TaskContext')
    @patch('cyber_connectors.Splunk.get_http_session')
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

    @patch('pyspark.TaskContext')
    @patch('cyber_connectors.Splunk.get_http_session')
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

        options = {
            "url": "http://localhost:8088",
            "token": "test-token",
            "time_column": "timestamp"
        }
        writer = SplunkHecBatchWriter(options)

        timestamp = datetime(2024, 1, 1, 12, 0, 0)
        rows = [Row(id=1, timestamp=timestamp)]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        call_args = mock_session.post.call_args
        data = call_args[1]['data']
        payload = json.loads(data)
        assert payload['time'] == timestamp.timestamp()

    @patch('pyspark.TaskContext')
    @patch('cyber_connectors.Splunk.get_http_session')
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

        options = {
            "url": "http://localhost:8088",
            "token": "test-token",
            "indexed_fields": "field1,field2"
        }
        writer = SplunkHecBatchWriter(options)

        rows = [Row(id=1, field1="value1", field2="value2", field3="value3")]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        call_args = mock_session.post.call_args
        data = call_args[1]['data']
        payload = json.loads(data)
        assert 'fields' in payload
        assert payload['fields']['field1'] == "value1"
        assert payload['fields']['field2'] == "value2"
        assert 'event' in payload

    @patch('pyspark.TaskContext')
    @patch('cyber_connectors.Splunk.get_http_session')
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

        options = {
            "url": "http://localhost:8088",
            "token": "test-token",
            "single_event_column": "message"
        }
        writer = SplunkHecBatchWriter(options)

        rows = [Row(id=1, message="This is a log message")]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        call_args = mock_session.post.call_args
        data = call_args[1]['data']
        payload = json.loads(data)
        assert payload['event'] == "This is a log message"
        assert payload['sourcetype'] == "text"

    @patch('pyspark.TaskContext')
    @patch('cyber_connectors.Splunk.get_http_session')
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

        options = {
            "url": "http://localhost:8088",
            "token": "test-token",
            "batch_size": "2"
        }
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
