"""Unit tests for REST API data source."""

import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType, TimestampType

from cyber_connectors.RestApi import RestApiBatchWriter, RestApiDataSource, RestApiStreamWriter


@pytest.fixture
def basic_options():
    """Basic required options for REST API data source."""
    return {"url": "http://localhost:8001/api/endpoint"}


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


class TestRestApiDataSource:
    """Test RestApiDataSource class."""

    def test_name(self):
        """Test that data source name is 'rest'."""
        assert RestApiDataSource.name() == "rest"

    def test_streamWriter(self, basic_options, sample_schema):
        """Test that streamWriter returns RestApiStreamWriter."""
        ds = RestApiDataSource(options=basic_options)
        writer = ds.streamWriter(sample_schema, overwrite=True)
        assert isinstance(writer, RestApiStreamWriter)

    def test_writer(self, basic_options, sample_schema):
        """Test that writer returns RestApiBatchWriter."""
        ds = RestApiDataSource(options=basic_options)
        writer = ds.writer(sample_schema, overwrite=True)
        assert isinstance(writer, RestApiBatchWriter)


class TestRestApiWriter:
    """Test RestApiWriter functionality."""

    def test_init_required_options(self, basic_options):
        """Test initialization with required options."""
        writer = RestApiBatchWriter(basic_options)
        assert writer.url == "http://localhost:8001/api/endpoint"
        assert writer.payload_format == "json"
        assert writer.http_method == "post"

    def test_init_missing_url(self):
        """Test that missing URL raises assertion error."""
        with pytest.raises(AssertionError):
            RestApiBatchWriter({})

    def test_init_with_custom_format(self):
        """Test initialization with custom format."""
        options = {"url": "http://localhost:8001", "http_format": "json"}
        writer = RestApiBatchWriter(options)
        assert writer.payload_format == "json"

    def test_init_with_custom_method(self):
        """Test initialization with custom HTTP method."""
        options = {"url": "http://localhost:8001", "http_method": "put"}
        writer = RestApiBatchWriter(options)
        assert writer.http_method == "put"

    def test_init_invalid_format(self):
        """Test that invalid format raises assertion error."""
        options = {"url": "http://localhost:8001", "http_format": "xml"}
        with pytest.raises(AssertionError):
            RestApiBatchWriter(options)

    def test_init_invalid_method(self):
        """Test that invalid HTTP method raises assertion error."""
        options = {"url": "http://localhost:8001", "http_method": "delete"}
        with pytest.raises(AssertionError):
            RestApiBatchWriter(options)

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_basic_post(self, mock_get_session, mock_task_context, basic_options):
        """Test basic write functionality with POST."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        writer = RestApiBatchWriter(basic_options)
        rows = [Row(id=1, name="test")]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.partition_id == 0
        assert commit_msg.count == 1
        assert mock_session.post.called
        call_args = mock_session.post.call_args
        assert call_args[0][0] == "http://localhost:8001/api/endpoint"

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_with_put(self, mock_get_session, mock_task_context):
        """Test write functionality with PUT method."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.put.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {"url": "http://localhost:8001", "http_method": "put"}
        writer = RestApiBatchWriter(options)
        rows = [Row(id=1, name="test")]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        assert mock_session.put.called
        assert not mock_session.post.called

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_json_format(self, mock_get_session, mock_task_context, basic_options):
        """Test that data is properly serialized to JSON."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        writer = RestApiBatchWriter(basic_options)
        timestamp = datetime(2024, 1, 1, 12, 0, 0)
        rows = [Row(id=1, name="test", timestamp=timestamp)]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        call_args = mock_session.post.call_args
        data = call_args[1]["data"]
        payload = json.loads(data)
        assert payload["id"] == 1
        assert payload["name"] == "test"
        assert "timestamp" in payload

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_multiple_rows(self, mock_get_session, mock_task_context, basic_options):
        """Test writing multiple rows."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        writer = RestApiBatchWriter(basic_options)
        rows = [Row(id=i, name=f"test{i}") for i in range(5)]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 5
        assert mock_session.post.call_count == 5

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_content_type_header(self, mock_get_session, mock_task_context, basic_options):
        """Test that Content-Type header is set correctly."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        writer = RestApiBatchWriter(basic_options)
        rows = [Row(id=1)]
        writer.write(iter(rows))

        # Check that get_http_session was called with correct headers
        call_args = mock_get_session.call_args
        headers = call_args[1]["additional_headers"]
        assert headers["Content-Type"] == "application/json"


class TestRestApiStreamWriter:
    """Test RestApiStreamWriter functionality."""

    def test_commit(self, basic_options):
        """Test commit method."""
        writer = RestApiStreamWriter(basic_options)
        messages = [Mock(count=10), Mock(count=20)]
        writer.commit(messages, batchId=1)

    def test_abort(self, basic_options):
        """Test abort method."""
        writer = RestApiStreamWriter(basic_options)
        messages = [Mock(count=10)]
        writer.abort(messages, batchId=1)
