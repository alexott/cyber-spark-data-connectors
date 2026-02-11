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

    def test_init_with_form_format(self):
        """Test initialization with form-data format."""
        options = {"url": "http://localhost:8001", "http_format": "form-data"}
        writer = RestApiBatchWriter(options)
        assert writer.payload_format == "form-data"

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

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_form_format(self, mock_get_session, mock_task_context):
        """Test write functionality with form data format."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {"url": "http://localhost:8001", "http_format": "form-data"}
        writer = RestApiBatchWriter(options)
        rows = [Row(id=1, name="test", value=123)]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        call_args = mock_session.post.call_args
        data = call_args[1]["data"]
        assert isinstance(data, dict)
        assert data["id"] == "1"
        assert data["name"] == "test"
        assert data["value"] == "123"

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_form_format_with_none_values(self, mock_get_session, mock_task_context):
        """Test write functionality with form data format and None values."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {"url": "http://localhost:8001", "http_format": "form-data"}
        writer = RestApiBatchWriter(options)
        rows = [Row(id=1, name=None, value=123)]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        call_args = mock_session.post.call_args
        data = call_args[1]["data"]
        assert isinstance(data, dict)
        assert data["id"] == "1"
        assert data["name"] == ""
        assert data["value"] == "123"

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_form_format_with_put(self, mock_get_session, mock_task_context):
        """Test write functionality with form data format and PUT method."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.put.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {"url": "http://localhost:8001", "http_format": "form-data", "http_method": "put"}
        writer = RestApiBatchWriter(options)
        rows = [Row(id=1, name="test")]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        assert mock_session.put.called
        assert not mock_session.post.called
        call_args = mock_session.put.call_args
        data = call_args[1]["data"]
        assert isinstance(data, dict)


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


class TestRestApiCustomHeaders:
    """Test custom HTTP headers functionality."""

    def test_init_with_custom_headers(self):
        """Test initialization with custom headers."""
        options = {
            "url": "http://localhost:8001",
            "http_header_Authorization": "Bearer token123",
            "http_header_X-API-Key": "secret",
        }
        writer = RestApiBatchWriter(options)
        assert writer.custom_headers["Authorization"] == "Bearer token123"
        assert writer.custom_headers["X-API-Key"] == "secret"

    def test_init_without_custom_headers(self, basic_options):
        """Test initialization without custom headers."""
        writer = RestApiBatchWriter(basic_options)
        assert writer.custom_headers == {}

    def test_custom_headers_with_underscores(self):
        """Test custom headers with underscores in name."""
        options = {
            "url": "http://localhost:8001",
            "http_header_X_Custom_Header": "value",
        }
        writer = RestApiBatchWriter(options)
        assert writer.custom_headers["X_Custom_Header"] == "value"

    def test_custom_headers_not_affected_by_other_options(self):
        """Test that other options don't interfere with custom headers."""
        options = {
            "url": "http://localhost:8001",
            "http_method": "post",
            "http_format": "json",
            "http_header_Authorization": "Bearer token",
            "some_other_option": "value",
        }
        writer = RestApiBatchWriter(options)
        assert len(writer.custom_headers) == 1
        assert writer.custom_headers["Authorization"] == "Bearer token"

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_with_custom_headers(self, mock_get_session, mock_task_context):
        """Test write functionality with custom headers."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {
            "url": "http://localhost:8001",
            "http_header_Authorization": "Bearer token123",
            "http_header_X-API-Key": "secret",
        }
        writer = RestApiBatchWriter(options)
        rows = [Row(id=1, name="test")]
        commit_msg = writer.write(iter(rows))

        assert commit_msg.count == 1
        # Check that get_http_session was called with custom headers
        call_args = mock_get_session.call_args
        headers = call_args[1]["additional_headers"]
        assert headers["Authorization"] == "Bearer token123"
        assert headers["X-API-Key"] == "secret"
        assert headers["Content-Type"] == "application/json"

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_custom_content_type_overrides_default(self, mock_get_session, mock_task_context):
        """Test that custom Content-Type header overrides default."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {
            "url": "http://localhost:8001",
            "http_format": "json",
            "http_header_Content-Type": "application/vnd.api+json",
        }
        writer = RestApiBatchWriter(options)
        rows = [Row(id=1)]
        writer.write(iter(rows))

        # Custom Content-Type should be used, not the default
        call_args = mock_get_session.call_args
        headers = call_args[1]["additional_headers"]
        assert headers["Content-Type"] == "application/vnd.api+json"

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_form_data_without_custom_content_type(self, mock_get_session, mock_task_context):
        """Test that form-data format doesn't set Content-Type by default."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {"url": "http://localhost:8001", "http_format": "form-data"}
        writer = RestApiBatchWriter(options)
        rows = [Row(id=1)]
        writer.write(iter(rows))

        call_args = mock_get_session.call_args
        headers = call_args[1]["additional_headers"]
        assert "Content-Type" not in headers

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_multiple_custom_headers(self, mock_get_session, mock_task_context):
        """Test write with multiple custom headers."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.post.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {
            "url": "http://localhost:8001",
            "http_header_Authorization": "Bearer token",
            "http_header_X-API-Key": "key123",
            "http_header_X-Request-ID": "req-456",
            "http_header_User-Agent": "Spark/4.0",
        }
        writer = RestApiBatchWriter(options)
        rows = [Row(id=1)]
        writer.write(iter(rows))

        call_args = mock_get_session.call_args
        headers = call_args[1]["additional_headers"]
        assert headers["Authorization"] == "Bearer token"
        assert headers["X-API-Key"] == "key123"
        assert headers["X-Request-ID"] == "req-456"
        assert headers["User-Agent"] == "Spark/4.0"

    @patch("pyspark.TaskContext")
    @patch("cyber_connectors.RestApi.get_http_session")
    def test_write_custom_headers_with_put(self, mock_get_session, mock_task_context):
        """Test custom headers work with PUT method."""
        mock_context = Mock()
        mock_context.partitionId.return_value = 0
        mock_task_context.get.return_value = mock_context

        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "ok"}'
        mock_session.put.return_value = mock_response
        mock_get_session.return_value = mock_session

        options = {
            "url": "http://localhost:8001",
            "http_method": "put",
            "http_header_Authorization": "Bearer token",
        }
        writer = RestApiBatchWriter(options)
        rows = [Row(id=1)]
        writer.write(iter(rows))

        assert mock_session.put.called
        call_args = mock_get_session.call_args
        headers = call_args[1]["additional_headers"]
        assert headers["Authorization"] == "Bearer token"
