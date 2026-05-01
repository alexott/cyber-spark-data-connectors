"""Unit tests for common utilities."""

import sys
from datetime import date, datetime
from unittest.mock import Mock, patch

from cyber_connectors.common import (
    DateTimeJsonEncoder,
    SimpleCommitMessage,
    get_http_session,
    get_service_credentials_provider,
)


class TestSimpleCommitMessage:
    """Test SimpleCommitMessage dataclass."""

    def test_creation(self):
        """Test creating a SimpleCommitMessage."""
        msg = SimpleCommitMessage(partition_id=0, count=100)
        assert msg.partition_id == 0
        assert msg.count == 100

    def test_attributes(self):
        """Test that SimpleCommitMessage has correct attributes."""
        msg = SimpleCommitMessage(partition_id=5, count=42)
        assert hasattr(msg, "partition_id")
        assert hasattr(msg, "count")


class TestDateTimeJsonEncoder:
    """Test DateTimeJsonEncoder class."""

    def test_encode_datetime(self):
        """Test encoding datetime objects."""
        import json

        dt = datetime(2024, 1, 1, 12, 30, 45)
        result = json.dumps({"timestamp": dt}, cls=DateTimeJsonEncoder)
        assert "2024-01-01T12:30:45" in result

    def test_encode_date(self):
        """Test encoding date objects."""
        import json

        d = date(2024, 1, 1)
        result = json.dumps({"date": d}, cls=DateTimeJsonEncoder)
        assert "2024-01-01" in result

    def test_encode_datetime_with_microseconds(self):
        """Test encoding datetime with microseconds."""
        import json

        dt = datetime(2024, 1, 1, 12, 30, 45, 123456)
        result = json.dumps({"timestamp": dt}, cls=DateTimeJsonEncoder)
        assert "2024-01-01T12:30:45.123456" in result

    def test_encode_regular_types(self):
        """Test that regular types still work."""
        import json

        data = {
            "string": "test",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "null": None,
            "list": [1, 2, 3],
            "dict": {"key": "value"},
        }
        result = json.dumps(data, cls=DateTimeJsonEncoder)
        parsed = json.loads(result)
        assert parsed["string"] == "test"
        assert parsed["number"] == 42
        assert parsed["float"] == 3.14
        assert parsed["boolean"] is True
        assert parsed["null"] is None
        assert parsed["list"] == [1, 2, 3]
        assert parsed["dict"] == {"key": "value"}

    def test_encode_mixed_types(self):
        """Test encoding mixed types including datetime."""
        import json

        dt = datetime(2024, 1, 1, 12, 0, 0)
        d = date(2024, 6, 15)
        data = {"timestamp": dt, "date": d, "string": "test", "number": 42}
        result = json.dumps(data, cls=DateTimeJsonEncoder)
        assert "2024-01-01T12:00:00" in result
        assert "2024-06-15" in result
        assert "test" in result
        assert "42" in result


class TestGetHttpSession:
    """Test get_http_session function."""

    def test_default_session(self):
        """Test creating a session with default parameters."""
        session = get_http_session()

        assert session is not None
        assert hasattr(session, "headers")

    def test_session_with_headers(self):
        """Test creating a session with additional headers."""
        headers = {"Authorization": "Bearer token123"}
        session = get_http_session(additional_headers=headers)

        assert session is not None
        assert session.headers.get("Authorization") == "Bearer token123"

    def test_session_with_retry(self):
        """Test creating a session with retry configuration."""
        session = get_http_session(retry=3)

        assert session is not None

    def test_session_without_retry(self):
        """Test creating a session without retry."""
        session = get_http_session(retry=0)

        assert session is not None

    def test_session_retry_on_post(self):
        """Test creating a session with retry on POST enabled."""
        session = get_http_session(retry=5, retry_on_post=True)

        assert session is not None

    def test_session_no_retry_on_post(self):
        """Test creating a session with retry on POST disabled."""
        session = get_http_session(retry=5, retry_on_post=False)

        assert session is not None

    def test_retry_status_codes(self):
        """Test that session is created successfully with retries."""
        session = get_http_session(retry=3)

        assert session is not None

    def test_session_with_multiple_headers(self):
        """Test creating a session with multiple headers."""
        headers = {
            "Authorization": "Bearer token123",
            "Content-Type": "application/json",
            "User-Agent": "TestClient/1.0",
        }
        session = get_http_session(additional_headers=headers)

        assert session is not None
        for key, value in headers.items():
            assert session.headers.get(key) == value


class TestGetServiceCredentialsProvider:
    """Test get_service_credentials_provider helper.

    The helper picks the right Unity Catalog credential API based on
    whether the call site is on a Spark executor (TaskContext present)
    or on the driver (TaskContext absent).
    """

    def test_executor_uses_databricks_service_credentials(self):
        """On an executor, resolve via databricks.service_credentials."""
        mock_credential = Mock()
        mock_service_credentials = Mock()
        mock_service_credentials.getServiceCredentialsProvider.return_value = mock_credential

        mock_databricks = Mock()
        mock_databricks.service_credentials = mock_service_credentials

        modules = {
            "databricks": mock_databricks,
            "databricks.service_credentials": mock_service_credentials,
        }
        with patch.dict(sys.modules, modules), patch("pyspark.TaskContext.get", return_value=Mock()):
            result = get_service_credentials_provider("my-credential")

        mock_service_credentials.getServiceCredentialsProvider.assert_called_once_with("my-credential")
        assert result is mock_credential

    def test_driver_uses_dbutils(self):
        """On the driver, fall back to dbutils.credentials."""
        mock_credential = Mock()
        mock_dbutils = Mock()
        mock_dbutils.credentials.getServiceCredentialsProvider.return_value = mock_credential

        with patch("pyspark.TaskContext.get", return_value=None), patch(
            "cyber_connectors.common._driver_dbutils", return_value=mock_dbutils
        ):
            result = get_service_credentials_provider("my-credential")

        mock_dbutils.credentials.getServiceCredentialsProvider.assert_called_once_with("my-credential")
        assert result is mock_credential
