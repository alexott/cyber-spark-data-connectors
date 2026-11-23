"""Unit tests for Google SecOps data sources."""

import hashlib
import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import Row, StringType, StructField, StructType

from cyber_connectors.common import DateTimeJsonEncoder
from cyber_connectors.GoogleSecOps import (
    GoogleSecOpsSoarBatchWriter,
    GoogleSecOpsSoarDataSource,
    GoogleSecOpsSoarStreamWriter,
)

SOAR_URL = "https://example.siemplify-soar.com/webhooks/abc?key=secret"


@pytest.fixture
def basic_options():
    """Basic required options for the SOAR data source."""
    return {"url": SOAR_URL}


@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return StructType([StructField("TicketId", StringType(), True)])


def _alert(**overrides):
    """Build a row with all mandatory SOAR alert fields."""
    fields = {
        "TicketId": "t-1",
        "SourceSystemName": "Databricks",
        "Name": "Suspicious login",
        "DeviceVendor": "Databricks",
        "RuleGenerator": "rule-1",
        "StartTime": datetime(2026, 1, 2, 3, 4, 5),
    }
    fields.update(overrides)
    return Row(**fields)


@pytest.fixture
def mock_task_context():
    """Patch TaskContext so write() can get a partition ID."""
    with patch("pyspark.TaskContext") as task_context:
        context = Mock()
        context.partitionId.return_value = 3
        task_context.get.return_value = context
        yield task_context


@pytest.fixture
def mock_session():
    """Patch the HTTP session and return a mock that answers 200 OK."""
    with patch("cyber_connectors.GoogleSecOps.get_http_session") as get_session:
        session = Mock()
        response = Mock()
        response.status_code = 200
        response.text = ""
        session.post.return_value = response
        get_session.return_value = session
        yield session


class TestGoogleSecOpsSoarDataSource:
    """Test GoogleSecOpsSoarDataSource class."""

    def test_name(self):
        """Test that data source name is 'google-secops-soar'."""
        assert GoogleSecOpsSoarDataSource.name() == "google-secops-soar"

    def test_writer(self, basic_options, sample_schema):
        """Test that writer returns the batch writer."""
        ds = GoogleSecOpsSoarDataSource(options=basic_options)
        assert isinstance(ds.writer(sample_schema, overwrite=True), GoogleSecOpsSoarBatchWriter)

    def test_stream_writer(self, basic_options, sample_schema):
        """Test that streamWriter returns the stream writer."""
        ds = GoogleSecOpsSoarDataSource(options=basic_options)
        assert isinstance(ds.streamWriter(sample_schema, overwrite=True), GoogleSecOpsSoarStreamWriter)


class TestGoogleSecOpsSoarWriter:
    """Test the SOAR webhook writer."""

    def test_missing_url(self):
        """Test that the url option is required."""
        with pytest.raises(ValueError, match="url"):
            GoogleSecOpsSoarBatchWriter({})

    def test_write_posts_one_alert_per_row(self, basic_options, mock_task_context, mock_session):
        """Test that each row is sent as a separate JSON alert."""
        writer = GoogleSecOpsSoarBatchWriter(basic_options)
        commit_msg = writer.write(iter([_alert(TicketId="t-1"), _alert(TicketId="t-2")]))

        assert commit_msg.partition_id == 3
        assert commit_msg.count == 2
        assert mock_session.post.call_count == 2
        first_call = mock_session.post.call_args_list[0]
        assert first_call[0][0] == SOAR_URL
        payload = json.loads(first_call[1]["data"])
        assert payload["TicketId"] == "t-1"
        assert payload["StartTime"] == "2026-01-02T03:04:05+00:00"

    def test_write_sends_nested_events_list(self, basic_options, mock_task_context, mock_session):
        """Test that EventsList with nested structs is sent as a JSON array of objects."""
        writer = GoogleSecOpsSoarBatchWriter(basic_options)
        writer.write(iter([_alert(EventsList=[Row(src_ip="10.0.0.1", user="bob")])]))

        payload = json.loads(mock_session.post.call_args[1]["data"])
        assert payload["EventsList"] == [{"src_ip": "10.0.0.1", "user": "bob"}]

    def test_write_missing_mandatory_field(self, basic_options, mock_task_context, mock_session):
        """Test that a row without mandatory fields fails before anything is sent."""
        writer = GoogleSecOpsSoarBatchWriter(basic_options)
        row = Row(TicketId="t-1", Name="Suspicious login", StartTime=None)
        with pytest.raises(ValueError, match="DeviceVendor, RuleGenerator, SourceSystemName, StartTime"):
            writer.write(iter([row]))
        assert not mock_session.post.called

    def test_write_fills_defaults_from_options(self, mock_task_context, mock_session):
        """Test that SourceSystemName, DeviceVendor and Environment are taken from options when missing."""
        options = {
            "url": SOAR_URL,
            "source_system_name": "Databricks",
            "device_vendor": "Databricks Lakehouse",
            "environment": "Default Environment",
        }
        writer = GoogleSecOpsSoarBatchWriter(options)
        row = Row(TicketId="t-1", Name="n", RuleGenerator="r", StartTime=1767322800000, DeviceVendor=None)
        writer.write(iter([row]))

        payload = json.loads(mock_session.post.call_args[1]["data"])
        assert payload["SourceSystemName"] == "Databricks"
        assert payload["DeviceVendor"] == "Databricks Lakehouse"
        assert payload["Environment"] == "Default Environment"
        assert payload["StartTime"] == 1767322800000

    def test_row_values_take_precedence_over_defaults(self, mock_task_context, mock_session):
        """Test that values present in the row are not overwritten by option defaults."""
        options = {"url": SOAR_URL, "device_vendor": "Default vendor"}
        writer = GoogleSecOpsSoarBatchWriter(options)
        writer.write(iter([_alert(DeviceVendor="Row vendor")]))

        payload = json.loads(mock_session.post.call_args[1]["data"])
        assert payload["DeviceVendor"] == "Row vendor"

    def test_write_error_response(self, basic_options, mock_task_context, mock_session):
        """Test that a non-2xx response fails the write with the response text."""
        mock_session.post.return_value.status_code = 400
        mock_session.post.return_value.text = "bad payload"
        writer = GoogleSecOpsSoarBatchWriter(basic_options)
        with pytest.raises(RuntimeError, match="400: bad payload"):
            writer.write(iter([_alert()]))

    def _sent_display_ids(self, mock_session):
        return [json.loads(c[1]["data"])["DisplayId"] for c in mock_session.post.call_args_list]

    def test_display_id_generated_from_all_columns(self, basic_options, mock_task_context, mock_session):
        """Test that by default DisplayId is a SHA-256 of all alert fields, independent of column order."""
        writer = GoogleSecOpsSoarBatchWriter(basic_options)
        alert = _alert()
        reordered = Row(**dict(reversed(list(alert.asDict().items()))))
        writer.write(iter([alert, reordered, _alert(Name="Other alert")]))

        ids = self._sent_display_ids(mock_session)
        expected = hashlib.sha256(
            json.dumps(_alert().asDict(), sort_keys=True, cls=DateTimeJsonEncoder).encode("utf-8")
        ).hexdigest()
        assert ids[0] == expected
        assert ids[1] == ids[0]
        assert ids[2] != ids[0]

    def test_display_id_ignores_null_display_id_column(self, basic_options, mock_task_context, mock_session):
        """Test that a null DisplayId column is replaced and doesn't change the hash."""
        writer = GoogleSecOpsSoarBatchWriter(basic_options)
        writer.write(iter([_alert(), _alert(DisplayId=None)]))

        ids = self._sent_display_ids(mock_session)
        assert ids[0] == ids[1]

    def test_display_id_from_row_is_kept(self, basic_options, mock_task_context, mock_session):
        """Test that a DisplayId present in the row isn't replaced."""
        writer = GoogleSecOpsSoarBatchWriter(basic_options)
        writer.write(iter([_alert(DisplayId="my-id")]))

        assert self._sent_display_ids(mock_session) == ["my-id"]

    def test_display_id_from_selected_columns(self, mock_task_context, mock_session):
        """Test that only the columns in display_id_columns are hashed."""
        options = {"url": SOAR_URL, "display_id_columns": "TicketId, RuleGenerator"}
        writer = GoogleSecOpsSoarBatchWriter(options)
        writer.write(iter([_alert(Name="First"), _alert(Name="Second"), _alert(TicketId="t-2")]))

        ids = self._sent_display_ids(mock_session)
        assert ids[0] == ids[1]
        assert ids[2] != ids[0]

    def test_display_id_includes_defaults(self, mock_task_context, mock_session):
        """Test that defaults from options are filled before hashing."""
        options = {"url": SOAR_URL, "display_id_columns": "DeviceVendor", "device_vendor": "Databricks"}
        writer = GoogleSecOpsSoarBatchWriter(options)
        writer.write(iter([_alert(DeviceVendor=None), _alert(DeviceVendor="Databricks")]))

        ids = self._sent_display_ids(mock_session)
        assert ids[0] == ids[1]

    def test_display_id_unknown_column(self, mock_task_context, mock_session):
        """Test that a column in display_id_columns that's missing in the row fails the write."""
        options = {"url": SOAR_URL, "display_id_columns": "TicketId,NoSuchColumn"}
        writer = GoogleSecOpsSoarBatchWriter(options)
        with pytest.raises(ValueError, match="NoSuchColumn"):
            writer.write(iter([_alert()]))
        assert not mock_session.post.called

    def test_display_id_columns_empty(self):
        """Test that an empty display_id_columns option is rejected."""
        with pytest.raises(ValueError, match="display_id_columns"):
            GoogleSecOpsSoarBatchWriter({"url": SOAR_URL, "display_id_columns": " , "})

    def test_stream_writer_commit_and_abort(self, basic_options):
        """Test that commit and abort are no-ops."""
        writer = GoogleSecOpsSoarStreamWriter(basic_options)
        writer.commit([], 0)
        writer.abort([], 0)
