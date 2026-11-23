"""Unit tests for the Lakewatch alerts data source."""

import pickle
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest

from cyber_connectors.Lakewatch import (
    ALERT_SCHEMA,
    LakewatchAlertsBatchReader,
    LakewatchAlertsDataSource,
    LakewatchAlertsPartition,
    LakewatchAlertsStreamReader,
    _alert_to_row,
    _build_filter,
    _get_workspace_client,
)

SAMPLE_ALERT = {
    "name": "rules/r1/alerts/a1",
    "alert_id": "a1",
    "seq_id": "sec-428",
    "catalog_id": "c1",
    "alert_type": "SECURITY_ALERT",
    "alert_state": "OPEN",
    "severity": "Medium",
    "title": "Account validation login",
    "origin": {"type": "ORIGIN_RULE", "resource_name": "rules/r1", "display_name": "cat.schema.rule"},
    "assignee": {"id": "123", "email": "analyst@example.com"},
    "create_time": "2026-10-23T07:32:13.762Z",
    "update_time": "2026-10-23T07:32:13.762Z",
    "first_event_summary": "Account validation login",
    "total_event_count": "3",
}


@pytest.fixture
def basic_options():
    return {"catalog_name": "lw_catalog"}


@pytest.fixture
def mock_ws():
    """Patch the WorkspaceClient factory used on the driver."""
    with patch("cyber_connectors.Lakewatch._get_workspace_client") as factory:
        w = Mock()
        w.config.host = "https://example.cloud.databricks.com/"
        w.config.authenticate.return_value = {"Authorization": "Bearer t1"}
        factory.return_value = w
        yield w


def _response(alerts, token=None):
    resp = Mock()
    resp.ok = True
    resp.json.return_value = {"alerts": alerts, **({"next_page_token": token} if token else {})}
    return resp


class TestDataSource:
    def test_name(self):
        assert LakewatchAlertsDataSource.name() == "lakewatch-alerts"

    def test_schema(self, basic_options):
        assert LakewatchAlertsDataSource(basic_options).schema() == ALERT_SCHEMA

    def test_readers(self, basic_options):
        ds = LakewatchAlertsDataSource(basic_options)
        assert isinstance(ds.reader(ALERT_SCHEMA), LakewatchAlertsBatchReader)
        assert isinstance(ds.streamReader(ALERT_SCHEMA), LakewatchAlertsStreamReader)


class TestOptions:
    def test_catalog_name_required(self):
        with pytest.raises(ValueError, match="catalog_name is required"):
            LakewatchAlertsBatchReader({}, ALERT_SCHEMA)

    @pytest.mark.parametrize(
        "value,expected", [("all", None), ("security", "SECURITY_ALERT"), ("SYSTEM", "SYSTEM_ALERT")]
    )
    def test_alert_type(self, basic_options, value, expected):
        reader = LakewatchAlertsBatchReader({**basic_options, "alert_type": value}, ALERT_SCHEMA)
        assert reader.alert_type == expected

    def test_invalid_alert_type(self, basic_options):
        with pytest.raises(ValueError, match="Invalid alert_type"):
            LakewatchAlertsBatchReader({**basic_options, "alert_type": "notables"}, ALERT_SCHEMA)

    def test_invalid_partition_duration(self, basic_options):
        with pytest.raises(ValueError, match="partition_duration"):
            LakewatchAlertsBatchReader({**basic_options, "partition_duration": "0"}, ALERT_SCHEMA)

    def test_invalid_batch_time(self, basic_options):
        with pytest.raises(ValueError, match="Invalid start_time"):
            LakewatchAlertsBatchReader({**basic_options, "start_time": "yesterday"}, ALERT_SCHEMA)

    def test_batch_start_after_end(self, basic_options):
        opts = {**basic_options, "start_time": "2026-02-01T00:00:00Z", "end_time": "2026-01-01T00:00:00Z"}
        with pytest.raises(ValueError, match="earlier than end_time"):
            LakewatchAlertsBatchReader(opts, ALERT_SCHEMA)

    def test_invalid_stream_start_time(self, basic_options):
        with pytest.raises(ValueError, match="Invalid start_time"):
            LakewatchAlertsStreamReader({**basic_options, "start_time": "beginning"}, ALERT_SCHEMA)

    def test_negative_safety_lag(self, basic_options):
        with pytest.raises(ValueError, match="safety_lag_seconds"):
            LakewatchAlertsStreamReader({**basic_options, "safety_lag_seconds": "-1"}, ALERT_SCHEMA)


class TestWorkspaceClient:
    @patch("databricks.sdk.WorkspaceClient")
    def test_token(self, ws):
        _get_workspace_client({"databricks_host": "https://h", "databricks_token": "dapi1"})
        ws.assert_called_once_with(host="https://h", token="dapi1", auth_type="pat")

    @patch("databricks.sdk.WorkspaceClient")
    def test_service_principal(self, ws):
        _get_workspace_client({"databricks_host": "https://h", "client_id": "id", "client_secret": "s"})
        ws.assert_called_once_with(host="https://h", client_id="id", client_secret="s", auth_type="oauth-m2m")

    def test_service_principal_needs_both(self):
        with pytest.raises(ValueError, match="client_id and client_secret"):
            _get_workspace_client({"client_id": "id"})

    @patch("databricks.sdk.WorkspaceClient")
    def test_default(self, ws):
        _get_workspace_client({})
        ws.assert_called_once_with()


class TestHelpers:
    def test_build_filter(self):
        assert _build_filter(None, None, None) is None
        assert _build_filter("SECURITY_ALERT", None, None) == "alert_type = SECURITY_ALERT"
        assert (
            _build_filter("SYSTEM_ALERT", "2026-01-01T00:00:00.000000Z", "2026-01-02T00:00:00.000000Z")
            == 'alert_type = SYSTEM_ALERT AND create_time >= "2026-01-01T00:00:00.000000Z"'
            ' AND create_time < "2026-01-02T00:00:00.000000Z"'
        )

    def test_alert_to_row(self):
        row = dict(zip([f.name for f in ALERT_SCHEMA.fields], _alert_to_row(SAMPLE_ALERT, ALERT_SCHEMA), strict=True))
        assert row["name"] == "rules/r1/alerts/a1"
        assert row["origin"] == ("ORIGIN_RULE", "rules/r1", "cat.schema.rule")
        assert row["assignee"] == ("123", None, "analyst@example.com", None)
        assert row["total_event_count"] == 3
        assert row["create_time"] == datetime(2026, 10, 23, 7, 32, 13, 762000, tzinfo=timezone.utc)
        assert row["classification"] is None
        assert row["first_event_time"] is None
        assert row["annotations"] is None

    def test_alert_to_row_missing_structs(self):
        alert = {k: v for k, v in SAMPLE_ALERT.items() if k not in ("assignee", "origin", "total_event_count")}
        row = dict(zip([f.name for f in ALERT_SCHEMA.fields], _alert_to_row(alert, ALERT_SCHEMA), strict=True))
        assert row["assignee"] is None
        assert row["origin"] is None
        assert row["total_event_count"] is None


class TestBatchReader:
    def test_single_partition_without_start(self, basic_options, mock_ws):
        parts = LakewatchAlertsBatchReader(basic_options, ALERT_SCHEMA).partitions()
        assert len(parts) == 1
        assert (parts[0].start, parts[0].end) == (None, None)
        assert parts[0].host == "https://example.cloud.databricks.com"
        assert parts[0].headers == {"Authorization": "Bearer t1"}

    def test_partitions_split_by_duration(self, basic_options, mock_ws):
        opts = {**basic_options, "start_time": "2026-01-01T00:00:00Z", "end_time": "2026-01-03T12:00:00Z"}
        parts = LakewatchAlertsBatchReader(opts, ALERT_SCHEMA).partitions()
        assert [(p.start, p.end) for p in parts] == [
            ("2026-01-01T00:00:00.000000Z", "2026-01-02T00:00:00.000000Z"),
            ("2026-01-02T00:00:00.000000Z", "2026-01-03T00:00:00.000000Z"),
            ("2026-01-03T00:00:00.000000Z", "2026-01-03T12:00:00.000000Z"),
        ]

    def test_partition_is_picklable(self):
        part = LakewatchAlertsPartition("s", "e", "https://h", {"Authorization": "Bearer t"})
        restored = pickle.loads(pickle.dumps(part))
        assert (restored.start, restored.end, restored.host, restored.headers) == (
            "s",
            "e",
            "https://h",
            {"Authorization": "Bearer t"},
        )

    @patch("cyber_connectors.Lakewatch.get_http_session")
    def test_read_paginates(self, get_session, basic_options):
        session = Mock()
        # An empty page with a next_page_token is not the end.
        session.get.side_effect = [
            _response([SAMPLE_ALERT], token="p2"),
            _response([], token="p3"),
            _response([SAMPLE_ALERT]),
        ]
        get_session.return_value = session
        reader = LakewatchAlertsBatchReader({**basic_options, "alert_type": "security"}, ALERT_SCHEMA)
        part = LakewatchAlertsPartition(
            "2026-01-01T00:00:00.000000Z", "2026-01-02T00:00:00.000000Z", "https://h", {"Authorization": "Bearer t"}
        )

        rows = list(reader.read(part))

        assert len(rows) == 2
        get_session.assert_called_once_with(additional_headers={"Authorization": "Bearer t"})
        assert session.get.call_count == 3
        url = session.get.call_args_list[0].args[0]
        assert url == "https://h/api/2.0/lakewatch/alerts"
        params = session.get.call_args_list[2].kwargs["params"]
        assert params["catalog_name"] == "lw_catalog"
        assert params["order_by"] == "create_time"
        assert params["page_token"] == "p3"
        assert params["filter"] == (
            'alert_type = SECURITY_ALERT AND create_time >= "2026-01-01T00:00:00.000000Z"'
            ' AND create_time < "2026-01-02T00:00:00.000000Z"'
        )

    @patch("cyber_connectors.Lakewatch.get_http_session")
    def test_read_no_filter(self, get_session, basic_options):
        session = Mock()
        session.get.return_value = _response([])
        get_session.return_value = session
        reader = LakewatchAlertsBatchReader(basic_options, ALERT_SCHEMA)
        list(reader.read(LakewatchAlertsPartition(None, None, "https://h", {})))
        assert "filter" not in session.get.call_args.kwargs["params"]

    @patch("cyber_connectors.Lakewatch.get_http_session")
    def test_read_error(self, get_session, basic_options):
        resp = Mock(ok=False, status_code=400, text="InvalidParameterValue")
        get_session.return_value.get.return_value = resp
        reader = LakewatchAlertsBatchReader(basic_options, ALERT_SCHEMA)
        with pytest.raises(RuntimeError, match="400"):
            list(reader.read(LakewatchAlertsPartition(None, None, "https://h", {})))


class TestStreamReader:
    def test_initial_offset_explicit(self, basic_options):
        reader = LakewatchAlertsStreamReader({**basic_options, "start_time": "2026-01-01T00:00:00Z"}, ALERT_SCHEMA)
        assert reader.initialOffset() == {"create_time": "2026-01-01T00:00:00.000000Z"}

    def test_initial_offset_latest(self, basic_options):
        reader = LakewatchAlertsStreamReader(basic_options, ALERT_SCHEMA)
        before = datetime.now(timezone.utc) - timedelta(seconds=60)
        offset = datetime.fromisoformat(reader.initialOffset()["create_time"].replace("Z", "+00:00"))
        assert before - timedelta(seconds=5) <= offset <= datetime.now(timezone.utc) - timedelta(seconds=60)

    def test_initial_offset_earliest(self, basic_options, mock_ws):
        mock_ws.api_client.do.return_value = {"alerts": [{"create_time": "2026-10-07T10:15:00.548Z"}]}
        reader = LakewatchAlertsStreamReader(
            {**basic_options, "start_time": "earliest", "alert_type": "system"}, ALERT_SCHEMA
        )
        assert reader.initialOffset() == {"create_time": "2026-10-07T10:15:00.548000Z"}
        mock_ws.api_client.do.assert_called_once_with(
            "GET",
            "/api/2.0/lakewatch/alerts",
            query={
                "catalog_name": "lw_catalog",
                "page_size": 1,
                "order_by": "create_time",
                "filter": "alert_type = SYSTEM_ALERT",
            },
        )

    def test_initial_offset_earliest_no_alerts(self, basic_options, mock_ws):
        mock_ws.api_client.do.return_value = {}
        reader = LakewatchAlertsStreamReader({**basic_options, "start_time": "earliest"}, ALERT_SCHEMA)
        offset = datetime.fromisoformat(reader.initialOffset()["create_time"].replace("Z", "+00:00"))
        assert offset <= datetime.now(timezone.utc)

    def test_latest_offset_never_moves_backwards(self, basic_options):
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        reader = LakewatchAlertsStreamReader({**basic_options, "start_time": future}, ALERT_SCHEMA)
        start = reader.initialOffset()
        assert reader.latestOffset() == start

    def test_latest_offset_lags_behind_now(self, basic_options):
        reader = LakewatchAlertsStreamReader({**basic_options, "safety_lag_seconds": "120"}, ALERT_SCHEMA)
        offset = datetime.fromisoformat(reader.latestOffset()["create_time"].replace("Z", "+00:00"))
        assert offset <= datetime.now(timezone.utc) - timedelta(seconds=120)

    def test_partitions(self, basic_options, mock_ws):
        reader = LakewatchAlertsStreamReader({**basic_options, "partition_duration": "3600"}, ALERT_SCHEMA)
        parts = reader.partitions(
            {"create_time": "2026-01-01T00:00:00.000000Z"}, {"create_time": "2026-01-01T01:30:00.000000Z"}
        )
        assert [(p.start, p.end) for p in parts] == [
            ("2026-01-01T00:00:00.000000Z", "2026-01-01T01:00:00.000000Z"),
            ("2026-01-01T01:00:00.000000Z", "2026-01-01T01:30:00.000000Z"),
        ]
        assert all(p.headers == {"Authorization": "Bearer t1"} for p in parts)
        assert reader._current_offset == datetime(2026, 1, 1, 1, 30, tzinfo=timezone.utc)

    def test_empty_range_has_no_partitions(self, basic_options, mock_ws):
        reader = LakewatchAlertsStreamReader(basic_options, ALERT_SCHEMA)
        offset = {"create_time": "2026-01-01T00:00:00.000000Z"}
        assert reader.partitions(offset, offset) == []
        mock_ws.config.authenticate.assert_not_called()

    def test_headers_refreshed_every_micro_batch(self, basic_options, mock_ws):
        mock_ws.config.authenticate.side_effect = [{"Authorization": "Bearer t1"}, {"Authorization": "Bearer t2"}]
        reader = LakewatchAlertsStreamReader(basic_options, ALERT_SCHEMA)
        p1 = reader.partitions({"create_time": "2026-01-01T00:00:00Z"}, {"create_time": "2026-01-01T00:01:00Z"})
        p2 = reader.partitions({"create_time": "2026-01-01T00:01:00Z"}, {"create_time": "2026-01-01T00:02:00Z"})
        assert p1[0].headers["Authorization"] == "Bearer t1"
        assert p2[0].headers["Authorization"] == "Bearer t2"


class TestDefaultAuthFailure:
    @patch(
        "databricks.sdk.WorkspaceClient", side_effect=ValueError("default auth: cannot configure default credentials")
    )
    def test_clear_error(self, ws):
        with pytest.raises(ValueError, match="Set databricks_host and either databricks_token"):
            _get_workspace_client({})
