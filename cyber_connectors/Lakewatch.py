"""Read alerts from Databricks Lakewatch (v2 REST API, ``/api/2.0/lakewatch/alerts``).

Authentication is resolved on the driver with the Databricks SDK: explicit options (token, or service principal),
or the SDK's default resolution (environment variables, ``~/.databrickscfg``).  The resolved host and auth headers
are passed to the executors inside each partition, so executors only need ``requests``.  Headers are resolved again
for every batch and micro-batch, so short-lived tokens are refreshed.

Inside Databricks, the data source code runs in Python worker processes that don't have the notebook's credentials,
so explicit options are required there.
"""

from datetime import datetime, timedelta, timezone
from typing import cast

from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceStreamReader, InputPartition
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType, TimestampType

from cyber_connectors.common import get_http_session

ALERTS_PATH = "/api/2.0/lakewatch/alerts"
PAGE_SIZE = 1000
ALERT_TYPES = {"all": None, "security": "SECURITY_ALERT", "system": "SYSTEM_ALERT"}

_PRINCIPAL = StructType(
    [
        StructField("id", StringType(), True),
        StructField("principal_type", StringType(), True),
        StructField("email", StringType(), True),
        StructField("display_name", StringType(), True),
    ]
)
_ORIGIN = StructType(
    [
        StructField("type", StringType(), True),
        StructField("resource_name", StringType(), True),
        StructField("display_name", StringType(), True),
    ]
)
ALERT_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("alert_id", StringType(), True),
        StructField("seq_id", StringType(), True),
        StructField("catalog_id", StringType(), True),
        StructField("alert_type", StringType(), True),
        StructField("alert_state", StringType(), True),
        StructField("classification", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("origin", _ORIGIN, True),
        StructField("assignee", _PRINCIPAL, True),
        StructField("first_event_summary", StringType(), True),
        StructField("total_event_count", LongType(), True),
        StructField("first_event_time", TimestampType(), True),
        StructField("last_event_time", TimestampType(), True),
        StructField("create_time", TimestampType(), True),
        StructField("update_time", TimestampType(), True),
        StructField("annotations", MapType(StringType(), StringType()), True),
    ]
)


def _parse_time(value: str) -> datetime:
    """Parse an ISO 8601 / RFC 3339 timestamp; a value without a time zone is taken as UTC."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _format_time(dt: datetime) -> str:
    """Format a datetime as an RFC 3339 UTC string, as expected by the Lakewatch filter."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_option_time(options: dict, key: str) -> datetime | None:
    value = options.get(key)
    if not value:
        return None
    try:
        return _parse_time(value)
    except ValueError as e:
        raise ValueError(f"Invalid {key}: {value!r}. Expected an ISO 8601 timestamp like 2026-01-01T00:00:00Z") from e


def _get_workspace_client(options: dict):
    """Create a Databricks SDK WorkspaceClient from options (driver side only)."""
    from databricks.sdk import WorkspaceClient

    host = options.get("databricks_host")
    token = options.get("databricks_token")
    client_id = options.get("client_id")
    client_secret = options.get("client_secret")
    if token:
        return WorkspaceClient(host=host, token=token, auth_type="pat")
    if client_id or client_secret:
        if not (client_id and client_secret):
            raise ValueError("Both client_id and client_secret are required for service principal auth")
        return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret, auth_type="oauth-m2m")
    try:
        return WorkspaceClient(host=host) if host else WorkspaceClient()
    except ValueError as e:
        raise ValueError(
            "Can't authenticate to Databricks. Set databricks_host and either databricks_token, or client_id and "
            "client_secret (service principal). Inside Databricks the notebook's own credentials aren't available "
            "to the data source."
        ) from e


def _build_filter(alert_type: str | None, start: str | None, end: str | None) -> str | None:
    """Build the AIP-160 filter. The time range is half-open: create_time >= start AND create_time < end."""
    predicates = []
    if alert_type:
        predicates.append(f"alert_type = {alert_type}")
    if start:
        predicates.append(f'create_time >= "{start}"')
    if end:
        predicates.append(f'create_time < "{end}"')
    return " AND ".join(predicates) or None


def _split_time_range(start: datetime, end: datetime, duration_seconds: int) -> list[tuple[datetime, datetime]]:
    """Split [start, end) into contiguous half-open ranges of at most ``duration_seconds``."""
    ranges = []
    step = timedelta(seconds=duration_seconds)
    current = start
    while current < end:
        next_end = min(current + step, end)
        ranges.append((current, next_end))
        current = next_end
    return ranges


def _struct(value: dict | None, struct_type: StructType) -> tuple | None:
    if not value:
        return None
    return tuple(value.get(field.name) for field in struct_type.fields)


def _alert_to_row(alert: dict, schema: StructType) -> tuple:
    """Convert one alert JSON object into a tuple that follows the given schema."""
    row = dict(alert)
    row["origin"] = _struct(alert.get("origin"), _ORIGIN)
    row["assignee"] = _struct(alert.get("assignee"), _PRINCIPAL)
    count = alert.get("total_event_count")
    row["total_event_count"] = int(count) if count is not None else None  # int64 may come as a JSON string
    for key in ("first_event_time", "last_event_time", "create_time", "update_time"):
        value = alert.get(key)
        row[key] = _parse_time(value) if value else None
    return tuple(row.get(field.name) for field in schema.fields)


class LakewatchAlertsPartition(InputPartition):
    """A half-open create_time range [start, end) plus the resolved host and auth headers."""

    def __init__(self, start: str | None, end: str | None, host: str, headers: dict):
        self.start = start
        self.end = end
        self.host = host
        self.headers = headers


class LakewatchAlertsDataSource(DataSource):
    """Data source to read Lakewatch alerts in batch and streaming modes.

    Common options:
    - catalog_name (required): Unity Catalog catalog of the Lakewatch environment
    - alert_type: ``all`` (default), ``security``, or ``system``
    - partition_duration: seconds of create_time per partition (default 86400)
    - databricks_host, databricks_token: explicit PAT auth
    - databricks_host, client_id, client_secret: explicit service principal (OAuth M2M) auth
    - without explicit auth, the Databricks SDK default resolution is used on the driver
      (env vars, ~/.databrickscfg); it doesn't work inside Databricks notebooks

    Batch options: start_time, end_time (ISO 8601, optional).
    Streaming options: start_time (``latest`` - default, ``earliest``, or ISO 8601),
    safety_lag_seconds (default 60).
    """

    @classmethod
    def name(cls):
        return "lakewatch-alerts"

    def schema(self):
        return ALERT_SCHEMA

    def reader(self, schema: StructType):
        return LakewatchAlertsBatchReader(self.options, schema)

    def streamReader(self, schema: StructType):
        return LakewatchAlertsStreamReader(self.options, schema)


class LakewatchAlertsReader:
    """Shared read logic: options, partition planning helpers, and the paginated alerts listing."""

    def __init__(self, options, schema: StructType):
        self.options = dict(options)
        self.schema = schema
        self.catalog_name = options.get("catalog_name")
        if not self.catalog_name:
            raise ValueError("catalog_name is required")

        alert_type = options.get("alert_type", "all").lower()
        if alert_type not in ALERT_TYPES:
            raise ValueError(f"Invalid alert_type: {alert_type!r}. Expected one of: all, security, system")
        self.alert_type = ALERT_TYPES[alert_type]

        self.partition_duration = int(options.get("partition_duration", "86400"))
        if self.partition_duration <= 0:
            raise ValueError("partition_duration must be a positive number of seconds")

    def _make_partitions(self, ranges: list[tuple[str | None, str | None]]) -> list[LakewatchAlertsPartition]:
        """Resolve auth on the driver and attach host and headers to every partition."""
        w = _get_workspace_client(self.options)
        host = w.config.host.rstrip("/")
        headers = w.config.authenticate()
        return [LakewatchAlertsPartition(start, end, host, headers) for start, end in ranges]

    def _time_ranges(self, start: datetime, end: datetime) -> list[tuple[str | None, str | None]]:
        return [(_format_time(s), _format_time(e)) for s, e in _split_time_range(start, end, self.partition_duration)]

    def read(self, partition):
        partition = cast(LakewatchAlertsPartition, partition)
        session = get_http_session(additional_headers=partition.headers)
        params = {
            "catalog_name": self.catalog_name,
            "page_size": PAGE_SIZE,
            "order_by": "create_time",
        }
        alert_filter = _build_filter(self.alert_type, partition.start, partition.end)
        if alert_filter:
            params["filter"] = alert_filter

        url = partition.host + ALERTS_PATH
        # Keep paging until next_page_token is empty: a short or empty page does not mean the end.
        while True:
            response = session.get(url, params=params, timeout=60)
            if not response.ok:
                raise RuntimeError(f"Lakewatch alerts request failed ({response.status_code}): {response.text}")
            data = response.json()
            for alert in data.get("alerts", []):
                yield _alert_to_row(alert, self.schema)
            token = data.get("next_page_token")
            if not token:
                break
            params["page_token"] = token


class LakewatchAlertsBatchReader(LakewatchAlertsReader, DataSourceReader):
    """Batch reader. Without start_time, all alerts are read in one partition."""

    def __init__(self, options, schema: StructType):
        super().__init__(options, schema)
        self.start_time = _parse_option_time(options, "start_time")
        self.end_time = _parse_option_time(options, "end_time")
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            raise ValueError("start_time must be earlier than end_time")

    def partitions(self):
        if self.start_time is None:
            end = _format_time(self.end_time) if self.end_time else None
            return self._make_partitions([(None, end)])
        end_time = self.end_time or datetime.now(timezone.utc)
        return self._make_partitions(self._time_ranges(self.start_time, end_time))


class LakewatchAlertsStreamReader(LakewatchAlertsReader, DataSourceStreamReader):
    """Streaming reader for new alerts, with offsets on create_time (``{"create_time": "<RFC 3339>"}``)."""

    def __init__(self, options, schema: StructType):
        super().__init__(options, schema)
        self.start_time = options.get("start_time", "latest")
        if self.start_time not in ("latest", "earliest"):
            _parse_option_time(options, "start_time")  # validate only
        # Stay behind "now" so alerts that are being created are not skipped.
        self.safety_lag_seconds = int(options.get("safety_lag_seconds", "60"))
        if self.safety_lag_seconds < 0:
            raise ValueError("safety_lag_seconds must be zero or positive")
        # Last planned offset, so that latestOffset() never moves backwards.
        self._current_offset: datetime | None = None

    def _now(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(seconds=self.safety_lag_seconds)

    def _earliest_create_time(self) -> datetime | None:
        """Find the create_time of the oldest alert that matches the alert_type filter."""
        w = _get_workspace_client(self.options)
        query = {"catalog_name": self.catalog_name, "page_size": 1, "order_by": "create_time"}
        alert_filter = _build_filter(self.alert_type, None, None)
        if alert_filter:
            query["filter"] = alert_filter
        alerts = (w.api_client.do("GET", ALERTS_PATH, query=query) or {}).get("alerts", [])
        if not alerts or not alerts[0].get("create_time"):
            return None
        return _parse_time(alerts[0]["create_time"])

    def initialOffset(self):
        if self.start_time == "latest":
            start = self._now()
        elif self.start_time == "earliest":
            start = self._earliest_create_time() or self._now()
        else:
            start = _parse_time(self.start_time)
        self._current_offset = start
        return {"create_time": _format_time(start)}

    def latestOffset(self):
        end = self._now()
        if self._current_offset is not None and end < self._current_offset:
            end = self._current_offset
        return {"create_time": _format_time(end)}

    def partitions(self, start, end):
        start_time = _parse_time(start["create_time"])
        end_time = _parse_time(end["create_time"])
        self._current_offset = end_time
        ranges = self._time_ranges(start_time, end_time)
        if not ranges:
            return []
        return self._make_partitions(ranges)

    def commit(self, end):
        pass
