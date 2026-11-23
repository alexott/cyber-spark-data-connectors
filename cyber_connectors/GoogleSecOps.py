"""Google Security Operations (SecOps) data sources."""

from collections.abc import Iterator
from typing import Any

from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import Row, StructType

from cyber_connectors.common import DateTimeJsonEncoder, SimpleCommitMessage, get_http_session

# Fields that every alert sent to a SOAR incoming webhook must have
SOAR_MANDATORY_FIELDS = ["TicketId", "SourceSystemName", "Name", "DeviceVendor", "RuleGenerator", "StartTime"]


class GoogleSecOpsSoarDataSource(DataSource):
    """Data source to push alerts into Google SecOps SOAR via an incoming webhook.

    Each row is sent as one alert (JSON object).  Row columns are used as alert fields, so the DataFrame should
    have columns TicketId, SourceSystemName, Name, DeviceVendor, RuleGenerator and StartTime, and optionally
    EndTime, Priority, Description, Environment, DisplayId and EventsList (array of structs with raw events).

    Write options:
    - url: SOAR incoming webhook URL (it includes the secret, so keep it in a secret scope)
    - source_system_name: (optional) value for SourceSystemName if it is missing in the row
    - device_vendor: (optional) value for DeviceVendor if it is missing in the row
    - environment: (optional) value for Environment if it is missing in the row
    - display_id_columns: (optional) comma-separated list of columns hashed to generate DisplayId when it is
                          missing in the row.  Default is `*` (all columns).

    """

    @classmethod
    def name(cls):
        return "google-secops-soar"

    def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
        return GoogleSecOpsSoarStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return GoogleSecOpsSoarBatchWriter(self.options)


class GoogleSecOpsSoarWriter:
    def __init__(self, options: dict[str, Any]):
        url = options.get("url")
        if not url:
            raise ValueError("url option is required")
        self.url: str = url

        # Values used for fields that are missing (or null) in the row
        self.defaults = {
            "SourceSystemName": options.get("source_system_name"),
            "DeviceVendor": options.get("device_vendor"),
            "Environment": options.get("environment"),
        }

        # Columns hashed to generate DisplayId when it's missing in the row: "*" (all columns) or a list of names
        display_id_columns = options.get("display_id_columns", "*").strip()
        if display_id_columns == "*":
            self.display_id_columns: list[str] | None = None
        else:
            self.display_id_columns = [column.strip() for column in display_id_columns.split(",") if column.strip()]
            if not self.display_id_columns:
                raise ValueError("display_id_columns must contain at least one column name or '*'")

    def _generate_display_id(self, alert: dict[str, Any]) -> str:
        """Generate DisplayId as a SHA-256 of the selected alert fields, so the same alert always gets the same ID."""
        import hashlib
        import json

        if self.display_id_columns is None:
            fields = {key: value for key, value in alert.items() if key != "DisplayId"}
        else:
            missing_columns = [column for column in self.display_id_columns if column not in alert]
            if missing_columns:
                raise ValueError(f"Columns in display_id_columns were not found in row: {', '.join(missing_columns)}")
            fields = {column: alert[column] for column in self.display_id_columns}

        data = json.dumps(fields, sort_keys=True, cls=DateTimeJsonEncoder)
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def _to_alert(self, row: Row) -> dict[str, Any]:
        """Convert a row into a SOAR alert, filling defaults, checking mandatory fields, and setting DisplayId."""
        alert = row.asDict(recursive=True)
        for field, value in self.defaults.items():
            if value is not None and alert.get(field) is None:
                alert[field] = value

        missing_fields = sorted(field for field in SOAR_MANDATORY_FIELDS if alert.get(field) is None)
        if missing_fields:
            raise ValueError(f"Alert is missing mandatory fields: {', '.join(missing_fields)}")

        if alert.get("DisplayId") is None:
            alert["DisplayId"] = self._generate_display_id(alert)
        return alert

    def write(self, iterator: Iterator[Row]):
        """Send rows to the SOAR webhook (one request per alert) and return the partition commit message."""
        import json

        from pyspark import TaskContext

        context = TaskContext.get()
        if context is None:
            raise RuntimeError("TaskContext is not available")
        partition_id = context.partitionId()

        # Retrying POST may send an alert twice - SOAR rejects the copy because it has the same DisplayId
        s = get_http_session(additional_headers={"Content-Type": "application/json"}, retry_on_post=True)
        cnt = 0
        for row in iterator:
            data = json.dumps(self._to_alert(row), cls=DateTimeJsonEncoder)
            response = s.post(self.url, data=data)
            if response.status_code >= 300:
                raise RuntimeError(f"Google SecOps SOAR webhook returned {response.status_code}: {response.text}")
            cnt += 1

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)


class GoogleSecOpsSoarBatchWriter(GoogleSecOpsSoarWriter, DataSourceWriter):
    def __init__(self, options):
        super().__init__(options)


class GoogleSecOpsSoarStreamWriter(GoogleSecOpsSoarWriter, DataSourceStreamWriter):
    def __init__(self, options):
        super().__init__(options)

    def commit(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """No-op on successful streaming batch commit."""
        pass

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """No-op on failed streaming batch commit."""
        pass
