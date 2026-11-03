"""Common helpers shared by connector implementations."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql.datasource import WriterCommitMessage
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    """Commit message containing the partition identifier and row count."""

    partition_id: int
    count: int


def to_utc_iso(value: datetime) -> str:
    """Convert a datetime to an ISO 8601 string in UTC (with the +00:00 offset).

    Spark passes TIMESTAMP values to Python as naive datetimes in the local time zone of the Python process,
    so a naive datetime is taken as local time.  Without the conversion, a non-UTC machine sends shifted times.
    TIMESTAMP_NTZ values are also naive and are treated the same way.
    """
    return value.astimezone(timezone.utc).isoformat()


class DateTimeJsonEncoder(json.JSONEncoder):
    """JSON encoder that serializes datetimes as ISO 8601 in UTC, and dates as ISO 8601."""

    def default(self, o: Any) -> Any:
        """Convert dates and datetimes to ISO strings before JSON encoding."""
        if isinstance(o, datetime):
            return to_utc_iso(o)
        if isinstance(o, date):
            return o.isoformat()

        return super().default(o)


def get_http_session(
    retry: int = 5,
    additional_headers: Mapping[str, str] | None = None,
    retry_on_post: bool = False,
) -> Session:
    session = Session()
    if additional_headers:
        session.headers.update(additional_headers)

    if retry > 0:
        allowed_methods = ["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"]
        if retry_on_post:
            allowed_methods.append("POST")
        retry_strategy = Retry(
            total=retry,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=allowed_methods,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

    return session
