"""Common helpers shared by connector implementations."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
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


class DateTimeJsonEncoder(json.JSONEncoder):
    """JSON encoder that serializes date and datetime values using ISO 8601."""

    def default(self, o: Any) -> Any:
        """Convert dates and datetimes to ISO strings before JSON encoding."""
        if isinstance(o, (datetime, date)):
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
