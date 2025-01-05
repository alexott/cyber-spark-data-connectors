import json
from dataclasses import dataclass
from datetime import date, datetime

from pyspark.sql.datasource import WriterCommitMessage


@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int


class DateTimeJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime) or isinstance(o, date):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


def get_http_session(retry: int=5, additional_headers: dict=None, retry_on_post: bool=False):
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry

    session = requests.Session()
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
