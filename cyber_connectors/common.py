from dataclasses import dataclass
from datetime import datetime, date
import json

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