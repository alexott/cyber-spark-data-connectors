from typing import Iterator

from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage, DataSourceWriter
from pyspark.sql.types import StructType, Row
from requests import Session

from cyber_connectors.common import SimpleCommitMessage, DateTimeJsonEncoder


class SplunkDataSource(DataSource):
    """

    """

    @classmethod
    def name(cls):
        return "splunk"

    def streamWriter(self, schema: StructType, overwrite: bool):
        return SplunkHecStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool):
        return SplunkHecBatchWriter(self.options)


class SplunkHecWriter:
    """

    """
    def __init__(self, options):
        self.options = options
        self.url = self.options.get("url")
        self.token = self.options.get("token")
        assert self.url is not None
        assert self.token is not None
        # extract optional parameters
        self.time_col = self.options.get("time_column")
        self.batch_size = int(self.options.get("batch_size", "50"))
        self.index = self.options.get("index")
        self.source = self.options.get("source")
        self.host = self.options.get("host")
        self.source_type = self.options.get("sourcetype", "_json")
        self.single_event_column = self.options.get("single_event_column")
        if self.single_event_column and self.source_type == "_json":
            self.source_type = "text"
        self.indexed_fields = str(self.options.get("indexed_fields", "")).split(",")
        self.omit_indexed_fields = self.options.get("remove_indexed_fields", False)
        if isinstance(self.omit_indexed_fields, str):
            self.omit_indexed_fields = self.omit_indexed_fields.lower() == "true"

    def _send_to_splunk(self, s: Session, msgs: list):
        if len(msgs) > 0:
            # TODO: add retries
            response = s.post(self.url, data="\n".join(msgs))
            print(response.status_code, response.text)

    def write(self, iterator: Iterator[Row]):
        """
        Writes the data, then returns the commit message of that partition.
        Library imports must be within the method.
        """
        from pyspark import TaskContext
        import requests
        import datetime
        import json

        context = TaskContext.get()
        partition_id = context.partitionId()
        cnt = 0
        s = requests.Session()
        s.headers.update({"Authorization": f"Splunk {self.token}"})
        msgs = []
        for row in iterator:
            cnt += 1
            rd = row.asDict()
            d = {"sourcetype": self.source_type}
            if self.index:
                d["index"] = self.index
            if self.source:
                d["source"] = self.source
            if self.host:
                d["host"] = self.host
            if self.time_col and self.time_col in rd:
                tm = rd.get(self.time_col, datetime.datetime.now())
                if isinstance(tm, datetime.datetime):
                    d["time"] = tm.timestamp()
                elif isinstance(tm, int) or isinstance(tm, float):
                    d["time"] = tm
                else:
                    d["time"] = datetime.datetime.now().timestamp()
            else:
                d["time"] = datetime.datetime.now().timestamp()
            if self.single_event_column and self.single_event_column in rd:
                d["event"] = rd.get(self.single_event_column)
            elif self.indexed_fields:
                idx_fields = {k: rd.get(k) for k in self.indexed_fields if k in rd}
                if idx_fields:
                    d["fields"] = idx_fields
                if self.omit_indexed_fields:
                    ev_fields = {k: v for k, v in rd.items() if k not in self.indexed_fields}
                    if ev_fields:
                        d["event"] = ev_fields
                else:
                    d["event"] = rd
            else:
                d["event"] = rd
            msgs.append(json.dumps(d, cls=DateTimeJsonEncoder))

            if len(msgs) >= self.batch_size:
                self._send_to_splunk(s, msgs)
                msgs = []

        self._send_to_splunk(s, msgs)

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)


class SplunkHecBatchWriter(SplunkHecWriter, DataSourceWriter):
    def __init__(self, options):
        super().__init__(options)


class SplunkHecStreamWriter(SplunkHecWriter, DataSourceStreamWriter):
    def __init__(self, options):
        super().__init__(options)

    def commit(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """
        Receives a sequence of :class:`WriterCommitMessage` when all write tasks have succeeded, then decides what to do with it.
        In this FakeStreamWriter, the metadata of the microbatch(number of rows and partitions) is written into a JSON file inside commit().
        """
        # status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))
        pass

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """
        Receives a sequence of :class:`WriterCommitMessage` from successful tasks when some other tasks have failed, then decides what to do with it.
        In this FakeStreamWriter, a failure message is written into a text file inside abort().
        """
        pass
