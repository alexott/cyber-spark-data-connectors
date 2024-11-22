from typing import Iterator

from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage, DataSourceWriter
from pyspark.sql.types import StructType, Row
from requests import Session

from cyber_connectors.common import SimpleCommitMessage



class SplunkDataSource(DataSource):
    """

    """

    @classmethod
    def name(cls):
        return "splunk"

    # needed only for reads without schema
    # def schema(self):
    #     return "name string, date string, zipcode string, state string"

    def streamWriter(self, schema: StructType, overwrite: bool):
        return SplunkHecStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool):
        return SplunkHecBatchWriter(self.options)


class SplunkHecWriter:
    def __init__(self, options):
        self.options = options
        self.uri = self.options.get("uri")
        self.token = self.options.get("token")
        self.time_col = self.options.get("time_column")
        self.batch_size = int(self.options.get("batch_size", "50"))
        self.index = self.options.get("index")
        self.source = self.options.get("source")
        self.host = self.options.get("host")
        assert self.uri is not None
        assert self.token is not None

    def _send_to_splunk(self, s: Session, msgs: list):
        if len(msgs) > 0:
            # TODO: add retries
            response = s.post(self.uri, json=msgs)
            print(response.status_code, response.text)

    def write(self, iterator: Iterator[Row]):
        """
        Writes the data, then returns the commit message of that partition. Library imports must be within the method.
        """
        from pyspark import TaskContext
        import requests
        import datetime
        context = TaskContext.get()
        partition_id = context.partitionId()
        cnt = 0
        s = requests.Session()
        s.headers.update({"Authorization": f"Splunk {self.token}"})
        msgs = []
        for row in iterator:
            cnt += 1
            rd = row.asDict()
            d = {"sourcetype": "_json", "event": rd}
            if self.index:
                d["index"] = self.index
            if self.source:
                d["source"] = self.source
            if self.host:
                d["host"] = self.host
            if self.time_col:
                d["time"] = int(rd.get(self.time_col, datetime.datetime.now()).timestamp())
            else:
                d["time"] = int(datetime.datetime.now().timestamp())
            msgs.append(d)

            if len(msgs) >= self.batch_size:
                self._send_to_splunk(s, msgs)
                msgs = []

        self._send_to_splunk(s, msgs)

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)


class SplunkHecBatchWriter(SplunkHecWriter, DataSourceWriter):
    def __init__(self, options):
        super().__init__(options)


# https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/FormateventsforHTTPEventCollector
class SplunkHecStreamWriter(SplunkHecWriter, DataSourceStreamWriter):
    def __init__(self, options):
        super().__init__(options)

    def commit(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """
        Receives a sequence of :class:`WriterCommitMessage` when all write tasks have succeeded, then decides what to do with it.
        In this FakeStreamWriter, the metadata of the microbatch(number of rows and partitions) is written into a JSON file inside commit().
        """
        status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """
        Receives a sequence of :class:`WriterCommitMessage` from successful tasks when some other tasks have failed, then decides what to do with it.
        In this FakeStreamWriter, a failure message is written into a text file inside abort().
        """
        pass
