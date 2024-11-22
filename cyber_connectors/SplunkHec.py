from dataclasses import dataclass
from typing import Iterator

from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage, DataSourceWriter
from pyspark.sql.types import StructType, Row


# TODO: Implement batch writer
# TODO: Refactor code such way so it will use common REST writer for all connectors


class SplunkDataSource(DataSource):
    """

    """

    @classmethod
    def name(cls):
        return "splunk"

    def schema(self):
        return "name string, date string, zipcode string, state string"

    def streamWriter(self, schema: StructType, overwrite: bool):
        return SplunkHecStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool):
        return SplunkHecWriter(self.options)


@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int


class SplunkHecWriter(DataSourceWriter):
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
                response = s.post(self.uri, json=msgs)
                print(response.status_code, response.text)
                msgs = []

        if len(msgs) > 0:
            response = s.post(self.uri, json=msgs)
            print(response.status_code, response.text)

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)


# https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/FormateventsforHTTPEventCollector
class SplunkHecStreamWriter(DataSourceStreamWriter):
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
                response = s.post(self.uri, json=msgs)
                print(response.status_code, response.text)
                msgs = []

        if len(msgs) > 0:
            response = s.post(self.uri, json=msgs)
            print(response.status_code, response.text)

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)

    def commit(self, messages, batchId) -> None:
        """
        Receives a sequence of :class:`WriterCommitMessage` when all write tasks have succeeded, then decides what to do with it.
        In this FakeStreamWriter, the metadata of the microbatch(number of rows and partitions) is written into a JSON file inside commit().
        """
        status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))
        # with open(os.path.join(self.path, f"{batchId}.json"), "a") as file:
        #     file.write(json.dumps(status) + "\n")

    def abort(self, messages, batchId) -> None:
        """
        Receives a sequence of :class:`WriterCommitMessage` from successful tasks when some other tasks have failed, then decides what to do with it.
        In this FakeStreamWriter, a failure message is written into a text file inside abort().
        """
        # with open(os.path.join(self.path, f"{batchId}.txt"), "w") as file:
        #     file.write(f"failed in batch {batchId}")
