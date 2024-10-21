from dataclasses import dataclass

from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage
from pyspark.sql.types import StructType


class SplunkStreamDataSource(DataSource):
    """

    """

    @classmethod
    def name(cls):
        return "splunk"

    def schema(self):
        return "name string, date string, zipcode string, state string"

    def streamWriter(self, schema: StructType, overwrite: bool):
        return SplunkHecStreamWriter(self.options)


@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int


# https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/FormateventsforHTTPEventCollector
class SplunkHecStreamWriter(DataSourceStreamWriter):
    def __init__(self, options):
        self.options = options
        self.uri = self.options.get("uri")
        self.token = self.options.get("token")
        assert self.uri is not None
        assert self.token is not None

    def write(self, iterator):
        """
        Writes the data, then returns the commit message of that partition. Library imports must be within the method.
        """
        from pyspark import TaskContext
        context = TaskContext.get()
        partition_id = context.partitionId()
        cnt = 0
        for row in iterator:
            cnt += 1
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
