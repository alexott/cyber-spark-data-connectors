from typing import Iterator

from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage, DataSourceWriter
from pyspark.sql.types import StructType, Row

from cyber_connectors.common import SimpleCommitMessage, DateTimeJsonEncoder


class RestApiDataSource(DataSource):
    """

    """

    @classmethod
    def name(cls):
        return "rest"

    # needed only for reads without schema
    # def schema(self):
    #     return "name string, date string, zipcode string, state string"

    def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
        return RestApiStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return RestApiBatchWriter(self.options)


class RestApiWriter:
    def __init__(self, options):
        self.options = options
        self.uri = self.options.get("uri")
        self.payload_format: str = self.options.get("http_format", "json").lower()
        self.http_method: str = self.options.get("http_method", "post").lower()
        assert self.uri is not None
        assert self.payload_format == "json"
        assert self.http_method in ["post", "put"]

    def write(self, iterator: Iterator[Row]):
        """
        Writes the data, then returns the commit message of that partition. Library imports must be within the method.
        """
        from pyspark import TaskContext
        import requests
        import json

        s = requests.Session()
        if self.payload_format == "json":
            s.headers.update({"Content-Type": "application/json"})
        context = TaskContext.get()
        partition_id = context.partitionId()
        cnt = 0
        for row in iterator:
            cnt += 1
            data = ""
            if self.payload_format == "json":
                data = json.dumps(row.asDict(), cls=DateTimeJsonEncoder)
            if self.http_method == "post":
                response = s.post(self.uri, data=data)
            elif self.http_method == "put":
                response = s.put(self.uri, data=data)
            else:
                raise ValueError(f"Unsupported http method: {self.http_method}")
            print(response.status_code, response.text)

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)


class RestApiBatchWriter(RestApiWriter, DataSourceWriter):
    def __init__(self, options):
        super().__init__(options)


class RestApiStreamWriter(RestApiWriter, DataSourceStreamWriter):
    def __init__(self, options):
        super().__init__(options)

    def commit(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """
        Receives a sequence of :class:`WriterCommitMessage` when all write tasks have succeeded, then decides what to do with it.
        In this FakeStreamWriter, the metadata of the microbatch(number of rows and partitions) is written into a JSON file inside commit().
        """
        status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))
        # with open(os.path.join(self.path, f"{batchId}.json"), "a") as file:
        #     file.write(json.dumps(status) + "\n")

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """
        Receives a sequence of :class:`WriterCommitMessage` from successful tasks when some other tasks have failed, then decides what to do with it.
        In this FakeStreamWriter, a failure message is written into a text file inside abort().
        """
        # with open(os.path.join(self.path, f"{batchId}.txt"), "w") as file:
        #     file.write(f"failed in batch {batchId}")
        pass
