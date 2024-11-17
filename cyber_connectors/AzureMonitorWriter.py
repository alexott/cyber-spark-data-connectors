from dataclasses import dataclass

from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage
from pyspark.sql.types import StructType


class AzureMonitorStreamDataSource(DataSource):
    """

    """

    @classmethod
    def name(cls):
        return "azure_monitor"

    def schema(self):
        return "name string, date string, zipcode string, state string"

    def streamWriter(self, schema: StructType, overwrite: bool):
        return AzureMonitorStreamWriter(self.options)


@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int


# https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-ingestion-readme?view=azure-python
class AzureMonitorStreamWriter(DataSourceStreamWriter):
    def __init__(self, options):
        self.options = options
        self.dce = self.options.get("dce")  # data_collection_endpoint
        self.dcr_id = self.options.get("dcr_id")  # data_collection_rule_id
        self.dcs = self.options.get("dcs")  # data_collection_stream
        self.tenant_id = self.options.get("tenant_id")
        self.client_id = self.options.get("client_id")
        self.client_secret = self.options.get("client_secret")
        assert self.dce is not None
        assert self.dcr_id is not None
        assert self.dcs is not None
        assert self.tenant_id is not None
        assert self.client_id is not None
        assert self.client_secret is not None

    def write(self, iterator):
        """
        Writes the data, then returns the commit message of that partition. Library imports must be within the method.
        """
        from pyspark import TaskContext
        from azure.identity import ClientSecretCredential
        from azure.monitor.ingestion import LogsIngestionClient
        from azure.core.exceptions import HttpResponseError
        import json

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
