
from azure.monitor.ingestion import LogsIngestionClient
from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from cyber_connectors.common import DateTimeJsonEncoder, SimpleCommitMessage


class AzureMonitorDataSource(DataSource):
    """Data source for Azure Monitor. Right now supports writing to Azure Monitor via REST API.

    Write options:
    - dce: data collection endpoint URL
    - dcr_id: data collection rule ID
    - dcs: data collection stream name
    - tenant_id: Azure tenant ID
    - client_id: Azure service principal ID
    - client_secret: Azure service principal client secret
    """

    @classmethod
    def name(cls):
        return "azure-monitor"

    def streamWriter(self, schema: StructType, overwrite: bool):
        return AzureMonitorStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool):
        return AzureMonitorBatchWriter(self.options)


class MicrosoftSentinelDataSource(AzureMonitorDataSource):
    """Same implementation as AzureMonitorDataSource, just exposed as ms-sentinel name."""

    @classmethod
    def name(cls):
        return "ms-sentinel"


# https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-ingestion-readme?view=azure-python
class AzureMonitorWriter:
    def __init__(self, options):
        self.options = options
        self.dce = self.options.get("dce")  # data_collection_endpoint
        self.dcr_id = self.options.get("dcr_id")  # data_collection_rule_id
        self.dcs = self.options.get("dcs")  # data_collection_stream
        self.tenant_id = self.options.get("tenant_id")
        self.client_id = self.options.get("client_id")
        self.client_secret = self.options.get("client_secret")
        self.batch_size = int(self.options.get("batch_size", "50"))
        assert self.dce is not None
        assert self.dcr_id is not None
        assert self.dcs is not None
        assert self.tenant_id is not None
        assert self.client_id is not None
        assert self.client_secret is not None

    def _send_to_sentinel(self, s: LogsIngestionClient, msgs: list):
        if len(msgs) > 0:
            # TODO: add retries
            s.upload(rule_id=self.dcr_id, stream_name=self.dcs, logs=msgs)

    def write(self, iterator):
        """Writes the data, then returns the commit message of that partition. Library imports must be within the method."""
        import json

        from azure.identity import ClientSecretCredential
        from azure.monitor.ingestion import LogsIngestionClient
        from pyspark import TaskContext
        # from azure.core.exceptions import HttpResponseError

        credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        logs_client = LogsIngestionClient(self.dce, credential)

        msgs = []

        context = TaskContext.get()
        partition_id = context.partitionId()
        cnt = 0
        for row in iterator:
            cnt += 1
            #  Workaround to convert datetime/date to string
            msgs.append(json.loads(json.dumps(row.asDict(), cls=DateTimeJsonEncoder)))
            if len(msgs) >= self.batch_size:
                self._send_to_sentinel(logs_client, msgs)
                msgs = []

        self._send_to_sentinel(logs_client, msgs)

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)


class AzureMonitorBatchWriter(AzureMonitorWriter, DataSourceWriter):
    def __init__(self, options):
        super().__init__(options)


class AzureMonitorStreamWriter(AzureMonitorWriter, DataSourceStreamWriter):
    def __init__(self, options):
        super().__init__(options)

    def commit(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """Receives a sequence of :class:`WriterCommitMessage` when all write tasks have succeeded, then decides what to do with it.
        In this FakeStreamWriter, the metadata of the microbatch(number of rows and partitions) is written into a JSON file inside commit().
        """
        # status = dict(num_partitions=len(messages), rows=sum(m.count for m in messages))
        # with open(os.path.join(self.path, f"{batchId}.json"), "a") as file:
        #     file.write(json.dumps(status) + "\n")
        pass

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """Receives a sequence of :class:`WriterCommitMessage` from successful tasks when some other tasks have failed, then decides what to do with it.
        In this FakeStreamWriter, a failure message is written into a text file inside abort().
        """
        # with open(os.path.join(self.path, f"{batchId}.txt"), "w") as file:
        #     file.write(f"failed in batch {batchId}")
        pass
