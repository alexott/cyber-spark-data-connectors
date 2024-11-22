from dataclasses import dataclass

from pyspark.sql.datasource import WriterCommitMessage


@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int