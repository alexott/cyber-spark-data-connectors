from dataclasses import dataclass
from datetime import datetime, date

from azure.monitor.ingestion import LogsIngestionClient
from pyspark.errors.exceptions.base import PySparkNotImplementedError
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamWriter,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType

from cyber_connectors.common import DateTimeJsonEncoder, SimpleCommitMessage


def _create_azure_credential(tenant_id, client_id, client_secret):
    """Create Azure ClientSecretCredential for authentication.

    Args:
        tenant_id: Azure tenant ID
        client_id: Azure service principal client ID
        client_secret: Azure service principal client secret

    Returns:
        ClientSecretCredential: Authenticated credential object

    """
    from azure.identity import ClientSecretCredential

    return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)


def _parse_time_range(timespan=None, start_time=None, end_time=None):
    """Parse time range from timespan or start_time/end_time options.

    Args:
        timespan: ISO 8601 duration string (e.g., "P1D", "PT1H")
        start_time: ISO 8601 datetime string (e.g., "2024-01-01T00:00:00Z")
        end_time: ISO 8601 datetime string (optional, defaults to now)

    Returns:
        tuple: (start_datetime, end_datetime) as datetime objects with timezone

    Raises:
        ValueError: If timespan format is invalid
        Exception: If neither timespan nor start_time is provided

    """
    import re
    from datetime import datetime, timedelta, timezone

    if timespan:
        # Parse ISO 8601 duration
        # Format: P[n]D or PT[n]H[n]M[n]S or combination P[n]DT[n]H[n]M[n]S
        match = re.match(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$", timespan)
        if match:
            days = int(match.group(1) or 0)
            hours = int(match.group(2) or 0)
            minutes = int(match.group(3) or 0)
            seconds = int(match.group(4) or 0)

            # Validate that at least one component was specified
            if days == 0 and hours == 0 and minutes == 0 and seconds == 0:
                raise ValueError(f"Invalid timespan format: {timespan} - must specify at least one duration component")

            delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
            end_time_val = datetime.now(timezone.utc)
            start_time_val = end_time_val - delta
            return (start_time_val, end_time_val)
        else:
            raise ValueError(f"Invalid timespan format: {timespan}")
    elif start_time:
        start_time_val = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        if end_time:
            end_time_val = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        else:
            end_time_val = datetime.now(timezone.utc)
        return (start_time_val, end_time_val)
    else:
        raise Exception("Either 'timespan' or 'start_time' must be provided")


def _execute_logs_query(
    workspace_id,
    query,
    timespan,
    tenant_id,
    client_id,
    client_secret,
):
    """Execute a KQL query against Azure Monitor Log Analytics workspace.

    Args:
        workspace_id: Log Analytics workspace ID
        query: KQL query to execute
        timespan: Time range as tuple (start_time, end_time)
        tenant_id: Azure tenant ID
        client_id: Azure service principal client ID
        client_secret: Azure service principal client secret

    Returns:
        Query response object from Azure Monitor

    Raises:
        Exception: If query fails

    """
    from azure.monitor.query import LogsQueryClient, LogsQueryStatus

    # Create authenticated client
    credential = _create_azure_credential(tenant_id, client_id, client_secret)
    client = LogsQueryClient(credential)

    # Execute query
    response = client.query_workspace(
        workspace_id=workspace_id,
        query=query,
        timespan=timespan,
        include_statistics=False,
        include_visualization=False,
    )

    if response.status != LogsQueryStatus.SUCCESS:
        raise Exception(f"Query failed with status: {response.status}")

    return response


def _convert_value_to_schema_type(value, spark_type):
    """Convert a value to match the expected PySpark schema type.

    Args:
        value: The raw value from Azure Monitor
        spark_type: The expected PySpark DataType

    Returns:
        Converted value matching the schema type

    Raises:
        ValueError: If conversion fails

    """
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
    )

    # Handle None/NULL values
    if value is None:
        return None

    try:
        # String type - convert everything to string
        if isinstance(spark_type, StringType):
            return str(value)

        # Boolean type
        elif isinstance(spark_type, BooleanType):
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                if value.lower() in ("true", "1", "yes"):
                    return True
                elif value.lower() in ("false", "0", "no"):
                    return False
                else:
                    raise ValueError(f"Cannot convert string '{value}' to boolean")
            elif isinstance(value, (int, float)):
                return bool(value)
            else:
                raise ValueError(f"Cannot convert {type(value).__name__} to boolean")

        # Integer types
        elif isinstance(spark_type, (IntegerType, LongType)):
            if isinstance(value, bool):
                # Don't convert bool to int (bool is subclass of int in Python)
                raise ValueError(f"Cannot convert boolean to integer")
            return int(value)

        # Float types
        elif isinstance(spark_type, (FloatType, DoubleType)):
            if isinstance(value, bool):
                raise ValueError(f"Cannot convert boolean to float")
            return float(value)

        # Timestamp type
        elif isinstance(spark_type, TimestampType):
            if isinstance(value, datetime):
                return value
            elif isinstance(value, str):
                # Try parsing ISO 8601 format
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            else:
                raise ValueError(f"Cannot convert {type(value).__name__} to timestamp")

        # Date type
        elif isinstance(spark_type, DateType):
            if isinstance(value, date) and not isinstance(value, datetime):
                return value
            elif isinstance(value, datetime):
                return value.date()
            elif isinstance(value, str):
                # Try parsing ISO 8601 date format
                return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
            else:
                raise ValueError(f"Cannot convert {type(value).__name__} to date")

        # Unsupported type - return as-is
        else:
            return value

    except (ValueError, TypeError) as e:
        raise ValueError(f"Failed to convert value '{value}' (type: {type(value).__name__}) to {spark_type}: {e}")


@dataclass
class TimeRangePartition(InputPartition):
    """Represents a time range partition for parallel query execution."""

    start_time: datetime
    end_time: datetime


class AzureMonitorDataSource(DataSource):
    """Data source for Azure Monitor. Supports reading from and writing to Azure Monitor.

    Write options:
    - dce: data collection endpoint URL
    - dcr_id: data collection rule ID
    - dcs: data collection stream name
    - tenant_id: Azure tenant ID
    - client_id: Azure service principal ID
    - client_secret: Azure service principal client secret

    Read options:
    - workspace_id: Log Analytics workspace ID
    - query: KQL query to execute
    - timespan: Time range for query in ISO 8601 duration format
    - tenant_id: Azure tenant ID
    - client_id: Azure service principal ID
    - client_secret: Azure service principal client secret
    """

    @classmethod
    def name(cls):
        return "azure-monitor"

    def schema(self):
        """Return the schema for reading data.

        If the user doesn't provide a schema, this method infers it by executing
        a sample query with limit 1.

        Returns:
            StructType: The schema of the data

        """
        # Check if we're being called for reading (workspace_id present)
        # vs writing (dce present)
        if self.options.get("workspace_id"):
            # Reading - infer schema from query
            return self._infer_read_schema()
        else:
            # Writing - schema will be provided by Spark
            raise PySparkNotImplementedError(
                errorClass="NOT_IMPLEMENTED",
                messageParameters={"feature": "schema for write operations"},
            )

    def _infer_read_schema(self):
        """Infer schema by executing a sample query with limit 1.

        Returns:
            StructType: The inferred schema

        Raises:
            Exception: If query returns no results or fails

        """
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
            DateType,
        )

        # Get read options
        workspace_id = self.options.get("workspace_id")
        query = self.options.get("query")
        tenant_id = self.options.get("tenant_id")
        client_id = self.options.get("client_id")
        client_secret = self.options.get("client_secret")
        timespan = self.options.get("timespan")
        start_time = self.options.get("start_time")
        end_time = self.options.get("end_time")

        # Validate required options
        assert workspace_id is not None, "workspace_id is required"
        assert query is not None, "query is required"
        assert tenant_id is not None, "tenant_id is required"
        assert client_id is not None, "client_id is required"
        assert client_secret is not None, "client_secret is required"

        # Parse time range using module-level function
        timespan_value = _parse_time_range(timespan=timespan, start_time=start_time, end_time=end_time)

        # Modify query to limit results to 1 row
        sample_query = query.strip()
        if not any(keyword in sample_query.lower() for keyword in ["| take 1", "| limit 1"]):
            sample_query = f"{sample_query} | take 1"

        # Execute sample query using module-level function
        response = _execute_logs_query(
            workspace_id=workspace_id,
            query=sample_query,
            timespan=timespan_value,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )

        # Check if we got any tables
        if not response.tables or len(response.tables) == 0:
            raise Exception("Schema inference failed: query returned no tables")

        table = response.tables[0]

        # Check if table has any columns
        if not table.columns or len(table.columns) == 0:
            raise Exception("Schema inference failed: query returned no columns")

        # Check if we have any rows to infer types from
        if not table.rows or len(table.rows) == 0:
            # No data to infer types from, use string type for all columns
            fields = [StructField(str(col), StringType(), nullable=True) for col in table.columns]
            return StructType(fields)

        # Infer schema from actual data in the first row
        # table.columns is always a list of strings (column names)
        first_row = table.rows[0]
        fields = []

        for i, column_name in enumerate(table.columns):
            # Get the value from the first row to infer type
            value = first_row[i] if i < len(first_row) else None

            # Infer PySpark type from Python type
            if value is None:
                # If first value is None, default to StringType
                spark_type = StringType()
            elif isinstance(value, bool):
                # Check bool before int (bool is subclass of int in Python)
                spark_type = BooleanType()
            elif isinstance(value, int):
                spark_type = LongType()
            elif isinstance(value, float):
                spark_type = DoubleType()
            elif isinstance(value, datetime):
                spark_type = TimestampType()
            elif isinstance(value, date):
                spark_type = DateType()
            elif isinstance(value, str):
                spark_type = StringType()
            else:
                # For any other type, use StringType
                spark_type = StringType()

            fields.append(StructField(column_name, spark_type, nullable=True))

        return StructType(fields)

    def reader(self, schema: StructType):
        return AzureMonitorBatchReader(self.options, schema)

    def streamWriter(self, schema: StructType, overwrite: bool):
        return AzureMonitorStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool):
        return AzureMonitorBatchWriter(self.options)


class MicrosoftSentinelDataSource(AzureMonitorDataSource):
    """Same implementation as AzureMonitorDataSource, just exposed as ms-sentinel name."""

    @classmethod
    def name(cls):
        return "ms-sentinel"


class AzureMonitorBatchReader(DataSourceReader):
    """Reader for Azure Monitor / Log Analytics workspaces."""

    def __init__(self, options, schema: StructType):
        """Initialize the reader with options and schema.

        Args:
            options: Dictionary of options containing workspace_id, query, time range, credentials
            schema: StructType schema (provided by DataSource.schema())

        """
        # Extract and validate required options
        self.workspace_id = options.get("workspace_id")
        self.query = options.get("query")
        self.tenant_id = options.get("tenant_id")
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")

        # Time range options (mutually exclusive)
        timespan = options.get("timespan")
        start_time = options.get("start_time")
        end_time = options.get("end_time")

        # Optional options
        self.num_partitions = int(options.get("num_partitions", "1"))

        # Validate required options
        assert self.workspace_id is not None, "workspace_id is required"
        assert self.query is not None, "query is required"
        assert self.tenant_id is not None, "tenant_id is required"
        assert self.client_id is not None, "client_id is required"
        assert self.client_secret is not None, "client_secret is required"

        # Parse time range using module-level function
        self.start_time, self.end_time = _parse_time_range(timespan=timespan, start_time=start_time, end_time=end_time)

        # Store schema (provided by DataSource.schema())
        self._schema = schema

    def partitions(self):
        """Generate list of non-overlapping time range partitions.

        Returns:
            List of TimeRangePartition objects, each containing start_time and end_time

        """
        # Calculate total time range duration
        total_duration = self.end_time - self.start_time

        # Split into N equal partitions
        partition_duration = total_duration / self.num_partitions

        partitions = []
        for i in range(self.num_partitions):
            partition_start = self.start_time + (partition_duration * i)
            partition_end = self.start_time + (partition_duration * (i + 1))

            # Ensure last partition ends exactly at end_time (avoid rounding errors)
            if i == self.num_partitions - 1:
                partition_end = self.end_time

            partitions.append(TimeRangePartition(partition_start, partition_end))

        return partitions

    def read(self, partition: TimeRangePartition):
        """Read data for the given partition time range.

        Args:
            partition: TimeRangePartition containing start_time and end_time

        Yields:
            Row objects from the query results

        """
        # Import inside method for partition-level execution
        from pyspark.sql import Row

        # Use partition's time range
        timespan_value = (partition.start_time, partition.end_time)

        # Execute query using module-level function
        response = _execute_logs_query(
            workspace_id=self.workspace_id,
            query=self.query,
            timespan=timespan_value,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        # Create a mapping of column names to their expected types from schema
        schema_field_map = {field.name: field.dataType for field in self._schema.fields}

        # Process all tables in response
        for table in response.tables:
            # Convert Azure Monitor rows to Spark Rows
            # table.columns is always a list of strings (column names)
            for row_idx, row_data in enumerate(table.rows):
                row_dict = {}

                # First, process columns from the query results
                for i, col in enumerate(table.columns):
                    # Handle both string columns (real API) and objects with .name attribute (test mocks)
                    column_name = str(col) if isinstance(col, str) else str(col.name)
                    raw_value = row_data[i]

                    # If column is in schema, convert to expected type
                    if column_name in schema_field_map:
                        expected_type = schema_field_map[column_name]
                        try:
                            converted_value = _convert_value_to_schema_type(raw_value, expected_type)
                            row_dict[column_name] = converted_value
                        except ValueError as e:
                            raise ValueError(f"Row {row_idx}, column '{column_name}': {e}")
                    # Note: columns not in schema are ignored (not included in row)

                # Second, add NULL values for schema columns that are not in query results
                for schema_column_name in schema_field_map.keys():
                    if schema_column_name not in row_dict:
                        row_dict[schema_column_name] = None

                yield Row(**row_dict)


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
