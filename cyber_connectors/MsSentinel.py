import time
from dataclasses import dataclass
from datetime import date, datetime

from azure.monitor.ingestion import LogsIngestionClient
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    DataSourceStreamWriter,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType

from cyber_connectors.common import DateTimeJsonEncoder, SimpleCommitMessage


def _get_azure_cloud_config(azure_cloud=None):
    """Get Azure cloud configuration for authority and Log Analytics endpoint.

    Maps the azure_cloud option to the appropriate authority host for authentication
    and Log Analytics API endpoint.

    Args:
        azure_cloud: Cloud environment - "public" (default), "government", or "china"

    Returns:
        tuple: (authority, logs_endpoint) where:
            - authority: Authority host URL for authentication (None for public cloud default)
            - logs_endpoint: Log Analytics API endpoint URL (None for public cloud default)

    Raises:
        ValueError: If azure_cloud value is not recognized

    """
    from azure.identity import AzureAuthorityHosts

    # Normalize input
    cloud = (azure_cloud or "public").lower().strip()

    cloud_configs = {
        "public": (None, None),  # Use defaults
        "government": (
            AzureAuthorityHosts.AZURE_GOVERNMENT,  # login.microsoftonline.us
            "https://api.loganalytics.us",
        ),
        "china": (
            AzureAuthorityHosts.AZURE_CHINA,  # login.chinacloudapi.cn
            "https://api.loganalytics.azure.cn",
        ),
    }

    if cloud not in cloud_configs:
        valid_clouds = ", ".join(cloud_configs.keys())
        raise ValueError(f"Invalid azure_cloud value '{azure_cloud}'. Valid values are: {valid_clouds}")

    return cloud_configs[cloud]


def _create_azure_credential(tenant_id, client_id, client_secret, authority=None):
    """Create Azure ClientSecretCredential for authentication.

    Args:
        tenant_id: Azure tenant ID
        client_id: Azure service principal client ID
        client_secret: Azure service principal client secret
        authority: Optional authority host URL for sovereign clouds
                   (e.g., AzureAuthorityHosts.AZURE_GOVERNMENT)

    Returns:
        ClientSecretCredential: Authenticated credential object

    """
    from azure.identity import ClientSecretCredential

    if authority:
        return ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret, authority=authority
        )
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


def _check_error_for_size_limit(error_obj):
    """Recursively check an error object for size limit indicators.

    Args:
        error_obj: Error object (dict or object with attributes)

    Returns:
        bool: True if size limit error is found

    """
    if error_obj is None:
        return False

    # Size limit error codes and message patterns
    size_limit_codes = [
        "QueryExecutionResultSizeLimitExceeded",
        "ResponsePayloadTooLarge",
        "QueryExecutionResponseSizeLimitExceeded",
        "E_QUERY_RESULT_SET_TOO_LARGE",
    ]
    size_limit_patterns = [
        "size limit",
        "too large",
        "e_query_result_set_too_large",
        "result set too large",
        "exceed",
    ]

    # Get code and message from object (handles both dict and object with attributes)
    code = None
    message = None

    if isinstance(error_obj, dict):
        code = error_obj.get("code")
        message = error_obj.get("message")
    else:
        if hasattr(error_obj, "code"):
            code = error_obj.code
        if hasattr(error_obj, "message"):
            message = error_obj.message

    # Check error code
    if code and code in size_limit_codes:
        return True

    # Check error message for patterns
    if message and isinstance(message, str):
        message_lower = message.lower()
        for pattern in size_limit_patterns:
            if pattern in message_lower:
                return True

    # Check nested 'details' array (can be list of dicts or objects)
    details = None
    if isinstance(error_obj, dict):
        details = error_obj.get("details")
    elif hasattr(error_obj, "details"):
        details = getattr(error_obj, "details", None)

    # Only process if details is actually a list/tuple (not Mock or other truthy object)
    if details is not None and isinstance(details, (list, tuple)):
        for detail in details:
            if _check_error_for_size_limit(detail):
                return True

    # Check nested 'innererror' field
    innererror = None
    if isinstance(error_obj, dict):
        innererror = error_obj.get("innererror")
    elif hasattr(error_obj, "innererror"):
        innererror = getattr(error_obj, "innererror", None)

    # Only recurse if innererror is a dict or has code/message attributes (avoid Mock infinite recursion)
    if innererror is not None and (isinstance(innererror, dict) or hasattr(innererror, "code")):
        # Prevent infinite recursion by checking if innererror is the same object
        if innererror is not error_obj and _check_error_for_size_limit(innererror):
            return True

    return False


def _is_result_size_limit_error(response):
    """Check if a PARTIAL response is due to result size limits being exceeded.

    Args:
        response: LogsQueryResult with PARTIAL or FAILURE status

    Returns:
        bool: True if the error is due to result size limits

    """
    # Check if partial_error exists
    if not hasattr(response, "partial_error") or response.partial_error is None:
        return False

    # First try structured check
    if _check_error_for_size_limit(response.partial_error):
        return True

    # Fallback: check the string representation for size limit patterns
    # This handles cases where the Azure SDK returns error in unexpected format
    error_str = str(response.partial_error).lower()
    size_limit_patterns = [
        "e_query_result_set_too_large",
        "result set too large",
        "results of this query exceed",
        "size limit",
    ]
    for pattern in size_limit_patterns:
        if pattern in error_str:
            return True

    return False


def _execute_logs_query(
    workspace_id,
    query,
    timespan,
    tenant_id,
    client_id,
    client_secret,
    max_retries=5,
    initial_backoff=1.0,
    azure_cloud=None,
):
    """Execute a KQL query against Azure Monitor Log Analytics workspace.

    Includes retry logic with exponential backoff for HTTP 429 throttling errors.

    Args:
        workspace_id: Log Analytics workspace ID
        query: KQL query to execute (could be just a table name)
        timespan: Time range as tuple (start_time, end_time)
        tenant_id: Azure tenant ID
        client_id: Azure service principal client ID
        client_secret: Azure service principal client secret
        max_retries: Maximum number of retry attempts for throttling (default: 5)
        initial_backoff: Initial backoff time in seconds (default: 1.0)
        azure_cloud: Azure cloud environment - "public" (default), "government", or "china"

    Returns:
        Query response object from Azure Monitor (may have PARTIAL status)

    Raises:
        HttpResponseError: If non-retryable HTTP error occurs after all retries

    """
    from azure.core.exceptions import HttpResponseError
    from azure.monitor.query import LogsQueryClient

    # Get cloud-specific configuration (authority and endpoint)
    authority, endpoint = _get_azure_cloud_config(azure_cloud)

    # Create authenticated client with cloud-specific authority
    credential = _create_azure_credential(tenant_id, client_id, client_secret, authority=authority)

    # Create LogsQueryClient with cloud-specific endpoint
    if endpoint:
        client = LogsQueryClient(credential, endpoint=endpoint)
    else:
        client = LogsQueryClient(credential)

    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            # Execute query
            response = client.query_workspace(
                workspace_id=workspace_id,
                query=query,
                timespan=timespan,
                include_statistics=False,
                include_visualization=False,
            )
            # Return response (may be SUCCESS or PARTIAL - caller handles status)
            return response

        except HttpResponseError as e:
            last_exception = e
            # Only retry on 429 (Too Many Requests) status code
            if e.status_code == 429 and attempt < max_retries:
                # Use Retry-After header if present, otherwise use exponential backoff
                retry_after = None
                if e.response and e.response.headers:
                    retry_after_header = e.response.headers.get("Retry-After")
                    if retry_after_header:
                        try:
                            retry_after = int(retry_after_header)
                        except (ValueError, TypeError):
                            pass

                if retry_after is None:
                    # Exponential backoff: 1s, 2s, 4s, 8s, 16s, ...
                    retry_after = initial_backoff * (2**attempt)

                time.sleep(retry_after)
            else:
                # Non-retryable error or max retries exceeded
                raise

    # Should not reach here, but raise last exception if we do
    if last_exception:
        raise last_exception


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
                raise ValueError("Cannot convert boolean to integer")
            return int(value)

        # Float types
        elif isinstance(spark_type, (FloatType, DoubleType)):
            if isinstance(value, bool):
                raise ValueError("Cannot convert boolean to float")
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
    - query: KQL query to execute (could be just a table name)
    - timespan: Time range for query in ISO 8601 duration format
    - tenant_id: Azure tenant ID
    - client_id: Azure service principal ID
    - client_secret: Azure service principal client secret
    - azure_cloud: Azure cloud environment - "public" (default), "government", or "china".
                   Automatically configures authentication authority and Log Analytics endpoint.
    - max_retries: Maximum retry attempts for throttling (default: 5)
    - initial_backoff: Initial backoff time in seconds for retries (default: 1.0)
    - min_partition_seconds: Minimum partition duration for subdivision (default: 60)
    """

    @classmethod
    def name(cls):
        return "azure-monitor"

    def schema(self):
        """Return the schema for reading data.

        If the user doesn't provide a schema, this method infers it by executing
        a sample query with limit 1. Only if inferSchema is true.

        Returns:
            StructType: The schema of the data

        """
        infer_schema = self.options.get("inferSchema", "true").lower() == "true"
        if infer_schema:
            return self._infer_read_schema()
        else:
            raise Exception("Must provide schema if inferSchema is false")

    def _infer_schema_from_query(self, workspace_id, query, timespan_value, tenant_id, client_id, client_secret, azure_cloud):
        """Helper method to infer schema by executing a query and analyzing the first row.

        Args:
            workspace_id: Log Analytics workspace ID
            query: KQL query to execute (should include | take 1 or | limit 1)
            timespan_value: Time range as tuple (start_time, end_time)
            tenant_id: Azure tenant ID
            client_id: Azure service principal client ID
            client_secret: Azure service principal client secret
            azure_cloud: Azure cloud environment

        Returns:
            StructType: The inferred schema

        Raises:
            Exception: If query fails or returns no data

        """
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )
        from azure.monitor.query import LogsQueryStatus

        # Execute query
        response = _execute_logs_query(
            workspace_id=workspace_id,
            query=query,
            timespan=timespan_value,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            azure_cloud=azure_cloud,
        )

        # Check query status
        if response.status != LogsQueryStatus.SUCCESS:
            raise Exception(f"Query failed with status: {response.status}")

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

    def _infer_read_schema(self):
        """Infer schema by executing a sample query with limit 1.

        Returns:
            StructType: The inferred schema

        Raises:
            Exception: If query returns no results or fails

        """
        # Get read options
        workspace_id = self.options.get("workspace_id")
        query = self.options.get("query")
        tenant_id = self.options.get("tenant_id")
        client_id = self.options.get("client_id")
        client_secret = self.options.get("client_secret")
        timespan = self.options.get("timespan")
        start_time = self.options.get("start_time")
        end_time = self.options.get("end_time")
        azure_cloud = self.options.get("azure_cloud", "public")

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
        if not any(keyword in sample_query.lower() for keyword in ["| take ", "| limit "]):
            sample_query = f"{sample_query} | take 1"

        # Use helper method to infer schema
        return self._infer_schema_from_query(
            workspace_id=workspace_id,
            query=sample_query,
            timespan_value=timespan_value,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            azure_cloud=azure_cloud,
        )

    def list_tables(self):
        """List all tables in the Log Analytics workspace.

        Returns:
            list[str]: List of table names sorted alphabetically

        Raises:
            Exception: If workspace_id or credentials are not provided, or if query fails

        """
        # Get required options
        workspace_id = self.options.get("workspace_id")
        tenant_id = self.options.get("tenant_id")
        client_id = self.options.get("client_id")
        client_secret = self.options.get("client_secret")
        azure_cloud = self.options.get("azure_cloud", "public")

        # Validate required options
        assert workspace_id is not None, "workspace_id is required"
        assert tenant_id is not None, "tenant_id is required"
        assert client_id is not None, "client_id is required"
        assert client_secret is not None, "client_secret is required"

        # KQL query to list all distinct table names
        list_tables_query = """
        search *
        | distinct $table
        | sort by $table asc
        """

        # Execute query using module-level function without timespan restriction
        # (None timespan allows querying all available data to discover all tables)
        from azure.monitor.query import LogsQueryStatus

        response = _execute_logs_query(
            workspace_id=workspace_id,
            query=list_tables_query,
            timespan=None,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            azure_cloud=azure_cloud,
        )

        # Check query status
        if response.status != LogsQueryStatus.SUCCESS:
            raise Exception(f"Query failed with status: {response.status}")

        # Check if we got any tables
        if not response.tables or len(response.tables) == 0:
            return []

        table = response.tables[0]

        # Extract table names from results
        # The $table column should be the first (and only) column
        table_names = []
        for row in table.rows:
            if row and len(row) > 0:
                table_name = row[0]
                if table_name:
                    table_names.append(str(table_name))

        # KQL query sorts by $table, but sort again here to ensure deterministic results
        return sorted(table_names)

    def get_table_schema(self, table_name: str):
        """Get the schema for a specific table by inferring it from a sample query.

        Args:
            table_name: Name of the table to get schema for

        Returns:
            StructType: The inferred schema for the table

        Raises:
            Exception: If workspace_id or credentials are not provided, or if query fails
            ValueError: If table_name is empty or invalid

        """
        # Validate table name
        if not table_name or not isinstance(table_name, str) or not table_name.strip():
            raise ValueError("table_name must be a non-empty string")

        table_name = table_name.strip()
        import re

        # Guard against KQL injection by validating allowed identifier characters.
        # We intentionally disallow whitespace, pipes, quotes, and other KQL operators/commands.
        if not re.fullmatch(r"[A-Za-z0-9_-]+", table_name):
            raise ValueError(
                "table_name contains invalid characters (allowed: letters, digits, underscore, hyphen)."
            )

        # Get required options
        workspace_id = self.options.get("workspace_id")
        tenant_id = self.options.get("tenant_id")
        client_id = self.options.get("client_id")
        client_secret = self.options.get("client_secret")
        azure_cloud = self.options.get("azure_cloud", "public")

        # Validate required options
        assert workspace_id is not None, "workspace_id is required"
        assert tenant_id is not None, "tenant_id is required"
        assert client_id is not None, "client_id is required"
        assert client_secret is not None, "client_secret is required"

        # Create query to get one row from the table
        sample_query = f"{table_name} | take 1"

        # Use helper method to infer schema without timespan restriction
        # (None timespan allows querying all available data)
        try:
            return self._infer_schema_from_query(
                workspace_id=workspace_id,
                query=sample_query,
                timespan_value=None,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                azure_cloud=azure_cloud,
            )
        except Exception as e:
            # Re-raise with table-specific error message
            if "Schema inference failed" in str(e) and table_name not in str(e):
                inner = str(e).replace("Schema inference failed: ", "")
                raise Exception(f"Schema inference failed for table '{table_name}': {inner}") from e
            raise

    def reader(self, schema: StructType):
        return AzureMonitorBatchReader(self.options, schema)

    def streamReader(self, schema: StructType):
        return AzureMonitorStreamReader(self.options, schema)

    def streamWriter(self, schema: StructType, overwrite: bool):
        return AzureMonitorStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool):
        return AzureMonitorBatchWriter(self.options)


class MicrosoftSentinelDataSource(AzureMonitorDataSource):
    """Same implementation as AzureMonitorDataSource, just exposed as ms-sentinel name."""

    @classmethod
    def name(cls):
        return "ms-sentinel"


class AzureMonitorReader:
    """Base reader class for Azure Monitor / Log Analytics workspaces.

    Shared read logic for batch and streaming reads.
    """

    def __init__(self, options, schema: StructType):
        """Initialize the reader with options and schema.

        Args:
            options: Dictionary of options containing workspace_id, query, credentials
            schema: StructType schema (provided by DataSource.schema())

        """
        # Extract and validate required options
        self.workspace_id = options.get("workspace_id")
        self.query = options.get("query")
        self.tenant_id = options.get("tenant_id")
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")

        # Validate required options
        assert self.workspace_id is not None, "workspace_id is required"
        assert self.query is not None, "query is required"
        assert self.tenant_id is not None, "tenant_id is required"
        assert self.client_id is not None, "client_id is required"
        assert self.client_secret is not None, "client_secret is required"

        # Azure cloud environment: "public" (default), "government", or "china"
        self.azure_cloud = options.get("azure_cloud", "public")

        # Retry and subdivision options
        self.max_retries = int(options.get("max_retries", "5"))
        self.initial_backoff = float(options.get("initial_backoff", "1.0"))
        self.min_partition_seconds = int(options.get("min_partition_seconds", "60"))

        # Store schema (provided by DataSource.schema())
        self._schema = schema

    def read(self, partition: TimeRangePartition):
        """Read data for the given partition time range.

        Handles throttling with retries and large result sets by subdividing time ranges.

        Args:
            partition: TimeRangePartition containing start_time and end_time

        Yields:
            Row objects from the query results

        """
        # Import inside method for partition-level execution
        from azure.monitor.query import LogsQueryStatus

        # Use partition's time range
        timespan_value = (partition.start_time, partition.end_time)

        # Execute query using module-level function (includes retry logic for 429)
        response = _execute_logs_query(
            workspace_id=self.workspace_id,
            query=self.query,
            timespan=timespan_value,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            max_retries=self.max_retries,
            initial_backoff=self.initial_backoff,
            azure_cloud=self.azure_cloud,
        )

        # Handle PARTIAL or FAILURE status - check if due to size limits
        # Azure may return either PARTIAL or FAILURE for size limit errors
        if response.status in (LogsQueryStatus.PARTIAL, LogsQueryStatus.FAILURE):
            if _is_result_size_limit_error(response):
                # Try to subdivide the time range
                yield from self._read_with_subdivision(partition)
                return
            else:
                # Error for other reasons - raise error
                error_msg = ""
                if hasattr(response, "partial_error") and response.partial_error:
                    error_msg = f": {response.partial_error}"
                raise Exception(f"Query failed with status {response.status}{error_msg}")

        # Process successful response
        yield from self._process_response(response)

    def _read_with_subdivision(self, partition: TimeRangePartition):
        """Subdivide a partition and recursively read smaller time ranges.

        Args:
            partition: TimeRangePartition that returned too many results

        Yields:
            Row objects from subdivided queries

        Raises:
            Exception: If partition cannot be subdivided further

        """
        from datetime import timedelta

        # Calculate partition duration in seconds
        duration = (partition.end_time - partition.start_time).total_seconds()

        # Check if we can subdivide further
        if duration <= self.min_partition_seconds:
            raise Exception(
                f"Cannot subdivide partition further. "
                f"Duration {duration}s is at or below minimum {self.min_partition_seconds}s. "
                f"Time range: {partition.start_time} to {partition.end_time}. "
                f"Consider using a more selective query or increasing min_partition_seconds."
            )

        # Split the time range in half
        midpoint = partition.start_time + timedelta(seconds=duration / 2)

        # Create two sub-partitions
        first_half = TimeRangePartition(partition.start_time, midpoint)
        second_half = TimeRangePartition(midpoint, partition.end_time)

        # Recursively read from each sub-partition
        yield from self.read(first_half)
        yield from self.read(second_half)

    def _process_response(self, response):
        """Process a successful query response and yield rows.

        Args:
            response: Successful LogsQueryResult

        Yields:
            Row objects converted according to schema

        """
        from pyspark.sql import Row

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


class AzureMonitorBatchReader(AzureMonitorReader, DataSourceReader):
    """Batch reader for Azure Monitor / Log Analytics workspaces."""

    def __init__(self, options, schema: StructType):
        """Initialize the batch reader with options and schema.

        Args:
            options: Dictionary of options containing workspace_id, query, time range, credentials
            schema: StructType schema (provided by DataSource.schema())

        """
        super().__init__(options, schema)

        # Time range options (mutually exclusive)
        timespan = options.get("timespan")
        start_time = options.get("start_time")
        end_time = options.get("end_time")

        # Optional options
        self.num_partitions = int(options.get("num_partitions", "1"))

        # Parse time range using module-level function
        self.start_time, self.end_time = _parse_time_range(timespan=timespan, start_time=start_time, end_time=end_time)

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


class AzureMonitorOffset:
    """Represents the offset for Azure Monitor streaming.

    The offset tracks the timestamp of the last processed data to enable incremental streaming.
    """

    def __init__(self, timestamp: str):
        """Initialize offset with ISO 8601 timestamp.

        Args:
            timestamp: ISO 8601 formatted timestamp string (e.g., "2024-01-01T00:00:00Z")

        """
        self.timestamp = timestamp

    def json(self):
        """Serialize offset to JSON string.

        Returns:
            JSON string representation of the offset

        """
        import json

        return json.dumps({"timestamp": self.timestamp})

    @staticmethod
    def from_json(json_str: str):
        """Deserialize offset from JSON string.

        Args:
            json_str: JSON string containing offset data

        Returns:
            AzureMonitorOffset instance

        """
        import json

        data = json.loads(json_str)
        return AzureMonitorOffset(data["timestamp"])


class AzureMonitorStreamReader(AzureMonitorReader, DataSourceStreamReader):
    """Stream reader for Azure Monitor / Log Analytics workspaces.

    Implements incremental streaming by tracking time-based offsets and splitting
    time ranges into partitions for parallel processing.
    """

    def __init__(self, options, schema: StructType):
        """Initialize the stream reader with options and schema.

        Args:
            options: Dictionary of options containing workspace_id, query, start_time, credentials
            schema: StructType schema (provided by DataSource.schema())

        """
        super().__init__(options, schema)

        # Stream-specific options
        start_time = options.get("start_time", "latest")
        # Support 'latest' as alias for current timestamp
        if start_time == "latest":
            from datetime import datetime, timezone

            self.start_time = datetime.now(timezone.utc).isoformat()
        else:
            # Validate that start_time is a valid ISO 8601 timestamp
            from datetime import datetime

            try:
                datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                self.start_time = start_time
            except (ValueError, AttributeError) as e:
                raise ValueError(
                    f"Invalid start_time format: {start_time}. Expected ISO 8601 format (e.g., '2024-01-01T00:00:00Z')"
                ) from e

        # Partition duration in seconds (default 1 hour)
        self.partition_duration = int(options.get("partition_duration", "3600"))

    def initialOffset(self):
        """Return the initial offset (start time minus 1 microsecond).

        The offset is adjusted by -1 microsecond to compensate for the +1 microsecond
        added in partitions() method. This prevents overlap between consecutive batches.

        Returns:
            JSON string representation of AzureMonitorOffset with the adjusted start time

        """
        from datetime import datetime, timedelta

        # Parse the start time and subtract 1 microsecond
        # This compensates for the +1µs added in partitions() to prevent batch overlap
        start_dt = datetime.fromisoformat(self.start_time.replace("Z", "+00:00"))
        adjusted_start = start_dt - timedelta(microseconds=1)
        return AzureMonitorOffset(adjusted_start.isoformat()).json()

    def latestOffset(self):
        """Return the latest offset (current time).

        Returns:
            JSON string representation of AzureMonitorOffset with the current UTC timestamp

        """
        from datetime import datetime, timezone

        current_time = datetime.now(timezone.utc).isoformat()
        return AzureMonitorOffset(current_time).json()

    def partitions(self, start, end):
        """Create partitions for the time range between start and end offsets.

        Splits the time range into fixed-duration partitions based on partition_duration.

        Args:
            start: JSON string representing AzureMonitorOffset for the start of the range
            end: JSON string representing AzureMonitorOffset for the end of the range

        Returns:
            List of TimeRangePartition objects

        """
        from datetime import datetime, timedelta

        # Deserialize JSON strings to offset objects
        start_offset = AzureMonitorOffset.from_json(start)
        end_offset = AzureMonitorOffset.from_json(end)

        # Parse timestamps
        start_time = datetime.fromisoformat(start_offset.timestamp.replace("Z", "+00:00"))
        end_time = datetime.fromisoformat(end_offset.timestamp.replace("Z", "+00:00"))

        # Add 1 microsecond to start to prevent overlap with previous batch's end
        # This works with the -1µs adjustment in initialOffset() to ensure:
        # - Initial batch: (start - 1µs) + 1µs = start (correct original start)
        # - Subsequent batches: previous_end + 1µs (no overlap with previous batch)
        start_time = start_time + timedelta(microseconds=1)

        # Calculate total duration
        total_duration = (end_time - start_time).total_seconds()

        # If total duration is less than partition_duration, create a single partition
        if total_duration <= self.partition_duration:
            return [TimeRangePartition(start_time, end_time)]

        # Split into fixed-duration partitions
        partitions = []
        current_start = start_time
        partition_delta = timedelta(seconds=self.partition_duration)

        while current_start < end_time:
            current_end = min(current_start + partition_delta, end_time)
            partitions.append(TimeRangePartition(current_start, current_end))
            # Next partition starts 1 microsecond after current partition ends to avoid overlap
            current_start = current_end + timedelta(microseconds=1)

        return partitions

    def commit(self, end):
        """Called when a batch is successfully processed.

        Args:
            end: AzureMonitorOffset representing the end of the committed batch

        """
        # Nothing special needed - Spark handles checkpointing
        pass


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
        """
        pass

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """Receives a sequence of :class:`WriterCommitMessage` from successful tasks when some other tasks have failed, then decides what to do with it.
        """
        pass
