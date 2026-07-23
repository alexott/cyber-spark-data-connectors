import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import cast

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


def _get_credential_from_options(
    tenant_id=None,
    client_id=None,
    client_secret=None,
    databricks_credential=None,
    azure_default_credential=False,
    authority=None,
):
    """Get Azure credential based on provided options.

    Supports three authentication methods (in order of precedence):
    1. Databricks Unity Catalog service credential (if databricks_credential is specified)
    2. Azure DefaultAzureCredential (if azure_default_credential is True)
    3. Azure Service Principal (if tenant_id, client_id, client_secret are provided)

    Args:
        tenant_id: Azure tenant ID (for Service Principal auth)
        client_id: Azure service principal client ID (for Service Principal auth)
        client_secret: Azure service principal client secret (for Service Principal auth)
        databricks_credential: Name of Unity Catalog service credential to use
        azure_default_credential: If True, use DefaultAzureCredential
        authority: Optional authority host URL for sovereign clouds

    Returns:
        Azure credential object

    Raises:
        AssertionError: If no valid authentication method is configured

    """
    # Priority 1: Databricks Unity Catalog service credential
    if databricks_credential:
        try:
            import databricks.service_credentials as service_credentials  # type: ignore[import-not-found]
        except ImportError as err:
            raise ImportError("databricks.service_credentials is required for databricks_credential auth") from err

        return service_credentials.getServiceCredentialsProvider(databricks_credential)

    # Priority 2: Azure DefaultAzureCredential (for managed identity, etc.)
    if azure_default_credential:
        from azure.identity import DefaultAzureCredential

        if authority:
            return DefaultAzureCredential(authority=authority)
        return DefaultAzureCredential()

    # Priority 3: Service Principal (requires all three parameters)
    assert tenant_id is not None, (
        "tenant_id is required when not using databricks_credential or azure_default_credential"
    )
    assert client_id is not None, (
        "client_id is required when not using databricks_credential or azure_default_credential"
    )
    assert client_secret is not None, (
        "client_secret is required when not using databricks_credential or azure_default_credential"
    )

    return _create_azure_credential(tenant_id, client_id, client_secret, authority=authority)


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
        if start_time.lower() in ("earliest", "latest"):
            raise ValueError(
                f"start_time='{start_time}' is only supported for streaming reads. "
                "For batch reads, provide an ISO 8601 timestamp (e.g. '2024-01-01T00:00:00Z') "
                "or a 'timespan' (e.g. 'P1D')."
            )
        start_time_val = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        if end_time:
            end_time_val = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        else:
            end_time_val = datetime.now(timezone.utc)
        return (start_time_val, end_time_val)
    else:
        raise Exception("Either 'timespan' or 'start_time' must be provided")


def _dedupe_column_names(columns):
    """Rename columns that collide case-insensitively so they become unique.

    Spark/Delta column names must be unique case-insensitively, but Log Analytics can
    return columns that differ only in case (e.g. "EventTimestamp_s" and
    "eventTimestamp_s") - common in catch-all tables like AzureDiagnostics. The first
    occurrence keeps its name; later collisions get a numeric suffix (_2, _3, ...),
    skipping any suffix that would itself collide. The result depends only on the input
    order, so schema inference and reads produce identical names.

    Args:
        columns: Ordered list of column name strings

    Returns:
        list[str]: Column names unique when compared case-insensitively

    """
    seen: set[str] = set()
    result = []
    for name in columns:
        candidate = name
        if candidate.lower() in seen:
            i = 2
            while f"{name}_{i}".lower() in seen:
                i += 1
            candidate = f"{name}_{i}"
        seen.add(candidate.lower())
        result.append(candidate)
    return result


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
    query,
    timespan,
    tenant_id=None,
    client_id=None,
    client_secret=None,
    workspace_id=None,
    resource_id=None,
    max_retries=5,
    initial_backoff=1.0,
    azure_cloud=None,
    databricks_credential=None,
    azure_default_credential=False,
):
    """Execute a KQL query against Azure Monitor workspace or resource.

    Supports querying both Log Analytics workspaces and Azure resources directly.
    Includes retry logic with exponential backoff for HTTP 429 throttling errors.

    Args:
        query: KQL query to execute (could be just a table name)
        timespan: Time range as tuple (start_time, end_time)
        tenant_id: Azure tenant ID (for Service Principal auth)
        client_id: Azure service principal client ID (for Service Principal auth)
        client_secret: Azure service principal client secret (for Service Principal auth)
        workspace_id: Log Analytics workspace ID (mutually exclusive with resource_id)
        resource_id: Azure resource ID (mutually exclusive with workspace_id)
        max_retries: Maximum number of retry attempts for throttling (default: 5)
        initial_backoff: Initial backoff time in seconds (default: 1.0)
        azure_cloud: Azure cloud environment - "public" (default), "government", or "china"
        databricks_credential: Name of Unity Catalog service credential to use
        azure_default_credential: If True, use Azure DefaultAzureCredential

    Returns:
        Query response object from Azure Monitor (may have PARTIAL status)

    Raises:
        ValueError: If both or neither workspace_id and resource_id are provided
        HttpResponseError: If non-retryable HTTP error occurs after all retries

    """
    from azure.core.exceptions import HttpResponseError
    from azure.monitor.query import LogsQueryClient

    # Validate that exactly one target is provided
    if workspace_id and resource_id:
        raise ValueError("Cannot specify both workspace_id and resource_id")
    if not workspace_id and not resource_id:
        raise ValueError("Must specify either workspace_id or resource_id")

    # Get cloud-specific configuration (authority and endpoint)
    authority, endpoint = _get_azure_cloud_config(azure_cloud)

    # Create authenticated client using appropriate credential method
    credential = _get_credential_from_options(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        databricks_credential=databricks_credential,
        azure_default_credential=azure_default_credential,
        authority=authority,
    )

    # Create LogsQueryClient with cloud-specific endpoint
    if endpoint:
        client = LogsQueryClient(credential, endpoint=endpoint)
    else:
        client = LogsQueryClient(credential)

    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            # Execute query using appropriate method
            if workspace_id:
                response = client.query_workspace(
                    workspace_id=workspace_id,
                    query=query,
                    timespan=timespan,
                    include_statistics=False,
                    include_visualization=False,
                )
            else:
                response = client.query_resource(
                    resource_id=resource_id,
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
        raise ValueError(
            f"Failed to convert value '{value}' (type: {type(value).__name__}) to {spark_type}: {e}"
        ) from e


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

    Read options (batch and streaming):
    - workspace_id: Log Analytics workspace ID (mutually exclusive with resource_id)
    - resource_id: Azure resource ID for direct resource query (mutually exclusive with workspace_id)
                   Format: /subscriptions/{id}/resourceGroups/{rg}/providers/{provider}/{type}/{name}
    - query: KQL query to execute (could be just a table name)
    - tenant_id: Azure tenant ID
    - client_id: Azure service principal ID
    - client_secret: Azure service principal client secret
    - azure_cloud: Azure cloud environment - "public" (default), "government", or "china".
                   Automatically configures authentication authority and Log Analytics endpoint.
    - max_retries: Maximum retry attempts for throttling (default: 5)
    - initial_backoff: Initial backoff time in seconds for retries (default: 1.0)
    - min_partition_seconds: Minimum partition duration for subdivision (default: 60)

    Batch read options:
    - timespan: Time range for query in ISO 8601 duration format
    - start_time: ISO 8601 datetime string (e.g., "2024-01-01T00:00:00Z")
    - end_time: ISO 8601 datetime string (optional, defaults to now)
    - num_partitions: Number of time-based partitions for parallel reading (default: 1)

    Streaming read options:
    - start_time: Starting timestamp for streaming. Supports three formats:
        * "latest" (default): Start from current time
        * "earliest": Automatically detect earliest timestamp in data
        * ISO 8601 timestamp string: Explicit start time
    - partition_duration: Duration of each partition in seconds (default: 3600 = 1 hour)
    - timestamp_column: Column name for timestamp when using "earliest" (default: "TimeGenerated")

    Potential issues with "earliest" option:
    - Performance: For very large tables without time-based indexes, the min(timestamp_column)
      query may be slow. This is a one-time cost during stream initialization.
    - Aggregated queries: If the query contains aggregations (e.g., | summarize), "earliest"
      will find the minimum timestamp from the aggregated results, not from raw data.
    - Empty tables: If the table has no data, falls back to current timestamp.
    """

    def __init__(self, options):
        """Initialize AzureMonitorDataSource with options.

        Extracts authentication options. Validation happens lazily when auth is needed.
        Extend this method to add new auth methods.

        Args:
            options: Dictionary of options from Spark

        """
        super().__init__(options)

        # Extract authentication options (centralized for easier extension)
        # Validation is deferred to _validate_auth() when auth is actually needed
        # 1. Databricks Unity Catalog service credential
        self.databricks_credential = self.options.get("databricks_credential")
        # 2. Azure DefaultAzureCredential (for managed identity, attached credential, etc.)
        self.azure_default_credential = self.options.get("azure_default_credential", "false").lower() == "true"
        # 3. Service Principal credentials
        self.tenant_id = self.options.get("tenant_id")
        self.client_id = self.options.get("client_id")
        self.client_secret = self.options.get("client_secret")
        self.azure_cloud = self.options.get("azure_cloud", "public")

    def _validate_auth(self):
        """Validate that authentication options are present and non-empty.

        Called by methods that require authentication.

        Raises:
            AssertionError: If required auth options are missing or empty

        """
        # Validate authentication: one of the three methods must be configured
        has_sp_auth = self.tenant_id and self.client_id and self.client_secret
        has_databricks_credential = bool(self.databricks_credential)
        has_default_credential = self.azure_default_credential

        if not (has_sp_auth or has_databricks_credential or has_default_credential):
            raise AssertionError(
                "Authentication required: provide either 'databricks_credential', "
                "'azure_default_credential=true', or all of 'tenant_id', 'client_id', 'client_secret'"
            )

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

    def _infer_schema_from_query(
        self, workspace_id, query, timespan_value, tenant_id, client_id, client_secret, azure_cloud
    ):
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
        from azure.monitor.query import LogsQueryStatus
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

        response = _execute_logs_query(
            query=query,
            timespan=timespan_value,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            workspace_id=workspace_id,
            azure_cloud=azure_cloud,
            databricks_credential=self.databricks_credential,
            azure_default_credential=self.azure_default_credential,
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

        # Resolve column names, de-duplicating case collisions if requested (Log Analytics
        # allows names differing only by case; Delta requires them unique).
        column_names = [str(col) for col in table.columns]
        if self.options.get("deduplicate_column_case", "false").lower() == "true":
            column_names = _dedupe_column_names(column_names)

        # Check if we have any rows to infer types from
        if not table.rows or len(table.rows) == 0:
            # No data to infer types from, use string type for all columns
            fields = [StructField(name, StringType(), nullable=True) for name in column_names]
            return StructType(fields)

        # Infer schema from actual data in the first row
        first_row = table.rows[0]
        fields = []

        for i, column_name in enumerate(column_names):
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

    def _infer_schema_from_resource_query(
        self, resource_id, query, timespan_value, tenant_id, client_id, client_secret, azure_cloud
    ):
        """Helper method to infer schema by executing a resource query and analyzing the first row.

        Args:
            resource_id: Azure resource ID
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
        from azure.monitor.query import LogsQueryStatus
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

        # Execute query against resource
        response = _execute_logs_query(
            query=query,
            timespan=timespan_value,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            resource_id=resource_id,
            azure_cloud=azure_cloud,
            databricks_credential=self.databricks_credential,
            azure_default_credential=self.azure_default_credential,
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

        # Resolve column names, de-duplicating case collisions if requested (Log Analytics
        # allows names differing only by case; Delta requires them unique).
        column_names = [str(col) for col in table.columns]
        if self.options.get("deduplicate_column_case", "false").lower() == "true":
            column_names = _dedupe_column_names(column_names)

        # Check if we have any rows to infer types from
        if not table.rows or len(table.rows) == 0:
            # No data to infer types from, use string type for all columns
            fields = [StructField(name, StringType(), nullable=True) for name in column_names]
            return StructType(fields)

        # Infer schema from actual data in the first row
        first_row = table.rows[0]
        fields = []

        for i, column_name in enumerate(column_names):
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

        Supports both workspace and resource queries.

        Returns:
            StructType: The inferred schema

        Raises:
            Exception: If query returns no results or fails

        """
        # Validate auth options
        self._validate_auth()

        # Get and validate read-specific options
        workspace_id = self.options.get("workspace_id")
        resource_id = self.options.get("resource_id")
        query = self.options.get("query")
        timespan = self.options.get("timespan")
        start_time = self.options.get("start_time")
        end_time = self.options.get("end_time")

        # Validate that exactly one of workspace_id or resource_id is provided
        if workspace_id and resource_id:
            raise ValueError("Cannot specify both workspace_id and resource_id. Use one or the other.")
        if not workspace_id and not resource_id:
            raise ValueError("Must specify either workspace_id or resource_id")

        assert query, "query is required"

        # Parse time range using module-level function.
        # 'earliest'/'latest' are streaming aliases, not real timestamps; for schema
        # inference we only need one sample row, so query all data (no time restriction).
        if start_time and start_time.lower() in ("earliest", "latest") and not timespan:
            timespan_value = None
        else:
            timespan_value = _parse_time_range(timespan=timespan, start_time=start_time, end_time=end_time)

        # Modify query to limit results to 1 row
        sample_query = query.strip()
        if not any(keyword in sample_query.lower() for keyword in ["| take ", "| limit "]):
            sample_query = f"{sample_query} | take 1"

        # Use appropriate helper method based on query target
        if workspace_id:
            return self._infer_schema_from_query(
                workspace_id=workspace_id,
                query=sample_query,
                timespan_value=timespan_value,
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
                azure_cloud=self.azure_cloud,
            )
        else:
            return self._infer_schema_from_resource_query(
                resource_id=resource_id,
                query=sample_query,
                timespan_value=timespan_value,
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
                azure_cloud=self.azure_cloud,
            )

    def list_tables(self):
        """List all tables in the Log Analytics workspace.

        Returns:
            list[str]: List of table names sorted alphabetically

        Raises:
            Exception: If workspace_id is not provided, or if query fails

        """
        # Validate auth options
        self._validate_auth()

        # Get and validate workspace_id
        workspace_id = self.options.get("workspace_id")
        assert workspace_id, "workspace_id is required"

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
            query=list_tables_query,
            timespan=None,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            workspace_id=workspace_id,
            azure_cloud=self.azure_cloud,
            databricks_credential=self.databricks_credential,
            azure_default_credential=self.azure_default_credential,
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
            Exception: If workspace_id is not provided, or if query fails
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
            raise ValueError("table_name contains invalid characters (allowed: letters, digits, underscore, hyphen).")

        # Validate auth options
        self._validate_auth()

        # Get and validate workspace_id
        workspace_id = self.options.get("workspace_id")
        assert workspace_id, "workspace_id is required"

        # Create query to get one row from the table
        sample_query = f"{table_name} | take 1"

        # Use helper method to infer schema without timespan restriction
        # (None timespan allows querying all available data)
        try:
            return self._infer_schema_from_query(
                workspace_id=workspace_id,
                query=sample_query,
                timespan_value=None,
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
                azure_cloud=self.azure_cloud,
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
    """Base reader class for Azure Monitor / Log Analytics workspaces and direct resource queries.

    Shared read logic for batch and streaming reads.
    Supports both workspace-based queries (via workspace_id) and direct resource queries (via resource_id).
    """

    def __init__(self, options, schema: StructType):
        """Initialize the reader with options and schema.

        Args:
            options: Dictionary of options containing workspace_id/resource_id, query, credentials
            schema: StructType schema (provided by DataSource.schema())

        """
        # Authentication options (three methods supported)
        # 1. Databricks Unity Catalog service credential
        self.databricks_credential = options.get("databricks_credential")
        # 2. Azure DefaultAzureCredential (for managed identity, attached credential, etc.)
        self.azure_default_credential = options.get("azure_default_credential", "false").lower() == "true"
        # 3. Service Principal credentials
        self.tenant_id = options.get("tenant_id")
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")
        self.azure_cloud = options.get("azure_cloud", "public")

        # Validate authentication: one of the three methods must be configured
        has_sp_auth = self.tenant_id and self.client_id and self.client_secret
        has_databricks_credential = bool(self.databricks_credential)
        has_default_credential = self.azure_default_credential

        if not (has_sp_auth or has_databricks_credential or has_default_credential):
            raise AssertionError(
                "Authentication required: provide either 'databricks_credential', "
                "'azure_default_credential=true', or all of 'tenant_id', 'client_id', 'client_secret'"
            )

        # Extract and validate read-specific options
        self.workspace_id = options.get("workspace_id")
        self.resource_id = options.get("resource_id")
        self.query = options.get("query")

        # Validate that exactly one of workspace_id or resource_id is provided
        if self.workspace_id and self.resource_id:
            raise ValueError("Cannot specify both workspace_id and resource_id. Use one or the other.")
        if not self.workspace_id and not self.resource_id:
            raise ValueError("Must specify either workspace_id or resource_id")

        assert self.query, "query is required"

        # Retry and subdivision options
        self.max_retries = int(options.get("max_retries", "5"))
        self.initial_backoff = float(options.get("initial_backoff", "1.0"))
        self.min_partition_seconds = int(options.get("min_partition_seconds", "60"))
        # RC2: Azure Monitor caps a query at ~500k rows and may return a truncated result
        # with SUCCESS status. A partition returning this many rows is assumed truncated
        # and is subdivided rather than trusted as complete.
        self.result_size_limit = int(options.get("result_size_limit", "500000"))
        # Rename columns that collide case-insensitively (Log Analytics allows them, Delta
        # does not). Applied consistently in schema inference and read. Default off.
        self.deduplicate_column_case = options.get("deduplicate_column_case", "false").lower() == "true"

        # Column carrying the event timestamp, used by the half-open read boundary.
        self.timestamp_column = options.get("timestamp_column", "TimeGenerated")
        # When True, read() bounds each partition with an explicit half-open KQL filter
        # (col >= start AND col < end) and passes timespan=None, instead of Azure's
        # inclusive/inclusive timespan. This makes adjacent partitions contiguous with no
        # sub-microsecond gaps or tick-precision truncation, so every row is read exactly
        # once even when many rows share a timestamp. Enabled for streaming reads.
        self.half_open_boundaries = False

        # Store schema (provided by DataSource.schema())
        self._schema = schema

    def read(self, partition: TimeRangePartition):
        """Read data for the given partition time range.

        Handles throttling with retries and large result sets by subdividing time ranges.
        Automatically uses the correct query method based on workspace_id or resource_id.

        Args:
            partition: TimeRangePartition containing start_time and end_time

        Yields:
            Row objects from the query results

        """
        # Import inside method for partition-level execution
        from azure.monitor.query import LogsQueryStatus

        # Determine how to bound the partition's time range.
        if self.half_open_boundaries:
            # Half-open [start, end) filter injected into the query; no timespan.
            # Contiguous with adjacent partitions, tick-precise, counts each row once.
            query = (
                f"{self.query} | where {self.timestamp_column} >= datetime({partition.start_time.isoformat()}) "
                f"and {self.timestamp_column} < datetime({partition.end_time.isoformat()})"
            )
            timespan_value = None
        else:
            # Legacy: rely on Azure's inclusive/inclusive timespan.
            query = self.query
            timespan_value = (partition.start_time, partition.end_time)

        # Execute query using unified function (handles both workspace and resource)
        response = _execute_logs_query(
            query=query,
            timespan=timespan_value,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            workspace_id=self.workspace_id,
            resource_id=self.resource_id,
            max_retries=self.max_retries,
            initial_backoff=self.initial_backoff,
            azure_cloud=self.azure_cloud,
            databricks_credential=self.databricks_credential,
            azure_default_credential=self.azure_default_credential,
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

        # RC2: a SUCCESS response at the row cap is silently truncated - Azure returns at
        # most result_size_limit rows without a size-limit error. Subdivide instead of
        # trusting it as complete (subdivision is safe even if the count was exact).
        total_rows = sum(len(table.rows) for table in response.tables)
        if total_rows >= self.result_size_limit:
            yield from self._read_with_subdivision(partition)
            return

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
            # Column names (real API returns strings; test mocks may use objects with .name)
            column_names = [str(col) if isinstance(col, str) else str(col.name) for col in table.columns]
            # Apply the same case de-duplication used during schema inference so row keys
            # match the schema field names.
            if self.deduplicate_column_case:
                column_names = _dedupe_column_names(column_names)

            # Convert Azure Monitor rows to Spark Rows
            for row_idx, row_data in enumerate(table.rows):
                row_dict = {}

                # First, process columns from the query results
                for i, column_name in enumerate(column_names):
                    raw_value = row_data[i]

                    # If column is in schema, convert to expected type
                    if column_name in schema_field_map:
                        expected_type = schema_field_map[column_name]
                        try:
                            converted_value = _convert_value_to_schema_type(raw_value, expected_type)
                            row_dict[column_name] = converted_value
                        except ValueError as e:
                            raise ValueError(f"Row {row_idx}, column '{column_name}': {e}") from e
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

    def read(self, partition):
        return super().read(cast(TimeRangePartition, partition))


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

    Stream-specific options:
    - start_time: Starting timestamp for streaming. Supports three formats:
        * "latest" (default): Start from current time
        * "earliest": Automatically detect earliest timestamp in data (executes query during init)
        * ISO 8601 timestamp string (e.g., "2024-01-01T00:00:00Z"): Explicit start time
    - partition_duration: Duration of each partition in seconds (default: 3600 = 1 hour)
    - timestamp_column: Column name for timestamp detection when using "earliest" (default: "TimeGenerated")

    Note: When using start_time="earliest", a query is executed during initialization to find
    the minimum timestamp in the data. This is a one-time cost but may be slow for very large tables.
    """

    def __init__(self, options, schema: StructType):
        """Initialize the stream reader with options and schema.

        Args:
            options: Dictionary of options containing workspace_id, query, start_time, credentials
            schema: StructType schema (provided by DataSource.schema())

        """
        super().__init__(options, schema)

        # Stream-specific options
        # Timestamp column for earliest detection (default: TimeGenerated)
        self.timestamp_column = options.get("timestamp_column", "TimeGenerated")

        start_time = options.get("start_time", "latest")
        # Support 'latest' as alias for current timestamp
        if start_time == "latest":
            from datetime import datetime, timezone

            self.start_time = datetime.now(timezone.utc).isoformat()
        elif start_time == "earliest":
            # Query to find the earliest timestamp in the data
            # Note: This executes a query during initialization (one-time cost)
            self.start_time = self._get_earliest_timestamp()
        else:
            # Validate that start_time is a valid ISO 8601 timestamp
            from datetime import datetime

            try:
                datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                self.start_time = start_time
            except (ValueError, AttributeError) as e:
                raise ValueError(
                    f"Invalid start_time format: {start_time}. Expected ISO 8601 format "
                    "(e.g., '2024-01-01T00:00:00Z') or 'latest' or 'earliest'"
                ) from e

        # Partition duration in seconds (default 1 hour)
        self.partition_duration = int(options.get("partition_duration", "3600"))

        # RC1: lag behind "now" to allow for Log Analytics ingestion latency.
        # Data with TimeGenerated inside the lag window may not be queryable yet;
        # querying it too early skips it permanently. Default 0 (backward compatible).
        self.safety_lag_seconds = int(options.get("safety_lag_seconds", "0"))
        # RC5: maximum event-time span a single micro-batch may advance. A large
        # backlog (initial catch-up, or recovery after downtime) is drained across
        # many completable batches instead of one oversized batch that OOMs.
        # 0 disables bounding.
        self.max_catchup_seconds = int(options.get("max_catchup_seconds", "3600"))
        # Last planned offset timestamp (ISO str), used to bound the catch-up window.
        self._current_offset = None

        # Streaming reads use half-open [start, end) boundaries so consecutive micro-batches
        # and their partitions are contiguous with no gaps or overlaps (see read()).
        self.half_open_boundaries = True

    def _get_earliest_timestamp(self):
        """Query to find the earliest timestamp in the data.

        Executes a KQL query to find the minimum value of the timestamp column
        in the dataset. This is used when start_time="earliest" is specified.

        Returns:
            str: ISO 8601 formatted timestamp of the earliest data point

        Raises:
            Exception: If query execution fails

        Note:
            - If the query returns no data (empty table), falls back to current time
            - Uses the timestamp_column option (default: "TimeGenerated")
            - Executes with timespan=None to query all available data
            - For tables with existing aggregations in the query, this finds the
              earliest timestamp from the aggregated results, not raw data

        """
        from datetime import datetime, timezone

        from azure.monitor.query import LogsQueryStatus

        # Construct query to find earliest timestamp
        # Format: {original_query} | summarize earliest=min({timestamp_column})
        earliest_query = f"{self.query} | summarize earliest=min({self.timestamp_column})"

        # Execute query without timespan restriction to find absolute earliest
        response = _execute_logs_query(
            query=earliest_query,
            timespan=None,  # No time restriction
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            workspace_id=self.workspace_id,
            resource_id=self.resource_id,
            max_retries=self.max_retries,
            initial_backoff=self.initial_backoff,
            azure_cloud=self.azure_cloud,
            databricks_credential=self.databricks_credential,
            azure_default_credential=self.azure_default_credential,
        )

        # Check query status
        if response.status != LogsQueryStatus.SUCCESS:
            # If query fails, fallback to current time
            return datetime.now(timezone.utc).isoformat()

        # Extract the earliest timestamp from response
        if response.tables and len(response.tables) > 0:
            table = response.tables[0]
            if table.rows and len(table.rows) > 0:
                earliest_value = table.rows[0][0]
                if earliest_value is not None:
                    # Handle both datetime objects and string values
                    if isinstance(earliest_value, datetime):
                        return earliest_value.isoformat()
                    elif isinstance(earliest_value, str):
                        # Validate and normalize the timestamp
                        try:
                            dt = datetime.fromisoformat(earliest_value.replace("Z", "+00:00"))
                            return dt.isoformat()
                        except (ValueError, AttributeError):
                            pass

        # Fallback if no data found or invalid timestamp - use current time
        return datetime.now(timezone.utc).isoformat()

    def initialOffset(self):
        """Return the initial offset (the configured start time, unadjusted).

        With half-open [start, end) partition boundaries no microsecond fudging is
        needed: the start is inclusive, so the first batch begins exactly at start_time.

        Returns:
            JSON string representation of AzureMonitorOffset with the start time

        """
        from datetime import datetime

        start_dt = datetime.fromisoformat(self.start_time.replace("Z", "+00:00"))
        # Seed the tracked offset so the first micro-batch's catch-up window is bounded (RC5)
        self._current_offset = start_dt.isoformat()
        return AzureMonitorOffset(start_dt.isoformat()).json()

    def latestOffset(self):
        """Return the latest offset to read up to.

        Applies two safeguards:
        - RC1: stays ``safety_lag_seconds`` behind "now" so not-yet-ingested data is
          not skipped.
        - RC5: advances at most ``max_catchup_seconds`` beyond the last planned offset,
          so a large backlog is drained across many bounded micro-batches instead of one
          oversized batch. (On a fresh restart the committed offset is unknown until the
          first ``partitions()`` call, so that first batch is not bounded here - use
          ``Trigger.AvailableNow`` for large backfills.)

        Returns:
            JSON string representation of AzureMonitorOffset

        """
        from datetime import datetime, timedelta, timezone

        end_time = datetime.now(timezone.utc) - timedelta(seconds=self.safety_lag_seconds)

        if self._current_offset is not None:
            base = datetime.fromisoformat(self._current_offset.replace("Z", "+00:00"))
            if self.max_catchup_seconds > 0:
                capped = base + timedelta(seconds=self.max_catchup_seconds)
                if capped < end_time:
                    end_time = capped
            # Never move the offset backwards (e.g. large safety lag near stream start)
            if end_time < base:
                end_time = base

        return AzureMonitorOffset(end_time.isoformat()).json()

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

        # Track the planned end so the next latestOffset() bounds its catch-up window (RC5)
        self._current_offset = end_offset.timestamp

        # Parse timestamps. Boundaries are half-open [start, end): the start is inclusive
        # and the end exclusive (see read()), so consecutive batches and partitions are
        # contiguous with no microsecond adjustments and no gaps or overlaps.
        start_time = datetime.fromisoformat(start_offset.timestamp.replace("Z", "+00:00"))
        end_time = datetime.fromisoformat(end_offset.timestamp.replace("Z", "+00:00"))

        # Calculate total duration
        total_duration = (end_time - start_time).total_seconds()

        # If total duration is less than partition_duration, create a single partition
        if total_duration <= self.partition_duration:
            return [TimeRangePartition(start_time, end_time)]

        # Split into fixed-duration partitions. Each partition ends exactly where the next
        # begins; the half-open comparison keeps a boundary row in exactly one partition.
        partitions = []
        current_start = start_time
        partition_delta = timedelta(seconds=self.partition_duration)

        while current_start < end_time:
            current_end = min(current_start + partition_delta, end_time)
            partitions.append(TimeRangePartition(current_start, current_end))
            current_start = current_end

        return partitions

    def read(self, partition):
        return super().read(cast(TimeRangePartition, partition))

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

        # Authentication options (three methods supported)
        # 1. Databricks Unity Catalog service credential
        self.databricks_credential = self.options.get("databricks_credential")
        # 2. Azure DefaultAzureCredential (for managed identity, attached credential, etc.)
        self.azure_default_credential = self.options.get("azure_default_credential", "false").lower() == "true"
        # 3. Service Principal credentials
        self.tenant_id = self.options.get("tenant_id")
        self.client_id = self.options.get("client_id")
        self.client_secret = self.options.get("client_secret")

        # Azure cloud environment: "public" (default), "government", or "china"
        self.azure_cloud = self.options.get("azure_cloud", "public")

        # Validate authentication: one of the three methods must be configured
        has_sp_auth = self.tenant_id and self.client_id and self.client_secret
        has_databricks_credential = bool(self.databricks_credential)
        has_default_credential = self.azure_default_credential

        if not (has_sp_auth or has_databricks_credential or has_default_credential):
            raise AssertionError(
                "Authentication required: provide either 'databricks_credential', "
                "'azure_default_credential=true', or all of 'tenant_id', 'client_id', 'client_secret'"
            )

        # Extract and validate write-specific options
        self.dce = self.options.get("dce")  # data_collection_endpoint
        self.dcr_id = self.options.get("dcr_id")  # data_collection_rule_id
        self.dcs = self.options.get("dcs")  # data_collection_stream
        self.batch_size = int(self.options.get("batch_size", "50"))

        assert self.dce, "dce (data collection endpoint) is required"
        assert self.dcr_id, "dcr_id (data collection rule ID) is required"
        assert self.dcs, "dcs (data collection stream) is required"

    def _send_to_sentinel(self, s: LogsIngestionClient, msgs: list):
        if len(msgs) > 0:
            # TODO: add retries
            s.upload(rule_id=self.dcr_id, stream_name=self.dcs, logs=msgs)

    def write(self, iterator):
        """Write the data and return the commit message for that partition."""
        import json

        from azure.monitor.ingestion import LogsIngestionClient
        from pyspark import TaskContext
        # from azure.core.exceptions import HttpResponseError

        # Get cloud-specific configuration (authority)
        authority, _ = _get_azure_cloud_config(self.azure_cloud)

        # Get credential using the appropriate method
        credential = _get_credential_from_options(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            databricks_credential=self.databricks_credential,
            azure_default_credential=self.azure_default_credential,
            authority=authority,
        )
        logs_client = LogsIngestionClient(self.dce, credential)

        msgs = []

        context = TaskContext.get()
        if context is None:
            raise RuntimeError("TaskContext is not available")
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
        """Handle a successfully written streaming batch."""
        pass

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """Handle a failed streaming batch."""
        pass
