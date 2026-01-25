# Custom data sources/sinks for Cybersecurity-related work

Based on [PySpark DataSource API](https://spark.apache.org/docs/preview/api/python/user_guide/sql/python_data_source.html) available with Spark 4 & [DBR 15.3+](https://docs.databricks.com/en/pyspark/datasources.html).  See [blog post](https://alexott.blogspot.com/2024/11/spark-custom-data-sources-and-sinks-for.html) for more details about implementation.

- [Custom data sources/sinks for Cybersecurity-related work](#custom-data-sourcessinks-for-cybersecurity-related-work)
  - [Available data sources](#available-data-sources)
    - [Splunk data source](#splunk-data-source)
      - [Writing to Splunk](#writing-to-splunk)
      - [Reading from Splunk](#reading-from-splunk)
        - [Batch Read](#batch-read)
        - [Streaming Read](#streaming-read)
    - [Microsoft Sentinel / Azure Monitor](#microsoft-sentinel--azure-monitor)
      - [Authentication Requirements](#authentication-requirements)
      - [Writing to Microsoft Sentinel / Azure Monitor](#writing-to-microsoft-sentinel--azure-monitor)
      - [Reading from Microsoft Sentinel / Azure Monitor](#reading-from-microsoft-sentinel--azure-monitor)
        - [Batch Read](#batch-read-1)
        - [Streaming Read](#streaming-read-1)
    - [Simple REST API](#simple-rest-api)
  - [Installation](#installation)
  - [Building](#building)
  - [References](#references)

## Available data sources

> [!NOTE]
> Most of these data sources/sinks are designed to work with relatively small amounts of data - alerts, etc.  If you need to read or write huge amounts of data, use native export/import functionality of corresponding external system.

### Splunk data source

This data source supports both reading from and writing to [Splunk](https://www.splunk.com/) - both batch and streaming modes. Registered data source name is `splunk`.

#### Writing to Splunk

By default, this data source will put all columns into the `event` object and send it to Splunk together with metadata (`index`, `source`, ...).  This behavior could be changed by providing `single_event_column` option to specify which string column should be used as the single value of `event`.

Batch write usage:

```python
from cyber_connectors import *
spark.dataSource.register(SplunkDataSource)

df = spark.range(10)
df.write.format("splunk").mode("overwrite") \
  .option("url", "http://localhost:8088/services/collector/event") \
  .option("token", "...").save()
```

Streaming write usage:

```python
from cyber_connectors import *
spark.dataSource.register(SplunkDataSource)

dir_name = "tests/samples/json/"
bdf = spark.read.format("json").load(dir_name)  # to infer schema - not use in the prod!

sdf = spark.readStream.format("json").schema(bdf.schema).load(dir_name)

stream_options = {
  "url": "http://localhost:8088/services/collector/event",
  "token": "....",
  "source": "zeek",
  "index": "zeek",
  "host": "my_host",
  "time_column": "ts",
  "checkpointLocation": "/tmp/splunk-checkpoint/"
}
stream = sdf.writeStream.format("splunk") \
  .trigger(availableNow=True) \
  .options(**stream_options).start()
```

Supported write options:

- `url` (string, required) - URL of the Splunk HTTP Event Collector (HEC) endpoint to send data to.  For example, `http://localhost:8088/collector/services/event`.
- `token` (string, required) - HEC token to [authenticate to HEC endpoint](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/FormateventsforHTTPEventCollector#HTTP_authentication).
- `index` (string, optional) - name of the Splunk index to send data to.  If omitted, the default index configured for HEC endpoint is used.
- `source` (string, optional) - the source value to assign to the event data.
- `host` (string, optional) - the host value to assign to the event data.
- `sourcetype` (string, optional, default: `_json`) - the sourcetype value to assign to the event data. 
- `single_event_column` (string, optional) - specify which string column will be used as `event` payload.  Typically this is used to ingest log files content.
- `time_column` (string, optional) - specify which column to use as event time value (the `time` value in Splunk payload).  Supported data types: `timestamp`, `float`, `int`, `long` (`float`/`int`/`long` values are treated as seconds since epoch).  If not specified, current timestamp will be used.
- `indexed_fields` (string, optional) - comma-separated list of string columns to be [indexed in the ingestion time](http://docs.splunk.com/Documentation/Splunk/9.3.1/Data/IFXandHEC).
- `remove_indexed_fields` (boolean, optional, default: `false`) - if indexed fields should be removed from the `event` object.
- `batch_size` (int. optional, default: 50) - the size of the buffer to collect payload before sending to Splunk.

#### Reading from Splunk

The data source supports both batch and streaming reads from Splunk using the [Splunk export API](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTsearch#search.2Fjobs.2Fexport). You can execute SPL (Search Processing Language) queries and bring results into Spark for further analysis.

> [!NOTE]
> This is not real-time event streaming. The export API returns results for bounded time windows; streaming mode implements micro-batch polling by repeatedly querying with advancing time ranges.

**Authentication Requirements:**

Reading from Splunk requires authentication to the Splunk management API (splunkd). You need:

- **Splunk management API URL** (`splunkd_url`): The base URL for Splunk's REST API, typically `https://<splunk-host>:8089`
- **Authentication token** (`splunkd_token`): A Splunk authentication token for API access (NOT the same as HEC token used for writing)
  - Can also be provided via `SPLUNK_AUTH_TOKEN` environment variable

##### Batch Read

Batch read usage with full SPL query:

```python
from cyber_connectors import *
spark.dataSource.register(SplunkDataSource)

# Full SPL query (recommended for complex queries)
read_options = {
    "splunkd_url": "https://splunk.example.com:8089",
    "splunkd_token": "your-splunkd-token",
    "query": "search index=main error | stats count by host",
    "timespan": "P1D",  # Last 1 day
}

df = spark.read.format("splunk") \
    .options(**read_options) \
    .load()

df.show()
```

Batch read with simple parameters (for basic searches):

```python
# Simple parameters mode (for basic searches only)
read_options = {
    "splunkd_url": "https://splunk.example.com:8089",
    "splunkd_token": "your-splunkd-token",
    "index": "security",
    "sourcetype": "firewall",
    "search_filter": "status=200",
    "start_time": "2024-01-01T00:00:00Z",
    "end_time": "2024-01-02T00:00:00Z",
    "num_partitions": "4",  # Parallel reading
}

df = spark.read.format("splunk") \
    .options(**read_options) \
    .load()
```

Supported batch read options:

**Required:**
- `splunkd_url` (string, required) - Splunk management API URL (e.g., `https://splunk.example.com:8089`)
- `splunkd_token` (string, required) - Splunk authentication token (or set `SPLUNK_AUTH_TOKEN` env var)

**Query options (choose one approach):**
- **Full SPL query** (preferred for complex queries):
  - `query` (string) - Complete SPL query (e.g., `"search index=main error | stats count by status"`)
- **Simple parameters** (for basic searches only - cannot use pipes or transforming commands):
  - `index` (string, required if no `query`) - Splunk index name (alphanumeric, underscore, hyphen only)
  - `sourcetype` (string, optional) - Source type filter (alphanumeric, colon, underscore, hyphen)
  - `search_filter` (string, optional) - Additional filters (cannot contain pipes `|` or commands)

**Time range options (choose one approach):**
- `timespan` (string) - ISO 8601 duration (e.g., `"P1D"` = 1 day, `"PT6H"` = 6 hours, `"P7D"` = 7 days)
- OR `start_time` (string) - Start time in ISO 8601 format (e.g., `"2024-01-01T00:00:00Z"`)
  - `end_time` (string, optional) - End time in ISO 8601 format (defaults to current time if not specified)

**Partitioning options:**
- `num_partitions` (int, optional, default: 1) - Number of parallel partitions for reading
- `partition_duration` (int, optional) - Partition duration in seconds (takes precedence over `num_partitions`)

**Schema options:**
- `inferSchema` (bool, optional, default: false) - Infer schema from sample data (queries with `| head 10`)
- `mode` (string, optional, default: `"FAILFAST"`) - Error handling mode:
  - `"FAILFAST"` - Raise exception on first type conversion error
  - `"PERMISSIVE"` - Set field to null on conversion error, continue processing

**Connection options:**
- `auth_scheme` (string, optional, default: `"Splunk"`) - Authorization scheme (`"Splunk"` or `"Bearer"`)
- `verify_ssl` (string/bool, optional, default: `"true"`) - TLS certificate verification:
  - `"true"` - Verify using system CA bundle
  - `"false"` - Disable verification (insecure, not recommended)
  - Path string (e.g., `"/path/to/ca-bundle.crt"`) - Use custom CA bundle
- `connect_timeout` (int, optional, default: 10) - Connect timeout in seconds
- `read_timeout` (int, optional, default: 300) - Read timeout in seconds
- `max_retries` (int, optional, default: 3) - Maximum retry attempts
- `initial_backoff` (float, optional, default: 1.0) - Initial backoff for retries in seconds
- `output_mode` (string, optional, default: `"json"`) - Splunk response format (`"json"` or `"json_rows"`)

**SPL Query Examples:**

```python
# Get recent firewall logs
query = "search index=security sourcetype=firewall | fields _time, src_ip, dest_ip, action"

# Search for errors with stats
query = "search index=main error | stats count by host, source"

# Complex query with multiple commands
query = "search index=web status>=400 | eval hour=strftime(_time, \"%H\") | stats count by hour"
```

**Default Schema:**

If you don't specify a schema, Splunk reader returns a default schema with common Splunk fields:
- `_time` (TimestampType) - Event timestamp
- `_indextime` (TimestampType) - Index timestamp
- `_raw` (StringType) - Raw event data
- `host` (StringType) - Host field
- `source` (StringType) - Source field
- `sourcetype` (StringType) - Sourcetype field
- `index` (StringType) - Index name

For custom fields or typed fields, provide an explicit schema or use `inferSchema=true`.

**Security Notes:**

- Simple parameter mode validates inputs to prevent injection attacks
- For queries with pipes, stats, eval, etc., use the `query` option (simple mode rejects these)
- Tokens are never logged and can be read from environment variables

##### Streaming Read

The data source supports streaming reads (micro-batch polling) from Splunk. The streaming reader uses `_indextime`-based offsets to track progress.

> [!IMPORTANT]
> Streaming mode only works with **event-returning queries** (non-transforming SPL). Transforming commands like `stats`, `chart`, `timechart`, etc. produce aggregated results without `_indextime` and will cause errors.

Streaming read usage:

```python
from cyber_connectors import *
spark.dataSource.register(SplunkDataSource)

stream_options = {
    "splunkd_url": "https://splunk.example.com:8089",
    "splunkd_token": "your-splunkd-token",
    "query": "search index=security sourcetype=firewall",  # Event-returning query only
    "start_time": "latest",  # Start from current time
    "partition_duration": "3600",  # 1 hour partitions
    "safety_lag_seconds": "60",  # 60 second lag for late events
    "checkpointLocation": "/tmp/splunk-stream-checkpoint/",
}

stream_df = spark.readStream.format("splunk") \
    .options(**stream_options) \
    .load()

# Write to console
query = stream_df.writeStream \
    .format("console") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/tmp/splunk-stream-checkpoint/") \
    .start()

query.awaitTermination()
```

Supported streaming read options:

**Required:**
- `splunkd_url` (string, required) - Splunk management API URL
- `splunkd_token` (string, required) - Splunk authentication token
- `query` or `index` (string, required) - SPL query or index name (must be event-returning, no transforming commands)

**Streaming-specific options:**
- `start_time` (string, optional, default: `"latest"`) - Start time:
  - `"latest"` - Start from current time
  - ISO 8601 timestamp (e.g., `"2024-01-01T00:00:00Z"`) - Start from specific time
- `partition_duration` (int, optional, default: 3600) - Partition duration in seconds (controls parallelism)
- `safety_lag_seconds` (int, optional, default: 60) - Lag behind current time for late-arriving events
- `allow_transforming_queries` (bool, optional, default: false) - Allow transforming queries (NOT recommended)

**Important notes for streaming:**

- The reader tracks `_indextime` (when Splunk indexed the event) as offset for reliable progression
- Queries must return raw events with `_indextime` field - transforming commands (stats, chart, etc.) are rejected
- Use `start_time: "latest"` to begin from current time (useful for monitoring new events)
- The `safety_lag_seconds` helps handle late-arriving events that may be indexed after their event time

**Query Validation:**

Streaming mode automatically validates queries and rejects transforming commands:

```python
# ✅ Valid for streaming (event-returning)
query = "search index=main error"
query = "search index=security | fields _time, src_ip, dest_ip"

# ❌ Invalid for streaming (transforming)
query = "search index=main | stats count by host"  # Will raise error
query = "search index=main | chart count by _time"  # Will raise error
```

To bypass validation (not recommended), set `allow_transforming_queries: "true"`.

### Microsoft Sentinel / Azure Monitor

This data source supports both reading from and writing to [Microsoft Sentinel](https://learn.microsoft.com/en-us/azure/sentinel/overview/) / [Azure Monitor Log Analytics](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/log-analytics-overview). Registered data source names are `ms-sentinel` and `azure-monitor`.

#### Authentication Requirements

This connector uses Azure Service Principal Client ID/Secret for authentication.

The service principal needs the following permissions:

- For reading: **Log Analytics Reader** role on the Log Analytics workspace
- For writing: **Monitoring Metrics Publisher** role on the DCE and DCR

Authentication options:

- `tenant_id` (string, required) - Azure Tenant ID
- `client_id` (string, required) - Application ID (client ID) of Azure Service Principal
- `client_secret` (string, required) - Client Secret of Azure Service Principal
- `azure_cloud` (string, optional, default: "public") - Azure cloud environment. Valid values:
  - `"public"` - Azure Public Cloud (default)
  - `"government"` - Azure Government (GovCloud)
  - `"china"` - Azure China (21Vianet)

#### Writing to Microsoft Sentinel / Azure Monitor

The integration uses [Logs Ingestion API of Azure Monitor](https://learn.microsoft.com/en-us/azure/sentinel/create-custom-connector#connect-with-the-log-ingestion-api) for writing data.

To push data you need to create Data Collection Endpoint (DCE), Data Collection Rule (DCR), and create a custom table in Log Analytics workspace.  See [documentation](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview) for description of this process.  The structure of the data in DataFrame should match the structure of the defined custom table.

You need to grant correct permissions (`Monitoring Metrics Publisher`) to the service principal on the DCE and DCR.

Batch write usage:

```python
from cyber_connectors import *
spark.dataSource.register(MicrosoftSentinelDataSource)

sentinel_options = {
    "dce": dc_endpoint,
    "dcr_id": dc_rule_id,
    "dcs": dc_stream_name,
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
  }

df = spark.range(10)
df.write.format("ms-sentinel") \
  .mode("overwrite") \
  .options(**sentinel_options) \
  .save()
```

Streaming write usage:

```python
from cyber_connectors import *
spark.dataSource.register(MicrosoftSentinelDataSource)

dir_name = "tests/samples/json/"
bdf = spark.read.format("json").load(dir_name)  # to infer schema - not use in the prod!

sdf = spark.readStream.format("json").schema(bdf.schema).load(dir_name)

sentinel_stream_options = {
    "dce": dc_endpoint,
    "dcr_id": dc_rule_id,
    "dcs": dc_stream_name,
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
    "checkpointLocation": "/tmp/sentinel-checkpoint/"
}

stream = sdf.writeStream.format("ms-sentinel") \
  .trigger(availableNow=True) \
  .options(**sentinel_stream_options).start()
```

Supported write options:

- `dce` (string, required) - URL of the Data Collection Endpoint.
- `dcr_id` (string, required) - ID of Data Collection Rule.
- `dcs` (string, required) - name of custom table created in the Log Analytics Workspace.
- `batch_size` (int. optional, default: 50) - the size of the buffer to collect payload before sending to MS Sentinel.

#### Reading from Microsoft Sentinel / Azure Monitor

The data source supports both batch and streaming reads from Azure Monitor / Log Analytics workspaces using KQL (Kusto Query Language) queries.  If schema isn't specified with `.schema`, it will be inferred automatically.

> [!NOTE]
> For streaming reads of big amounts of data, it's recommended to export necessary tables to EventHubs, and consume from there.

##### Batch Read

Batch read usage:

```python
from cyber_connectors import *
spark.dataSource.register(AzureMonitorDataSource)

# Option 1: Using timespan (ISO 8601 duration)
read_options = {
    "workspace_id": "your-workspace-id",
    "query": "AzureActivity | where TimeGenerated > ago(1d) | take 100",
    "timespan": "P1D",  # ISO 8601 duration: 1 day
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
}

# Option 2: Using start_time and end_time (ISO 8601 timestamps)
read_options = {
    "workspace_id": "your-workspace-id",
    "query": "AzureActivity | take 100",
    "start_time": "2024-01-01T00:00:00Z",
    "end_time": "2024-01-02T00:00:00Z",
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
}

# Option 3: Using only start_time (end_time defaults to current time)
read_options = {
    "workspace_id": "your-workspace-id",
    "query": "AzureActivity | take 100",
    "start_time": "2024-01-01T00:00:00Z",  # Query from start_time to now
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
}

df = spark.read.format("azure-monitor") \
    .options(**read_options) \
    .load()

df.show()
```

Supported read options:

- `workspace_id` (string, required for workspace queries) - Log Analytics workspace ID (mutually exclusive with `resource_id`)
- `resource_id` (string, required for direct resource queries) - Azure resource ID in format `/subscriptions/{id}/resourceGroups/{rg}/providers/{provider}/{type}/{name}` (mutually exclusive with `workspace_id`)
- `query` (string, required) - KQL query to execute (could be just a table name)
- **Time range options (choose one approach):**
  - `timespan` (string) - Time range in ISO 8601 duration format (e.g., "P1D" = 1 day, "PT1H" = 1 hour, "P7D" = 7 days)
  - `start_time` (string) - Start time in ISO 8601 format (e.g., "2024-01-01T00:00:00Z"). If provided without `end_time`, queries from `start_time` to current time
  - `end_time` (string, optional) - End time in ISO 8601 format. Only valid when `start_time` is specified
  - **Note**: `timespan` and `start_time/end_time` are mutually exclusive - choose one approach
- `num_partitions` (int, optional, default: 1) - Number of partitions for reading data
- `inferSchema` (bool, optional, default: true) - if we do the schema inference by sampling result.
- `max_retries` (int, optional, default: 5) - Maximum retry attempts for HTTP 429 throttling errors
- `initial_backoff` (float, optional, default: 1.0) - Initial backoff time in seconds for retries (uses exponential backoff)
- `min_partition_seconds` (int, optional, default: 60) - Minimum partition duration in seconds when subdividing large result sets

**KQL Query Examples:**

```python
# Get recent Azure Activity logs
query = "AzureActivity | where TimeGenerated > ago(24h) | project TimeGenerated, OperationName, ResourceGroup"

# Get security alerts
query = "SecurityAlert | where TimeGenerated > ago(7d) | project TimeGenerated, AlertName, Severity"

# Custom table query
query = "MyCustomTable_CL | where TimeGenerated > ago(1h)"
```

**Azure Sovereign Clouds:**

For Azure Government or Azure China environments, use the `azure_cloud` option:

```python
# Azure Government (GovCloud)
read_options = {
    "workspace_id": "your-workspace-id",
    "query": "SecurityEvent | take 100",
    "timespan": "P1D",
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
    "azure_cloud": "government",  # Uses login.microsoftonline.us and api.loganalytics.us
}

# Azure China (21Vianet)
read_options = {
    # ... other options ...
    "azure_cloud": "china",  # Uses login.chinacloudapi.cn and api.loganalytics.azure.cn
}
```

**Direct Resource Query (without Log Analytics Workspace):**

You can query logs directly from Azure resources without requiring a Log Analytics workspace. Instead of specifying `workspace_id`, provide the `resource_id` of the resource you want to query:

```python
# Query logs from an Azure Storage Account
read_options = {
    "resource_id": "/subscriptions/{subscription-id}/resourceGroups/{rg-name}/providers/Microsoft.Storage/storageAccounts/{storage-name}",
    "query": "StorageBlobLogs | where TimeGenerated > ago(1d) | take 100",
    "timespan": "P1D",
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
}

# Query logs from Azure Event Grid
read_options = {
    "resource_id": "/subscriptions/{subscription-id}/resourceGroups/{rg-name}/providers/Microsoft.EventGrid/topics/{topic-name}",
    "query": "AzureDiagnostics | take 100",
    "start_time": "2024-01-01T00:00:00Z",
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
}

df = spark.read.format("azure-monitor") \
    .options(**read_options) \
    .load()
```

**Note**: `workspace_id` and `resource_id` are mutually exclusive - use one or the other, not both. All other options (time ranges, partitioning, retries, etc.) work the same way for both workspace and resource queries.

**Automatic Throttling and Large Result Set Handling:**

The connector automatically handles common Azure Monitor query issues:

- **Throttling (HTTP 429)**: When Azure Monitor returns rate limit errors, the connector automatically retries with exponential backoff. Configure with `max_retries` and `initial_backoff` options.

- **Large Result Sets**: When a query exceeds Azure's [result size limits](https://learn.microsoft.com/en-us/azure/azure-monitor/service-limits#query-api) (500,000 records or ~64MB), the connector automatically subdivides the time range into smaller chunks and queries each separately. Configure the minimum subdivision size with `min_partition_seconds`.

##### Streaming Read

The data source supports streaming reads from Azure Monitor / Log Analytics. The streaming reader uses time-based offsets to track progress and splits time ranges into partitions for parallel processing.

**Basic streaming read usage:**

```python
from cyber_connectors import *
spark.dataSource.register(AzureMonitorDataSource)

# Stream from a specific timestamp
stream_options = {
    "workspace_id": "your-workspace-id",
    "query": "AzureActivity | project TimeGenerated, OperationName, ResourceGroup",
    "start_time": "2024-01-01T00:00:00Z",  # Start streaming from this timestamp
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
    "partition_duration": "3600",  # Optional: partition size in seconds (default 1 hour)
}

# Read stream
stream_df = spark.readStream.format("azure-monitor") \
    .options(**stream_options) \
    .load()

# Write to console or another sink
query = stream_df.writeStream \
    .format("console") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/tmp/azure-monitor-checkpoint/") \
    .start()

query.awaitTermination()
```

Supported streaming read options:

- `workspace_id` (string, required) - Log Analytics workspace ID
- `query` (string, required) - KQL query to execute (could be just a table name). Note: *it should not include time filters - these are added automatically!*
- `start_time` (string, optional, default: "latest") - Starting timestamp for streaming. Supports three formats:
  - `"latest"` (default) - Start from current time (monitor new data)
  - `"earliest"` - Automatically detect and start from the earliest timestamp in the data
  - ISO 8601 timestamp (e.g., "2024-01-01T00:00:00Z") - Start from a specific time
- `timestamp_column` (string, optional, default: "TimeGenerated") - Column name to use for timestamp when `start_time="earliest"` is specified
- `partition_duration` (int, optional, default: 3600) - Duration in seconds for each partition (controls parallelism)

**Important notes for streaming:**

- The reader automatically tracks the timestamp of the last processed data in checkpoints
- Time ranges are split into partitions based on `partition_duration` for parallel processing
- The query should NOT include time filters (e.g., `where TimeGenerated > ago(1d)`) - the reader adds these automatically based on offsets
- Use `start_time: "latest"` to begin streaming from the current time (useful for monitoring real-time data)
- Use `start_time: "earliest"` to automatically detect and start from the earliest timestamp in the data

**Start time options:**

The `start_time` option supports three modes:

1. **`"latest"` (default)**: Start streaming from current time - useful for monitoring new events
2. **`"earliest"`: Automatically detect earliest timestamp** - queries the data to find the minimum timestamp and starts from there. Useful for backfilling historical data.
   - Uses `TimeGenerated` column by default
   - Override with `timestamp_column` option for custom timestamp columns
   - **Note**: Executes a query during initialization (one-time cost); may be slow for very large tables
3. **ISO 8601 timestamp** (e.g., `"2024-01-01T00:00:00Z"`): Start from specific time

**Example with "earliest":**

```python
# Start from the earliest data available in the table
stream_options = {
    "workspace_id": "your-workspace-id",
    "query": "SecurityEvent",
    "start_time": "earliest",  # Auto-detect earliest timestamp
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
}

stream_df = spark.readStream.format("azure-monitor") \
    .options(**stream_options) \
    .load()
```

**Example with custom timestamp column:**

```python
# Use custom timestamp column with "earliest"
stream_options = {
    "workspace_id": "your-workspace-id",
    "query": "CustomTable_CL",
    "start_time": "earliest",
    "timestamp_column": "EventTime",  # Use custom column instead of TimeGenerated
    "tenant_id": tenant_id,
    "client_id": client_id,
    "client_secret": client_secret,
}
```

**Additional streaming options:**

- `inferSchema` (bool, optional, default: true) - if we do the schema inference by sampling result
- `max_retries` (int, optional, default: 5) - Maximum retry attempts for HTTP 429 throttling errors
- `initial_backoff` (float, optional, default: 1.0) - Initial backoff time in seconds for retries
- `min_partition_seconds` (int, optional, default: 60) - Minimum partition duration in seconds when subdividing large result sets

**Potential Issues with "earliest":**

- **Performance**: For very large tables without time-based indexes, the `min(timestamp_column)` query may be slow. This is a one-time cost during stream initialization.
- **Aggregated queries**: If the query contains aggregations (e.g., `| summarize`), "earliest" will find the minimum timestamp from the aggregated results, not from raw data.
- **Empty tables**: If the table has no data, falls back to current timestamp.

### Simple REST API

Right now only implements writing to arbitrary REST API - both batch & streaming.  Registered data source name is `rest`.

Basic usage:

```python
from cyber_connectors import *

spark.dataSource.register(RestApiDataSource)

df = spark.range(10)
df.write.format("rest").mode("overwrite") \
  .option("url", "http://localhost:8001/") \
  .save()
```

Usage with authentication and custom headers:

```python
df.write.format("rest").mode("overwrite") \
  .option("url", "http://api.example.com/data") \
  .option("http_header_Authorization", "Bearer token123") \
  .option("http_header_X-API-Key", "secret") \
  .save()
```

Usage with form data:

```python
df.write.format("rest").mode("overwrite") \
  .option("url", "http://api.example.com/form") \
  .option("http_format", "form-data") \
  .option("http_method", "post") \
  .save()
```

Supported options:

- `url` (string, required) - URL of the REST API endpoint to send data to.
- `http_format` (string, optional, default: `json`) - Payload format to use. Supported values:
  - `json` - Send data as JSON (sets `Content-Type: application/json`)
  - `form-data` - Send data as form-encoded data (all values converted to strings)
- `http_method` (string, optional, default: `post`) - HTTP method to use (`post` or `put`).
- `http_header_*` (string, optional) - Custom HTTP headers. Use prefix `http_header_` followed by the header name.
  - Example: `http_header_Authorization`, `http_header_X-API-Key`, `http_header_Content-Type`
  - Custom headers take precedence over default headers (e.g., you can override `Content-Type` for special API requirements)

**Using with Tines webhook:**

This data source can be easily used to write to Tines webhook. Just specify [Tines webhook URL](https://www.tines.com/docs/actions/types/webhook/#secrets-in-url) as `url` option:

```python
df.write.format("rest").mode("overwrite") \
  .option("url", "https://tenant.tines.com/webhook/<path>/<secret>") \
  .save()
```

**Custom Content-Type example:**

Some APIs require specific Content-Type headers:

```python
df.write.format("rest").mode("overwrite") \
  .option("url", "http://api.example.com/jsonapi") \
  .option("http_format", "json") \
  .option("http_header_Content-Type", "application/vnd.api+json") \
  .save()
```

## Installation

Just install the package from PyPI:

```shell
pip install cyber-spark-data-connectors
```

## Building

This project uses [Poetry](https://python-poetry.org/) to manage dependencies and building the package. 

Initial setup & build:

- Install Poetry
- Set the Poetry environment with `poetry env use 3.10` (or higher Python version)
- Activate Poetry environment with `. $(poetry env info -p)/bin/activate`
- Build the wheel file with `poetry build`. Generated file will be stored in the `dist` directory.

> [!CAUTION]
> Right now, some dependencies aren't included into manifest, so if you will try it with OSS Spark, you will need to make sure that you have following dependencies set: `pyspark[sql]` (version `4.0.0.dev2` or higher), `grpcio` (`>=1.48,<1.57`), `grpcio-status` (`>=1.48,<1.57`), `googleapis-common-protos` (`1.56.4`).

## References

- Splunk: [Format events for HTTP Event Collector](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/FormateventsforHTTPEventCollector)



