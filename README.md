# Custom data sources/sinks for Cybersecurity-related work

> [!WARNING]
> **Experimental! Work in progress**

Based on [PySpark DataSource API](https://spark.apache.org/docs/preview/api/python/user_guide/sql/python_data_source.html) available with Spark 4 & [DBR 15.3+](https://docs.databricks.com/en/pyspark/datasources.html).

- [Custom data sources/sinks for Cybersecurity-related work](#custom-data-sourcessinks-for-cybersecurity-related-work)
- [Available data sources](#available-data-sources)
  - [Splunk data source](#splunk-data-source)
  - [Microsoft Sentinel / Azure Monitor](#microsoft-sentinel--azure-monitor)
    - [Authentication Requirements](#authentication-requirements)
    - [Writing to Microsoft Sentinel / Azure Monitor](#writing-to-microsoft-sentinel--azure-monitor)
    - [Reading from Microsoft Sentinel / Azure Monitor](#reading-from-microsoft-sentinel--azure-monitor)
  - [Simple REST API](#simple-rest-api)
- [Building](#building)
- [References](#references)


# Available data sources

> [!NOTE]
> Most of these data sources/sinks are designed to work with relatively small amounts of data - alerts, etc.  If you need to read or write huge amounts of data, use native export/import functionality of corresponding external system.

## Splunk data source

Right now only implements writing to [Splunk](https://www.splunk.com/) - both batch & streaming. Registered data source name is `splunk`.

By default, this data source will put all columns into the `event` object and send it to Splunk together with metadata (`index`, `source`, ...).  This behavior could be changed by providing `single_event_column` option to specify which string column should be used as the single value of `event`.

Batch usage:

```python
from cyber_connectors import *
spark.dataSource.register(SplunkDataSource)

df = spark.range(10)
df.write.format("splunk").mode("overwrite") \
  .option("url", "http://localhost:8088/services/collector/event") \
  .option("token", "...").save()
```

Streaming usage:

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

Supported options:

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

## Microsoft Sentinel / Azure Monitor

This data source supports both reading from and writing to [Microsoft Sentinel](https://learn.microsoft.com/en-us/azure/sentinel/overview/) / [Azure Monitor Log Analytics](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/log-analytics-overview). Registered data source names are `ms-sentinel` and `azure-monitor`.

### Authentication Requirements

This connector supports three authentication methods (in order of precedence):

1. **Databricks Unity Catalog Service Credential** (`databricks_credential`): Use a named service credential configured in Unity Catalog. This is the recommended approach when running on Databricks with Unity Catalog enabled. See [Unity Catalog Service Credentials documentation](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-services/use-service-credentials).

2. **Azure DefaultAzureCredential** (`azure_default_credential`): Use Azure's DefaultAzureCredential, which automatically discovers credentials from the environment (managed identity, environment variables, etc.). This is useful when running on compute with attached service credentials or managed identity.

3. **Azure Service Principal** (`tenant_id`, `client_id`, `client_secret`): Use explicit service principal credentials. This is the traditional approach and requires all three parameters.

The service principal (or managed identity) needs the following permissions:
- For reading: **Log Analytics Reader** role on the Log Analytics workspace
- For writing: **Monitoring Metrics Publisher** role on the DCE and DCR


### Writing to Microsoft Sentinel / Azure Monitor

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
- **Authentication options (choose one):**
  - `databricks_credential` (string) - Name of Unity Catalog service credential to use for authentication. Recommended when running on Databricks.
  - `azure_default_credential` (boolean, default: false) - If true, use Azure DefaultAzureCredential (managed identity, environment, etc.)
  - `tenant_id`, `client_id`, `client_secret` (strings) - Azure Service Principal credentials. All three are required if using this method.
- `azure_cloud` (string, optional, default: "public") - Azure cloud environment ("public", "government", or "china")
- `batch_size` (int. optional, default: 50) - the size of the buffer to collect payload before sending to MS Sentinel.

### Reading from Microsoft Sentinel / Azure Monitor

The data source supports both batch and streaming reads from Azure Monitor / Log Analytics workspaces using KQL (Kusto Query Language) queries.  If schema isn't specified with `.schema`, it will be inferred automatically.

> [!NOTE]
> For streaming reads of big amounts of data, it's recommended to export necessary tables to EventHubs, and consume from there.

#### Batch Read

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

- `workspace_id` (string, required) - Log Analytics workspace ID
- `query` (string, required) - KQL query to execute
- **Time range options (choose one approach):**
  - `timespan` (string) - Time range in ISO 8601 duration format (e.g., "P1D" = 1 day, "PT1H" = 1 hour, "P7D" = 7 days)
  - `start_time` (string) - Start time in ISO 8601 format (e.g., "2024-01-01T00:00:00Z"). If provided without `end_time`, queries from `start_time` to current time
  - `end_time` (string, optional) - End time in ISO 8601 format. Only valid when `start_time` is specified
  - **Note**: `timespan` and `start_time/end_time` are mutually exclusive - choose one approach
- **Authentication options (choose one):**
  - `databricks_credential` (string) - Name of Unity Catalog service credential to use for authentication. Recommended when running on Databricks.
  - `azure_default_credential` (boolean, default: false) - If true, use Azure DefaultAzureCredential (managed identity, environment, etc.)
  - `tenant_id`, `client_id`, `client_secret` (strings) - Azure Service Principal credentials. All three are required if using this method.
- `azure_cloud` (string, optional, default: "public") - Azure cloud environment. Valid values:
  - `"public"` - Azure Public Cloud (default)
  - `"government"` - Azure Government (GovCloud)
  - `"china"` - Azure China (21Vianet)
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

**Authentication Examples:**

Using Unity Catalog Service Credential (recommended for Databricks):

```python
# Using a named service credential from Unity Catalog
read_options = {
    "workspace_id": "your-workspace-id",
    "query": "AzureActivity | take 100",
    "timespan": "P1D",
    "databricks_credential": "my-azure-credential",  # Name of UC service credential
}

df = spark.read.format("azure-monitor") \
    .options(**read_options) \
    .load()
```

Using Azure DefaultAzureCredential (for managed identity or attached credentials):

```python
# Uses the default credential chain (managed identity, environment, etc.)
read_options = {
    "workspace_id": "your-workspace-id",
    "query": "AzureActivity | take 100",
    "timespan": "P1D",
    "azure_default_credential": "true",
}

df = spark.read.format("azure-monitor") \
    .options(**read_options) \
    .load()
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

**Automatic Throttling and Large Result Set Handling:**

The connector automatically handles common Azure Monitor query issues:

- **Throttling (HTTP 429)**: When Azure Monitor returns rate limit errors, the connector automatically retries with exponential backoff. Configure with `max_retries` and `initial_backoff` options.

- **Large Result Sets**: When a query exceeds Azure's [result size limits](https://learn.microsoft.com/en-us/azure/azure-monitor/service-limits#query-api) (500,000 records or ~64MB), the connector automatically subdivides the time range into smaller chunks and queries each separately. Configure the minimum subdivision size with `min_partition_seconds`.

#### Streaming Read

The data source supports streaming reads from Azure Monitor / Log Analytics. The streaming reader uses time-based offsets to track progress and splits time ranges into partitions for parallel processing.

Streaming read usage:

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
    "checkpointLocation": "/tmp/azure-monitor-checkpoint/",
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
- `query` (string, required) - KQL query to execute (should not include time filters - these are added automatically)
- `start_time` (string, optional, default: "latest") - Start time in ISO 8601 format (e.g., "2024-01-01T00:00:00Z"). Use "latest" to start from current time
- `partition_duration` (int, optional, default: 3600) - Duration in seconds for each partition (controls parallelism)
- **Authentication options (choose one):**
  - `databricks_credential` (string) - Name of Unity Catalog service credential to use for authentication. Recommended when running on Databricks.
  - `azure_default_credential` (boolean, default: false) - If true, use Azure DefaultAzureCredential (managed identity, environment, etc.)
  - `tenant_id`, `client_id`, `client_secret` (strings) - Azure Service Principal credentials. All three are required if using this method.
- `azure_cloud` (string, optional, default: "public") - Azure cloud environment ("public", "government", or "china")
- `checkpointLocation` (string, required) - Directory path for Spark streaming checkpoints

**Important notes for streaming:**
- The reader automatically tracks the timestamp of the last processed data in checkpoints
- Time ranges are split into partitions based on `partition_duration` for parallel processing
- The query should NOT include time filters (e.g., `where TimeGenerated > ago(1d)`) - the reader adds these automatically based on offsets
- Use `start_time: "latest"` to begin streaming from the current time (useful for monitoring real-time data)

## Simple REST API

Right now only implements writing to arbitrary REST API - both batch & streaming.  Registered data source name is `rest`.

Usage:

```python
from cyber_connectors import *

spark.dataSource.register(RestApiDataSource)

df = spark.range(10)
df.write.format("rest").mode("overwrite") \
  .option("url", "http://localhost:8001/") \ 
  .save()
```

Supported options:

- `url` (string, required) - URL of the REST API endpoint to send data to.
- `http_format` (string, optional, default: `json`) what payload format to use (right now only `json` is supported)
- `http_method` (string, optional, default: `post`) what HTTP method to use (`post` or `put`).

This data source could be easily used to write to Tines webhook.  Just specify [Tines webhook URL](https://www.tines.com/docs/actions/types/webhook/#secrets-in-url) as `url` option:

```python
df.write.format("rest").mode("overwrite") \
  .option("url", "https://tenant.tines.com/webhook/<path>/<secret>") \
  .save()
```

# Building

This project uses [Poetry](https://python-poetry.org/) to manage dependencies and building the package. 

Initial setup & build:

- Install Poetry
- Set the Poetry environment with `poetry env use 3.10` (or higher Python version)
- Activate Poetry environment with `. $(poetry env info -p)/bin/activate`
- Build the wheel file with `poetry build`. Generated file will be stored in the `dist` directory.

> [!CAUTION]
> Right now, some dependencies aren't included into manifest, so if you will try it with OSS Spark, you will need to make sure that you have following dependencies set: `pyspark[sql]` (version `4.0.0.dev2` or higher), `grpcio` (`>=1.48,<1.57`), `grpcio-status` (`>=1.48,<1.57`), `googleapis-common-protos` (`1.56.4`).


# References

- Splunk: [Format events for HTTP Event Collector](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/FormateventsforHTTPEventCollector)



