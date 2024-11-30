# Custom data sources/sinks for Cybersecurity-related work

> [!WARNING]
> **Experimental! Work in progress**

Based on [PySpark DataSource API](https://spark.apache.org/docs/preview/api/python/user_guide/sql/python_data_source.html) available with Spark 4 & [DBR 15.3+](https://docs.databricks.com/en/pyspark/datasources.html).

  - [Available data sources](#available-data-sources)
    - [Splunk data source](#splunk-data-source)
    - [Simple REST API](#simple-rest-api)
  - [Building](#building)
  - [TODOs](#todos)
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

Right now only implements writing to [Microsoft Sentinel](https://learn.microsoft.com/en-us/azure/sentinel/overview/) - both batch & streaming. Registered data source name is `ms-sentinel`.  The integration uses [Logs Ingestion API of Azure Monitor](https://learn.microsoft.com/en-us/azure/sentinel/create-custom-connector#connect-with-the-log-ingestion-api), so it's also exposed as `azure-monitor`.

To push data you need to create Data Collection Endpoint (DCE), Data Collection Rule (DCR), and create a custom table in Log Analytics workspace.  See [documentation](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview) for description of this process.  The structure of the data in DataFrame should match the structure of the defined custom table. 

This connector uses Azure Service Principal Client ID/Secret for authentication - you need to grant correct permissions (`Monitoring Metrics Publisher`) to the service principal on the DCE and DCR.

Batch usage:

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

Streaming usage:

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
    "checkpointLocation": "/tmp/splunk-checkpoint/"
}
stream = sdf.writeStream.format("splunk") \
  .trigger(availableNow=True) \
  .options(**sentinel_stream_options).start()
```

Supported options:

- `dce` (string, required) - URL of the Data Collection Endpoint.
- `dcr_id` (string, required) - ID of Data Collection Rule.
- `dcs` (string, required) - name of custom table created in the Log Analytics Workspace.
- `tenant_id` (string, required) - Azure Tenant ID.
- `client_id` (string, required) - Application ID (client ID) of Azure Service Principal.
- `client_secret` (string, required) - Client Secret of Azure Service Principal.
- `batch_size` (int. optional, default: 50) - the size of the buffer to collect payload before sending to MS Sentinel.

## Simple REST API

Right now only implements writing to arbitrary REST API - both batch & streaming.  Registered data source name is `rest`.

Usage:

```python
from cyber_connectors import *

spark.dataSource.register(RestApiDataSource)

df = spark.range(10)
df.write.format("rest").mode("overwrite").option("url", "http://localhost:8001/").save()
```

Supported options:

- `url` (string, required) - URL of the REST API endpoint to send data to.
- `http_format` (string, optional, default: `json`) what payload format to use (right now only `json` is supported)
- `http_method` (string, optional, default: `post`) what HTTP method to use (`post` or `put`).

# Building

This project uses [Poetry](https://python-poetry.org/) to manage dependencies and building the package. 

Initial setup & build:

- Install Poetry
- Set the Poetry environment with `poetry env use 3.10` (or higher Python version)
- Activate Poetry environment with `. $(poetry env info -p)/bin/activate`
- Build the wheel file with `poetry build`. Generated file will be stored in the `dist` directory.

> [!CAUTION]
> Right now, some dependencies aren't included into manifest, so if you will try it with OSS Spark, you will need to make sure that you have following dependencies set: `pyspark[sql]` (version `4.0.0.dev2` or higher), `grpcio` (`>=1.48,<1.57`), `grpcio-status` (`>=1.48,<1.57`), `googleapis-common-protos` (`1.56.4`).


# TODOs

- \[ \] add tests - need to mock REST API
- \[x\] Splunk: add support sending a single-value as `event`, will require setting the column name?
- \[ \] Splunk: add support for sending raw events?
- \[x\] Splunk: add support for [indexed fields extraction](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/IFXandHEC)
- \[x\] Splunk: add retries
- \[ \] Implement reading from Splunk
- \[ \] Splunk: correctly handle `abort`/`commit` events
- \[x\] REST API: add retries
- \[ \] REST API: correctly handle `abort`/`commit` events
- \[x\] Implement writing to Azure Monitor
- \[ \] Azure Monitor: add retries
- \[ \] Implement reading from Azure Monitor
- \[ \] Azure Monitor: correctly handle `abort`/`commit` events
- \[ \] Think how to generate dependencies for use with OSS Spark


# References

- Splunk: [Format events for HTTP Event Collector](https://docs.splunk.com/Documentation/Splunk/9.3.1/Data/FormateventsforHTTPEventCollector)



