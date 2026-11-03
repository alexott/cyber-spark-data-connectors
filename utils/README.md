# Utilities for Cyber Spark Data Connectors

This directory contains utility scripts for manual testing and development of the cyber-spark-data-connectors package.

## Contents

### `http_server.py`

A simple HTTP server for manual testing of the REST API data source.

**Purpose:**
- Manual/integration testing of the REST API connector
- Debugging HTTP payload format
- Verifying data is correctly sent to REST endpoints

**Usage:**

1. Start the test server:
   ```bash
   python3 utils/http_server.py
   ```
   
   The server will start on port 8001 and print received POST requests to the console.

2. In a separate terminal, run your Spark code with the REST API data source:
   ```python
   from cyber_connectors import RestApiDataSource
   spark.dataSource.register(RestApiDataSource)
   
   df = spark.range(10)
   df.write.format("rest").mode("overwrite") \
     .option("url", "http://localhost:8001/") \
     .save()
   ```

3. Observe the server output to verify the data being sent.

**Example Output:**
```
Starting httpd on port 8001
Received POST request: b'{"id": 0}'
Received POST request: b'{"id": 1}'
Received POST request: b'{"id": 2}'
...
```

**Note:** 
- This is for **manual testing only** and is not used by the automated unit test suite.
- The automated tests mock HTTP requests for speed and reliability.
- You can modify the port by editing the `port` parameter in the script or passing it as an argument.

## Adding More Utilities

Feel free to add more utility scripts here for:
- Mock servers for Splunk or other endpoints
- Data generation tools
- Testing helpers
- Development utilities

Keep utility scripts separate from the automated test suite in the `tests/` directory.

### `sentinel_nested_check.py`

End-to-end check that nested columns (structs, arrays of structs) reach a Log Analytics `dynamic` column as JSON
objects.  It writes test rows to the `Details` column of `SentinelTIAlerts_CL` through the `ms-sentinel` writer,
then reads them back with KQL and checks that `Details.ip`, `Details.tags[0].k` and similar paths work.

The DCE, DCR and workspace IDs come from the Terraform state (`--tf-dir`), or from `--dce`, `--dcr-id` and
`--workspace-id`.  `write` authenticates with `DefaultAzureCredential` (set `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
`AZURE_CLIENT_SECRET` for a service principal with "Monitoring Metrics Publisher" on the DCR and DCE).  `check`
uses the Azure CLI login.

```bash
# released version, for comparison (expected to FAIL: nested values arrive as arrays)
uv run --isolated --no-project --with cyber-spark-data-connectors==0.0.8 --with pyspark==4.2.1 --with pyarrow \
  python utils/sentinel_nested_check.py write --label v0.0.8 --tf-dir <terraform dir>
# current checkout (expected to PASS)
uv run python utils/sentinel_nested_check.py write --label dev --tf-dir <terraform dir>
# a few minutes later
uv run python utils/sentinel_nested_check.py check --tf-dir <terraform dir>
```
