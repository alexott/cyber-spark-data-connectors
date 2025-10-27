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
