# Splunk Reader Design

**Date:** 2025-01-01
**Status:** Design Review Complete
**Use Cases:** Hybrid batch/micro-batch polling reads, ad-hoc analytics, real-time monitoring (small volumes)

## Overview

Add read functionality to the existing Splunk data source connector to support:
- **Batch reads**: Pull historical data from Splunk indexes for ETL, archival, and analytics
- **Micro-batch polling**: Query for new events in time windows (not true streaming/tailing)
- **Ad-hoc queries**: Run SPL queries and bring results into Spark for further analysis

**Important:** This is not real-time event streaming. The export API returns results for bounded time windows; we implement micro-batch polling by repeatedly querying with advancing time ranges.

## Architecture

### API Choice: Splunk Export Endpoint

Use `/services/search/jobs/export` for both batch and streaming reads.

**Advantages:**
- Unified implementation for batch and streaming
- No job polling overhead (vs. Search Jobs API)
- Handles large datasets via streaming responses
- Available in both Splunk Enterprise and Cloud

**Request Format:**
```
POST /services/search/jobs/export
Authorization: Splunk <token>
Content-Type: application/x-www-form-urlencoded

search=search index=main | stats count by status
earliest_time=1672531200.000
latest_time=1672617600.000
output_mode=json
exec_mode=normal
```

**Critical:** Time bounds are passed as **separate form fields** (`earliest_time`, `latest_time` as epoch seconds), NOT embedded in the SPL `search` string. Embedding time filters in SPL may be treated as search terms and silently ignored.

**Response Format:**
```json
{"result": {"_time": "1234567890.123", "host": "web01", "status": "200"}}
{"result": {"_time": "1234567891.456", "host": "web02", "status": "404"}}
```

Streaming JSON (one object per line) parsed incrementally.

### Query Interface

Support two approaches:

1. **Full SPL queries** (`query` option):
```python
.option("query", "search index=main sourcetype=access | stats count by status")
```

2. **Simple parameters** (for common cases):
```python
.option("index", "main")
.option("sourcetype", "access_combined")
.option("search_filter", "status=200")
```

When both provided, `query` takes precedence. Simple parameters build SPL internally:
```
search index={index} sourcetype={sourcetype} {search_filter}
```

### Authentication

Token-based authentication only (initial implementation):
- `token` option provides Splunk authentication token (can also read from environment variable)
- Sent as `Authorization: Splunk <token>` header (Splunk's standard auth scheme)
- `auth_scheme` option allows override (default: `Splunk`, can use `Bearer` for some Cloud configurations)
- Username/password can be added later if needed

**Security:**
- Tokens must not be logged or included in error messages (redact automatically)
- Support reading token from environment variable (e.g., `SPLUNK_TOKEN`) if `token` option not provided

### Deployment Support

Works with both Splunk Enterprise and Splunk Cloud deployments.

## Batch Reading (Phase 1)

### Time Range Specification

For consistency with Azure Monitor connector:
- `timespan`: ISO 8601 duration (e.g., `P1D` for 1 day, `PT6H` for 6 hours) - queries back from now
  - Limited support: `P<n>D` (days) and `PT<n>H` (hours) only initially
  - Python stdlib lacks full ISO8601 duration parsing; use simple regex parser
- OR `start_time` + `end_time`: ISO 8601 timestamps (e.g., `2024-01-01T00:00:00Z`)
  - Must include timezone (UTC assumed if `Z` suffix)
  - Converted to epoch seconds with millisecond precision

**Conversion to Splunk format:**
- Convert ISO timestamps to epoch seconds: `earliest_time=1672531200.000`
- Pass as separate POST form fields, NOT in SPL query string
- Compute all time calculations on driver (not executors) for consistency across partitions

### Partitioning Strategy

Split time range across multiple Spark executors using:
- `num_partitions`: Number of parallel partitions (default: 1)
- OR `partition_duration`: Duration per partition in seconds
- Precedence: If both specified, `partition_duration` takes precedence

**Partition Boundaries:**
- Time ranges are **left-inclusive, right-exclusive**: `[start, end)`
- No overlap between partitions (prevents duplicate results)
- Example: 3 partitions for 0-3000 seconds → `[0,1000), [1000,2000), [2000,3000)`

**Implementation:**
- Reuse `TimeRangePartition` pattern from Azure Monitor reader
- Store start/end epoch times in partition object
- Each partition constructs its own export request with `earliest_time`/`latest_time`

**Considerations:**
- Events are not evenly distributed; partitions may be imbalanced
- Users should tune `partition_duration` based on expected event rate
- Document that skew is expected for bursty workloads

### Schema Handling

**Default Approach: All Fields as Strings**

Splunk returns most fields as strings, and field presence is inconsistent. **Default schema strategy:**
- All fields are `StringType` with `nullable=True`
- Only well-known Splunk fields get automatic type mapping (see below)
- User must provide explicit schema for typed fields

**Automatic Type Mapping for Well-Known Splunk Fields:**
- `_time` → TimestampType (parse from string epoch or convert from float)
- `_indextime` → TimestampType (parse from string epoch or convert from float)
- `_raw` → StringType
- `host`, `source`, `sourcetype`, `index`, `_cd` → StringType
- `linecount` → IntegerType (only if present and numeric)

**Schema Inference (Opt-In):**

When `inferSchema=true` (default: `false`):
1. Execute query with `| head 10` (sample multiple events, not just 1)
2. Union all field names across sample events (handles sparse fields)
3. For each field:
   - If well-known Splunk field → use automatic mapping
   - Else → StringType (safest default)
4. Schema is non-deterministic and may vary between runs

**Critical Limitations of Inference:**
- First N events may not represent all fields
- Type inference from string values is unreliable
- Different partitions may see different schemas
- Transforming queries (stats, eval) produce arbitrary schemas

**Recommended:**
- For production: always provide explicit schema
- For exploration: use `inferSchema=true` with caution
- For typed fields: provide schema with proper types

**Type Conversion:**
Reuse `_convert_value_to_schema_type()` from MsSentinel with special handling:
- String epoch → `datetime.fromtimestamp(float(value), tz=timezone.utc)`
- Float epoch → `datetime.fromtimestamp(value, tz=timezone.utc)`
- Handle both formats for `_time`/`_indextime`

**Error Handling:**
- `mode` option: `FAILFAST` (default) or `PERMISSIVE`
- FAILFAST: Raise on first type conversion error
- PERMISSIVE: Set field to null on conversion error, log warning

### Query Construction Flow

**Full SPL Query (Preferred):**
```python
if 'query' in options:
    spl = options['query']
    # Use query as-is; user is responsible for syntax
    # Do NOT append time bounds to SPL - use form fields
```

**Simple Parameters (Requires Validation):**
```python
else:
    # Strict validation to prevent injection
    index = options.get('index')
    sourcetype = options.get('sourcetype')
    search_filter = options.get('search_filter')

    # Validate index: alphanumeric, underscore, hyphen only
    if index and not re.match(r'^[a-zA-Z0-9_-]+$', index):
        raise ValueError(f"Invalid index name: {index}")

    # Validate sourcetype: alphanumeric, colon, underscore, hyphen
    if sourcetype and not re.match(r'^[a-zA-Z0-9:_-]+$', sourcetype):
        raise ValueError(f"Invalid sourcetype: {sourcetype}")

    # search_filter: MUST NOT contain pipes or SPL commands
    if search_filter:
        dangerous = ['|', '`', '$', ';']
        if any(char in search_filter for char in dangerous):
            raise ValueError(f"search_filter cannot contain: {dangerous}")

    # Build SPL
    spl = f"search index={index}"
    if sourcetype:
        spl += f" sourcetype={sourcetype}"
    if search_filter:
        spl += f" {search_filter}"  # Simple field=value filters only

# Time bounds: NEVER in SPL, always in form fields
# earliest_time={start_epoch}
# latest_time={end_epoch}

# For schema inference:
if inferring:
    spl += " | head 10"  # Sample 10 events
```

**Critical Security Notes:**
- Simple param mode only supports basic searches (index + sourcetype + simple filters)
- For complex queries (pipes, evals, stats): user MUST use full `query` option
- Never use f-strings with user input without validation
- Document that `search_filter` cannot contain pipes or commands

### Example Usage

```python
# Full SPL query
df = spark.read.format("splunk") \
    .option("splunkd_url", "https://splunk.example.com:8089") \
    .option("splunkd_token", "my-splunkd-token") \
    .option("query", "search index=main error") \
    .option("timespan", "P7D") \
    .option("num_partitions", "8") \
    .load()

# Simple parameters
df = spark.read.format("splunk") \
    .option("splunkd_url", "https://splunk.example.com:8089") \
    .option("splunkd_token", "my-splunkd-token") \
    .option("index", "security") \
    .option("sourcetype", "firewall") \
    .option("start_time", "2024-01-01T00:00:00Z") \
    .option("end_time", "2024-01-02T00:00:00Z") \
    .load()
```

## Micro-Batch Polling (Phase 2)

**Terminology:** This is NOT real-time streaming. It's micro-batch polling with advancing time windows.

### Offset Tracking Strategy

Use `_indextime` (when event was indexed) with tie-breaker for reliable, checkpointable offset tracking:

```python
class SplunkOffset:
    def __init__(self, indextime_epoch: int, tie_breaker: str = "", version: int = 1):
        self.indextime_epoch = indextime_epoch  # Integer seconds (Splunk _indextime precision)
        self.tie_breaker = tie_breaker  # Last seen _cd value at this indextime
        self.version = version  # For future schema changes

    def json(self) -> str:
        return json.dumps({
            "version": self.version,
            "indextime_epoch": self.indextime_epoch,
            "tie_breaker": self.tie_breaker
        })

    @staticmethod
    def from_json(json_str: str) -> SplunkOffset:
        data = json.loads(json_str)
        return SplunkOffset(
            data["indextime_epoch"],
            data.get("tie_breaker", ""),
            data.get("version", 1)
        )
```

**Why `_indextime` instead of `_time`:**
- `_time` is event timestamp (can be in the past, duplicated, out of order)
- `_indextime` is when Splunk indexed the event (monotonically increasing)
- Using `_indextime` prevents missing late-arriving events

**Tie-Breaker Strategy (No Overlap State Needed):**

Instead of maintaining dedupe state across restarts:
1. Store last seen `(_indextime, _cd)` in offset
2. Query with: `_indextime > last_indextime OR (_indextime == last_indextime AND _cd > last_tie_breaker)`
3. Sort results by `(_indextime, _cd)` to ensure deterministic ordering
4. Update offset to last event's `(_indextime, _cd)`

**Benefits:**
- No dedupe state to checkpoint (all state is in offset)
- Survives Spark streaming restarts/executor failures
- Deterministic across retries

**Fallback when `_cd` not available:**
- Use `_cd` if present in results
- If missing: Can use stable hash of `(_indextime, _time, _raw)` as tie-breaker
  - Use `hashlib.sha256()` for stable hash across processes (NOT Python's built-in `hash()` which is randomized)
  - Example: `tie_breaker = hashlib.sha256(f"{_indextime}:{_time}:{_raw}".encode("utf-8")).hexdigest()[:16]`
- Offset only tracks `_indextime` without tie-breaker = risk of duplicates at second boundaries
- Warn user that `_cd` field is strongly recommended for exactly-once semantics

**Offset Methods:**
- `initialOffset()`: Returns `start_time` option or (now - `safety_lag_seconds`)
- `latestOffset()`: Returns (current time - `safety_lag_seconds`)
  - Safety lag accounts for ingestion delay (default: 60 seconds)
- `partitions(start, end)`: Splits time range by `partition_duration`

**Configuration:**
- `safety_lag_seconds`: Lag behind current time (default: 60)

### Query Execution Per Batch

Each micro-batch:
1. Gets time range from `partitions(start_offset, end_offset)`
2. Extracts tie-breaker from start offset: `(last_indextime, last_tie_breaker)`
3. **Validates query compatibility** (see below)
4. Executes export query with `_indextime` bounds and tie-breaker:
   - Prepend to SPL: `search (_indextime > {last_indextime} OR (_indextime == {last_indextime} AND _cd > "{last_tie_breaker}")) _indextime < {end} | {user_query} | sort _indextime, _cd`
   - This ensures monotonic progression and deterministic ordering
5. Returns results as Spark Rows
6. Updates offset to last event's `(_indextime, _cd)` as new tie-breaker

**Query Validation for Streaming:**

Streaming mode requires **event-returning queries** (non-transforming SPL). Transforming commands produce aggregated results without `_indextime` field.

**Prohibited commands in streaming mode:**
- `stats`, `chart`, `timechart`, `eventstats`, `streamstats` (aggregation)
- `transaction` (session creation)
- `top`, `rare` (statistical)
- Other commands that don't return raw events

**Validation approach:**
1. Check if query contains transforming commands (regex scan)
2. If found: raise error with clear message: "Streaming mode requires event-returning queries. Remove transforming commands (stats, chart, etc.) or use batch mode."
3. Allow opt-out: `allow_transforming_queries=true` (user accepts risk that `_indextime` may not exist)

**Critical Implementation Details:**
- Compute batch windows on **driver only** (prevents clock skew across executors)
- Prepend `_indextime` and tie-breaker filter to user query (not append)
- Sort results by `(_indextime, _cd)` to ensure deterministic ordering
- All state is in offset - no dedupe state to maintain across restarts

### Partitioning

Similar to Azure Monitor streaming:
- Split batch time range if duration exceeds `partition_duration`
- Each partition queries time slice in parallel
- No overlap needed (tie-breaker in offset prevents duplicates)

### Example Usage

```python
# Micro-batch polling with SPL query
df = spark.readStream.format("splunk") \
    .option("splunkd_url", "https://splunk.example.com:8089") \
    .option("splunkd_token", "my-splunkd-token") \
    .option("query", "search index=main error") \
    .option("start_time", "latest") \
    .option("partition_duration", "3600") \
    .load()

# Micro-batch polling with simple params
df = spark.readStream.format("splunk") \
    .option("splunkd_url", "https://splunk.example.com:8089") \
    .option("splunkd_token", "my-splunkd-token") \
    .option("index", "security") \
    .option("sourcetype", "firewall") \
    .option("start_time", "2024-01-01T00:00:00Z") \
    .load()
```

## Result Parsing & Error Handling

### Supported Output Modes

Splunk export endpoint supports multiple output formats. We support:

| output_mode | Response Format | Top-Level Key | Parsing Strategy |
|-------------|----------------|---------------|------------------|
| `json` (default) | Streaming JSON objects | `"result": {...}` | Extract `result` dict per line |
| `json_rows` | Streaming JSON arrays | `"rows": [...]` | Parse array, extract each row |

**`json` mode (default):**
```json
{"result": {"_time": "1234567890.123", "host": "web01", "status": "200"}}
{"result": {"_time": "1234567891.456", "host": "web02", "status": "404"}}
```
- One result object per line
- Extract `result` dict containing field key-value pairs

**`json_rows` mode:**
```json
{"fields": [{"name":"_time"},{"name":"host"},{"name":"status"}]}
{"rows": [["1234567890.123","web01","200"]]}
{"rows": [["1234567891.456","web02","404"]]}
```
- First line contains `fields` metadata (column names)
- Subsequent lines contain `rows` arrays
- Combine field names with row values to create dict

**Recommendation:** Use `json` mode (default) - simpler parsing, field names in every record.

### Response Parsing

Export endpoint returns streaming JSON with multiple record types:

**Result records** (data we want in `json` mode):
```json
{"result": {"_time": "1234567890.123", "host": "web01", "status": "200"}}
```

**Non-result records** (ignore or handle):
```json
{"preview": true}
{"messages": [{"type": "INFO", "text": "..."}]}
{"fields": [{"name": "_time"}, {"name": "host"}]}
{"init_offset": 0}
```

**Parse incrementally:**
1. Read response line-by-line with `iter_lines()`
2. Skip empty lines (keep-alives)
3. Parse JSON for each non-empty line
4. **Filter:** Only process records with `"result"` key
5. Extract `result` dict
6. Apply schema and type conversions (handle errors per `mode`)
7. Yield as Spark Row

**Edge Cases:**
- **Blank lines**: Skip (Splunk sends as keep-alive)
- **Invalid JSON**: Log warning with raw line sample, skip or fail based on `mode`
- **Missing `result` key**: Ignore (non-data record)
- **Large lines**: Set reasonable line size limit (e.g., 10MB), fail if exceeded
- **Encoding issues**: Expect UTF-8, handle decode errors gracefully
- **Partial reads**: If connection drops mid-stream, raise with context (partition, offset, line count)

**Implementation:**
```python
for line in response.iter_lines(decode_unicode=True):
    if not line.strip():
        continue  # Skip blanks

    try:
        record = json.loads(line)
    except json.JSONDecodeError as e:
        if mode == "FAILFAST":
            raise ValueError(f"Invalid JSON: {line[:100]}...") from e
        else:
            log.warning(f"Skipping invalid JSON: {line[:100]}...")
            continue

    if "result" not in record:
        continue  # Non-data record

    result = record["result"]
    # ... convert to Row
```

### Error Handling

**Authentication Errors (401):**
- Raise immediately with clear message: "Invalid Splunk token"

**Query Syntax Errors (400):**
- Parse Splunk error response
- Raise with Splunk's error message

**Rate Limiting (429):**
- Retry with exponential backoff
- Implemented in custom retry loop (see Connection Management section)

**Large Result Sets:**
- Export API handles streaming naturally
- Document that users should write selective queries with time bounds

### Connection Management

**HTTP Session Configuration:**

Do NOT use `get_http_session()` for streaming POSTs - it may not handle mid-stream failures correctly.

Instead, manual session with explicit settings:
```python
session = requests.Session()
session.headers.update({
    "Authorization": f"{auth_scheme} {token}",  # Splunk or Bearer
    "Content-Type": "application/x-www-form-urlencoded"
})

# Timeouts: (connect, read)
timeout = (10, 300)  # 10s connect, 5min read

# TLS verification
session.verify = verify_ssl  # Default True, allow user override

# Proxy support (via environment vars automatically)
# Respects HTTP_PROXY, HTTPS_PROXY, NO_PROXY
```

**Retry Strategy:**

Only retry the **initial request**, not mid-stream reads:
1. Try POST with timeout
2. On failure (connection error, 429, 5xx):
   - If haven't started reading response: retry with backoff
   - If mid-stream read: raise (don't retry partial reads)
3. Max retries: 3 (configurable)
4. Backoff: exponential (1s, 2s, 4s)

**Timeout Behavior:**
- Connect timeout: 10 seconds (user configurable)
- Read timeout: 300 seconds (user configurable)
- If read times out mid-stream: raise descriptive error with partition info

**Implementation:**
```python
for attempt in range(max_retries + 1):
    try:
        response = session.post(url, data=form_data, timeout=timeout, stream=True)
        response.raise_for_status()
        # Success - start reading
        return response
    except (ConnectionError, Timeout) as e:
        if attempt < max_retries:
            wait = initial_backoff * (2 ** attempt)
            time.sleep(wait)
        else:
            raise
    except HTTPError as e:
        if e.response.status_code in [429, 500, 502, 503] and attempt < max_retries:
            wait = initial_backoff * (2 ** attempt)
            time.sleep(wait)
        else:
            raise
```

## Configuration Options

### Required Options

**Note on Breaking Change:** The existing Splunk writer uses `url` and `token` for HEC writes. To avoid collision and confusion, read operations use distinct option names:

- `splunkd_url`: Splunk management API base URL (e.g., `https://splunk.example.com:8089`)
- `splunkd_token`: Splunk authentication token for splunkd API (or set `SPLUNK_AUTH_TOKEN` env var)
  - **Not the same as HEC token**: HEC tokens are for data ingestion, splunkd tokens are for API access

**Existing writer options (unchanged for backward compatibility):**
- `url`: HEC endpoint URL (e.g., `https://splunk.example.com:8088/services/collector`)
- `token`: HEC token for writes

### Query Options (choose one approach)

- `query`: Full SPL query string (**preferred** for complex queries)
- OR simple parameters (for basic searches only):
  - `index`: Splunk index name (alphanumeric, underscore, hyphen only)
  - `sourcetype`: Source type filter (optional, alphanumeric, colon, underscore, hyphen)
  - `search_filter`: Additional filters (optional, cannot contain pipes or commands)

**Note:** For any query with pipes, stats, eval, etc., use `query` option.

### Time Range Options (Batch only)

- `timespan`: ISO 8601 duration (e.g., `P1D`, `PT6H`) - limited to days and hours initially
- OR `start_time` + `end_time`: ISO 8601 timestamps with timezone (required for batch if no timespan)

### Micro-Batch Polling Options (Phase 2)

- `start_time`: ISO 8601 timestamp or `"latest"` (default: `"latest"`)
- `partition_duration`: Seconds per partition (default: `3600`)
- `safety_lag_seconds`: Lag behind current time for ingestion delay (default: `60`)
- `allow_transforming_queries`: Allow queries with transforming commands (default: `false`, not recommended)

### Schema Options

- `inferSchema`: Infer schema from data (default: `false` - provide schema explicitly)
- `mode`: Error handling - `FAILFAST` (default) or `PERMISSIVE`

### Performance Options

- `num_partitions`: Parallel partitions for batch (default: `1`)
- `partition_duration`: Partition duration in seconds (precedence over num_partitions)
- `output_mode`: Splunk format - `json` (default) or `json_rows`

### Connection Options

- `auth_scheme`: Authorization scheme - `Splunk` (default) or `Bearer`
- `verify_ssl`: TLS certificate verification - `true` (default), `false`, or path to CA bundle
  - `true`: Verify using system CA bundle
  - `false`: Disable verification (insecure, not recommended)
  - Path string (e.g., `/path/to/ca-bundle.crt`): Use custom CA bundle for internal PKI
- `connect_timeout`: Connect timeout in seconds (default: `10`)
- `read_timeout`: Read timeout in seconds (default: `300`)
- `max_retries`: Maximum retry attempts (default: `3`)
- `initial_backoff`: Initial backoff for retries in seconds (default: `1.0`)

## Testing Strategy

### Unit Tests

Follow `test_mssentinel_reader.py` pattern:

**DataSource Tests:**
- Format name registration
- Reader/streamReader method existence

**Schema Inference Tests:**
- Mock export responses with Splunk results
- Test automatic type mapping (`_time`, `_indextime`, etc.)
- Test mixed field types
- Test user-provided schema bypass

**Batch Reader Tests:**
- Mock export endpoint
- Time range parsing (timespan vs. start_time/end_time)
- Partition generation
- Query construction (SPL vs. simple params)
- Empty results handling

**Stream Reader Tests:**
- Offset serialization/deserialization
- initialOffset/latestOffset
- Partition generation for batches
- Time-based progression

**Error Handling Tests:**
- 401 authentication failure
- 400 query syntax errors
- 429 rate limiting with retries
- Connection failures

### Mock Strategy

- Mock `requests.Session.post()` for streaming export responses
- Mock response must support `.iter_lines()` method for line-by-line parsing
- Use `unittest.mock.patch` for API calls
- Create fixture helpers for Splunk response formats (json and json_rows modes)
- Mock response examples:
  ```python
  mock_response = Mock()
  mock_response.status_code = 200
  mock_response.iter_lines.return_value = [
      '{"result": {"_time": "1234567890.123", "host": "web01"}}',
      '{"result": {"_time": "1234567891.456", "host": "web02"}}'
  ]
  session_mock.post.return_value = mock_response
  ```

### Integration Testing

Document (but don't automate initially):
- Test against real Splunk Enterprise
- Test against Splunk Cloud
- Verify large result set handling
- Verify streaming offset progression

## Implementation Phases

### Phase 1: Batch Reading (Priority)

Covers hybrid (batch + streaming) and ad-hoc analytics use cases.

**Components:**
- `SplunkDataSource.reader()` and `SplunkBatchReader`
- Schema inference with automatic field mapping
- Time range parsing (timespan/start_time/end_time)
- Time-based partitioning
- Query construction (full SPL + simple params)
- Error handling and retries

**Estimated:** ~600-800 LOC + ~400-600 LOC tests

### Phase 2: Streaming Reading

Add streaming support on top of Phase 1.

**Components:**
- `SplunkDataSource.streamReader()` and `SplunkStreamReader`
- `SplunkOffset` for time-based tracking
- Partition generation for streaming batches
- Reuse batch reader's query execution

**Estimated:** ~200-300 LOC + ~200-300 LOC tests

## Security & Operational Concerns

### Token Security

- **Never log tokens**: Redact from all log messages and error output
- **Environment variable support**: Allow `SPLUNK_TOKEN` env var instead of inline option
- **Error messages**: Ensure tokens don't appear in exceptions or debug output

### TLS/SSL

- **Verify certificates by default**: `verify_ssl=true` (default)
- **CA bundle support**: Allow custom CA bundle path for internal PKI
- **Document insecure mode**: Clearly warn if user disables verification

### Proxy Support

- Requests library respects standard environment variables automatically:
  - `HTTP_PROXY`, `HTTPS_PROXY`: Proxy URLs
  - `NO_PROXY`: Bypass list
- Document proxy configuration in README

### Resource Limits

- **Memory**: Deduplication state bounded by overlap window only
- **Network**: Large result sets can saturate network; document query selectivity
- **Splunk load**: Parallel partitions increase Splunk load; document tuning guidance

### Logging & Monitoring

- Log query execution time per partition
- Log result counts per partition
- Redact sensitive data in logs (tokens, query contents if containing PII)
- Include partition/offset context in all error messages

## Tradeoffs & Limitations

### Design Decisions

1. **`_indextime` offsets with overlap/dedupe**: More complex but reliable. Prevents missing late-arriving events. Using `_cd` for deduplication when available.

2. **Export API only**: Simpler than Search Jobs API, handles large datasets. Blocking per partition is acceptable with parallelism control.

3. **Token auth only initially**: Covers most automation use cases. Username/password can be added later.

4. **Schema defaults to strings**: Safest default given Splunk's type inconsistency. Users provide explicit schema for typed fields.

5. **Simple param validation**: Strict allow-lists prevent injection. Complex queries require full `query` option.

### Known Limitations

- **Micro-batch polling latency**: Not true real-time; watermark lag determined by `safety_lag_seconds` and trigger interval
- **Partition skew**: Event distribution is uneven; some partitions may process much more data than others
- **Query selectivity**: Users must write selective queries; no automatic pagination or result size limiting
- **`_cd` field availability**: Deduplication works best when `_cd` is present; fallback to hash-based dedupe is less reliable
- **Clock skew**: Multi-node Splunk clusters may have clock differences affecting time range queries

### Future Enhancements

- Support username/password authentication
- Add search job progress tracking for long queries (Search Jobs API alternative)
- Support savedsearch/alert execution
- Add metrics for query performance monitoring (events/sec, query latency)
- Support custom CA bundle paths for TLS
- Add watermark tracking with event-time semantics (advanced streaming)

## References

- [Splunk REST API - Export Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTsearch#search.2Fjobs.2Fexport)
- Azure Monitor reader implementation (cyber_connectors/MsSentinel.py)
- Existing Splunk writer implementation (cyber_connectors/Splunk.py)
