import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Mapping

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    DataSourceStreamWriter,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType, TimestampType
from requests import Session

from cyber_connectors.common import DateTimeJsonEncoder, SimpleCommitMessage, get_http_session


def _parse_time_range(timespan=None, start_time=None, end_time=None):
    """Parse time range from timespan or start_time/end_time options.

    Args:
        timespan: ISO 8601 duration string (e.g., "P1D", "PT6H")
        start_time: ISO 8601 datetime string (e.g., "2024-01-01T00:00:00Z")
        end_time: ISO 8601 datetime string (optional, defaults to now)

    Returns:
        tuple: (start_datetime, end_datetime) as datetime objects with timezone

    Raises:
        ValueError: If timespan format is invalid
        Exception: If neither timespan nor start_time is provided

    """
    if timespan:
        # Parse ISO 8601 duration - limited support for P[n]D and PT[n]H
        match = re.match(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?)?$", timespan)
        if match:
            days = int(match.group(1) or 0)
            hours = int(match.group(2) or 0)

            # Validate that at least one component was specified
            if days == 0 and hours == 0:
                raise ValueError(f"Invalid timespan format: {timespan} - must specify days (P<n>D) or hours (PT<n>H)")

            delta = timedelta(days=days, hours=hours)
            end_time_val = datetime.now(timezone.utc)
            start_time_val = end_time_val - delta
            return (start_time_val, end_time_val)
        else:
            raise ValueError(f"Invalid timespan format: {timespan}. Use P<n>D for days or PT<n>H for hours")
    elif start_time:
        start_time_val = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        if end_time:
            end_time_val = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        else:
            end_time_val = datetime.now(timezone.utc)
        return (start_time_val, end_time_val)
    else:
        raise Exception("Either 'timespan' or 'start_time' must be provided")


def _build_spl_query_from_options(options: Mapping[str, Any]) -> str:
    """Build SPL query from options.

    If `query` is provided, it is returned as-is. Otherwise, build a simple SPL query from:
    - index (required if query is not provided)
    - sourcetype (optional)
    - search_filter (optional, validated for unsafe characters)
    """
    query = options.get("query")
    if query:
        return str(query)

    # Build from simple parameters
    index = options.get("index")
    sourcetype = options.get("sourcetype")
    search_filter = options.get("search_filter")

    # Validate simple parameters
    if index and not re.match(r"^[a-zA-Z0-9_-]+$", str(index)):
        raise ValueError(f"Invalid index name: {index}")

    if sourcetype and not re.match(r"^[a-zA-Z0-9:_-]+$", str(sourcetype)):
        raise ValueError(f"Invalid sourcetype: {sourcetype}")

    if search_filter:
        dangerous = ["|", "`", "$", ";"]
        if any(char in str(search_filter) for char in dangerous):
            raise ValueError(f"search_filter cannot contain: {dangerous}")

    # Build SPL
    if not index:
        raise ValueError("Either 'query' or 'index' must be provided")

    spl = f"search index={index}"
    if sourcetype:
        spl += f" sourcetype={sourcetype}"
    if search_filter:
        spl += f" {search_filter}"

    return spl


def _convert_value_to_schema_type(value, spark_type):
    """Convert a value to match the expected PySpark schema type.

    Handles Splunk-specific conversions (e.g., _time and _indextime epoch timestamps).

    Args:
        value: The raw value from Splunk
        spark_type: The expected PySpark DataType

    Returns:
        Converted value matching the schema type

    Raises:
        ValueError: If conversion fails

    """
    # Handle None/NULL values
    if value is None:
        return None

    try:
        # String type - convert everything to string
        if isinstance(spark_type, StringType):
            return str(value)

        # Integer types
        elif isinstance(spark_type, IntegerType):
            if isinstance(value, bool):
                raise ValueError("Cannot convert boolean to integer")
            return int(value)

        # Timestamp type - special handling for Splunk epoch timestamps
        elif isinstance(spark_type, TimestampType):
            if isinstance(value, datetime):
                return value
            elif isinstance(value, str):
                # Try parsing string epoch timestamp (e.g., "1234567890.123")
                try:
                    epoch_float = float(value)
                    return datetime.fromtimestamp(epoch_float, tz=timezone.utc)
                except ValueError:
                    # Not a numeric string, try ISO 8601 format
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
            elif isinstance(value, (int, float)):
                # Numeric epoch timestamp
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            else:
                raise ValueError(f"Cannot convert {type(value).__name__} to timestamp")

        # Unsupported type - return as-is
        else:
            return value

    except (ValueError, TypeError) as e:
        raise ValueError(f"Failed to convert value '{value}' (type: {type(value).__name__}) to {spark_type}: {e}")


@dataclass
class SplunkTimeRangePartition(InputPartition):
    """Represents a time range partition for parallel Splunk export queries."""

    start_epoch: float  # Start time as epoch seconds (inclusive)
    end_epoch: float  # End time as epoch seconds (exclusive)


class SplunkOffset:
    """Represents the offset for Splunk streaming reads.

    Tracks _indextime and tie-breaker (_cd or hash) for reliable offset tracking.
    """

    def __init__(self, indextime_epoch: int, tie_breaker: str = "", version: int = 1):
        """Initialize offset.

        Args:
            indextime_epoch: Integer epoch seconds (Splunk _indextime precision)
            tie_breaker: Last seen _cd value or hash at this indextime
            version: Version number for future schema changes

        """
        self.indextime_epoch = indextime_epoch
        self.tie_breaker = tie_breaker
        self.version = version

    def json(self):
        """Serialize offset to JSON string."""
        return json.dumps(
            {"version": self.version, "indextime_epoch": self.indextime_epoch, "tie_breaker": self.tie_breaker}
        )

    @staticmethod
    def from_json(json_str: str):
        """Deserialize offset from JSON string."""
        data = json.loads(json_str)
        return SplunkOffset(data["indextime_epoch"], data.get("tie_breaker", ""), data.get("version", 1))


class SplunkDataSource(DataSource):
    """Data source for Splunk. Supports reading from and writing to Splunk.

    Write options:
    - url: Splunk HEC URL
    - token: Splunk HEC token
    - time_column: (optional) column name to use as event time
    - batch_size: (optional) number of events to batch before sending to Splunk. (default: 50)
    - index: (optional) Splunk index
    - source: (optional) Splunk source
    - host: (optional) Splunk host
    - sourcetype: (optional) Splunk sourcetype (default: _json)
    - single_event_column: (optional) column name to use as the full event payload (i.e., text column)
    - indexed_fields: (optional) comma separated list of fields to index
    - remove_indexed_fields: (optional) remove indexed fields from event payload (default: false)

    Read options:
    - splunkd_url: Splunk management API URL (e.g., https://splunk.example.com:8089)
    - splunkd_token: Splunk authentication token for API access (or set SPLUNK_AUTH_TOKEN env var)
    - query: Full SPL query string (preferred for complex queries)
    OR simple parameters (for basic searches):
      - index: Splunk index name
      - sourcetype: Source type filter (optional)
      - search_filter: Additional filters (optional, no pipes or commands)
    - timespan: Time range in ISO 8601 duration format (e.g., P1D, PT6H) - for batch reads
    - start_time: Start time in ISO 8601 format (e.g., 2024-01-01T00:00:00Z)
    - end_time: End time in ISO 8601 format (optional, defaults to now)
    - num_partitions: Number of parallel partitions (default: 1)
    - partition_duration: Partition duration in seconds (takes precedence over num_partitions)
    - inferSchema: Infer schema from data (default: false)
    - mode: Error handling mode - FAILFAST (default) or PERMISSIVE
    - auth_scheme: Authorization scheme - Splunk (default) or Bearer
    - verify_ssl: TLS certificate verification - true (default), false, or path to CA bundle
    - connect_timeout: Connect timeout in seconds (default: 10)
    - read_timeout: Read timeout in seconds (default: 300)
    - max_retries: Maximum retry attempts (default: 3)
    - initial_backoff: Initial backoff for retries in seconds (default: 1.0)
    - output_mode: Splunk response format - json (default) or json_rows
    """

    @classmethod
    def name(cls):
        return "splunk"

    def schema(self):
        """Return the schema for reading data.

        Returns default schema with all fields as strings unless user provides explicit schema.

        Returns:
            StructType: The schema of the data

        """
        infer_schema = self.options.get("inferSchema", "false").lower() == "true"
        if infer_schema:
            return self._infer_read_schema()
        else:
            # Default schema: _time, _indextime, _raw, and common Splunk fields
            return StructType(
                [
                    StructField("_time", TimestampType(), nullable=True),
                    StructField("_indextime", TimestampType(), nullable=True),
                    StructField("_raw", StringType(), nullable=True),
                    StructField("host", StringType(), nullable=True),
                    StructField("source", StringType(), nullable=True),
                    StructField("sourcetype", StringType(), nullable=True),
                    StructField("index", StringType(), nullable=True),
                ]
            )

    def _infer_read_schema(self):
        """Infer schema by executing a sample query with | head 10.

        Returns:
            StructType: The inferred schema

        """
        # Get read options
        splunkd_url = self.options.get("splunkd_url")
        import os

        splunkd_token = self.options.get("splunkd_token") or os.environ.get("SPLUNK_AUTH_TOKEN")
        query = self._build_spl_query()

        # Validate required options
        assert splunkd_url is not None, "splunkd_url is required"
        assert splunkd_token is not None, "splunkd_token is required (or set SPLUNK_AUTH_TOKEN env var)"

        # Add | head 10 to sample query
        sample_query = query.strip()
        if "| head" not in sample_query.lower():
            sample_query = f"{sample_query} | head 10"

        # Execute sample query with a small time range for schema inference
        timespan = self.options.get("timespan", "PT1H")  # Default to 1 hour for sampling
        start_time, end_time = _parse_time_range(timespan=timespan)

        # Build form data for export request
        form_data = {
            "search": sample_query,
            "earliest_time": str(start_time.timestamp()),
            "latest_time": str(end_time.timestamp()),
            "output_mode": "json",
            "exec_mode": "normal",
        }

        # Execute query
        session = self._create_session()
        response = self._execute_export_request(session, splunkd_url, form_data)

        # Parse first 10 results to infer schema
        all_fields = {}  # Track all field names and sample values
        for line in response.iter_lines(decode_unicode=True):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            if "result" not in record:
                continue

            result = record["result"]
            # Union all field names
            for field_name, field_value in result.items():
                if field_name not in all_fields and field_value is not None:
                    all_fields[field_name] = field_value

        # Build schema with automatic type mapping for well-known Splunk fields
        fields = []
        for field_name in sorted(all_fields.keys()):
            if field_name in ("_time", "_indextime"):
                field_type = TimestampType()
            elif field_name == "linecount":
                field_type = IntegerType()
            else:
                field_type = StringType()

            fields.append(StructField(field_name, field_type, nullable=True))

        return StructType(fields) if fields else StructType([StructField("_raw", StringType(), nullable=True)])

    def _build_spl_query(self):
        """Build SPL query from options.

        Returns:
            str: The SPL query string

        """
        return _build_spl_query_from_options(self.options)

    def _create_session(self):
        """Create HTTP session with auth and retry logic.

        Returns:
            requests.Session: Configured session

        """
        import os

        splunkd_token = self.options.get("splunkd_token") or os.environ.get("SPLUNK_AUTH_TOKEN")
        assert splunkd_token is not None, "splunkd_token is required (or set SPLUNK_AUTH_TOKEN env var)"

        auth_scheme = self.options.get("auth_scheme", "Splunk")
        session = Session()
        session.headers.update(
            {"Authorization": f"{auth_scheme} {splunkd_token}", "Content-Type": "application/x-www-form-urlencoded"}
        )

        # TLS verification
        verify_ssl = self.options.get("verify_ssl", "true")
        if isinstance(verify_ssl, str):
            if verify_ssl.lower() == "false":
                session.verify = False
            elif verify_ssl.lower() == "true":
                session.verify = True
            else:
                # Path to CA bundle
                session.verify = verify_ssl
        else:
            session.verify = bool(verify_ssl)

        return session

    def _execute_export_request(self, session, splunkd_url, form_data):
        """Execute export request with retry logic.

        Args:
            session: requests.Session
            splunkd_url: Base URL for Splunk management API
            form_data: Dict of form parameters

        Returns:
            requests.Response: Streaming response

        """
        max_retries = int(self.options.get("max_retries", "3"))
        initial_backoff = float(self.options.get("initial_backoff", "1.0"))
        connect_timeout = int(self.options.get("connect_timeout", "10"))
        read_timeout = int(self.options.get("read_timeout", "300"))
        timeout = (connect_timeout, read_timeout)

        export_url = f"{splunkd_url.rstrip('/')}/services/search/jobs/export"

        for attempt in range(max_retries + 1):
            try:
                response = session.post(export_url, data=form_data, timeout=timeout, stream=True)
                response.raise_for_status()
                return response
            except Exception:
                if attempt < max_retries:
                    wait = initial_backoff * (2**attempt)
                    time.sleep(wait)
                else:
                    raise

    def reader(self, schema: StructType):
        return SplunkBatchReader(self.options, schema)

    def streamReader(self, schema: StructType):
        return SplunkStreamReader(self.options, schema)

    def streamWriter(self, schema: StructType, overwrite: bool):
        return SplunkHecStreamWriter(self.options)

    def writer(self, schema: StructType, overwrite: bool):
        return SplunkHecBatchWriter(self.options)


class SplunkHecWriter:
    """ """

    def __init__(self, options):
        self.options = options
        self.url = self.options.get("url")
        self.token = self.options.get("token")
        assert self.url is not None
        assert self.token is not None
        # extract optional parameters
        self.time_col = self.options.get("time_column")
        self.batch_size = int(self.options.get("batch_size", "50"))
        self.index = self.options.get("index")
        self.source = self.options.get("source")
        self.host = self.options.get("host")
        self.source_type = self.options.get("sourcetype", "_json")
        self.single_event_column = self.options.get("single_event_column")
        if self.single_event_column and self.source_type == "_json":
            self.source_type = "text"
        self.indexed_fields = str(self.options.get("indexed_fields", "")).split(",")
        self.omit_indexed_fields = self.options.get("remove_indexed_fields", False)
        if isinstance(self.omit_indexed_fields, str):
            self.omit_indexed_fields = self.omit_indexed_fields.lower() == "true"

    def _send_to_splunk(self, s: Session, msgs: list):
        if len(msgs) > 0:
            response = s.post(self.url, data="\n".join(msgs))
            print(response.status_code, response.text)

    def write(self, iterator: Iterator[Row]):
        """Writes the data, then returns the commit message of that partition.
        Library imports must be within the method.
        """
        import datetime

        from pyspark import TaskContext

        context = TaskContext.get()
        partition_id = context.partitionId()
        cnt = 0
        s = get_http_session(additional_headers={"Authorization": f"Splunk {self.token}"}, retry_on_post=True)

        msgs = []
        for row in iterator:
            cnt += 1
            rd = row.asDict()
            d = {"sourcetype": self.source_type}
            if self.index:
                d["index"] = self.index
            if self.source:
                d["source"] = self.source
            if self.host:
                d["host"] = self.host
            if self.time_col and self.time_col in rd:
                tm = rd.get(self.time_col, datetime.datetime.now())
                if isinstance(tm, datetime.datetime):
                    d["time"] = tm.timestamp()
                elif isinstance(tm, int) or isinstance(tm, float):
                    d["time"] = tm
                else:
                    d["time"] = datetime.datetime.now().timestamp()
            else:
                d["time"] = datetime.datetime.now().timestamp()
            if self.single_event_column and self.single_event_column in rd:
                d["event"] = rd.get(self.single_event_column)
            elif self.indexed_fields:
                idx_fields = {k: rd.get(k) for k in self.indexed_fields if k in rd}
                if idx_fields:
                    d["fields"] = idx_fields
                if self.omit_indexed_fields:
                    ev_fields = {k: v for k, v in rd.items() if k not in self.indexed_fields}
                    if ev_fields:
                        d["event"] = ev_fields
                else:
                    d["event"] = rd
            else:
                d["event"] = rd
            msgs.append(json.dumps(d, cls=DateTimeJsonEncoder))

            if len(msgs) >= self.batch_size:
                self._send_to_splunk(s, msgs)
                msgs = []

        self._send_to_splunk(s, msgs)

        return SimpleCommitMessage(partition_id=partition_id, count=cnt)


class SplunkHecBatchWriter(SplunkHecWriter, DataSourceWriter):
    def __init__(self, options):
        super().__init__(options)


class SplunkHecStreamWriter(SplunkHecWriter, DataSourceStreamWriter):
    def __init__(self, options):
        super().__init__(options)

    def commit(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """Receives a sequence of :class:`WriterCommitMessage` when all write tasks have succeeded, then decides what to do with it."""
        pass

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        """Receives a sequence of :class:`WriterCommitMessage` from successful tasks when some other tasks have failed, then decides what to do with it."""
        pass


class SplunkReader:
    """Base reader class for Splunk export API.

    Shared read logic for batch and streaming reads.
    """

    def __init__(self, options, schema: StructType):
        """Initialize the reader with options and schema.

        Args:
            options: Dictionary of options containing splunkd_url, query, credentials
            schema: StructType schema (provided by DataSource.schema())

        """
        import os

        # Extract and validate required options
        self.splunkd_url = options.get("splunkd_url")
        self.splunkd_token = options.get("splunkd_token") or os.environ.get("SPLUNK_AUTH_TOKEN")

        # Validate required options
        assert self.splunkd_url is not None, "splunkd_url is required"
        assert self.splunkd_token is not None, "splunkd_token is required (or set SPLUNK_AUTH_TOKEN env var)"

        # Build SPL query
        self.spl_query = self._build_spl_query(options)

        # Connection options
        self.auth_scheme = options.get("auth_scheme", "Splunk")
        self.verify_ssl = self._parse_verify_ssl(options.get("verify_ssl", "true"))
        self.connect_timeout = int(options.get("connect_timeout", "10"))
        self.read_timeout = int(options.get("read_timeout", "300"))
        self.max_retries = int(options.get("max_retries", "3"))
        self.initial_backoff = float(options.get("initial_backoff", "1.0"))
        self.output_mode = options.get("output_mode", "json")

        # Error handling mode
        self.mode = options.get("mode", "FAILFAST").upper()

        # Store schema (provided by DataSource.schema())
        self._schema = schema

    def _build_spl_query(self, options):
        """Build SPL query from options."""
        return _build_spl_query_from_options(options)

    def _parse_verify_ssl(self, verify_ssl):
        """Parse verify_ssl option."""
        if isinstance(verify_ssl, str):
            if verify_ssl.lower() == "false":
                return False
            elif verify_ssl.lower() == "true":
                return True
            else:
                # Path to CA bundle
                return verify_ssl
        else:
            return bool(verify_ssl)

    def _create_session(self):
        """Create HTTP session with auth headers."""
        session = Session()
        session.headers.update(
            {
                "Authorization": f"{self.auth_scheme} {self.splunkd_token}",
                "Content-Type": "application/x-www-form-urlencoded",
            }
        )
        session.verify = self.verify_ssl
        return session

    def _execute_export_request(self, session, form_data):
        """Execute export request with retry logic.

        Args:
            session: requests.Session
            form_data: Dict of form parameters

        Returns:
            requests.Response: Streaming response

        """
        export_url = f"{self.splunkd_url.rstrip('/')}/services/search/jobs/export"
        timeout = (self.connect_timeout, self.read_timeout)

        for attempt in range(self.max_retries + 1):
            try:
                response = session.post(export_url, data=form_data, timeout=timeout, stream=True)
                response.raise_for_status()
                return response
            except Exception:
                if attempt < self.max_retries:
                    wait = self.initial_backoff * (2**attempt)
                    time.sleep(wait)
                else:
                    raise

    def _parse_response(self, response):
        """Parse streaming JSON response from export endpoint.

        Args:
            response: requests.Response with streaming=True

        Yields:
            Dict: Parsed result objects

        """
        for line in response.iter_lines(decode_unicode=True):
            if not line.strip():
                continue  # Skip blank lines (keep-alives)

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                if self.mode == "FAILFAST":
                    raise ValueError(f"Invalid JSON: {line[:100]}...") from e
                else:
                    # PERMISSIVE mode: skip invalid JSON
                    continue

            if "result" not in record:
                continue  # Skip non-result records (metadata, preview, etc.)

            yield record["result"]

    def _process_result_to_row(self, result_dict):
        """Convert a Splunk result dict to a Spark Row.

        Args:
            result_dict: Dict containing Splunk event fields

        Returns:
            Row: Spark Row with schema-typed fields

        """
        # Create a mapping of column names to their expected types from schema
        schema_field_map = {field.name: field.dataType for field in self._schema.fields}

        row_dict = {}

        # Process fields that exist in both result and schema
        for field_name, expected_type in schema_field_map.items():
            if field_name in result_dict:
                raw_value = result_dict[field_name]
                try:
                    converted_value = _convert_value_to_schema_type(raw_value, expected_type)
                    row_dict[field_name] = converted_value
                except ValueError as e:
                    if self.mode == "FAILFAST":
                        raise ValueError(f"Column '{field_name}': {e}")
                    else:
                        # PERMISSIVE mode: set to null on conversion error
                        row_dict[field_name] = None
            else:
                # Field in schema but not in result - set to null
                row_dict[field_name] = None

        return Row(**row_dict)

    def read(self, partition: SplunkTimeRangePartition):
        """Read data for the given partition time range.

        Args:
            partition: SplunkTimeRangePartition containing start_epoch and end_epoch

        Yields:
            Row objects from the query results

        """
        # Build form data for export request
        form_data = {
            "search": self.spl_query,
            "earliest_time": str(partition.start_epoch),
            "latest_time": str(partition.end_epoch),
            "output_mode": self.output_mode,
            "exec_mode": "normal",
        }

        # Execute query
        session = self._create_session()
        response = self._execute_export_request(session, form_data)

        # Parse and convert results
        for result in self._parse_response(response):
            yield self._process_result_to_row(result)


class SplunkBatchReader(SplunkReader, DataSourceReader):
    """Batch reader for Splunk export API."""

    def __init__(self, options, schema: StructType):
        """Initialize the batch reader with options and schema.

        Args:
            options: Dictionary of options containing splunkd_url, query, time range, credentials
            schema: StructType schema (provided by DataSource.schema())

        """
        super().__init__(options, schema)

        # Time range options (mutually exclusive)
        timespan = options.get("timespan")
        start_time = options.get("start_time")
        end_time = options.get("end_time")

        # Parse time range
        self.start_time, self.end_time = _parse_time_range(timespan=timespan, start_time=start_time, end_time=end_time)

        # Partitioning options
        self.num_partitions = int(options.get("num_partitions", "1"))
        partition_duration = options.get("partition_duration")
        if partition_duration:
            self.partition_duration = int(partition_duration)
        else:
            self.partition_duration = None

    def partitions(self):
        """Generate list of non-overlapping time range partitions.

        Returns:
            List of SplunkTimeRangePartition objects

        """
        # Convert to epoch seconds for Splunk API
        start_epoch = self.start_time.timestamp()
        end_epoch = self.end_time.timestamp()
        total_duration = end_epoch - start_epoch

        # Determine partition strategy
        if self.partition_duration:
            # Use fixed partition duration
            partitions = []
            current_start = start_epoch
            while current_start < end_epoch:
                current_end = min(current_start + self.partition_duration, end_epoch)
                partitions.append(SplunkTimeRangePartition(current_start, current_end))
                current_start = current_end
            return partitions
        else:
            # Split into N equal partitions
            partition_duration = total_duration / self.num_partitions
            partitions = []
            for i in range(self.num_partitions):
                partition_start = start_epoch + (partition_duration * i)
                partition_end = start_epoch + (partition_duration * (i + 1))

                # Ensure last partition ends exactly at end_epoch (avoid rounding errors)
                if i == self.num_partitions - 1:
                    partition_end = end_epoch

                partitions.append(SplunkTimeRangePartition(partition_start, partition_end))

            return partitions


class SplunkStreamReader(SplunkReader, DataSourceStreamReader):
    """Stream reader for Splunk export API (micro-batch polling).

    Implements incremental streaming by tracking _indextime-based offsets with tie-breaker.
    """

    def __init__(self, options, schema: StructType):
        """Initialize the stream reader with options and schema.

        Args:
            options: Dictionary of options containing splunkd_url, query, start_time, credentials
            schema: StructType schema (provided by DataSource.schema())

        """
        super().__init__(options, schema)

        # Stream-specific options
        start_time = options.get("start_time", "latest")
        # Support 'latest' as alias for current timestamp
        if start_time == "latest":
            self.start_time = int(datetime.now(timezone.utc).timestamp())
        else:
            # Parse ISO 8601 timestamp and convert to epoch seconds
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            self.start_time = int(dt.timestamp())

        # Partition duration in seconds (default 1 hour)
        self.partition_duration = int(options.get("partition_duration", "3600"))

        # Safety lag in seconds (default 60 seconds)
        self.safety_lag_seconds = int(options.get("safety_lag_seconds", "60"))

        # Query validation for streaming
        allow_transforming = options.get("allow_transforming_queries", "false").lower() == "true"
        if not allow_transforming:
            self._validate_query_for_streaming()

    def _validate_query_for_streaming(self):
        """Validate that query is suitable for streaming (non-transforming)."""
        transforming_commands = [
            "stats",
            "chart",
            "timechart",
            "eventstats",
            "streamstats",
            "transaction",
            "top",
            "rare",
        ]
        query_lower = self.spl_query.lower()
        for cmd in transforming_commands:
            if f"| {cmd}" in query_lower or f"|{cmd}" in query_lower:
                raise ValueError(
                    f"Streaming mode requires event-returning queries. "
                    f"Remove transforming commands ({cmd}, etc.) or use batch mode. "
                    f"To allow transforming queries (not recommended), set allow_transforming_queries=true."
                )

    def initialOffset(self):
        """Return the initial offset (start time minus 1 second for tie-breaker logic)."""
        # Subtract 1 second to compensate for +1 second added in partitions()
        adjusted_start = self.start_time - 1
        return SplunkOffset(adjusted_start, "").json()

    def latestOffset(self):
        """Return the latest offset (current time minus safety lag)."""
        current_time = int(datetime.now(timezone.utc).timestamp())
        latest_time = current_time - self.safety_lag_seconds
        return SplunkOffset(latest_time, "").json()

    def partitions(self, start, end):
        """Create partitions for the time range between start and end offsets.

        Args:
            start: JSON string representing SplunkOffset for the start of the range
            end: JSON string representing SplunkOffset for the end of the range

        Returns:
            List of SplunkTimeRangePartition objects

        """
        # Deserialize JSON strings to offset objects
        start_offset = SplunkOffset.from_json(start)
        end_offset = SplunkOffset.from_json(end)

        # Add 1 second to start to prevent overlap with previous batch's end
        start_time = start_offset.indextime_epoch + 1
        end_time = end_offset.indextime_epoch

        # Calculate total duration
        total_duration = end_time - start_time

        # If total duration is less than partition_duration, create a single partition
        if total_duration <= self.partition_duration:
            return [SplunkTimeRangePartition(float(start_time), float(end_time))]

        # Split into fixed-duration partitions
        partitions = []
        current_start = start_time

        while current_start < end_time:
            current_end = min(current_start + self.partition_duration, end_time)
            partitions.append(SplunkTimeRangePartition(float(current_start), float(current_end)))
            current_start = current_end

        return partitions
