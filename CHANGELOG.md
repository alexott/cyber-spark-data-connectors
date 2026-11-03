# Changelog

All notable changes to this project are documented in this file.  The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).  Releases are published to
[PyPI](https://pypi.org/project/cyber-spark-data-connectors/) and
[GitHub](https://github.com/alexott/cyber-spark-data-connectors/releases).

## [0.0.9] - 2026-11-03

### All writers

- **Timestamps are sent in UTC**: Spark passes `TIMESTAMP` values to Python as naive datetimes in the local time
  zone of the Python process.  The writers sent them as ISO strings without an offset, so on a machine whose
  time zone is not UTC every timestamp was shifted by the UTC offset (for example, `TimeGenerated` in Log Analytics
  arrived 1 hour in the future from a CET machine, and time-ranged queries did not show the rows).  Datetimes in
  Microsoft Sentinel / Azure Monitor payloads, Splunk event bodies and REST API payloads, query parameters and form
  data are now converted to UTC and sent with the `+00:00` offset.  Clusters that run in UTC (the Databricks
  default) were not affected, except for the new offset in the strings.  The Splunk HEC `time` value was already
  correct.  `TIMESTAMP_NTZ` values are also naive, so they are treated as local time too.

### Splunk

- **Errors fail the HEC write**: a non-2xx response from HEC (for example, an invalid token, a wrong index or a
  payload that is too large) now raises an error with Splunk's response text.  Before, the writer printed the
  status and continued, so data was lost silently, and a streaming query still moved its checkpoint forward.
  Retries for 429/5xx responses are unchanged (#195).
- **Nested columns as JSON objects**: struct columns (and arrays of structs) are now sent as JSON objects.
  Before, they were sent as JSON arrays without field names (#195).
- New write options (#195).  All are off by default, so the existing behavior does not change:
  - `use_ack`, `ack_timeout`, `ack_poll_interval`: HEC indexer acknowledgement.  The write finishes only after
    Splunk confirms that all requests are indexed.  The HEC token must have indexer acknowledgement enabled.
  - `max_payload_bytes`: also send the buffer before the request body becomes larger than this size, to stay
    below the HEC `max_content_length` limit.
  - `remove_time_column`: do not repeat `time_column` in the event body (it is still sent as HEC `time`).
  - `host_column`, `source_column`: take `host`/`source` from a column for each event.  A null value falls back
    to the static `host`/`source` option.
- Delivery is at-least-once in all modes: a retried task or micro-batch can send the same events again.

### Microsoft Sentinel / Azure Monitor

- **Nested columns as JSON objects**: struct columns (and arrays of structs) are now sent as JSON objects, so
  `dynamic` columns in Log Analytics get the field names.  Before, they were sent as JSON arrays.

### REST API

- **Errors fail the write**: a non-2xx response now raises an error with the response text.  Before, the writer
  printed the status and continued, so rows were lost silently, and a streaming query still moved its
  checkpoint forward.  Retries for 429/5xx responses are unchanged.
- **Nested columns**: struct columns (and arrays of structs) are now sent as JSON objects in JSON payloads.  In
  form data and URL query parameters, nested values (structs, arrays, maps) are now sent as JSON strings,
  instead of the Python `Row(...)` text.

### Build, packaging & tooling

- Fixed the Dependabot configuration for uv.
- Development dependency updates only.  The runtime dependencies are unchanged.  Tests now use PySpark 4.2.1.

## [0.0.8] - 2026-07-23

### Microsoft Sentinel / Azure Monitor

- Fixed data-loss and correctness issues in the streaming reader for high-volume tables (#172):
  - **Bounded catch-up window** (`max_catchup_seconds`): a large backlog is now read across many micro-batches,
    instead of one very large batch that could exhaust cluster memory and crash-loop.
  - **Safety lag** (`safety_lag_seconds`): `latestOffset()` now stays behind "now", so data that Log Analytics
    has not ingested yet is not lost.
  - **Result-size-cap detection** (`result_size_limit`): truncated `SUCCESS` responses are now detected and the
    time range is split, instead of silently dropping rows.
  - **Half-open read boundaries**: each streaming partition uses an explicit `col >= start AND col < end`
    filter, so partitions and micro-batches are contiguous and every row is read exactly once, also when many
    rows have the same timestamp.
  - **Case-insensitive column collisions** (`deduplicate_column_case`): columns whose names differ only in case
    (for example, `EventTimestamp_s` and `eventTimestamp_s`), which Delta cannot store, are now renamed
    consistently in schema inference and in reads.
  - Clearer handling of `earliest`/`latest` in schema inference.  Batch reads now reject them with a clear
    message.
- **Unity Catalog service credentials** for authentication (#121, resolves #103, #104).  Schema inference with
  UC credentials logs a warning.
- **Direct queries of Azure resources** without Log Analytics (#136, resolves #132).
- `earliest` as a value for the `start_time` option (#133).

### REST API

- Send data as query parameters, in addition to the request body (#150, resolves #123).

### Build, packaging & tooling

- Migrated packaging and CI (GitHub Actions) to **uv**.
- Minimum Python version is now **3.10**.
- Ruff and mypy cleanup.
- PySpark 4.1.2 and updated core dependencies (protobuf, urllib3, azure-core, azure-identity, pyarrow).

## [0.0.7] - 2026-01-04

### Splunk

- **Splunk reader** with batch and streaming support, using the Splunk export API (#125).

### Microsoft Sentinel / Azure Monitor

- Additional functions for Lakeflow Connect integration (#126).

### Build, packaging & tooling

- Automated publishing to PyPI with a release workflow.

## [0.0.6] - 2025-12-31

- Refined the project file and README for publishing.

## [0.0.5] - 2025-12-31

### Microsoft Sentinel / Azure Monitor

- **Batch reads** from Log Analytics (#107).
- **Streaming reads**, with non-overlapping micro-batches.
- Support for Azure Government (GovCloud) and Azure China clouds.
- Graceful handling of too many results and of throttling.
- Schema inference only when it is enabled (enabled by default).
- Fixed the stream format name in the documentation (`ms-sentinel`, not `splunk`) (#89).

### REST API

- Send data as form data.
- Set arbitrary HTTP headers.

### Build, packaging & tooling

- The package is available on [PyPI](https://pypi.org/project/cyber-spark-data-connectors/).
- Unit tests for all data sources (#99) and a GitHub Actions workflow.

## [0.0.4] - 2025-05-09

- Allow all Python 3.12 versions (`<3.13`).
- Applied `ruff` and `mypy` and fixed the issues they found.
- Added Dependabot configuration.

## [0.0.3] - 2024-11-30

- Retries for the Splunk and REST API data sources.
- Documentation for the classes.

## [0.0.2] - 2024-11-29

- Initial release: batch and streaming writers for Splunk (HEC), Microsoft Sentinel / Azure Monitor, and a
  simple REST API writer.

[0.0.9]: https://github.com/alexott/cyber-spark-data-connectors/compare/v0.0.8...v0.0.9
[0.0.8]: https://github.com/alexott/cyber-spark-data-connectors/compare/v0.0.7...v0.0.8
[0.0.7]: https://github.com/alexott/cyber-spark-data-connectors/compare/v0.0.6...v0.0.7
[0.0.6]: https://github.com/alexott/cyber-spark-data-connectors/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/alexott/cyber-spark-data-connectors/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/alexott/cyber-spark-data-connectors/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/alexott/cyber-spark-data-connectors/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/alexott/cyber-spark-data-connectors/releases/tag/v0.0.2
