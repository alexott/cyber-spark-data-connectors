# PCAP Data Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `pcap` Python data source that reads PCAP files into Spark rows in batch mode and micro-batch streaming mode, using Spark-native file discovery and offset-only streaming progress.

**Architecture:** Implement the PCAP connector in one new module, `cyber_connectors/Pcap.py`, and keep the first version simple: use Spark's `binaryFile` source on the driver for discovery and content loading, then parse PCAP bytes in Python using adapted parser code. Batch reads are stateless; streaming tracks progress only through Spark-checkpointed offsets ordered by `(modification_time_ms, path)` so there is no separate reader-side state location.

**Tech Stack:** Python, PySpark DataSource API, Spark `binaryFile`, dpkt, pytest, pytest-spark, Ruff, mypy

---

## File Structure

**Create**
- `cyber_connectors/Pcap.py` - PCAP schema, parser helpers, batch reader, streaming reader, offset helpers, and `PcapDataSource`
- `tests/test_pcap.py` - unit tests for parser, batch reader option parsing, and file filtering
- `tests/test_pcap_stream_reader.py` - unit tests for offset ordering, batch selection, and streaming reader behavior

**Modify**
- `pyproject.toml` - add `dpkt` dependency
- `cyber_connectors/__init__.py` - export `PcapDataSource`
- `README.md` - add usage examples and supported options for the new `pcap` source
- `docs/plans/2026-05-10-pcap-tracked-file-source-design.md` - sync design notes with the implementation choices (stateless batch, no reader-side `checkpointLocation`, offset-only streaming)

## Implementation Notes

- Stick to **one new connector module** for the first version. Do not introduce a generic tracked-file framework yet; keep helpers in `cyber_connectors/Pcap.py` until there are at least two connectors using them.
- Use Spark-native discovery via `spark.read.format("binaryFile")...load(...)` so the first version works with local files, UC volumes, and supported cloud storage without provider-specific SDKs.
- Because Python custom stream readers do **not** receive a Spark-managed metadata path, implement streaming progress as an **offset watermark**:
  - sort eligible files by `(modification_time_ms, path)`
  - store the last processed pair in the offset
  - on each batch, select files strictly greater than the last processed file and up to the new end offset
- Keep imports inside methods where code runs on executors.
- Use Databricks-compatible common option names where possible: `recursiveFileLookup`, `pathGlobFilter`, `fileNamePattern`, `modifiedAfter`, `modifiedBefore`, `ignoreCorruptFiles`, `ignoreMissingFiles`, `maxFilesPerBatch`.

### Task 1: Add the dependency, parser core, and source registration

**Files:**
- Modify: `pyproject.toml`
- Create: `cyber_connectors/Pcap.py`
- Modify: `cyber_connectors/__init__.py`
- Test: `tests/test_pcap.py`

- [ ] **Step 1: Write the failing parser and registration tests**

```python
from datetime import timezone

import pytest
from pyspark.sql.types import StructType

from cyber_connectors import PcapDataSource
from cyber_connectors.Pcap import (
    PCAP_SCHEMA,
    _open_pcap_bytes,
    _parse_pcap_content,
)


class TestPcapDataSource:
    def test_name(self):
        assert PcapDataSource.name() == "pcap"

    def test_schema_is_struct_type(self):
        ds = PcapDataSource(options={})
        assert isinstance(ds.schema(), StructType)


class TestPcapParser:
    def test_parse_empty_content_returns_no_rows(self):
        assert list(_parse_pcap_content(b"")) == []

    def test_open_pcap_bytes_handles_gzip(self, gzip_pcap_bytes):
        rows = list(_parse_pcap_content(gzip_pcap_bytes))
        assert rows
        assert rows[0][0].tzinfo == timezone.utc
```

- [ ] **Step 2: Run the targeted test file to verify it fails**

Run: `poetry run pytest tests/test_pcap.py -v`

Expected: FAIL with import errors for `PcapDataSource`, `PCAP_SCHEMA`, or parser helpers.

- [ ] **Step 3: Add `dpkt`, create the parser module, and export the source**

Run: `poetry add dpkt`

Add the new export:

```python
from cyber_connectors.MsSentinel import AzureMonitorDataSource, MicrosoftSentinelDataSource
from cyber_connectors.Pcap import PcapDataSource
from cyber_connectors.RestApi import RestApiDataSource
from cyber_connectors.Splunk import SplunkDataSource
```

Start `cyber_connectors/Pcap.py` with the schema and parser helpers adapted from the existing project:

```python
import gzip
import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

import dpkt
from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceStreamReader, InputPartition
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


PCAP_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("src_ip", StringType(), nullable=False),
        StructField("dst_ip", StringType(), nullable=False),
        StructField("src_port", IntegerType(), nullable=True),
        StructField("dst_port", IntegerType(), nullable=True),
        StructField("protocol", StringType(), nullable=False),
        StructField("pkt_len", IntegerType(), nullable=False),
    ]
)


def _open_pcap_bytes(content: bytes):
    if not content:
        return None
    raw = gzip.decompress(content) if content[:2] == b"\x1f\x8b" else content
    return dpkt.pcap.Reader(io.BytesIO(raw))


def _parse_pcap_content(content: bytes) -> Iterator[tuple]:
    reader = _open_pcap_bytes(content)
    if reader is None:
        return iter(())
    return _parse_pcap_reader(reader)


class PcapDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "pcap"

    def schema(self) -> StructType:
        return PCAP_SCHEMA
```

Keep the rest of the parser logic as close as possible to the existing `cyberspark/sources/pcap.py`.

- [ ] **Step 4: Run the parser tests again**

Run: `poetry run pytest tests/test_pcap.py -v`

Expected: PASS for source registration and parser smoke tests.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml poetry.lock cyber_connectors/Pcap.py cyber_connectors/__init__.py tests/test_pcap.py
git commit -m "feat: add pcap parser and datasource registration"
```

### Task 2: Implement stateless batch reading with Databricks-style file options

**Files:**
- Modify: `cyber_connectors/Pcap.py`
- Test: `tests/test_pcap.py`

- [ ] **Step 1: Write the failing batch-reader tests**

```python
from datetime import datetime, timezone

from cyber_connectors.Pcap import (
    PcapBatchReader,
    _build_binary_file_dataframe,
    _filter_file_entries,
)


def test_filter_file_entries_respects_modified_after():
    entries = [
        {"path": "file:///a.pcap", "modification_time_ms": 1000, "length": 10},
        {"path": "file:///b.pcap", "modification_time_ms": 2000, "length": 10},
    ]
    filtered = _filter_file_entries(
        entries,
        modified_after=datetime.fromtimestamp(1.5, tz=timezone.utc),
        modified_before=None,
    )
    assert [entry["path"] for entry in filtered] == ["file:///b.pcap"]


def test_batch_reader_parses_common_file_options(sample_schema):
    reader = PcapBatchReader(
        {
            "path": "/tmp/pcaps",
            "recursiveFileLookup": "true",
            "pathGlobFilter": "*.pcap",
            "fileNamePattern": "*.pcap.gz",
            "modifiedAfter": "2024-01-01T00:00:00Z",
            "ignoreCorruptFiles": "true",
            "ignoreMissingFiles": "true",
        },
        sample_schema,
    )
    assert reader.recursive_file_lookup is True
    assert reader.file_pattern == "*.pcap.gz"
    assert reader.ignore_corrupt_files is True
    assert reader.ignore_missing_files is True
```

- [ ] **Step 2: Run the batch-reader tests to verify they fail**

Run: `poetry run pytest tests/test_pcap.py -k "filter_file_entries or batch_reader" -v`

Expected: FAIL because the batch reader and option helpers are not implemented yet.

- [ ] **Step 3: Implement the batch reader and discovery helpers**

Add batch file discovery helpers that use Spark's `binaryFile` source on the driver:

```python
def _build_binary_file_dataframe(options: dict[str, str]):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("An active SparkSession is required for PCAP file discovery")

    path = options.get("path")
    if not path:
        raise ValueError("Option 'path' is required")

    reader = spark.read.format("binaryFile")
    for key in ("recursiveFileLookup", "pathGlobFilter"):
        value = options.get(key)
        if value is not None:
            reader = reader.option(key, value)
    if options.get("fileNamePattern") and not options.get("pathGlobFilter"):
        reader = reader.option("pathGlobFilter", options["fileNamePattern"])

    return reader.load(path).select("path", "modificationTime", "length", "content")


def _filter_file_entries(entries, modified_after, modified_before):
    filtered = []
    for entry in entries:
        ts = entry["modification_time_ms"]
        if modified_after is not None and ts <= int(modified_after.timestamp() * 1000):
            continue
        if modified_before is not None and ts >= int(modified_before.timestamp() * 1000):
            continue
        filtered.append(entry)
    return filtered


class PcapBatchReader(DataSourceReader):
    def __init__(self, options, schema: StructType):
        self.options = options
        self.schema = schema
        self.ignore_corrupt_files = options.get("ignoreCorruptFiles", "false").lower() == "true"
        self.ignore_missing_files = options.get("ignoreMissingFiles", "false").lower() == "true"
        self.recursive_file_lookup = options.get("recursiveFileLookup", "false").lower() == "true"
        self.file_pattern = options.get("fileNamePattern") or options.get("pathGlobFilter")
        self.modified_after = _parse_optional_timestamp(options.get("modifiedAfter"))
        self.modified_before = _parse_optional_timestamp(options.get("modifiedBefore"))

    def partitions(self):
        rows = _build_binary_file_dataframe(self.options).collect()
        entries = [
            {
                "path": row.path,
                "modification_time_ms": int(row.modificationTime.timestamp() * 1000),
                "length": row.length,
                "content": bytes(row.content),
            }
            for row in rows
        ]
        selected = _filter_file_entries(entries, self.modified_after, self.modified_before)
        return [PcapInputPartition(entry) for entry in selected]
```

Also wire `reader()` in `PcapDataSource`:

```python
def reader(self, schema: StructType) -> DataSourceReader:
    return PcapBatchReader(self.options, schema)
```

- [ ] **Step 4: Run the batch-reader tests again**

Run: `poetry run pytest tests/test_pcap.py -k "filter_file_entries or batch_reader" -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cyber_connectors/Pcap.py tests/test_pcap.py
git commit -m "feat: implement stateless pcap batch reader"
```

### Task 3: Implement offset-only streaming progress and micro-batch selection

**Files:**
- Modify: `cyber_connectors/Pcap.py`
- Create: `tests/test_pcap_stream_reader.py`

- [ ] **Step 1: Write the failing streaming-reader tests**

```python
from cyber_connectors.Pcap import PcapStreamOffset, _select_stream_batch


def test_stream_offset_roundtrip():
    offset = PcapStreamOffset(1704067200000, "file:///b.pcap")
    restored = PcapStreamOffset.from_json(offset.json())
    assert restored.modification_time_ms == 1704067200000
    assert restored.path == "file:///b.pcap"


def test_select_stream_batch_uses_modification_time_then_path():
    entries = [
        {"path": "file:///b.pcap", "modification_time_ms": 2000, "length": 10},
        {"path": "file:///a.pcap", "modification_time_ms": 2000, "length": 10},
        {"path": "file:///c.pcap", "modification_time_ms": 3000, "length": 10},
    ]
    start = PcapStreamOffset(2000, "file:///a.pcap")
    selected = _select_stream_batch(entries, start, max_files_per_batch=1)
    assert [entry["path"] for entry in selected] == ["file:///b.pcap"]
```

- [ ] **Step 2: Run the streaming-reader tests to verify they fail**

Run: `poetry run pytest tests/test_pcap_stream_reader.py -v`

Expected: FAIL because the offset and selection helpers do not exist yet.

- [ ] **Step 3: Implement the stream offset, stream reader, and deterministic batch planning**

Add the offset and helper:

```python
@dataclass
class PcapStreamOffset:
    modification_time_ms: int
    path: str
    version: int = 1

    def json(self) -> str:
        return json.dumps(
            {
                "version": self.version,
                "modification_time_ms": self.modification_time_ms,
                "path": self.path,
            }
        )

    @staticmethod
    def from_json(value: str | dict) -> "PcapStreamOffset":
        data = json.loads(value) if isinstance(value, str) else value
        return PcapStreamOffset(
            modification_time_ms=int(data["modification_time_ms"]),
            path=data["path"],
            version=int(data.get("version", 1)),
        )
```

Select files in strict order:

```python
def _entry_sort_key(entry):
    return (entry["modification_time_ms"], entry["path"])


def _select_stream_batch(entries, start_offset: PcapStreamOffset, max_files_per_batch: int | None):
    ordered = sorted(entries, key=_entry_sort_key)
    selected = [
        entry
        for entry in ordered
        if _entry_sort_key(entry) > (start_offset.modification_time_ms, start_offset.path)
    ]
    if max_files_per_batch:
        return selected[:max_files_per_batch]
    return selected
```

Implement the stream reader using offsets only:

```python
class PcapStreamReader(DataSourceStreamReader):
    def __init__(self, options, schema: StructType):
        self.options = options
        self.schema = schema
        self.max_files_per_batch = int(options["maxFilesPerBatch"]) if options.get("maxFilesPerBatch") else None
        self.settling_delay_seconds = int(options.get("settling_delay_seconds", "0"))

    def initialOffset(self):
        return PcapStreamOffset(0, "").json()

    def latestOffset(self):
        entries = _discover_file_entries(self.options, include_content=False)
        eligible = _apply_settling_delay(entries, self.settling_delay_seconds)
        start = getattr(self, "_last_start_offset", PcapStreamOffset(0, ""))
        selected = _select_stream_batch(eligible, start, self.max_files_per_batch)
        if not selected:
            return start.json()
        last = selected[-1]
        return PcapStreamOffset(last["modification_time_ms"], last["path"]).json()

    def partitions(self, start, end):
        start_offset = PcapStreamOffset.from_json(start)
        end_offset = PcapStreamOffset.from_json(end)
        if (end_offset.modification_time_ms, end_offset.path) <= (start_offset.modification_time_ms, start_offset.path):
            return []
        entries = _discover_file_entries(self.options, include_content=True)
        eligible = [
            entry for entry in sorted(entries, key=_entry_sort_key)
            if (start_offset.modification_time_ms, start_offset.path) < _entry_sort_key(entry) <= (end_offset.modification_time_ms, end_offset.path)
        ]
        return [PcapInputPartition(entry) for entry in eligible]

    def commit(self, end):
        pass
```

Wire `streamReader()` in `PcapDataSource`:

```python
def streamReader(self, schema: StructType) -> DataSourceStreamReader:
    return PcapStreamReader(self.options, schema)
```

- [ ] **Step 4: Run the streaming tests**

Run: `poetry run pytest tests/test_pcap_stream_reader.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cyber_connectors/Pcap.py tests/test_pcap_stream_reader.py
git commit -m "feat: implement pcap stream reader offsets"
```

### Task 4: Add end-to-end tests, docs, and validation

**Files:**
- Modify: `tests/test_pcap.py`
- Modify: `tests/test_pcap_stream_reader.py`
- Modify: `README.md`
- Modify: `docs/plans/2026-05-10-pcap-tracked-file-source-design.md`

- [ ] **Step 1: Add failing end-to-end tests for batch and streaming**

```python
def test_pcap_batch_read_with_local_files(spark):
    spark.dataSource.register(PcapDataSource)
    df = spark.read.format("pcap").load("tests/samples/pcap")
    assert df.count() > 0
    assert set(df.columns) >= {"timestamp", "src_ip", "dst_ip", "protocol"}


def test_pcap_stream_available_now_does_not_reprocess_files(spark, tmp_path):
    spark.dataSource.register(PcapDataSource)
    input_dir = tmp_path / "pcaps"
    input_dir.mkdir()
    checkpoint_dir = tmp_path / "checkpoint"
    sample = Path("tests/samples/pcap/sample.pcap")
    shutil.copy(sample, input_dir / "sample-1.pcap")

    df = spark.readStream.format("pcap").option("maxFilesPerBatch", "1").load(str(input_dir))
    rows_per_batch = []

    def capture(batch_df, batch_id):
        rows_per_batch.append((batch_id, batch_df.count()))

    q = df.writeStream.foreachBatch(capture).option("checkpointLocation", str(checkpoint_dir)).trigger(availableNow=True).start()
    q.awaitTermination()

    q2 = df.writeStream.foreachBatch(capture).option("checkpointLocation", str(checkpoint_dir)).trigger(availableNow=True).start()
    q2.awaitTermination()

    assert rows_per_batch == [(0, rows_per_batch[0][1])]
```

- [ ] **Step 2: Run the end-to-end tests to verify they fail**

Run: `poetry run pytest tests/test_pcap.py tests/test_pcap_stream_reader.py -k "local_files or does_not_reprocess" -v`

Expected: FAIL because the tests need sample files, end-to-end wiring, or streaming replay behavior fixes.

- [ ] **Step 3: Finish the connector, add docs, and sync the design note**

Document the source in `README.md`:

```markdown
### PCAP data source

The `pcap` data source reads packet capture files and converts them into one Spark row per packet.

```python
from cyber_connectors import PcapDataSource
spark.dataSource.register(PcapDataSource)

df = spark.read.format("pcap") \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "*.pcap") \
    .load("/Volumes/security/raw/pcap/")
```

Streaming usage:

```python
stream_df = spark.readStream.format("pcap") \
    .option("maxFilesPerBatch", "10") \
    .load("/Volumes/security/raw/pcap/")

query = stream_df.writeStream \
    .option("checkpointLocation", "/Volumes/security/checkpoints/pcap-job") \
    .trigger(availableNow=True) \
    .start()
```
```

Update the design doc to note the implemented compromise:

```markdown
- batch reads are stateless
- streaming progress is stored in Spark query checkpoints via the source offset only
- the first implementation uses Spark-native `binaryFile` discovery/content loading and does not add Hadoop `FileSystem` fallback yet
```

- [ ] **Step 4: Run validation commands**

Run:

```bash
poetry run pytest tests/test_pcap.py tests/test_pcap_stream_reader.py -v
poetry run pytest
poetry run ruff check cyber_connectors/
poetry run mypy cyber_connectors/
```

Expected:
- targeted PCAP tests PASS
- full test suite PASS
- Ruff reports no issues
- mypy reports no issues in `cyber_connectors/`

- [ ] **Step 5: Commit**

```bash
git add README.md docs/plans/2026-05-10-pcap-tracked-file-source-design.md tests/test_pcap.py tests/test_pcap_stream_reader.py
git commit -m "feat: add pcap data source"
```

## Self-Review

- **Spec coverage:** This plan covers parser reuse, batch reading, streaming reading without a separate `trackingLocation`, Databricks-compatible file options, Spark-native discovery, tests, and docs.
- **Placeholder scan:** No `TODO`, `TBD`, or "similar to above" placeholders remain. Each task has named files, concrete code, and concrete commands.
- **Type consistency:** The plan consistently uses `PcapDataSource`, `PcapBatchReader`, `PcapStreamReader`, `PcapStreamOffset`, and `PcapInputPartition`.
