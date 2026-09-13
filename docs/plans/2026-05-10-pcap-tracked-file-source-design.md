# PCAP Tracked File Source Design

**Date:** 2026-05-10
**Status:** Design Review Complete
**Use Cases:** Batch ingestion of PCAP files, micro-batch file discovery with checkpointed progress, reusable tracked-file base for other file formats

## Overview

Add a new Python data source for reading PCAP files and converting them into PySpark rows.

The actual PCAP parsing logic already exists and is tested in another project, so this work focuses on:

- **File discovery** from local paths, cloud storage, and Unity Catalog volumes
- **Processed-file tracking** so the same input file is not processed multiple times
- **Reusable file-source base classes** so other file-backed sources can reuse the same tracking behavior

The source should support both:

- **Batch reads**: read all currently unprocessed files from a path
- **Streaming reads**: micro-batch discovery of newly arrived files with checkpoint-based progress

## Goals and Non-Goals

### Goals

- Reuse the existing PCAP parser logic by copying/adapting it into this repository
- Provide a reusable tracked-file base for future sources
- Support both batch and streaming modes
- Persist streaming progress through Spark-checkpointed source offsets
- Work in classic Spark environments and remain compatible with Databricks Serverless constraints

### Non-Goals

- Building a JVM-native Spark data source
- Depending on cloud-specific SDKs such as `boto3`, Google Cloud Storage clients, or Azure storage SDKs
- Supporting mutable input files as a primary ingestion model
- Implementing true continuous streaming; this is micro-batch file discovery

## Architecture

### Recommended Direction

Implement file tracking in this library, not in Spark itself.

Spark's native file source already demonstrates the correct high-level model:

- discover candidate files
- maintain a set of seen files
- persist committed file entries in a metadata log
- restore state from checkpoint after restart

We should reuse that design pattern, but implement it in Python in a way that fits this repository's custom data source architecture.

### Main Components

#### `TrackedFileSourceDataSource`

Top-level `DataSource` subclass that provides:

- `reader()` for batch mode
- `streamReader()` for streaming mode
- shared schema handling contract for file-backed readers

This is the reusable entry point for future file formats.

#### `TrackedFileReader`

Shared base reader for batch and streaming file-backed sources. Responsibilities:

- parse and validate common options
- run file discovery through a pluggable strategy
- decide which files are new and eligible
- create partitions
- support offset-based progress for streaming

This class owns tracking logic, but not file-format parsing.

#### `FileDiscoveryStrategy`

Abstraction for obtaining candidate files and their metadata. It isolates discovery from tracking so the source can adapt to different runtimes.

Two implementations are recommended:

1. **`SparkNativeDiscovery`** (primary)
2. **`HadoopFsDiscovery`** (secondary)

#### `PcapDataSource` / `PcapFileReader`

PCAP-specific source built on top of `TrackedFileReader`. Responsibilities:

- define schema for packet rows
- open a file or binary payload
- call the adapted PCAP parser
- yield parsed rows

### Why Discovery Must Be Pluggable

Using Py4J to call Hadoop `FileSystem` APIs is portable on classic Spark clusters, but may not be reliable or available in Databricks Serverless environments.

Databricks Serverless documentation explicitly says:

- only Spark Connect APIs are supported
- RDD APIs are not supported
- access patterns must align with Unity Catalog and serverless limitations

Because of that, the design should not make JVM gateway access a hard dependency for the source.

## Discovery Strategy

### Primary Strategy: `SparkNativeDiscovery`

This is the default discovery path.

The core idea is to let Spark enumerate files using Spark-supported file access instead of direct Python-side filesystem calls. The most likely implementation path is to use a Spark-native file listing mechanism such as `binaryFile` metadata enumeration with:

- input path
- optional recursive lookup
- optional glob/path filters

Then extract portable metadata:

- `path`
- `modificationTime`
- `length`

This approach is the safest choice for Databricks Serverless because it avoids direct dependence on the PySpark gateway and Hadoop `FileSystem` calls from Python code.

### Secondary Strategy: `HadoopFsDiscovery`

For classic Spark clusters, we can optionally support discovery through Hadoop `FileSystem` APIs using the Spark session's Hadoop configuration.

This strategy can:

- qualify paths consistently
- list directories recursively
- apply glob patterns
- return file metadata efficiently

This should be treated as an optimization or fallback for environments where JVM-side filesystem access is known to be available.

### Storage Portability

The design intentionally avoids cloud-specific SDKs. Portability should come from Spark-supported storage access:

- local filesystem paths
- `s3a://...`
- `gs://...`
- `abfss://...`
- Unity Catalog volumes such as `/Volumes/...`

In practice, "works out of the box" means:

- the runtime already has the required storage connector
- credentials and Unity Catalog permissions are already configured for Spark

That matches the operational model used by Spark's built-in file sources.

## File Identity and Tracking

### File Identity

Default file identity should be the **fully qualified path**.

Each tracked entry should also store:

- `path`
- `modification_time`
- `length`
- `batch_id`
- tracking format version

Do not use filename-only identity by default. Spark supports that mode, but it is too risky for cloud storage paths and multi-directory layouts because files with the same basename may be distinct inputs.

### Processed-File State

Batch reads do not need durable processed-file tracking and remain stateless.

For streaming, the implemented first version uses an offset watermark rather than a separate source metadata log:

- files are ordered by `(modification_time_ms, path)`
- the source offset stores the last processed pair
- restart resumes from the offset stored in the Spark query checkpoint

This avoids a separate `trackingLocation` and works with the currently exposed Python streaming source API.

### Retention and Purging

The offset-only implementation does not create a separate file-entry metadata log, so there is nothing extra to compact or purge in the first version.

## Batch Read Behavior

Batch mode should:

1. discover candidate files
2. apply file-selection filters such as glob and modification time
3. create partitions from the selected files
4. parse the selected files into packet rows

Batch mode should not require any tracking state and should read all currently matching files each time it runs.

## Streaming Read Behavior

Streaming mode should implement micro-batch file discovery, not continuous streaming.

### Offset Model

The streaming offset is file-based rather than time-based.

Implemented shape:

- offset version
- `modification_time_ms`
- `path`

The offset is a stable watermark over files ordered by `(modification_time_ms, path)`.

### Streaming Flow

For each micro-batch:

1. discover current candidate files
2. apply a settling delay so only stable files are eligible
3. select files whose `(modification_time_ms, path)` sort key is greater than the current offset
4. cap the batch by `maxFilesPerBatch` when configured
5. parse those files and emit packet rows
6. rely on Spark to checkpoint the returned end offset

### Settling Delay

Add a source option such as `settling_delay_seconds` for streaming.

This prevents newly uploaded or copied files from being picked up before they are stable. This is especially important for object storage and volume-backed ingestion.

### Trigger Expectations

On Databricks Serverless, the design should assume `Trigger.AvailableNow()` as the preferred mode because:

- serverless does not support processing-time triggers
- documentation recommends `AvailableNow` for serverless streaming workloads

The source should still be valid for standard Spark structured streaming APIs where supported.

## PCAP Parsing Integration

### Reuse Strategy

Copy/adapt the PCAP parser from:

`/Users/ott/development/cybersecurity/spark-cybersecurity-python/cyberspark/sources/pcap.py`

Reasons:

- avoids a runtime dependency on another local project
- keeps this connector self-contained
- allows shaping the parser to this repo's data source conventions

### Integration Style

Keep the parser logic separate from tracking logic.

Implemented split:

- parsing helpers and schema in `cyber_connectors/Pcap.py`
- file discovery, batch reader, and streaming reader helpers in the same file for the first version

The parser should accept either:

- file path input, or
- file bytes input

This keeps it flexible for whichever discovery/read path is simplest in the final implementation.

### Output Schema

Use the existing parser's row model as the initial schema, including fields such as:

- packet timestamp
- source and destination IPs
- source and destination ports
- protocol
- packet length
- parsed protocol-specific substructures where available

The schema should be predefined, not inferred from input files.

## Options

### Common File Source Options

Use Databricks-compatible option names where practical, especially for file-discovery behavior.

- `path` - input path or directory
- `recursiveFileLookup` - whether to recurse into subdirectories
- `pathGlobFilter` - optional glob filter for file selection
- `fileNamePattern` - alias of `pathGlobFilter` for API familiarity
- `modifiedAfter` - only consider files modified after the given timestamp
- `modifiedBefore` - only consider files modified before the given timestamp
- `ignoreCorruptFiles` - skip corrupt or unreadable PCAP files instead of failing the read
- `ignoreMissingFiles` - skip files that disappear after discovery but before parsing
- `maxFilesPerBatch` - optional limit for streaming or large batch runs
- `mode` - `FAILFAST` or `PERMISSIVE`
- `settling_delay_seconds` - source-specific option controlling how long a newly discovered file must age before it is eligible

### Option Semantics

- `modifiedAfter` and `modifiedBefore` filter candidate files by file modification timestamp before processed-file tracking is applied.
- `ignoreCorruptFiles=true` means malformed or unreadable PCAP inputs are skipped with explicit logging.
- `ignoreMissingFiles=true` means files missing at read time are skipped with explicit logging.
- `mode` is retained because it matches existing connector conventions in this repo, but for this source it should align with `ignoreCorruptFiles` behavior rather than promise full Spark parser-mode parity.
- These options should be documented as **Databricks-compatible where applicable**, not as a strict guarantee of identical Auto Loader internals.

### PCAP-Specific Options

Keep the initial PCAP-specific option set small. Add only what is clearly needed, for example:

- compression handling if gzipped PCAP files are expected
- parser mode toggles only if the existing parser already needs them

## Error Handling

### Discovery Errors

Fail fast on:

- invalid paths
- storage permission failures
- unsupported path access in the current runtime

These are infrastructure errors and should not be hidden.

### Corrupt PCAP Files

Support both compatibility flags and parser behavior:

- **default behavior**: fail the read on the first unreadable file
- **`ignoreCorruptFiles=true`**: skip the bad file and continue
- **`mode=PERMISSIVE`**: same effective behavior as `ignoreCorruptFiles=true`

Skipped corrupt files must be logged explicitly. The source should not silently swallow corruption.

### Missing Files

If a file was discovered but no longer exists when a partition tries to read it:

- **default behavior**: fail the read
- **`ignoreMissingFiles=true`**: skip the missing file and continue

### Checkpoint and Offset Errors

Fail fast if the source offset is malformed or if the query checkpoint cannot be used to restore progress.

## Testing Strategy

### Unit Tests for Tracking Base

Test the reusable tracking logic independently from PCAP parsing:

- offset round-trip serialization
- candidate filtering against modification-time constraints
- partition planning
- offset ordering behavior
- settling delay behavior
- restart behavior with an existing checkpoint

### Unit Tests for PCAP Reader

Test PCAP-specific behavior with small sample files:

- normal PCAP parsing
- empty files
- corrupted files
- gzipped PCAP if supported

### Integration-Style Tests

Add end-to-end tests for:

- batch read from local files
- streaming read with `availableNow`
- no reprocessing after restart from the same checkpoint
- incremental processing when new files arrive between runs

Where direct serverless testing is not practical in this repository, design the tests so the `SparkNativeDiscovery` path is still exercised as much as possible.

## Work Estimate

Estimated effort for a first solid implementation:

| Scope | Estimate |
|---|---|
| Base tracked-file abstraction and common options | 2-3 days |
| `SparkNativeDiscovery` prototype and tests | 1-2 days |
| `HadoopFsDiscovery` optional support | 1 day |
| PCAP parser adaptation and connector integration | 1-2 days |
| Streaming semantics, restart behavior, polish | 2-3 days |
| Documentation and test stabilization | 1 day |

Total estimate: **about 1 to 2 weeks** of focused work.

## Spark-Side vs Library-Side Tracking

### Library-Side Tracking (Recommended)

**Pros:**

- matches the current repository architecture
- reusable for future file-backed sources
- much lower implementation and maintenance cost
- does not require maintaining Spark internals or JVM extensions

**Cons:**

- we must implement file-tracking metadata logic ourselves in Python
- native Spark file-source behavior cannot be reused directly

### Spark-Side Tracking

This would mean implementing tracking in Spark internals or as a JVM-native source.

**Pros:**

- closest to built-in Spark file source semantics

**Cons:**

- much higher complexity
- cross-language maintenance burden
- version coupling to Spark internals
- poor fit for this repository

For this project, Spark-side tracking is not worth the cost.

## Recommendation

Implement a reusable **tracked file source base** in Python with:

- **Serverless-compatible Spark-native discovery as the primary strategy**
- **offset-only streaming progress through Spark checkpoints**
- **PCAP parsing adapted from the existing tested parser**

This provides the best balance of correctness, portability, simplicity, and reuse for future file-backed cybersecurity data sources.

## References

- Existing parser: `spark-cybersecurity-python/cyberspark/sources/pcap.py`
- Existing connector patterns in this repo: `cyber_connectors/Splunk.py`, `cyber_connectors/MsSentinel.py`
- Spark file source tracking concepts:
  - `FileStreamSource.scala`
  - `FileStreamSourceLog.scala`
  - `CompactibleFileStreamLog.scala`
- Databricks references:
  - PySpark custom data sources
  - Serverless compute limitations
