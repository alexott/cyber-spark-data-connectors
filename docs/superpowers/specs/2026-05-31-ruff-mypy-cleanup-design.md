# Ruff and Mypy Cleanup Design

**Date:** 2026-05-31
**Status:** Approved for implementation
**Topic:** Fix existing Ruff and mypy failures in the connector package

## Overview

Resolve the current `ruff check` and `mypy` failures in `cyber_connectors/` with focused code and configuration
changes. Prefer real code fixes for genuine typing and style issues, and use narrow exceptions only where external
APIs or established public module names conflict with Ruff or mypy expectations.

## Goals

1. Make `uv run ruff check cyber_connectors/` pass
2. Make `uv run mypy cyber_connectors/` pass
3. Preserve current connector behavior and public package usage
4. Keep exceptions targeted and justified rather than broadly suppressing checks

## Non-Goals

- Refactoring connector behavior unrelated to the reported failures
- Changing dependencies unless a typing requirement makes that unavoidable
- Reworking the package structure beyond what is necessary for check compliance
- Expanding lint or type-check coverage beyond the current `cyber_connectors/` scope

## Evaluated Approaches

### 1. Balanced cleanup with narrow exceptions (**selected**)

Fix genuine typing/style issues in code and reserve targeted Ruff or mypy exceptions for Spark API naming,
optional platform imports, or stable public module names.

**Why selected:**

- improves code quality instead of hiding real issues
- avoids risky public-surface churn for style-only concerns
- fits the repository's preference for simple, focused changes

### 2. Suppression-heavy cleanup

Add per-file ignores and type suppressions for most failures while changing as little code as possible.

This is faster but leaves real problems in place and makes the checks less meaningful.

### 3. Broad normalization refactor

Rename modules, reshape interfaces, and refactor code aggressively until it aligns with all linter and typing
preferences.

This is cleaner in theory but introduces unnecessary churn and compatibility risk for a library package.

## Detailed Design

### Scope and Boundaries

The cleanup stays focused on the files that currently fail checks:

- `cyber_connectors/common.py`
- `cyber_connectors/RestApi.py`
- `cyber_connectors/Splunk.py`
- `cyber_connectors/MsSentinel.py`

Additional changes are allowed only where they directly support those fixes, such as a narrow Ruff configuration
adjustment or an export-preserving package change.

### Code Cleanup Strategy

`common.py` will receive the type-oriented fixes: explicit signatures, safer return types, and clearer typing around
HTTP session helpers and JSON encoding support.

`RestApi.py`, `Splunk.py`, and `MsSentinel.py` will receive focused fixes for:

- untyped or loosely typed payload assembly
- optional values that need explicit narrowing
- exception handling patterns Ruff flags
- SDK or service interactions that need clearer annotations for mypy

### Targeted Exceptions

If Spark's Python DataSource API requires camelCase method names such as `streamReader`, `streamWriter`,
`initialOffset`, `latestOffset`, or parameter names like `batchId`, those names will be preserved and handled with
narrow Ruff exceptions rather than forced renames.

For the mixed-case `MsSentinel.py` filename, the preferred path is to preserve compatibility. If renaming would
change the public surface or require a compatibility shim only to satisfy style rules, use a targeted Ruff exception
for that file instead of renaming it.

For optional platform-specific imports such as `databricks.service_credentials`, prefer an explicit typing-safe import
pattern or narrow mypy handling over broad `ignore_errors` configuration.

### Validation

Implementation is complete when:

1. `uv run ruff check cyber_connectors/` passes
2. `uv run mypy cyber_connectors/` passes
3. Existing tests still pass after the cleanup
4. Any new ignore or exception is narrowly scoped and directly justified by framework or compatibility constraints
