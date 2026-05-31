# Python 3.10 Minimum Version Design

**Date:** 2026-05-31
**Status:** Approved for implementation
**Topic:** Raise the project's minimum supported Python version from 3.9 to 3.10

## Overview

Raise the repository's minimum supported Python version to 3.10 and update related version declarations so the
project communicates a single, consistent Python floor across packaging, linting, and contributor guidance.

## Goals

1. Change package metadata to require Python 3.10 or newer
2. Align tooling configuration with the new minimum version
3. Update contributor-facing documentation that states the supported Python range
4. Keep the change narrow and avoid unrelated code or dependency changes

## Non-Goals

- Changing runtime connector behavior
- Updating dependency versions
- Expanding or shrinking the CI matrix beyond what already exists
- Refactoring documentation unrelated to Python version support

## Evaluated Approaches

### 1. Update metadata, tooling, and docs together (**selected**)

Apply the Python 3.10 minimum consistently in package metadata, Ruff target configuration, and repository guidance.

**Why selected:**

- avoids conflicting version declarations
- matches the user's request to bump the project minimum version
- keeps the change small while making the repository internally consistent

### 2. Update only package metadata

This is the smallest possible change, but it leaves the repository documenting and linting against older Python
versions.

### 3. Update metadata and docs, but not tooling

This removes some inconsistency, but still leaves Ruff targeting an older Python baseline than the package
declares.

## Detailed Design

### Packaging

Update `pyproject.toml` so `requires-python` becomes `>=3.10,<3.15`.

### Tooling

Update Ruff's `target-version` in `pyproject.toml` from `py39` to `py310` so linting rules align with the new
minimum version.

### Documentation

Update repository guidance in `AGENTS.md` so the documented supported Python range becomes `3.10-3.14`.

### CI

No GitHub Actions changes are required for this bump because the CI matrix already starts at Python 3.10.

## Validation

Implementation is complete when:

1. `pyproject.toml` requires Python 3.10 or newer
2. Ruff targets Python 3.10
3. `AGENTS.md` no longer claims Python 3.9 support
4. Existing repository checks still run under the current configuration
