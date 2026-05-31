# UV Migration Design

**Date:** 2026-05-31
**Status:** Approved for implementation
**Topic:** Replace Poetry with `uv` across local development, packaging, and GitHub Actions

## Overview

Migrate the project from Poetry-based dependency management and build tooling to a `uv`-based workflow.
The migration will fully remove Poetry from project metadata, contributor workflows, and GitHub Actions while
preserving the existing package layout, test/lint/type-check tools, and release outputs.

## Current State

The repository currently uses a mixed packaging setup:

- `pyproject.toml` stores package metadata and dependencies under `tool.poetry`
- `pyproject.toml` already contains a partial `project` table with URLs and license fields
- the build backend is `poetry-core`
- `README.md` documents local commands using `poetry`
- `.github/workflows/onpush.yml` installs Poetry, caches Poetry state, and runs tests with `poetry run`
- `.github/workflows/release.yaml` installs Poetry and builds release artifacts with `poetry build`

## Goals

1. Use `uv` as the standard tool for dependency resolution, environment sync, command execution, and builds
2. Remove Poetry-specific metadata, commands, and CI setup from the repository
3. Keep package publishing standards-compliant so wheels and source distributions still build normally
4. Commit `uv.lock` so local development and CI use the same resolved dependency set
5. Avoid behavior changes in the library itself

## Non-Goals

- Refactoring connector code or changing package import paths
- Upgrading dependencies unless required to complete the migration
- Changing the project release process beyond replacing Poetry-specific steps
- Introducing new lint, test, or type-check tools

## Evaluated Approaches

### 1. PEP 621 metadata + Hatchling backend + `uv` workflow (**selected**)

Move package metadata and dependencies into standard `[project]` fields and dependency groups, switch the build
backend to Hatchling, and use `uv sync`, `uv run`, and `uv build` for local and CI workflows.

**Why selected:**

- fully removes Poetry from the repository
- keeps packaging standards-compliant
- matches the requested end state of moving projects to `uv`
- keeps the migration focused on tooling rather than code structure

### 2. PEP 621 metadata + setuptools backend + `uv` workflow

This would also fully remove Poetry, but it adds more packaging configuration without a clear benefit for this
simple library layout.

### 3. Keep Poetry metadata and only swap command runner

This would shrink the diff, but it leaves the project half-migrated and does not satisfy the goal of fully moving
away from Poetry.

## Detailed Design

### Packaging and Metadata

`pyproject.toml` will become the single source of truth using standard PEP 621 metadata:

- move package name, version, description, authors, readme, keywords, requires-python, and dependencies into
  `[project]`
- preserve existing project URLs and license metadata
- express optional/development dependencies using dependency groups compatible with `uv`
- replace the `poetry-core` build backend with `hatchling`

The package layout remains unchanged. The published package name, import paths, and distribution artifacts remain
the same.

### Lockfile and Environment Management

The repository will commit `uv.lock`.

Expected developer workflow:

- `uv sync` to create/update the managed environment
- `uv run pytest ...` for tests
- `uv run ruff check ...` and `uv run ruff format ...`
- `uv run mypy ...`
- `uv build` for wheel and source distribution creation

This keeps tool usage explicit while ensuring commands run inside the managed environment instead of through
Poetry wrappers.

### Documentation Changes

Documentation updates will be limited to migration-related content:

- replace Poetry setup and command examples in `README.md`
- update contributor-facing command snippets to use `uv`
- remove any wording that suggests Poetry is required

No broader README restructuring is part of this work.

### GitHub Actions

Both GitHub Actions workflows will be updated to use `uv` instead of Poetry.

#### CI workflow (`.github/workflows/onpush.yml`)

- remove the Poetry version matrix entry
- stop installing Poetry with `pipx`
- set up Python for the existing matrix versions
- install `uv`
- restore/use `uv` caching
- run `uv sync --frozen` against the committed lockfile
- execute the existing test command through `uv run`

The test command itself should remain functionally equivalent to the current one so coverage and report outputs do
not change unexpectedly.

#### Release workflow (`.github/workflows/release.yaml`)

- stop installing Poetry
- set up Python
- install `uv`
- build wheel and source distribution with `uv build`
- continue uploading artifacts and publishing to PyPI through the existing publish action

This preserves the release shape while changing only the environment/build tool.

### Validation

Implementation will be considered complete when all of the following are true:

1. `uv sync` succeeds for local setup
2. the repository's existing test, lint, type-check, and build commands run through `uv`
3. `uv build` produces the expected distribution artifacts
4. CI no longer installs or references Poetry
5. repository documentation no longer directs contributors to use Poetry

If the migration reveals a mismatch between local and CI behavior, that will be treated as a migration bug to fix
rather than bypassing checks.

## Implementation Boundaries

This migration intentionally stays narrow:

- no connector logic changes
- no package/module renames
- no speculative cleanup outside packaging, docs, and CI

Any code changes made during implementation must be directly required to restore the same behavior under the new
tooling.

## Risks and Mitigations

### Risk: metadata translation changes package behavior

**Mitigation:** keep package name, version, package inclusion, and build outputs aligned with the current release
shape.

### Risk: CI and local resolution diverge

**Mitigation:** commit `uv.lock` and use `uv sync --frozen` in CI.

### Risk: contributor confusion during transition

**Mitigation:** update the README and command examples in the same change that updates project metadata and
workflows.
