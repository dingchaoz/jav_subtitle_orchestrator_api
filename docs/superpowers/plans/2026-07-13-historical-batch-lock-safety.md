# Historical Batch Lock-Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make historical batch snapshots bounded, transactionally consistent,
and migration-safe without performing file I/O under a SQLite write lock.

**Architecture:** Hold jobs-root EX while creating a bounded filesystem view,
then open a short SQLite transaction and build the plan solely from that view
and one database snapshot. Persist a RIFF metadata fingerprint rather than an
audio content hash, atomically rebuild incompatible legacy repair tables, and
perform a final parent/target verification in the private plan writer.

**Tech Stack:** Python 3.12, SQLite/WAL, POSIX `fcntl`/`openat`/`pread`, pytest.

---

### Task 1: Bounded audio snapshot contract

**Files:**
- Modify: `orchestrator/historical_batch.py`
- Modify: `orchestrator/store.py`
- Test: `tests/test_historical_batch.py`
- Test: `tests/test_store_worker_claims.py`

- [x] Add failing strict-schema and sparse-WAV tests expecting
  `audio_snapshot_sha256`, deterministic probe metadata, and no more than
  `MAX_AUDIO_PROBE_BYTES` bytes read from a tens-of-gigabytes sparse file.
- [x] Run the new tests and confirm they fail on the old `audio_sha256` full
  file hash.
- [x] Add a bounded RIFF/WAVE parser with fixed read-byte and chunk-count
  budgets; include only canonical persisted metadata in its SHA-256.
- [x] Bump the strict plan version and rename all plan/record/schema identity
  fields to `audio_snapshot_sha256`.
- [x] Run the focused tests to green.

### Task 2: Filesystem prescan before short SQLite transaction

**Files:**
- Modify: `orchestrator/historical_batch.py`
- Modify: `orchestrator/store.py`
- Test: `tests/test_historical_batch.py`

- [x] Add failing instrumentation tests that pause prescan and prove an
  unrelated `BEGIN IMMEDIATE` writer commits without waiting for the five-second
  busy timeout.
- [x] Add a failing guard test that raises if any file snapshot/stat operation
  occurs after enqueue starts its SQLite write transaction.
- [x] Replace transaction-time `_build_plan` file reads with a root-locked
  `_FilesystemScan` created before `BEGIN`; load jobs and repairs once and build
  only from those immutable in-memory views.
- [x] Use `BEGIN` explicitly for plan reads and `BEGIN IMMEDIATE` only after
  enqueue prescan/revalidation; commit both before releasing root EX.
- [x] Run concurrency, lock-order, idempotency, and bounded-FD tests to green.

### Task 3: Atomic legacy table rebuild

**Files:**
- Modify: `orchestrator/store.py`
- Test: `tests/test_store_submit.py`
- Test: `tests/test_store_worker_claims.py`

- [x] Add failing real-old-table tests for `PRAGMA notnull=1`, FK/index
  preservation, non-null source backfill, and null-source/null-audio runnable
  rows becoming deterministic permanent failures.
- [x] Add a single-transaction rename/create/copy/drop rebuild that maps
  compatible values, uses the zero digest sentinel for unavailable identity,
  and never relabels legacy audio content hashes as snapshot fingerprints.
- [x] Run migration twice and assert identical rows/schema on the second call.
- [x] Run migration and store contract tests to green.

### Task 4: Close private-plan final binding window

**Files:**
- Modify: `orchestrator/historical_batch.py`
- Test: `tests/test_historical_batch.py`

- [x] Add a failing race test that renames/replaces the parent immediately after
  the second binding check returns.
- [x] Add final parent binding and target inode/regular-file/0600 verification
  after link, temporary unlink, and parent fsync, immediately before clearing
  cleanup identity and returning.
- [x] Assert cleanup through the held parent fd removes only the invocation's
  final inode from the moved directory.
- [x] Run all private-plan safety tests to green.

### Task 5: Deterministic reporting and final verification

**Files:**
- Modify: `orchestrator/historical_batch.py`
- Test: `tests/test_historical_batch.py`

- [x] Add failing report assertions for `scan_entries` and
  `audio_probe_max_bytes`, with no elapsed time in the plan digest.
- [x] Populate the deterministic fields from the complete allowlist scan and
  render them without paths or subtitle text.
- [x] Run historical/store focused tests, then full pytest, compileall, and
  `git diff --check`.
- [x] Inspect the final diff for file I/O after `BEGIN`, schema false claims,
  and lock-order regressions; commit only after all checks pass.
