# Catalog Sync Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Accept the deployed catalog cache-key receipt, restart the updated services, and resume eligible catalog-only failures without republishing or retranslating subtitles.

**Architecture:** Keep strict catalog response validation, but recognize the deployed versioned full-cache key while still requiring the exact canonical movie code and matching light-cache key. Add a transactional `JobStore` recovery method that only resets terminal catalog failures with a verified publication receipt. Use that method for the twelve recoverable jobs visible in the dashboard and leave `scpx-557` isolated because production reports `catalog_movie_missing`.

**Tech Stack:** Python 3.11, pytest, SQLite, launchd, requests

---

### Task 1: Catalog response compatibility

**Files:**
- Modify: `tests/test_catalog_sync.py`
- Modify: `orchestrator/catalog_sync.py`

- [ ] Add a test whose successful response contains `movie:full:v3:roe-291` and `movie:light:roe-291`.
- [ ] Run the focused test and confirm it fails with `catalog_response_mismatch`.
- [ ] Update cache-key validation to accept one exact canonical light key and one full key whose final segment is the canonical code, with at most one non-empty version segment.
- [ ] Keep malformed, unrelated, duplicate, and wrong-canonical keys rejected.
- [ ] Run `pytest tests/test_catalog_sync.py -q` and confirm it passes.

### Task 2: Safe catalog-only retry

**Files:**
- Modify: `tests/test_store_worker_claims.py`
- Modify: `orchestrator/store.py`

- [ ] Add a test that prepares a verified publication receipt, exhausts catalog sync, retries it, and confirms publication fields are preserved while status becomes `catalog_sync_pending` and the catalog counter resets.
- [ ] Add rejection coverage for a non-catalog failure.
- [ ] Run the focused tests and confirm they fail because the recovery method does not exist.
- [ ] Implement the transactional recovery method with status, ownership, origin, error, and verified-receipt checks.
- [ ] Run `pytest tests/test_store_worker_claims.py -q` and confirm it passes.

### Task 3: Verification and live recovery

**Files:**
- No additional source files.

- [ ] Run `pytest tests/test_catalog_sync.py tests/test_store_worker_claims.py tests/test_mac_worker.py tests/test_api_dashboard.py tests/test_api_subtitle_audits.py -q`.
- [ ] Restart `com.javsubtitle.orchestrator-api` and `com.javsubtitle.mac-translation-worker` with launchd.
- [ ] Verify `/dashboard` contains the new tabs and `/dashboard/state` reports both services online.
- [ ] Use `JobStore.retry_failed_catalog_sync` for the twelve approved catalog failures except `scpx-557`.
- [ ] Poll until recovered jobs become `english_srt_ready` or expose a new classified failure.
- [ ] Confirm `scpx-557` remains failed and unchanged.
