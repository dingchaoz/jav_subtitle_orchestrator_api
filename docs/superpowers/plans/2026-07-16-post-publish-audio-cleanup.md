# Post-Publish Audio Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete Mac job audio only after verified subtitle publication, expose the configuration in the API/dashboard, and defer low-disk downloads without consuming retries.

**Architecture:** A focused `audio_cleanup` helper safely unlinks only the canonical job WAV and logs every outcome. `MacTranslationWorker` invokes it after `complete_supabase_publication`, while the adapter/worker/store use a dedicated exception and state transition for low-disk pauses. Dashboard state reports the environment-backed setting.

**Tech Stack:** Python 3.11, FastAPI, Pydantic settings, SQLite, pytest, vanilla dashboard HTML/JavaScript, launchd.

---

### Task 1: Safe post-publication cleanup

**Files:**
- Create: `orchestrator/audio_cleanup.py`
- Modify: `orchestrator/mac_worker.py`
- Modify: `orchestrator/dashboard.py`
- Test: `tests/test_audio_cleanup.py`
- Test: `tests/test_mac_worker.py`

- [ ] **Step 1: Write failing cleanup and publication tests**

Add tests proving the helper deletes a canonical regular `audio.wav`, treats missing audio as success, refuses symlinks/out-of-root paths, and logs deletion errors. Add worker tests proving verified publication plus durable completion deletes audio, disabled cleanup retains it, and publication failure retains it.

- [ ] **Step 2: Run tests to verify failure**

Run `python -m pytest tests/test_audio_cleanup.py tests/test_mac_worker.py -q`; expect import/signature failures before implementation.

- [ ] **Step 3: Implement the helper and hook**

Implement `delete_published_job_audio(job, jobs_root_mac) -> AudioCleanupResult` using the canonical path from `build_job_paths`, no-follow filesystem checks, and `append_job_log`. Add `delete_audio_after_publish: bool = True` to `MacTranslationWorker`; call cleanup only after `complete_supabase_publication` returns.

- [ ] **Step 4: Run focused tests**

Run `python -m pytest tests/test_audio_cleanup.py tests/test_mac_worker.py -q`; expect all passing.

### Task 2: Configuration and dashboard/API visibility

**Files:**
- Modify: `orchestrator/config.py`
- Modify: `orchestrator/__main__.py`
- Modify: `orchestrator/models.py`
- Modify: `orchestrator/api.py`
- Modify: `orchestrator/dashboard.py`
- Modify: `.env.example`
- Test: `tests/test_config_paths.py`
- Test: `tests/test_api_dashboard.py`
- Test: `tests/test_dashboard_state.py`

- [ ] **Step 1: Write failing configuration and dashboard tests**

Assert `MacSettings.delete_audio_after_publish` defaults true and honors `DELETE_AUDIO_AFTER_PUBLISH=false`. Assert `/dashboard/state` returns `{enabled, trigger}` and dashboard HTML renders an Audio cleanup health card.

- [ ] **Step 2: Run tests to verify failure**

Run `python -m pytest tests/test_config_paths.py tests/test_api_dashboard.py tests/test_dashboard_state.py -q`; expect missing-field/card failures.

- [ ] **Step 3: Wire the setting**

Pass the setting into both `create_app` and every `MacTranslationWorker` construction. Extend `DashboardStateResponse`, `build_dashboard_state`, and the Operations renderer. Document `DELETE_AUDIO_AFTER_PUBLISH=true` in `.env.example`.

- [ ] **Step 4: Run focused tests**

Run the same pytest command; expect all passing.

### Task 3: Low-disk deferral

**Files:**
- Modify: `orchestrator/missav_adapter.py`
- Modify: `orchestrator/mac_worker.py`
- Modify: `orchestrator/store.py`
- Test: `tests/test_missav_adapter.py`
- Test: `tests/test_mac_worker.py`
- Test: `tests/test_store_worker_claims.py`

- [ ] **Step 1: Write failing adapter, worker, and store tests**

Simulate exit zero with `Pausing before next download` and no WAV; require `DownloadDeferredError`. Require `defer_download_job` to return an active download to `queued` without changing `attempt_count`. Require `MacDownloadWorker.process_one()` to log `download_deferred`, leave the job queued, preserve attempts, and return false.

- [ ] **Step 2: Run tests to verify failure**

Run `python -m pytest tests/test_missav_adapter.py tests/test_mac_worker.py tests/test_store_worker_claims.py -q`; expect missing exception/method failures.

- [ ] **Step 3: Implement minimal deferral flow**

Classify only the known low-space output marker after an otherwise successful process. Add the guarded store transition and catch it separately in the worker before generic failure accounting.

- [ ] **Step 4: Run focused tests**

Run the same pytest command; expect all passing.

### Task 4: Verification, integration, and production recovery

**Files:**
- No new production files expected.

- [ ] **Step 1: Verify code**

Run `python -m pytest -q`, `python -m compileall -q orchestrator`, and `git diff --check`; expect zero failures.

- [ ] **Step 2: Merge and push**

Commit the feature branch, merge it into the clean local `main`, rerun the full suite, and push `main` to `origin`.

- [ ] **Step 3: Reclaim space**

Back up the live SQLite database, recompute the conservative published-and-catalog-synced allowlist, delete only its WAV files, and verify free space exceeds the downloader's 5 GiB floor.

- [ ] **Step 4: Restart and restore**

Restart the API, Mac translation worker, and launchd-managed Mac downloader from latest `main`. Force-reset only the exact low-disk missing-audio failures.

- [ ] **Step 5: Observe recovery**

Confirm the dashboard reports cleanup enabled, the subtitle sync pipeline remains configured, at least one restored job advances beyond download, and no new low-disk missing-file failures appear.
