# Windows Transcription and Mac Translation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use executing-plans to implement this plan task-by-task.

**Goal:** Make Windows produce only Japanese SRT files and hand translation work to a quality-gated Mac translation worker.

**Architecture:** Windows claims only `audio_ready`, transcribes, and atomically hands the job back as `transcription_done` through a dedicated API endpoint. A Mac translation worker claims `transcription_done`, creates the English SRT with the configured Mac runtime, validates it, quarantines failures, and only then marks `english_srt_ready` for the existing publishing path.

**Tech Stack:** Python 3.12, FastAPI, SQLite, pytest, existing SRT translator and quality checker.

---

### Task 1: Add the transcription handoff state transition

**Files:**
- Modify: `orchestrator/models.py`
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/api.py`
- Test: `tests/test_api_worker.py`
- Test: `tests/test_store_worker_claims.py`

1. Write failing tests for a transcription-complete API call that requires the claimed worker and an existing Japanese SRT.
2. Implement a store transition to `transcription_done` that records Mac/Windows Japanese paths and releases the lease.
3. Run the API and store tests.

### Task 2: Remove translation from the Windows worker

**Files:**
- Modify: `orchestrator/windows_worker.py`
- Modify: `orchestrator/__main__.py`
- Modify: `orchestrator/config.py`
- Test: `tests/test_windows_worker.py`

1. Write failing tests proving Windows calls `transcription_complete`, never invokes a translator, never creates English SRT, and never calls complete.
2. Remove translator construction and translation startup smoke from the Windows command.
3. Keep transcription heartbeat, retry, and failure reporting behavior.
4. Run Windows worker tests.

### Task 3: Add the Mac translation queue worker

**Files:**
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/mac_worker.py`
- Modify: `orchestrator/config.py`
- Modify: `orchestrator/__main__.py`
- Test: `tests/test_mac_worker.py`

1. Write failing tests for atomic claim of `transcription_done`, quality-pass completion, transient retry, permanent quality failure, quarantine, and no false `english_srt_ready`.
2. Implement Mac translation claim, lease recovery, completion, and failure transitions.
3. Run a Mac translation startup smoke test before the Mac pipeline loop.
4. Alternate download and translation work in the existing Mac worker command.

### Task 4: Update configuration and runbooks

**Files:**
- Modify: `.env.example`
- Modify: `.env.windows.example`
- Modify: `README.md`
- Modify: `docs/setup/mac.md`
- Modify: `docs/setup/windows.md`
- Modify: `docs/setup/e2e-lan-test.md`

1. Document Mac translation runtime settings and the new state flow.
2. Mark Windows translation settings unused in transcription-only mode.
3. Document safe startup order: Mac API, Mac worker smoke, Windows worker.

### Task 5: Verify

1. Run all targeted API/store/Windows/Mac/quality tests.
2. Run `pytest -q`.
3. Confirm no Windows worker process was started and no production job was requeued or uploaded.
