# Translation-Only Repair Supervisor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a safe one-command supervisor that repeatedly plans/enqueues translation-only historical repair batches, waits for terminal states, verifies local quality logs, DB publication fields, and javsubtitle.com visibility, and stops on failures without requiring manual babysitting.

**Architecture:** Reuse the existing `plan_translation_only_repair_batch` and `enqueue_translation_only_repair_batch` primitives. Add a focused supervisor module that owns loop/monitor/verify behavior and a CLI with explicit confirmation so dry-run remains default and bulk execution cannot start accidentally.

**Tech Stack:** Python 3.11, SQLite job store, existing Mac worker, `requests`, pytest.

---

### Task 1: Add supervisor planning tests

**Files:**
- Test: `tests/test_translation_only_supervisor.py`
- Create: `orchestrator/translation_only_supervisor.py`

- [ ] Write tests for dry-run summary, explicit confirm requirement, batch limit, and no execution without `--execute`.
- [ ] Run `pytest -q tests/test_translation_only_supervisor.py` and confirm RED.

### Task 2: Implement supervisor core

**Files:**
- Create: `orchestrator/translation_only_supervisor.py`

- [ ] Implement `TranslationOnlySupervisorConfig`, `TranslationOnlySupervisorResult`, and `run_translation_only_supervisor(...)`.
- [ ] Use existing plan/enqueue functions.
- [ ] Monitor selected job ids until all terminal or timeout.
- [ ] Stop immediately on `failed`, stuck timeout, worker not running, or publication/quality verification failure.
- [ ] Produce JSONL receipts with no subtitle text.

### Task 3: Add verification helpers

**Files:**
- Modify: `orchestrator/translation_only_supervisor.py`

- [ ] Verify DB status is `english_srt_ready`.
- [ ] Verify `published_subtitle_id`, `published_storage_path`, `published_content_sha256`, `published_file_size` are populated.
- [ ] Verify latest `logs/quality.log` record has `passed=true`.
- [ ] Optionally verify javsubtitle.com API has `English_AI` for each completed movie when configured.

### Task 4: Add CLI

**Files:**
- Modify: `orchestrator/__main__.py`
- Test: `tests/test_translation_only_supervisor.py`

- [ ] Add `run-translation-only-repair-supervisor` subcommand.
- [ ] Require `--allowlist-file`, `--batch-size`, `--max-jobs`, `--work-dir`.
- [ ] Default to dry-run. Require `--execute --confirm-remaining-count N` for DB-changing runs.
- [ ] Add `--verify-public-api`, `--poll-interval-seconds`, `--batch-timeout-seconds`, and `--stop-on-failure`.

### Task 5: Verify and commit

**Files:**
- Tests above

- [ ] Run `pytest -q tests/test_translation_only_supervisor.py tests/test_subtitle_repair.py tests/test_mac_worker.py`.
- [ ] Run full `pytest -q`.
- [ ] Commit and push.
