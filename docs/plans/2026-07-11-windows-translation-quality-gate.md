# Windows Translation Runtime and Quality Gate Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use executing-plans to implement this plan task-by-task.

**Goal:** Repair the Windows TranslateLocally production path and prevent structurally or semantically collapsed English subtitles from reaching `client.complete()`.

**Architecture:** Keep the checked-in worker wrapper as the only production entry point, strengthen its bounded batch runner with timeouts, retries, per-batch telemetry, and atomic output, and add an independent SRT quality module. The Windows worker validates after translation and before completion, quarantines rejected output, reports a permanent translating-stage failure, and stops claiming work after repeated deterministic quality failures. Startup runs a fixed safe Japanese smoke test against the actual configured wrapper/runtime before polling.

**Tech Stack:** Python 3.12, standard library, pytest, TranslateLocally CLI.

---

### Task 1: Capture and reproduce the Windows runtime failure

**Files:**
- Read: `.env.windows`
- Read: `scripts/translatelocally_translate_single.py`
- Read: `scripts/translate_srt_translatelocally.py`
- Read: `M:\roe-179\roe-179.Japanese.srt`
- Read: `M:\hodv-21554\hodv-21554.Japanese.srt`

1. Stop the active worker and record process/job state without deleting artifacts.
2. Record Git state, selected non-secret configuration, executable/model metadata, and hashes.
3. Run the fixed ten-sentence smoke test per sentence, as one batch, through the SRT translator, and through the worker wrapper.
4. Change one runtime variable at a time to establish the first failing layer and exact root cause.
5. Run both production SRTs translation-only into a temporary directory and retain statistics only.

### Task 2: Add failing quality-checker tests

**Files:**
- Create: `tests/test_subtitle_quality.py`
- Create: `tests/fixtures/collapsed_translation.srt`
- Create: `orchestrator/subtitle_quality.py`

1. Test a diverse aligned SRT pair passes.
2. Test known-bad collapse, generic dominant collapse, empty output, parse errors, cue mismatch, broken index/timestamp structure, refusal templates, and mojibake fail with structured reason codes.
3. Test realistic short interjection repetition with sufficient overall diversity passes.
4. Run `pytest tests/test_subtitle_quality.py -q` and confirm failures precede implementation.
5. Implement strict parsing, normalized metrics, hard-fail thresholds, and warning-only ASR extension metrics.
6. Re-run the targeted tests.

### Task 3: Enforce the gate in WindowsWorker

**Files:**
- Modify: `orchestrator/windows_worker.py`
- Modify: `orchestrator/__main__.py`
- Modify: `orchestrator/config.py`
- Modify: `tests/test_windows_worker.py`

1. Add failing tests proving a rejected translation never calls `complete`, does call permanent `failed` at `translating`, is moved to a timestamped rejected artifact, and writes `logs/quality.log`.
2. Add the validator between translation and completion.
3. Format errors as `quality_gate_failed:<comma-separated-reason-codes>` and add an API-compatible permanent flag or deterministic permanent marker without breaking older Mac servers.
4. Add a consecutive-quality-failure circuit breaker that prevents further `next_job` calls after the configured threshold.
5. Test the passing path still completes normally.

### Task 4: Harden TranslateLocally batching and output publication

**Files:**
- Modify: `scripts/translate_srt_translatelocally.py`
- Modify: `scripts/translatelocally_translate_single.py`
- Modify: `orchestrator/translation.py`
- Modify: `tests/test_translation.py`

1. Add failing tests for 50-line and 4000-character limits, a fresh process per batch, timeout, bounded retries, line-count mismatch, and no partial final output.
2. Implement character-aware batching and explicit UTF-8 subprocess/file handling.
3. Log only batch number, line/character counts, output count, duration, and return code.
4. Write beside the final destination and use `os.replace` only after every batch succeeds.
5. Run targeted translation tests.

### Task 5: Add startup runtime smoke protection

**Files:**
- Modify: `orchestrator/windows_worker.py`
- Modify: `orchestrator/__main__.py`
- Modify: `tests/test_windows_worker.py`
- Modify: `docs/setup/windows.md`

1. Test startup rejects collapsed, wrong-count, known-bad, or failed runtime output before polling.
2. Run the checked-in production wrapper on the fixed ten safe sentences in a temporary directory.
3. Log executable, model, wrapper path, Git commit, and aggregate result without secrets or subtitle bodies.
4. Document the guarded restart and verification commands.

### Task 6: Verify with automated and real canary tests

**Files:**
- Test: `tests/test_subtitle_quality.py`
- Test: `tests/test_windows_worker.py`
- Test: `tests/test_translation.py`

1. Run all directly targeted tests and record pass/fail counts.
2. Run `pytest -q` and record full pass/fail counts.
3. Translate roe-179 and hodv-21554 to a temporary directory only; do not call the Mac API.
4. Validate cue preservation, unique/dominant/known-bad metrics, and quality reports.
5. Re-run the ten-sentence startup smoke test.
6. Leave the production worker stopped until explicit user approval.
