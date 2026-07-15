# Translation Input Replacement-Character Sanitization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove preexisting U+FFFD characters only from in-memory Japanese model input, preserve the Japanese SRT byte-for-byte, and safely continue the three already authorized historical repairs.

**Architecture:** Add one pure sanitizer and a small immutable result type to the existing TranslateLocally SRT script. `translate_srt` sanitizes only the collected text lines, writes aggregate counts to the existing statistics-only batch log, and leaves the original source and quality gate unchanged. After TDD and deployment, process the three fixed job IDs sequentially with exact-job one-shot workers and independent local/Supabase verification.

**Tech Stack:** Python 3.11, standard library `dataclasses`/`unicodedata`-free literal U+FFFD handling, TranslateLocally CLI, pytest, SQLite, Supabase Storage/PostgREST.

---

## File map

- Modify `scripts/translate_srt_translatelocally.py`: pure input sanitizer, aggregate statistics, safe error, and `translate_srt` integration.
- Modify `tests/test_translate_srt_translatelocally.py`: unit, integration, source-preservation, empty-line, and privacy regression tests.
- Modify `docs/setup/translatelocally.md`: document in-memory U+FFFD removal and unchanged quality/publication boundary.
- Use, but do not modify, `orchestrator/subtitle_quality.py`, `orchestrator/mac_worker.py`, and `orchestrator/supabase_publisher.py`.

### Task 1: Add the pure sanitizer with TDD

**Files:**
- Modify: `tests/test_translate_srt_translatelocally.py`
- Modify: `scripts/translate_srt_translatelocally.py`

- [ ] **Step 1: Write failing tests for removal, preservation, and empty input**

Append these tests to `tests/test_translate_srt_translatelocally.py`:

```python
def test_sanitize_translation_input_removes_only_replacement_characters():
    result = tl.sanitize_translation_input(
        ["前\ufffd後", "unchanged punctuation!?", "normal"]
    )

    assert result.lines == ("前後", "unchanged punctuation!?", "normal")
    assert result.replacement_character_count == 1
    assert result.sanitized_line_count == 1


def test_sanitize_translation_input_rejects_line_empty_after_removal():
    with pytest.raises(
        ValueError,
        match=(
            r"translation_input_corrupt: line 2 empty after removing "
            r"replacement characters"
        ),
    ):
        tl.sanitize_translation_input(["normal", "\ufffd\ufffd"])
```

- [ ] **Step 2: Run the new tests and observe the expected RED failure**

Run:

```bash
source /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/activate
pytest -q \
  tests/test_translate_srt_translatelocally.py::test_sanitize_translation_input_removes_only_replacement_characters \
  tests/test_translate_srt_translatelocally.py::test_sanitize_translation_input_rejects_line_empty_after_removal
```

Expected: both tests fail because `sanitize_translation_input` does not exist.

- [ ] **Step 3: Implement the minimal pure sanitizer**

Add the import and type near the constants in
`scripts/translate_srt_translatelocally.py`:

```python
from dataclasses import dataclass


REPLACEMENT_CHARACTER = "\ufffd"


@dataclass(frozen=True)
class SanitizedTranslationInput:
    lines: tuple[str, ...]
    replacement_character_count: int
    sanitized_line_count: int
```

Add this function immediately before `run_translate_locally`:

```python
def sanitize_translation_input(lines: list[str]) -> SanitizedTranslationInput:
    sanitized: list[str] = []
    replacement_count = 0
    sanitized_line_count = 0
    for line_number, line in enumerate(lines, start=1):
        line_replacement_count = line.count(REPLACEMENT_CHARACTER)
        cleaned = line.replace(REPLACEMENT_CHARACTER, "")
        if line.strip() and not cleaned.strip():
            raise ValueError(
                "translation_input_corrupt: "
                f"line {line_number} empty after removing replacement characters"
            )
        if line_replacement_count:
            sanitized_line_count += 1
            replacement_count += line_replacement_count
        sanitized.append(cleaned)
    return SanitizedTranslationInput(
        lines=tuple(sanitized),
        replacement_character_count=replacement_count,
        sanitized_line_count=sanitized_line_count,
    )
```

- [ ] **Step 4: Run the two tests and observe GREEN**

Run the Step 2 command again.

Expected: `2 passed`.

- [ ] **Step 5: Commit the pure sanitizer**

```bash
git add scripts/translate_srt_translatelocally.py \
  tests/test_translate_srt_translatelocally.py
git commit -m "fix: sanitize replacement characters from translation input"
```

### Task 2: Integrate sanitization and privacy-safe statistics with TDD

**Files:**
- Modify: `tests/test_translate_srt_translatelocally.py`
- Modify: `scripts/translate_srt_translatelocally.py`

- [ ] **Step 1: Write a failing integration test**

Append:

```python
def test_translate_srt_sanitizes_model_input_preserves_source_and_logs_counts(
    tmp_path, monkeypatch
):
    input_srt = tmp_path / "safe.Japanese.srt"
    source_bytes = (
        "1\n00:00:00,000 --> 00:00:01,000\n"
        "safe-test-before\ufffdsafe-test-after\n\n"
    ).encode("utf-8")
    input_srt.write_bytes(source_bytes)
    output_srt = tmp_path / "safe.English.srt"
    batch_log = tmp_path / "logs" / "translate-batches.log"
    observed_lines: list[list[str]] = []

    monkeypatch.setattr(tl, "ensure_model_available", lambda *args: None)

    def fake_run(lines, **kwargs):
        observed_lines.append(list(lines))
        return ["clean English"] * len(lines)

    monkeypatch.setattr(tl, "run_translate_locally", fake_run)

    tl.translate_srt(
        input_srt,
        output_srt,
        translate_locally_path=sys.executable,
        model="ja-en-tiny",
        batch_log_path=batch_log,
    )

    assert observed_lines == [["safe-test-beforesafe-test-after"]]
    assert input_srt.read_bytes() == source_bytes
    assert "\ufffd" not in output_srt.read_text(encoding="utf-8")
    log_text = batch_log.read_text(encoding="utf-8")
    assert '"event": "input_sanitization"' in log_text
    assert '"input_replacement_character_count": 1' in log_text
    assert '"sanitized_input_line_count": 1' in log_text
    assert "safe-test-before" not in log_text
    assert "safe-test-after" not in log_text


def test_translate_srt_rejects_empty_sanitized_line_before_model(
    tmp_path, monkeypatch
):
    input_srt = tmp_path / "safe.Japanese.srt"
    source_bytes = (
        "1\n00:00:00,000 --> 00:00:01,000\n\ufffd\ufffd\n\n"
    ).encode("utf-8")
    input_srt.write_bytes(source_bytes)
    output_srt = tmp_path / "safe.English.srt"
    model_called = False

    monkeypatch.setattr(tl, "ensure_model_available", lambda *args: None)

    def fake_run(lines, **kwargs):
        nonlocal model_called
        model_called = True
        return ["unexpected"] * len(lines)

    monkeypatch.setattr(tl, "run_translate_locally", fake_run)

    with pytest.raises(ValueError, match="translation_input_corrupt"):
        tl.translate_srt(
            input_srt,
            output_srt,
            translate_locally_path=sys.executable,
            model="ja-en-tiny",
        )

    assert model_called is False
    assert input_srt.read_bytes() == source_bytes
    assert not output_srt.exists()
```

- [ ] **Step 2: Run the integration test and observe RED**

```bash
pytest -q \
  tests/test_translate_srt_translatelocally.py::test_translate_srt_sanitizes_model_input_preserves_source_and_logs_counts \
  tests/test_translate_srt_translatelocally.py::test_translate_srt_rejects_empty_sanitized_line_before_model
```

Expected: both tests FAIL because `translate_srt` still passes unsanitized lines,
does not reject the empty sanitized line, and does not write the sanitization event.

- [ ] **Step 3: Integrate the sanitizer before executable/model discovery**

In `translate_srt`, move executable discovery and `ensure_model_available` below
source parsing and sanitization. Replace the function's setup and direct
`source_text` translation with:

```python
    source = input_srt.read_bytes().decode("utf-8-sig")
    newline = detect_newline(source)
    lines = source.splitlines()
    text_indexes = collect_text_line_indexes(lines)
    source_text = [lines[index] for index in text_indexes]
    sanitized_input = sanitize_translation_input(source_text)
    effective_batch_log_path = batch_log_path or (
        Path(os.environ["TRANSLATE_BATCH_LOG_PATH"])
        if os.environ.get("TRANSLATE_BATCH_LOG_PATH")
        else None
    )
    _append_batch_log(
        effective_batch_log_path,
        {
            "event": "input_sanitization",
            "input_replacement_character_count": (
                sanitized_input.replacement_character_count
            ),
            "sanitized_input_line_count": sanitized_input.sanitized_line_count,
        },
    )

    translate_locally = find_translate_locally(translate_locally_path)
    selected_model = model or os.environ.get("TRANSLATELOCALLY_MODEL") or DEFAULT_MODEL
    ensure_model_available(translate_locally, selected_model)
    translated_text = run_translate_locally_batched(
        list(sanitized_input.lines),
        translate_locally=translate_locally,
        model=selected_model,
        batch_lines=batch_lines,
        batch_chars=batch_chars,
        timeout_seconds=timeout_seconds,
        retries=retries,
        batch_log_path=effective_batch_log_path,
    )
```

Remove the old earlier executable/model discovery, source parsing, and duplicated
inline batch-log-path expression. This ordering is required so a corrupt input line
fails before any TranslateLocally subprocess, including model discovery, is invoked.

- [ ] **Step 4: Run focused translation tests**

```bash
pytest -q tests/test_translate_srt_translatelocally.py
```

Expected: `11 passed` with no subtitle text in captured output.

- [ ] **Step 5: Commit integration**

```bash
git add scripts/translate_srt_translatelocally.py \
  tests/test_translate_srt_translatelocally.py
git commit -m "test: preserve source while sanitizing model input"
```

### Task 3: Document and verify the code change

**Files:**
- Modify: `docs/setup/translatelocally.md`

- [ ] **Step 1: Document the boundary**

Add this paragraph after the Mac smoke-test instructions:

```markdown
Before model invocation, the wrapper removes any preexisting Unicode U+FFFD
replacement characters from its in-memory translation input. It never rewrites the
Japanese SRT. The wrapper logs aggregate removal counts without subtitle text and
fails before model invocation if a source line becomes empty. The unchanged
Japanese/English pair quality gate still decides whether publication is allowed.
```

- [ ] **Step 2: Run formatting and full tests**

```bash
git diff --check
source /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/activate
pytest -q
```

Expected: `248 passed, 1 warning` and no failures.

- [ ] **Step 3: Run the real fixed smoke test**

```bash
python -m orchestrator mac-translation-smoke-test
```

Expected: exit 0, `cues=10`, `unique_ratio=1.000`, `known_bad=0`.

- [ ] **Step 4: Commit documentation**

```bash
git add docs/setup/translatelocally.md
git commit -m "docs: explain replacement-character input handling"
```

### Task 4: Merge and deploy the fix without widening repair scope

**Files:**
- Merge branch only; do not edit `.env` or reports.

- [ ] **Step 1: Verify the feature branch is clean**

```bash
git status --short
git log --oneline codex/windows-transcription-mac-translation..HEAD
```

Expected: clean worktree and only sanitizer-related commits.

- [ ] **Step 2: Merge into the deployment branch**

From `/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator`:

```bash
git merge --no-ff codex/translation-input-sanitization \
  -m "merge: sanitize translation replacement characters"
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Expected: `248 passed, 1 warning`.

- [ ] **Step 3: Push the deployment branch**

```bash
git push origin codex/windows-transcription-mac-translation
git ls-remote origin \
  refs/heads/codex/windows-transcription-mac-translation
```

Expected: remote SHA equals local `git rev-parse HEAD`; draft PR #1 remains open.

- [ ] **Step 4: Run production smoke before any prepare call**

```bash
python -m orchestrator mac-translation-smoke-test
```

Expected: exit 0, 10 English cues, unique ratio at least 0.5, known bad 0.

### Task 5: Continue the exact authorized three-job list

**Files:**
- Read: `reports/subtitle-audit/english-ai-local-20260712/batch-5/*-selection.json`
- Preserve: `/Users/ytt/MissAVJobs/<movie>/<movie>.Japanese.srt`
- Quarantine: `/Users/ytt/MissAVJobs/<movie>/rejected/`

The fixed jobs and pre-repair evidence are:

| Movie | Job ID | Japanese SHA-256 | Old English/Supabase SHA-256 |
|---|---|---|---|
| `awd-148` | `job_fcb6bb7962a54fbbade2c7dc54010e62` | `c55163aebbdc7a22b815f0149c1e31854f80f3938111c460b6efb4ddc837ed12` | `4a68bafba65a781ca82abeda17ef5eaacb568257bb7ea5d7699ccc234ce4fc7e` |
| `same-057` | `job_d91a4b99aeb440d9a7df097d6622850f` | `dd86880b3373787e3610618ef34495444167b2e37fafeb26f44ce17723a189a8` | `1b05d9817f8aba8cbd3efc2dac794848f9045b1b1e8ec26b38cc69311a94dc77` |
| `jame-003` | `job_ffc2b4e3fe1a42a798a7624ab602ef0d` | `96a8a6815a9ff2cabc85d9da84a3ce066ac966902d94d7d29ed62dfaebade1b9` | `bdaf8c0b6d3acb40cf99680f3132ebdbe7df3c8b7e9a5cda9623b992115509a2` |

All three had `audio_preexisting=false` and zero rejected files at selection time.

- [ ] **Step 1: Process `awd-148` only**

```bash
python -m orchestrator prepare-historical-repair-canary \
  --allowlist-file reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --movie awd-148 --limit 1 \
  --confirm-job-id job_fcb6bb7962a54fbbade2c7dc54010e62
python -m orchestrator mac-translation-worker-once \
  --job-id job_fcb6bb7962a54fbbade2c7dc54010e62
```

Expected: exact one-shot exits 0 and the job becomes `english_srt_ready`.

- [ ] **Step 2: Independently verify `awd-148` before continuing**

Verify, without printing subtitle text:

- Japanese SHA-256 remains the value in the table;
- `audio.wav` remains absent;
- exactly one rejected SRT matches the old English SHA-256;
- final `quality.log` record has `passed=true` and empty `reason_codes`;
- new local English SHA-256 differs from the old value;
- authenticated Storage GET SHA-256 equals the new local hash;
- the exact `English_AI` catalog row path and size match.

Stop the entire continuation on any mismatch.

- [ ] **Step 3: Process and independently verify `same-057`**

```bash
python -m orchestrator prepare-historical-repair-canary \
  --allowlist-file reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --movie same-057 --limit 1 \
  --confirm-job-id job_d91a4b99aeb440d9a7df097d6622850f
python -m orchestrator mac-translation-worker-once \
  --job-id job_d91a4b99aeb440d9a7df097d6622850f
```

Before continuing to `jame-003`, require all of these exact checks:

- database status is `english_srt_ready` for
  `job_d91a4b99aeb440d9a7df097d6622850f`;
- Japanese SHA-256 is still
  `dd86880b3373787e3610618ef34495444167b2e37fafeb26f44ce17723a189a8`;
- `audio.wav` remains absent;
- exactly one rejected SRT has SHA-256
  `1b05d9817f8aba8cbd3efc2dac794848f9045b1b1e8ec26b38cc69311a94dc77`;
- the final quality record has `passed=true` and no reason codes;
- new English differs from the old hash;
- authenticated Storage GET equals the new local SHA-256;
- the exact `English_AI` catalog row has the expected path and new local size.

Stop the entire continuation on any mismatch.

- [ ] **Step 4: Process and independently verify `jame-003`**

```bash
python -m orchestrator prepare-historical-repair-canary \
  --allowlist-file reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --movie jame-003 --limit 1 \
  --confirm-job-id job_ffc2b4e3fe1a42a798a7624ab602ef0d
python -m orchestrator mac-translation-worker-once \
  --job-id job_ffc2b4e3fe1a42a798a7624ab602ef0d
```

Require all of these exact checks:

- database status is `english_srt_ready` for
  `job_ffc2b4e3fe1a42a798a7624ab602ef0d`;
- Japanese SHA-256 is still
  `96a8a6815a9ff2cabc85d9da84a3ce066ac966902d94d7d29ed62dfaebade1b9`;
- `audio.wav` remains absent;
- exactly one rejected SRT has SHA-256
  `bdaf8c0b6d3acb40cf99680f3132ebdbe7df3c8b7e9a5cda9623b992115509a2`;
- the final quality record has `passed=true` and no reason codes;
- new English differs from the old hash;
- authenticated Storage GET equals the new local SHA-256;
- the exact `English_AI` catalog row has the expected path and new local size.

Stop on any mismatch; do not select a replacement historical job.

### Task 6: Final verification and worker restoration

**Files:**
- No code modifications.

- [ ] **Step 1: Verify the failure and success boundaries**

Require:

- `ugug-059` remains `failed`, its remote hash remains
  `2459a36e4d1498bedd9c1d50ef844d3947860c50ae1fc3db57a9a6b0255f2d4d`,
  and its two rejected SRTs remain present;
- `mimk-282`, `awd-148`, `same-057`, and `jame-003` are
  `english_srt_ready`;
- every successful repair has `quality.log passed=true`, unchanged Japanese,
  continued audio absence, preserved old English, and matching local/remote hashes;
- no sixth historical job changed state.

- [ ] **Step 2: Run a fresh full test suite**

```bash
source .venv/bin/activate
pytest -q
```

Expected: `248 passed, 1 warning`.

- [ ] **Step 3: Restore the normal Mac translation worker**

```bash
python -m orchestrator mac-translation-worker
```

Expected startup output: smoke passes with 10 cues, unique ratio `1.000`, known bad
0. Confirm API, downloader, and translation worker processes are running.

- [ ] **Step 4: Report without subtitle text or secrets**

Report root cause, code commits, pytest and smoke output, each fixed movie/job ID,
structured quality results, old/new hashes, Supabase/catalog verification, unchanged
Japanese/audio state, retained rejected files, `ugug-059` permanent failure, and the
updated number of audited failures not processed. Do not print subtitle contents,
credentials, or `.env` values.
