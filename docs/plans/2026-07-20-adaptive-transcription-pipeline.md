# Adaptive Japanese Transcription Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make every new built-in Windows transcription job use a 90-second primary pass plus targeted dual-grid gap repair that improves Japanese speech coverage without publishing single-pass noise.

**Architecture:** Extend `FasterWhisperTranscriber` with pure gap-analysis and consensus helpers, then run two globally aligned 30-second grids only over suspicious primary-pass gaps. Wire benchmarked defaults through `WindowsSettings`, preserve atomic SRT output and existing retry semantics, and record a compact repair report in the per-job Whisper log.

**Tech Stack:** Python 3.12, faster-whisper 1.2.1, CTranslate2/CUDA, FFmpeg/ffprobe, Pydantic Settings, pytest.

---

### Task 1: Add pure repair-analysis primitives

**Files:**
- Modify: `orchestrator/transcription.py:1-33`
- Modify: `tests/test_transcription.py`

**Step 1: Write failing tests**

Add tests proving:

```python
assert normalize_transcript_text(" マスク、取って。") == "マスク取って"
assert transcript_text_similarity(
    "マスク", "マスク取ってもらえませんか"
) == 1.0
```

Also test that a 30-second `ご視聴ありがとうございました` cue and a
low-diversity repeated-character cue are suspicious, while normal dialogue is
not.

**Step 2: Run the focused tests and verify failure**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_transcription.py -q
```

Expected: FAIL because the new helpers do not exist.

**Step 3: Implement the minimal pure helpers**

Add Unicode NFKC normalization, Japanese/alphanumeric filtering,
`transcript_text_similarity`, definite-hallucination detection, and
repair-trigger detection. Keep these helpers independent of faster-whisper and
FFmpeg.

**Step 4: Run the focused tests**

Expected: all `test_transcription.py` tests pass.

### Task 2: Detect and align targeted repair windows

**Files:**
- Modify: `orchestrator/transcription.py`
- Modify: `tests/test_transcription.py`

**Step 1: Write failing tests**

Test that:

- trusted primary cues separated by at least 60 seconds create a padded repair
  window;
- a suspicious long cue does not hide the gap;
- overlapping padded windows coalesce;
- 30-second chunk starts use global `0, 30, 60…` alignment;
- the second grid uses global `15, 45, 75…` alignment.

**Step 2: Run the tests and verify failure**

Expected: FAIL for missing `find_repair_windows` and
`repair_grid_chunk_starts`.

**Step 3: Implement window discovery**

Implement:

```python
def find_repair_windows(
    segments: list[Segment],
    duration: float,
    gap_seconds: float,
    padding_seconds: float,
) -> list[TimeWindow]:
    ...
```

and a deterministic global-grid start generator. Clamp all windows and chunks
to the audio duration.

**Step 4: Run the focused tests**

Expected: PASS.

### Task 3: Add dual-grid consensus and safe merge

**Files:**
- Modify: `orchestrator/transcription.py`
- Modify: `tests/test_transcription.py`

**Step 1: Write failing tests**

Use the confirmed DVRT-6701 pattern:

```python
grid_a = [
    Segment(1292.61, 1294.61, "顔が見たいんで"),
    Segment(1294.61, 1298.53, "マスク"),
    Segment(1298.53, 1300.53, "取ってもらえませんか"),
]
grid_b = [
    Segment(1275.94, 1294.10, "あのー顔が見たいんで"),
    Segment(1294.10, 1299.91, "マスク取ってもらえませんか"),
]
```

Assert that all three first-grid cues are confirmed despite different
segmentation. Assert that an unmatched boundary phrase and repeated-vocal cue
are rejected. Assert that merging does not duplicate text already present in
the primary pass and removes only definite hallucinations.

**Step 2: Run the tests and verify failure**

Expected: FAIL for missing consensus and merge functions.

**Step 3: Implement consensus and merge**

Compare each first-grid cue with individual and short consecutive second-grid
cue sequences within five seconds of its time interval. Require similarity
`>= 0.72`. Sort the merged result by timestamp and let the existing atomic SRT
writer reindex cues.

**Step 4: Run the focused tests**

Expected: PASS.

### Task 4: Integrate adaptive repair into FasterWhisperTranscriber

**Files:**
- Modify: `orchestrator/transcription.py:35-107`
- Modify: `tests/test_transcription.py`

**Step 1: Write failing orchestration tests**

With model/audio operations mocked, assert:

- default primary chunks are 90 seconds;
- repair is skipped when no qualifying gap exists;
- qualifying gaps run both globally aligned 30-second grids;
- only confirmed candidates reach the output SRT;
- an FFmpeg or model exception leaves no final SRT;
- unknown audio duration preserves the single-pass fallback.

**Step 2: Run the tests and verify failure**

Expected: FAIL because `FasterWhisperTranscriber` still performs only the
primary pass.

**Step 3: Implement adaptive orchestration**

Add constructor options for the benchmarked defaults, reuse `_extract_audio_chunk`,
deduplicate repair chunks across coalesced windows, and return a frozen
`TranscriptionReport`. Continue using language `ja`, beam 5, default VAD, and
`condition_on_previous_text=False`.

**Step 4: Run focused tests**

Expected: PASS.

### Task 5: Wire Windows settings and job logging

**Files:**
- Modify: `orchestrator/config.py:58-91`
- Modify: `orchestrator/__main__.py:66-85`
- Modify: `orchestrator/windows_worker.py:85-122`
- Modify: `tests/test_config_paths.py`
- Modify: `tests/test_windows_worker.py`

**Step 1: Write failing tests**

Assert the new settings defaults, constructor wiring, and a `whisper.log`
summary after successful transcription. Assert external-script mode remains
preferred when configured and does not require a report.

**Step 2: Run focused tests and verify failure**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_config_paths.py tests\test_windows_worker.py -q
```

Expected: FAIL for missing settings and report logging.

**Step 3: Implement settings and logging**

Add validated environment aliases:

```text
WHISPER_CHUNK_SECONDS=90
WHISPER_GAP_REPAIR_ENABLED=true
WHISPER_REPAIR_GAP_SECONDS=60
WHISPER_REPAIR_CHUNK_SECONDS=30
WHISPER_REPAIR_OFFSET_SECONDS=15
WHISPER_REPAIR_PADDING_SECONDS=15
WHISPER_REPAIR_MIN_SIMILARITY=0.72
```

Pass them into the built-in transcriber. Make periodic-heartbeat execution
return the operation result, then append the report summary to the job log.

**Step 4: Run focused tests**

Expected: PASS.

### Task 6: Document production configuration

**Files:**
- Modify: `.env.windows.example`
- Modify: `.env.windows`
- Modify: `docs/setup/windows.md`

**Step 1: Add the benchmarked settings**

Document that repair settings affect only the built-in transcriber and that an
external script bypasses them.

**Step 2: Verify settings load**

Run:

```powershell
.\.venv\Scripts\python.exe -c "from orchestrator.config import WindowsSettings; s=WindowsSettings(); print(s.whisper_chunk_seconds, s.whisper_gap_repair_enabled)"
```

Expected output begins with:

```text
90 True
```

### Task 7: Full verification and deployment

**Files:**
- No additional source files expected.

**Step 1: Run the complete automated suite**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Expected: all tests pass.

**Step 2: Run a DVRT-6701 local smoke test**

Use the exact M-drive copy already retained at:

```text
%LOCALAPPDATA%\jav-sub-orchestrator\benchmarks\dvrt-6701\audio.from-m-drive.wav
```

Write output only under the local benchmark directory. Verify that the 21:32
mask dialogue is present and that long repeated-character output is absent.

**Step 3: Inspect worker state**

Confirm no Windows process is currently inside an active transcription before
restart. If idle, restart `python -m orchestrator windows-worker` with the
repository `.venv`; if active, let that job finish first.

**Step 4: Verify next-job readiness**

Confirm the worker starts with the new settings and can reach the Mac API.
Do not submit a synthetic production job.
