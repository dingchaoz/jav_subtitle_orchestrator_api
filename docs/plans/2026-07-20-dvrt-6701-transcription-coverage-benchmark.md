# DVRT-6701 Transcription Coverage Benchmark Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a reproducible local benchmark that improves Japanese speech coverage on DVRT-6701 without materially increasing hallucinated or noisy subtitles.

**Architecture:** Keep production artifacts untouched. Run parameterized faster-whisper experiments against a verified local copy of the production-format WAV, record SRT plus machine-readable telemetry for every run, rank candidates on known missed intervals and control intervals, and promote only candidates that also pass a full-movie verification.

**Tech Stack:** Python 3.12, faster-whisper 1.2.1, CTranslate2/CUDA, FFmpeg/ffprobe, pytest.

---

### Task 1: Add deterministic transcript coverage metrics

**Files:**
- Create: `scripts/benchmark_transcription_coverage.py`
- Create: `tests/test_benchmark_transcription_coverage.py`

**Step 1: Write failing tests**

Add tests for:

- parsing valid SRT cues;
- computing unioned subtitle-covered seconds;
- finding inter-cue gaps without double-counting overlapping cues;
- counting cues longer than 10 and 20 seconds;
- reporting Japanese character density;
- comparing a candidate against the production baseline inside named time windows.

**Step 2: Run tests and verify failure**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_benchmark_transcription_coverage.py -q
```

Expected: failure because the benchmark module does not exist.

**Step 3: Implement the metrics**

Implement pure functions and dataclasses first. Keep scoring separate from model execution so metrics can be tested without loading CUDA.

**Step 4: Run tests and verify pass**

Run the same pytest command.

Expected: all benchmark metric tests pass.

### Task 2: Add a parameterized faster-whisper runner

**Files:**
- Modify: `scripts/benchmark_transcription_coverage.py`
- Modify: `tests/test_benchmark_transcription_coverage.py`

**Step 1: Write failing command-construction tests**

Cover:

- outer chunk size;
- VAD disabled, default, and custom modes;
- VAD threshold, minimum silence, minimum speech, and speech padding;
- beam size, patience, temperature, no-speech threshold, log-probability threshold, repetition penalty, and previous-text conditioning;
- optional initial prompt;
- JSON manifest containing model, runtime, parameters, elapsed time, per-segment confidence, and output paths.

**Step 2: Run the focused tests and verify failure**

Expected: failure for missing runner configuration and manifest serialization.

**Step 3: Implement the runner**

Use the same FFmpeg chunk extraction and timestamp offset behavior as production. Write each run into its own directory and never overwrite a completed run unless `--force` is explicitly supplied.

**Step 4: Run tests**

Expected: all benchmark tests pass without requiring a model or GPU.

### Task 3: Screen candidate configurations on DVRT-6701

**Files:**
- Output only: `%LOCALAPPDATA%\jav-sub-orchestrator\benchmarks\dvrt-6701\runs\`

**Step 1: Reproduce production baseline**

Use `large-v3-turbo`, float16, beam 5, default VAD, 900-second outer chunks, and `condition_on_previous_text=False`.

Expected: reproduce the known `00:21:10 -> 00:36:32` gap after applying the 15-minute clip offset.

**Step 2: Run high-information ablations**

Run:

1. VAD off;
2. relaxed VAD at thresholds 0.35 and 0.25;
3. relaxed VAD plus shorter silence duration;
4. shorter outer chunks with overlap;
5. previous-text conditioning enabled;
6. `large-v3` versus `large-v3-turbo`;
7. a WhisperJAV/faster-whisper alternative if its installed CLI exposes comparable settings.

**Step 3: Rank on missed and control windows**

Missed windows:

- 00:21:10–00:36:32
- 00:38:16–00:45:09
- 00:55:42–01:00:00

Control windows:

- 00:19:54–00:21:10
- 00:36:32–00:38:16
- 01:00:00–01:05:00

Reject candidates with large increases in repeated phrases, known hallucination phrases, implausibly long cues, or text emitted over objectively silent windows.

### Task 4: Full-movie validation and production recommendation

**Files:**
- Output only: benchmark run directories and comparison report
- Potential future modification after approval: `orchestrator/transcription.py`
- Potential future modification after approval: `orchestrator/subtitle_quality.py`

**Step 1: Run the top candidates on the full 73:52 WAV**

Record runtime, cue count, covered seconds, maximum gap, Japanese characters per minute, long-cue ratios, and confidence distributions.

**Step 2: Validate audio-aware quality gates**

Add a proposed gate that fails rather than warns when:

- speech detected by the benchmark VAD has no nearby subtitle for a sustained interval;
- cue density is abnormally low;
- a cue spans an implausibly long interval;
- the subtitle/audio duration mismatch is too large.

**Step 3: Produce the recommendation**

Recommend:

- a default production preset;
- a fallback repair pass for suspicious gaps;
- a conservative noise-control rule;
- logging and artifact-retention changes needed for future debugging.

Do not change the production worker until the user approves the selected preset.

---

## Execution results (2026-07-20)

### Verified source

- M-drive source copied to
  `%LOCALAPPDATA%\jav-sub-orchestrator\benchmarks\dvrt-6701\audio.from-m-drive.wav`.
- SHA-256:
  `559882B0B25CA1E7165D1F5FA2450CC829BFC0E765E8801B94ED2E87F818015E`.
- Format: PCM signed 16-bit little-endian, mono, 16 kHz, 4432.597313 seconds,
  141,843,192 bytes.
- The independently recovered WAV differs from the M-drive file in only 10,949
  of 70,921,557 samples, with a maximum difference of one 16-bit quantization
  unit and correlation `0.9999999999887447`. Screening results from the recovered
  file are therefore comparable; the final production-baseline run used the
  exact M-drive copy.

### Root-cause evidence

- A full rerun with the production settings (`chunk_seconds=900`, default
  Silero VAD, beam 5, previous-text conditioning off) reproduced the 922.29
  second gap. The first 125 cues were byte-for-byte identical in text and
  timestamps to the saved production SRT.
- Silero VAD at thresholds 0.50, 0.35, and 0.25 marked nearly all of the
  21:00–36:00 interval as non-speech, even though audible low-level dialogue is
  present.
- Disabling VAD recovered much more text but produced severe repeated-vocal
  hallucinations. Lowering the VAD threshold alone did not produce a safe
  improvement.
- Enabling previous-text conditioning produced a 41% repeated-text ratio and
  did not repair the principal gap.
- Reducing the independent outer chunk size resets decoding often enough to
  recover quiet dialogue that the 900-second call loses. Two 30-second grids,
  offset by 15 seconds, independently recovered the 21:32 dialogue:
  `顔が見たいんで / マスク / 取ってもらえませんか / お金払うんで /
  それは無理です`.

The primary defect is therefore segmentation/VAD interaction plus an
insufficient quality gate, not a lack of Japanese vocabulary that should be
addressed first with model fine-tuning.

### Full-movie comparison

| Configuration | Runtime | Cues | Japanese chars/min | Maximum gap | Observed noise |
|---|---:|---:|---:|---:|---|
| Saved production SRT | — | 134 | 20.90 | 922.29 s | Very long low-text cues |
| Production settings rerun, 900 s | 27.57 s | 127 | 22.93 | 922.29 s | Reproduces omission |
| Default VAD, 90 s chunks | 45.68 s | 202 | 31.16 | 255.95 s | Moderate; best single-pass balance |
| Default VAD, 30 s chunks | 96.17 s | 284 | 73.18 | 121.89 s | High coverage but repeated vocals and boundary hallucinations |
| 90 s base + dual-grid 30 s consensus prototype | about 238 s total | 209 | 27.19 cleaned | 358.67 s | Aggressive heuristic removed all flagged repetitions, but needs more validation |

The 30-second raw character count is inflated by repeated non-speech output and
must not be treated as accuracy. The dual-grid test is useful as a targeted
repair pass, not as the sole transcript.

### Tool-version findings

- The installed `faster-whisper` 1.2.1 with local
  `faster-whisper-large-v3-turbo` was stable and fast.
- WhisperJAV 1.8.14 `faster/aggressive`, forced to use the same local model,
  completed but did not repair the principal gap.
- WhisperJAV reported a known 1.8.x routing fallback from its intended
  segmenter to Silero for the tested mode. A `large-v2` balanced run also failed
  to initialize in a reasonable time while the production GPU worker was
  active. It should not replace the production path based on this test.
- The original WhisperJAV configuration was restored after the experiment and
  verified by matching SHA-256 hashes.

### Recommended production design

1. Change the normal faster-whisper outer chunk size from 900 seconds to 90
   seconds, retaining default VAD, beam 5, and
   `condition_on_previous_text=False`.
2. Detect suspicious gaps after the primary pass. For those ranges only, run
   two 30-second grids offset by 15 seconds and accept lexical dialogue when
   both grids agree in time and normalized text.
3. Reject long low-text cues, low-diversity repeated characters, and unconfirmed
   long boundary phrases. Keep the raw secondary output for audit rather than
   publishing it.
4. Make low Japanese density and a sustained subtitle gap hard quality failures,
   not warnings. Record actual audio duration and per-segment confidence.
5. Do not delete `audio.wav` until transcription and quality validation pass.
   Retain failed-job audio and manifests for a bounded debugging window.
6. Validate the 90-second preset and gap repair on several other known-problem
   movies before changing the production worker.

Actual model fine-tuning should be considered only after this segmentation
repair is deployed and a labeled false-negative/false-positive evaluation set
shows a remaining model-level error.
