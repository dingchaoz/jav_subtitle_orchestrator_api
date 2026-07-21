# Adaptive Japanese Transcription Pipeline Design

## Objective

Future Windows transcription jobs should automatically use the DVRT-6701
benchmark findings: a 90-second primary faster-whisper pass for materially
better coverage, followed by a conservative repair pass only where the primary
transcript has suspiciously long gaps. Existing completed jobs are not changed.
The built-in `FasterWhisperTranscriber` remains the production implementation;
an explicitly configured external transcription script keeps its existing
behavior.

## Architecture and data flow

The primary pass divides the audio into globally aligned 90-second WAV chunks
and transcribes each chunk with the current stable decoding settings:
Japanese, beam size 5, default Silero VAD, and previous-text conditioning
disabled. The transcriber then builds a set of trusted subtitle intervals.
Known long boundary hallucinations, low-diversity repeated characters, and
very long low-text cues do not count as trusted coverage when detecting gaps.

Any trusted-coverage gap of at least 60 seconds becomes a repair window, with
15 seconds of context added to both sides. Overlapping repair windows are
coalesced. Each repair window is covered by two independent 30-second grids:
one aligned at 0, 30, 60… seconds on the original movie timeline, and one
aligned at 15, 45, 75… seconds. Global alignment is important because it
recreates the DVRT-6701 experiment and makes results deterministic regardless
of where a gap begins.

A candidate from the first repair grid is accepted only if the second grid
contains sufficiently similar normalized Japanese text at a nearby time.
Matching supports cues that are split differently between the two passes by
comparing a cue against short consecutive sequences from the other grid.
Confirmed candidates are merged into the primary transcript; near-duplicate
primary cues are not added again. Definite long template hallucinations and
repeated-character output are removed from the published SRT.

## Configuration and failure behavior

`WindowsSettings` exposes the primary chunk size, repair enable flag, gap
threshold, repair chunk size, stagger offset, context padding, and minimum text
similarity. Defaults select the benchmarked production preset:

- primary chunk: 90 seconds;
- repair enabled: true;
- suspicious gap: 60 seconds;
- repair chunk: 30 seconds;
- second-grid offset: 15 seconds;
- context padding: 15 seconds;
- minimum normalized-text similarity: 0.72.

Invalid values fail settings validation at worker startup rather than silently
running an unsafe configuration. If FFprobe cannot determine the duration, the
transcriber preserves the current fallback behavior: transcribe the input once
and skip gap repair. FFmpeg extraction or Whisper inference failures remain job
failures and flow through the existing retry mechanism. The output SRT is still
written atomically only after all passes and merging complete, so a failed
repair cannot leave a partial final subtitle.

The Windows worker writes a compact per-job repair summary to `whisper.log`,
including audio duration, primary cue count, repair-window count, confirmed cue
count, final cue count, and final maximum gap. This makes future coverage
regressions diagnosable without retaining raw model text in central logs.

## Testing and rollout

Pure unit tests cover text normalization, hallucination detection, repair-window
discovery, global staggered-grid alignment, split-cue consensus, and
deduplication. Orchestration tests use a fake transcriber/model boundary to
prove the 90-second primary pass is followed by targeted repair and that the
final SRT is atomic. Configuration tests prove the new defaults and wiring from
`.env.windows` into `FasterWhisperTranscriber`. Worker tests prove repair
statistics are appended without changing the API completion sequence.

After the focused tests pass, the complete suite must pass. A final local smoke
run against the retained DVRT-6701 audio should verify that the production
implementation recovers the confirmed 21:32 dialogue and does not reintroduce
the rejected long repeated outputs. The running Windows worker must be restarted
only after confirming it is not actively transcribing a claimed job. The new
code then applies automatically to every subsequently claimed API job.
