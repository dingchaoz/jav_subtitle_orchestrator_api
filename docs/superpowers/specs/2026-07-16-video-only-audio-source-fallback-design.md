# Video-Only Audio Source Fallback Design

## Problem

The Mac downloader can resolve a valid MissAV HLS source that contains video
renditions but no audio stream. The downstream pipeline first fails direct WAV
extraction, then downloads a video-only temporary file and fails local audio
extraction. Repeating that sequence consumes bandwidth and eventually exhausts
the orchestrator job's download attempts.

`mfyd-123` is the production example. Its base source exposes four H.264-only
renditions. The catalog also contains `mfyd-123-uncensored-leak`, whose stream
contains H.264 video and AAC audio.

## Goals

- Recover a requested base movie when an authoritative same-base catalog
  variant has usable audio.
- Preserve the requested orchestrator movie code and output paths.
- Avoid consuming another orchestrator attempt when the fallback succeeds.
- Avoid repeatedly downloading a known video-only temporary file.
- Keep retry and error behavior explicit when no usable alternate exists.

## Non-goals

- Searching arbitrary third-party providers.
- Treating unrelated movie codes as interchangeable.
- Publishing an SRT before the existing validation and catalog-sync stages.
- Changing subtitle publication, catalog synchronization, or audio cleanup
  receipt requirements.

## Design

### Source classification

The adapter will recognize the upstream ffmpeg signature for a source without
an audio stream, including `Output file does not contain any stream`. This is
distinct from low disk space, missing ffmpeg, HTTP resolution failures, and
ordinary corrupt downloads.

The MissAV pipeline will stop its fallback sequence when direct extraction has
proven that the selected source contains no audio. Downloading the same source
to a temporary video cannot create a missing audio stream, so that work is
futile.

### Same-base catalog fallback

On an explicit no-audio result, `MissAVAdapter` will inspect the existing
release catalog for entries whose normalized base movie ID matches the
requested movie. Existing supported suffix semantics are used, such as
`-uncensored-leak`, `-uncensored`, and subtitle variants.

Candidates are bounded, deterministic, and catalog-backed. The requested entry
is excluded. The adapter retries candidates inside the same `download_audio`
call and validates the produced path using the candidate code. A successful WAV
is moved to the original requested job's canonical `audio.wav` path, so all
downstream stages continue to use the requested code.

### Exhaustion behavior

If no authoritative same-base candidate has audio, the adapter returns a clear
`source_no_audio` error containing the attempted candidates. Existing durable
retry backoff prevents rapid retry exhaustion. The job remains diagnosable and
does not silently substitute an unrelated source.

### Logging

Job logs and stored errors identify:

- the primary source as video-only;
- the alternate catalog code selected;
- fallback success or candidate exhaustion.

No cookies, authorization headers, or full ffmpeg command lines are logged.

## Data flow

1. The Mac worker downloads metadata for the requested movie.
2. The adapter invokes the primary audio source.
3. If it succeeds, behavior is unchanged.
4. If it explicitly reports no audio, the adapter enumerates same-base catalog
   candidates and retries them in deterministic order.
5. The first validated WAV is moved to the requested canonical path.
6. The existing worker marks the requested job `audio_ready`.
7. Windows transcription, Mac translation, Supabase publication, catalog sync,
   and verified post-publish audio cleanup run unchanged.

## Testing

- Pipeline test: a direct no-audio error skips the temp-video fallback.
- Adapter test: a video-only primary source selects a catalog-backed same-base
  variant and preserves the requested output path.
- Adapter test: unrelated catalog entries are never selected.
- Adapter test: exhausted variants return a clear `source_no_audio` error.
- Existing low-disk, retry-backoff, publication, sync, and cleanup tests remain
  green.
- Production verification promotes `mfyd-123`, recovers `mism-281`, resumes the
  downloader, and confirms worker/API health plus an HTTP 200 catalog-sync
  receipt after a restored job completes.

## Operational rollout

1. Keep the downloader paused while implementing and testing.
2. Back up the live job database.
3. Deploy the tested orchestrator and pipeline changes.
4. Reset `mfyd-123` download counters and recover interrupted `mism-281`.
5. Resume the downloader and monitor both jobs.
6. Confirm the dashboard, API, downloader, Windows worker, translation worker,
   catalog synchronization, and verified audio cleanup.
