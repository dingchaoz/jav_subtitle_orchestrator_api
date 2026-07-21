# Recent Transcription Backfill Design

## Goal and inventory

Reprocess movies created during a configurable recent interval with the new
90-second adaptive Japanese transcriber, without interrupting or racing normal
API jobs. The default selection window is the previous 72 hours. The batch must
produce a consistent Japanese subtitle, English translation, quality report,
and published artifact; replacing only the Japanese file would leave the
English and published subtitle stale.

The 2026-07-20 inventory found 338 jobs created during the preceding 72 hours:

| State | Count |
|---|---:|
| `english_srt_ready` | 155 |
| `failed` | 176 |
| `audio_ready` | 2 |
| `downloading_audio` | 3 |
| `queued` | 2 |

Of the 155 completed jobs, cleanup had removed `audio.wav` for 154. Only five
job directories still contained audio. Two were invalid short extracts
(`ebwh-342`, 17.88 seconds; `hxaw-003`, 12.97 seconds). Three contained
full-length audio: `dvrt-6701` (already validated with the new method), `n-599`,
and `hbad-125`. Therefore a full three-day backfill must re-download audio; it
cannot be implemented as a Windows-only loop over existing WAV files.

The 176 metadata/download failures are not transcription backfill candidates.
They should remain in a separate source-recovery workflow.

## Approaches considered

### Recommended: low-priority backfill through the existing queue

Add a read-only inventory/dry-run command and a batch producer that submits
eligible movie numbers as explicit low-priority retranscription jobs. The
producer never loads Whisper. The existing Mac downloader restores each audio,
the one Windows GPU worker runs the adaptive transcriber, and the Mac worker
translates, quality-checks, publishes, and cleans up. Real-time jobs use a
higher priority and are selected first. A backfill movie already in inference
is not preempted, so a newly arriving real-time job may wait for the current
movie, normally a few minutes.

This is the only approach that preserves one source of truth for job state,
leases, retries, heartbeat, translation, publication, and cleanup while also
preventing CUDA concurrency.

### Separate GPU batch transcriber

Rejected as the default. A second process can load the same CUDA model while
the API worker is running, causing slowdown or out-of-memory failures. Directly
overwriting canonical `.Japanese.srt` files also races translation and leaves
the existing English/published artifact inconsistent. It is safe only if the
real-time worker is stopped and all output is written to a separate staging
directory for later promotion.

### Overnight maintenance window

Operationally simple for a one-time emergency: stop the real-time worker, run a
staged batch, then restart it. New API jobs remain queued during the window. It
does not meet the desired simultaneous real-time plus batch behavior and is not
recommended as the long-term design.

## Required queue and force-reset fixes

The current `force=true` API path is not sufficient for retranscription:

1. `_force_reset` clears database paths but leaves physical Japanese and English
   SRT files in the job directory.
2. `WindowsWorker` skips transcription whenever the Japanese SRT already
   exists, so a forced job can silently reuse the old 900-second transcript.
3. `_force_reset` does not update the requested priority, preventing reliable
   real-time-over-backfill scheduling.
4. Replacing Japanese without regenerating English and republishing would
   create inconsistent artifacts.

Before enqueueing the three-day batch, add an explicit retranscription reset:

- reject reset while a job is in an active leased state;
- set the requested low priority and a fresh queue timestamp;
- atomically move old Japanese, English, and quality artifacts into a timestamped
  `history/<run-id>/` directory rather than deleting them;
- clear publication/translation state in the deployed schema;
- re-download missing audio through the Mac pipeline;
- retain the new audio until Japanese transcription, English translation,
  quality validation, and publication all succeed;
- restore or leave the old published artifact untouched when the replacement
  fails.

## Scheduling and locking

Use a single logical GPU executor. Real-time jobs should have priority `100`;
backfill jobs should use priority `1000` or greater. Claim queries already sort
ascending by priority, so new real-time work is selected before the next batch
movie. Only one Windows worker should claim either category.

As defense in depth, add a named/file GPU lease shared by every command capable
of loading faster-whisper. The long-running worker holds the lease only during
inference, and any maintenance CLI must acquire the same lease before loading
the model. The queue remains the primary scheduler; the lease prevents an
operator from accidentally starting a second independent CUDA process.

The batch producer should support:

- `--since-hours 72`;
- `--dry-run` by default;
- `--status english_srt_ready` eligibility;
- `--priority 1000`;
- `--limit` and an explicit confirmation/apply flag;
- checkpointed JSON manifest with selected, submitted, skipped, and failed
  items;
- pause when the API is unreachable;
- idempotent resume by run ID.

## Rollout

1. Implement and test retranscription reset, artifact history, priority update,
   and GPU lease.
2. Test one failed/publication job with retained audio (`n-599` or `hbad-125`)
   without overwriting its canonical files until quality passes.
3. Test one completed job whose audio must be re-downloaded.
4. Run a five-movie canary at backfill priority while submitting one normal API
   job; verify the normal job runs before the next batch item.
5. Compare old/new Japanese coverage statistics and validate regenerated
   English before promotion.
6. Enqueue the remaining eligible completed jobs in checkpointed groups.

At the measured DVRT-6701 cost, 155 movies are likely an 8–16 hour GPU workload
plus re-download and translation time. It should be treated as a resumable
backfill campaign, not a single untracked shell process.
