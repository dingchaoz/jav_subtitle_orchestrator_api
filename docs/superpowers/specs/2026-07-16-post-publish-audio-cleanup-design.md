# Post-Publish Audio Cleanup and Low-Disk Recovery Design

## Goal

Prevent completed subtitle jobs from filling the Mac disk, make the behavior visible in the API/dashboard, and prevent the upstream downloader's low-disk safety pause from exhausting job retries.

## Confirmed failure

The Mac data volume fell below the upstream downloader's 5 GiB safety threshold. The downloader exited successfully after printing `Pausing before next download` but produced no WAV. The adapter converted that into `Downloaded audio ... not found`, and the Mac worker consumed all three attempts. The current production database contains 29 failures with that exact signature.

Completed jobs also retain large WAV files. A conservative production allowlist currently contains 61 jobs (14.95 GiB) that are `english_srt_ready`, have a verified publication receipt, completed catalog sync, both local SRTs, and an existing WAV.

## Publication boundary and ownership

The Mac translation worker owns cleanup for normal jobs. Historical repair jobs retain audio because their preservation contract verifies the original audio through later repair stages. A normal job may delete only the expected `audio.wav` after all of these are true:

1. Supabase publication returned a verified receipt.
2. `JobStore.complete_supabase_publication` durably recorded the receipt and moved the artifact to `english_srt_ready`.
3. `DELETE_AUDIO_AFTER_PUBLISH` is enabled.
4. The audio is a regular file in the expected job directory under the configured jobs root.

The cleanup helper independently validates the ready state, normal origin, and complete verified receipt before it can unlink anything. Cleanup holds the shared root/exclusive job-files lock and a SQLite write transaction across the final state validation, unlink, and durable path clearing, so historical conversion and force-reset cannot race an irreversible deletion. After deletion (or an already-missing file), the store clears the durable audio paths. Each translation-worker poll reconciles a bounded batch of verified jobs whose audio paths remain set, closing the crash window between durable publication and deletion and retrying transient cleanup errors without republishing or retranslating.

Catalog synchronization remains non-blocking and may retry after cleanup because it uses the publication receipt, not the source audio. Translation, quality, publication, or durable-transition failures retain audio. A missing audio file is idempotent success. An unsafe path or OS deletion error is logged without changing the successfully published job to failed.

The Windows worker does not own this cleanup. Explicit preservation canaries may construct a worker with cleanup disabled for that run.

## API and dashboard visibility

`GET /dashboard/state` includes an `audio_cleanup` object with `enabled` and `trigger="verified_supabase_publication"`. The Operations health grid displays the setting so operators can confirm the running API loaded the intended configuration. `audio-cleanup.log` is available through the existing job-log API and dashboard log viewer.

The environment setting defaults to enabled and is documented in `.env.example`. Both the API and Mac translation worker load the same setting on startup.

## Low-disk deferral

When the downloader exits zero, produces no WAV, and its output contains the known safety-pause marker, the adapter raises `DownloadDeferredError`. The Mac download worker returns the job to `queued`, preserves `attempt_count`, records a deferred log/worker state, and returns `False` so its outer loop sleeps before polling again. Other failures continue to follow the existing retry policy.

## Production recovery

Before deleting anything, recompute the conservative allowlist from the live database and filesystem. Delete only those allowlisted WAVs and verify reclaimed space. After merging and restarting current `main`, reset only jobs whose error exactly matches the low-disk missing-audio signature; do not reset unrelated short-audio, catalog, translation, or publication failures. Observe at least one restored job advance beyond download.

## Non-goals

- Do not delete SRT, metadata, logs, videos, or Supabase objects.
- Do not require catalog sync success for future automatic cleanup.
- Do not build a general-purpose disk cleaner or merge the unrelated cross-site downloader branch.
