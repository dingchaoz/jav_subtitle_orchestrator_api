# Normal-First Historical Subtitle Repair Design

**Date:** 2026-07-13

**Status:** Approved direction; written specification awaiting operator review

## Goal

Complete one new end-to-end production canary, then repair the approved historical
subtitle allowlist in bounded batches without delaying or racing normal Windows
transcription work. A job is complete only after the Mac quality gate passes,
Supabase publication is verified, the javsubtitle.com D1/KV catalog is synchronized,
and the public movie API exposes the repaired subtitle.

## Production Evidence at Design Time

The dashboard snapshot taken before this design showed:

- the API and Cloudflare tunnel running;
- `windows-gpu-1` online and polling, but idle;
- `mac-translation-1` online and polling, but idle;
- no `mac-worker` downloader process;
- 51 queued jobs and one job, `roe-291`, stale in `downloading_audio`;
- a complete-looking staged WAV for `roe-291` at
  `/Users/ytt/MissAVJobs/roe-291/audio/roe-291.wav`;
- no process holding that staged file;
- the staged WAV parsing as PCM signed 16-bit, 16 kHz, mono, with a non-zero
  duration and a size of 221,773,824 bytes; and
- the orchestrator expecting the final file at
  `/Users/ytt/MissAVJobs/roe-291/audio.wav`.

The per-job log stops after `downloading_audio`. The downloader code moves the
staged file to the final path and marks `audio_ready` only after the child downloader
returns. Therefore the proximate failure is an abrupt downloader termination between
staged-file production and finalization. No application exception was recorded, so
the exact external signal or terminal shutdown that stopped the old process is not
known.

## Scope

This work includes:

1. safe recovery of the exact staged `roe-291` audio as the new-job canary;
2. durable operation of exactly one Mac downloader and one Mac translation worker;
3. automatic post-publication javsubtitle.com D1/KV catalog synchronization;
4. a normal-first, single-translation-slot historical repair lane;
5. explicit allowlists, bounded batches, dry-run reports, and resumable progress;
6. quality, publication, website, and preservation verification for every repair;
7. dashboard visibility for normal and historical activity; and
8. automatic continuation through the approved allowlist while safety gates remain
   healthy.

This work excludes:

- Windows translation;
- two concurrent TranslateLocally processes;
- bulk re-transcription;
- resetting download or transcription stages for historical jobs;
- deleting Japanese SRTs, `audio.wav`, or rejected English SRTs;
- `force=True`;
- unbounded job selection; and
- synchronizing movies outside the explicit allowlist.

## Architecture Decision

Use one translation execution slot with two logical lanes:

1. **Normal lane:** new Windows-produced `transcription_done` jobs.
2. **Historical lane:** allowlisted repair records waiting in a separate SQLite
   repair queue.

The normal lane always wins. A historical repair may start only when there is no
claimable normal translation or normal publication job. Once one historical repair
has started, it is allowed to finish its translation, quality, Supabase, and catalog
publication unit. A newly arriving normal job waits at most for that single in-flight
unit; it is selected before the next historical repair.

This design deliberately does not run a second TranslateLocally process. It avoids
CPU contention, duplicate claims, shared quality-failure counters, and publication
races while still using otherwise idle Mac capacity for repair work.

## Components

### 1. Interrupted Download Recovery

Add an exact-job recovery operation for a job that is stale in
`downloading_audio`. The operation must:

1. require an exact job ID and movie code;
2. require the database status to still be `downloading_audio`;
3. reject symlinks and paths outside the exact job directory;
4. verify that the final `audio.wav` does not already exist;
5. locate only the adapter's recognized staged output path;
6. validate the WAV container, PCM format, sample rate, channel count, frame count,
   and file-size consistency without printing audio content or metadata;
7. calculate and report SHA-256, byte count, and duration statistics;
8. use `os.replace` to atomically move the staged file to the expected `audio.wav`
   path;
9. update the same job to `audio_ready` with Mac and Windows paths; and
10. leave the staged file and job state unchanged if any precondition fails.

The recovery command is not a general selector and has no batch or force option.
Because the filesystem move and SQLite commit cannot share one transaction, the
operation must be crash-resumable: if a process stops after `os.replace` but before
the database commit, rerunning the exact command validates the final `audio.wav` and
completes only the missing state transition without redownloading or overwriting it.
The ordinary downloader restart must also recover stale database leases without
deleting completed audio.

### 2. Durable Mac Worker Supervision

Run the API, downloader, and translation worker as three separate services. Add or
document launchd definitions for exactly one downloader and one translation worker.
Each service must have:

- an explicit working directory and virtual-environment Python path;
- stdout/stderr logs under the orchestrator `logs/` directory;
- restart-on-unexpected-exit behavior with bounded backoff;
- a stable worker ID; and
- a pre-start duplicate-process check.

The downloader is the only component that changes `queued` jobs to `audio_ready`.
The Windows worker remains the only component that changes `audio_ready` through
transcription to `transcription_done`.

### 3. Automatic Website Catalog Synchronization

After the existing Mac publisher has passed the quality gate, uploaded or upserted
the English SRT, and verified Supabase Storage and catalog metadata, it must call:

`POST /api/admin/catalog/sync-subtitles`

with exactly the canonical movie code and a bounded payload. Configuration uses a
server-only API base URL and admin token. The token must never be returned, logged,
placed in job snapshots, or exposed through the dashboard.

The website call is idempotent. It reads the authoritative Supabase catalog, updates
D1 transactionally, and invalidates only the exact full/light KV keys. The
orchestrator accepts success only when the response contains the requested canonical
code, `success=true`, `synced=1`, and no failures.

If Supabase publication succeeds but catalog synchronization fails, the job enters a
catalog-sync-pending state. Retries repeat only Supabase verification and catalog
synchronization; they do not retranslate, delete audio, or replace a verified English
file. `english_srt_ready` is assigned only after the catalog sync succeeds.

### 4. Historical Repair Queue

Store repair intent separately from normal job status. Each repair record contains:

- repair ID and batch ID;
- exact job ID and canonical movie code;
- source allowlist identity or digest;
- state: `planned`, `pending`, `running`, `succeeded`, `permanent_failed`, or
  `retry_wait`;
- bounded attempt counters and next-retry time;
- structured reason code without subtitle text;
- timestamps; and
- hashes needed to prove Japanese/audio preservation and English publication.

Planning is GET/read-only and prints what would be repaired, quarantined, requeued,
and overwritten. Applying a plan requires its digest, the same allowlist, and a
positive batch limit. Unknown jobs, changed paths/hashes, active claims, missing
Japanese SRTs, or missing audio are rejected before any state change.

The repair record is claimed atomically. Only then is the exact job's translation
stage reset. The normal polling query must never see unclaimed historical repair
records as ordinary `transcription_done` work.

### 5. Normal-First Scheduling

For every loop, the single Mac translation worker performs this order:

1. recover expired normal publication and translation leases;
2. finish an already in-flight publication/catalog-sync unit;
3. claim one normal publication retry, if present;
4. claim one normal `transcription_done` job, if present;
5. otherwise claim one due historical repair record; and
6. otherwise record itself idle and sleep for the configured poll interval.

Historical queue insertion does not change the ordering of normal jobs. Historical
jobs are processed sequentially. A batch controls reporting and safety gates, not
translation concurrency.

Normal and historical consecutive-quality-failure counters are separate. Three
consecutive deterministic historical quality failures pause the historical lane and
leave the normal lane polling. Existing normal-worker protection remains in force for
normal jobs.

### 6. Historical Translation and Publication

For each claimed repair:

1. snapshot Japanese SRT and audio hashes;
2. preserve the Japanese SRT and `audio.wav` in place;
3. move the old English SRT to `rejected/` with a collision-safe name;
4. translate only from the existing Japanese SRT;
5. run the same server-side Japanese/English quality gate and thresholds;
6. on deterministic quality failure, preserve the rejected candidate, mark the
   repair permanently failed, and never upload or overwrite Supabase;
7. on success, upsert the exact Supabase Storage path and catalog row;
8. verify remote SHA-256, size, catalog UUID/path, and metadata status;
9. synchronize D1/KV through the admin endpoint;
10. verify the public movie API exposes the expected subtitle ID; and
11. mark both repair and job successful only after all checks pass.

Transient network or catalog failures use bounded retry and resume from the latest
verified stage. They must not trigger retranslation. Error logs use structured reason
codes and statistics only; they never include full subtitle text, adult metadata, or
secrets.

### 7. Batch Policy

The initial historical batch contains at most five allowlisted jobs. If the batch
finishes without a safety stop, subsequent batches may contain 10 to 20 allowlisted
jobs. Translation remains one job at a time.

The controller may continue through the approved allowlist without a new approval
between healthy batches, but it must pause when any of these conditions occurs:

- any normal job or normal publication retry is waiting;
- three consecutive historical deterministic quality failures occur;
- catalog authentication or configuration fails;
- Supabase verification cannot establish the expected remote object/catalog state;
- the website sync returns an unsafe, malformed, or mismatched result;
- Japanese or audio preservation hashes change;
- the normal Mac translation-worker process count is not exactly one;
- the allowlist or plan digest changes; or
- the operator stops the controller.

When normal work arrives, historical processing resumes automatically only after the
normal translation/publication backlog returns to zero and the other safety gates are
healthy.

### 8. Dashboard and Reporting

The dashboard must distinguish:

- Mac downloader activity;
- Windows transcription activity;
- normal Mac translation activity;
- historical repair activity;
- current repair batch and bounded progress counts; and
- paused state with a structured reason code.

Reports contain job IDs, canonical movie codes, state transitions, attempt counts,
hashes, sizes, quality statistics, catalog result counts, and reason codes. Reports do
not contain subtitle text, credentials, or adult titles/descriptions.

## Canary Rollout

`roe-291` is the exact new-job canary.

1. Record the staged audio SHA-256, byte count, duration, Japanese/English absence,
   and current database status.
2. Run the exact interrupted-download recovery command.
3. Require `audio_ready` and the expected final Mac/Windows paths.
4. Start or confirm exactly one durable downloader and one normal translation worker.
5. Observe Windows claim the job and finish at `transcription_done` with a Japanese
   SRT and no Windows-created English SRT.
6. Observe the Mac worker claim `translating`, create English, and pass the quality
   gate.
7. Verify Supabase Storage/catalog and D1/KV synchronization.
8. Verify `english_srt_ready`, unchanged Japanese/audio hashes, and website subtitle
   controls in a real browser.
9. Run website catalog and playback regression smokes.
10. Stop the rollout if any requirement fails; do not start historical repairs.

Only after all ten steps pass may the first historical batch be applied.

## Testing

Automated tests must prove:

- exact staged-audio recovery accepts a complete WAV and rejects partial, symlinked,
  mismatched, wrong-job, and already-finalized inputs;
- recovery uses an atomic filesystem replace, is crash-resumable across the SQLite
  update, and does not redownload or delete audio;
- downloader and translation services refuse duplicate instances;
- normal translation work wins over pending historical work;
- an in-flight historical repair finishes, then a newly arrived normal job wins;
- historical quality failures do not stop the normal lane;
- bad historical English never reaches Supabase or the website sync client;
- successful publication invokes the website sync exactly once with the exact code;
- website-sync failure cannot mark `english_srt_ready` and retries without
  retranslation;
- Supabase same-path upsert still repairs an existing subtitle;
- Japanese and audio hashes remain unchanged for success and failure;
- old and rejected English files are preserved with collision-safe names;
- dry-run and changed-plan checks make zero state changes;
- batches never select a code outside the explicit allowlist or limit; and
- dashboard output contains structured state but no subtitle text or secrets.

Production verification must include the `roe-291` canary, a five-job historical
batch, public API checks, a real-browser subtitle-control check, and the existing
catalog/playback regression scripts.

## Security and Data Safety

- Supabase service-role and website admin tokens remain server-side.
- No token, subtitle body, or adult metadata enters logs, reports, SQLite repair
  records, or dashboard JSON.
- All filesystem operations are confined to canonical job directories and reject
  symlinks.
- Supabase publication uses the existing verified same-path upsert behavior.
- D1/KV synchronization uses the already deployed admin endpoint and exact cache-key
  scope.
- No operation deletes audio, Japanese SRT, or rejected English SRT.
- No operation uses `force=True`.

## Success Criteria

The work is complete when:

1. `roe-291` completes the full new-job path and is visible on javsubtitle.com;
2. exactly one durable downloader and one normal translation worker are healthy;
3. normal Windows-originated work always takes priority over historical repair;
4. the approved historical allowlist is exhausted or every remaining item has a
   structured permanent/blocking result;
5. every successful repair is verified in Supabase, D1/KV, the public API, and local
   preservation hashes; and
6. final reports show counts and reason codes without secrets or subtitle text.
