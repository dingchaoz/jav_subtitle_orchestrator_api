# JAV Subtitle Orchestrator Design

Date: 2026-07-04

## Purpose

Build a standalone repository named `JAV-Subtitle-Orchestrator` that coordinates subtitle generation across two machines on the same home network:

- Mac: receives API requests, downloads MissAV metadata, downloads audio, owns the shared job directory, and stores job state.
- Windows NVIDIA laptop: polls for ready jobs, reads audio from the Mac SMB share, runs Japanese transcription, runs English SRT translation, writes final subtitle files back to the same share.

The system should accept one or many movie numbers, queue them reliably, survive restarts, avoid public file transfer costs, and produce an English `.srt` file per movie.

## Goals

- Accept API submissions such as `ktb-096`, `ktb-095`, and `ktb-093`.
- Support batch submission of many movie numbers.
- Keep this code outside `MissAV-Pipeline` in its own local Git repository.
- Reuse the existing MissAV scripts rather than copying their internals.
- Reuse the existing SRT translation script rather than rewriting translation logic in the first version.
- Keep large media files on the local network through SMB, not through Cloudflare, public object storage, or cloud GPU storage.
- Make Windows automatically pick up work without manual command execution per movie.
- Keep first-version concurrency conservative: one Mac download job and one Windows AI job at a time.
- Make job status visible through API endpoints.

## Non-Goals

- Do not expose SMB to the public internet.
- Do not build a web UI in the first version.
- Do not move all MissAV scraping/downloading into the new repo.
- Do not require a cloud GPU server for the first version.
- Do not run multiple Windows GPU jobs in parallel until the single-worker flow is stable.
- Do not create a GitHub remote automatically. The first version creates a standalone local repo that can later be pushed to GitHub.

## Repositories And Local Paths

New standalone repo:

```text
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
```

Existing MissAV repo used by adapter code:

```text
/Users/ytt/Documents/startup/MissAV-Pipeline
```

Mac-owned shared job root:

```text
/Users/ytt/MissAVJobs
```

Windows mapped SMB drive:

```text
M:\
```

The Mac path and Windows path refer to the same files:

```text
Mac:     /Users/ytt/MissAVJobs/ktb-096/audio.wav
Windows: M:\ktb-096\audio.wav
```

## Existing Code To Reuse

From `MissAV-Pipeline`:

- `new-release/unified_download.py`: existing release catalog, metadata, m3u8, and queue flow.
- `new-release/batch_audio_downloader.py`: existing audio downloader that extracts WAV audio.
- `audio_download_queue.py`: queue helper patterns and movie number normalization.
- `missav_stream_downloader.py`: authenticated stream resolution and ffmpeg audio extraction.

From `E2E-download-subtitle-generation-translation-scripts`:

- `scripts/subtitle_translate.py`: SRT parser and OpenAI-backed subtitle translation.

The new repo should call these scripts or import stable functions where practical. If direct imports are too coupled, the first implementation can shell out to wrappers with explicit input and output paths.

## High-Level Architecture

```text
User or automation
  |
  | POST /jobs or POST /jobs/batch
  v
Mac FastAPI service
  |
  | writes job row to SQLite
  v
Mac downloader worker
  |
  | downloads metadata and audio into /Users/ytt/MissAVJobs/<movie>/
  v
SQLite status = audio_ready
  |
  | Windows worker polls GET /worker/next-job
  v
Windows worker
  |
  | reads M:\<movie>\audio.wav
  | runs faster-whisper
  | writes M:\<movie>\<movie>.Japanese.srt
  | runs existing translate SRT script
  | writes M:\<movie>\<movie>.English.srt
  v
Mac API status = english_srt_ready
```

Mac never needs to remotely execute commands on Windows. The Windows worker is a long-running process that asks the Mac API for work.

## Components

### Mac API Service

Runs on the Mac with FastAPI and listens on the LAN:

```text
http://0.0.0.0:8000
```

Responsibilities:

- Validate submitted movie numbers.
- Create or return existing jobs.
- Support batch job creation.
- Serve job status.
- Let workers claim jobs atomically.
- Accept worker heartbeats, completion events, and failure reports.
- Return final file metadata and optionally serve the final English SRT.

### SQLite Job Store

SQLite is the source of truth for job state. It is better than JSON for this system because multiple processes will read and update jobs, and batch queueing needs atomic claim behavior.

Database path:

```text
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3
```

Primary table:

```text
jobs
  id TEXT PRIMARY KEY
  movie_number TEXT NOT NULL
  normalized_movie_number TEXT NOT NULL
  status TEXT NOT NULL
  priority INTEGER NOT NULL DEFAULT 100
  attempt_count INTEGER NOT NULL DEFAULT 0
  worker_attempt_count INTEGER NOT NULL DEFAULT 0
  claimed_by TEXT
  lease_expires_at TEXT
  created_at TEXT NOT NULL
  updated_at TEXT NOT NULL
  error TEXT
  job_dir_mac TEXT NOT NULL
  job_dir_windows TEXT NOT NULL
  metadata_path_mac TEXT
  audio_path_mac TEXT
  audio_path_windows TEXT
  japanese_srt_path_mac TEXT
  japanese_srt_path_windows TEXT
  english_srt_path_mac TEXT
  english_srt_path_windows TEXT
```

Unique index:

```text
unique(normalized_movie_number)
```

This prevents duplicate jobs when the same movie is submitted twice.

### Mac Downloader Worker

Runs on the Mac as a separate process from the API.

Responsibilities:

- Poll SQLite for `queued` jobs.
- Mark one job as `downloading_metadata`.
- Fetch metadata through the existing MissAV code path.
- Write `metadata.json` in the job folder.
- Mark job as `downloading_audio`.
- Download or extract `audio.wav`.
- Mark job as `audio_ready`.
- On retryable failures, increment `attempt_count` and move job back to `queued` after a delay.
- On terminal failures, mark job as `failed`.

First-version concurrency:

```text
MAC_DOWNLOAD_CONCURRENCY=1
```

This is intentionally conservative because MissAV downloading is the part most likely to hit rate limits, auth issues, or Cloudflare behavior.

### Windows Worker

Runs on Windows as a long-running Python process.

Responsibilities:

- Poll `GET /worker/next-job`.
- Claim one `audio_ready` job.
- Read audio from `M:\<movie>\audio.wav`.
- Run `faster-whisper` with Japanese language mode.
- Write `M:\<movie>\<movie>.Japanese.srt`.
- Run the existing SRT translation script.
- Write `M:\<movie>\<movie>.English.srt`.
- Report completion to the Mac API.
- Send heartbeat while processing so the Mac knows the worker is alive.
- Report failures with error text.

First-version concurrency:

```text
WINDOWS_WORKER_CONCURRENCY=1
```

The queue can contain many movies, but one Windows GPU job runs at a time. This avoids VRAM pressure and makes failures easier to reason about.

## Job Folder Layout

Each movie gets one folder under the Mac SMB share:

```text
/Users/ytt/MissAVJobs/
  ktb-096/
    job.json
    metadata.json
    audio.wav
    ktb-096.Japanese.srt
    ktb-096.English.srt
    logs/
      mac-download.log
      windows-worker.log
      whisper.log
      translate.log
```

`job.json` is a convenience snapshot for humans and debugging. SQLite remains the source of truth.

## Job Status Model

Statuses:

```text
queued
downloading_metadata
downloading_audio
audio_ready
transcription_claimed
transcribing
transcription_done
translating
english_srt_ready
failed
cancelled
```

Mac transitions:

```text
queued -> downloading_metadata
downloading_metadata -> downloading_audio
downloading_audio -> audio_ready
audio_ready -> transcription_claimed
transcription_claimed/transcribing/translating -> failed when worker reports failure or lease expires too many times
```

Windows transitions through API reports:

```text
transcription_claimed -> transcribing
transcribing -> transcription_done
transcription_done -> translating
translating -> english_srt_ready
```

## Queue Behavior

The API accepts many movie IDs:

```bash
curl -X POST http://mac-ip:8000/jobs/batch \
  -H "Content-Type: application/json" \
  -d '{"movie_numbers":["ktb-096","ktb-095","ktb-093"]}'
```

Each valid movie number becomes a row in SQLite. The queue is ordered by:

```text
priority ASC, created_at ASC
```

Default priority is `100`. Lower numbers run earlier.

Duplicate behavior:

- If `ktb-096` already exists, `POST /jobs` returns the existing job.
- If `force=true` and the existing job is not actively claimed, the system resets that existing job to `queued`, clears output paths and error text, increments no counters, and preserves the same job ID.
- If `force=true` and the existing job is actively claimed, the API returns HTTP `409 Conflict` with the current job status. Version 1 does not create parallel duplicate jobs for the same normalized movie number.

## Worker Claim And Lease

Worker claim must be atomic so future multiple Windows workers do not process the same job.

Claim flow:

```text
Windows: GET /worker/next-job?worker_id=windows-gpu-1
Mac API:
  1. starts SQLite transaction
  2. finds oldest audio_ready job with no valid lease
  3. sets status = transcription_claimed
  4. sets claimed_by = windows-gpu-1
  5. sets lease_expires_at = now + 30 minutes
  6. commits
  7. returns paths
```

Heartbeat:

```text
POST /worker/jobs/{job_id}/heartbeat
```

This extends `lease_expires_at`.

If a worker crashes:

- The lease expires.
- Mac API or a cleanup loop can move the job back to `audio_ready`.
- `worker_attempt_count` increments.
- After a configured maximum, the job becomes `failed`.

## API Contract

### Submit One Job

```http
POST /jobs
Content-Type: application/json

{
  "movie_number": "ktb-096",
  "priority": 100,
  "force": false
}
```

Response:

```json
{
  "id": "job_...",
  "movie_number": "ktb-096",
  "status": "queued",
  "job_dir_mac": "/Users/ytt/MissAVJobs/ktb-096",
  "job_dir_windows": "M:\\ktb-096"
}
```

### Submit Batch

```http
POST /jobs/batch
Content-Type: application/json

{
  "movie_numbers": ["ktb-096", "ktb-095", "ktb-093"],
  "priority": 100,
  "force": false
}
```

Response:

```json
{
  "created": [
    {"movie_number": "ktb-096", "id": "job_...", "status": "queued"}
  ],
  "existing": [
    {"movie_number": "ktb-095", "id": "job_...", "status": "audio_ready"}
  ],
  "invalid": []
}
```

### List Jobs

```http
GET /jobs?status=audio_ready
```

### Get One Job

```http
GET /jobs/{job_id}
```

### Worker Gets Next Job

```http
GET /worker/next-job?worker_id=windows-gpu-1
```

No work response:

```json
{
  "job": null
}
```

Work response:

```json
{
  "job": {
    "id": "job_...",
    "movie_number": "ktb-096",
    "audio_path_windows": "M:\\ktb-096\\audio.wav",
    "japanese_srt_path_windows": "M:\\ktb-096\\ktb-096.Japanese.srt",
    "english_srt_path_windows": "M:\\ktb-096\\ktb-096.English.srt"
  }
}
```

### Worker Heartbeat

```http
POST /worker/jobs/{job_id}/heartbeat
Content-Type: application/json

{
  "worker_id": "windows-gpu-1",
  "stage": "transcribing"
}
```

### Worker Complete

```http
POST /worker/jobs/{job_id}/complete
Content-Type: application/json

{
  "worker_id": "windows-gpu-1",
  "japanese_srt_path_windows": "M:\\ktb-096\\ktb-096.Japanese.srt",
  "english_srt_path_windows": "M:\\ktb-096\\ktb-096.English.srt"
}
```

### Worker Failed

```http
POST /worker/jobs/{job_id}/failed
Content-Type: application/json

{
  "worker_id": "windows-gpu-1",
  "stage": "transcribing",
  "error": "CUDA out of memory"
}
```

## Configuration

Mac `.env`:

```text
ORCHESTRATOR_HOST=0.0.0.0
ORCHESTRATOR_PORT=8000
ORCHESTRATOR_DB_PATH=/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3
MISSAV_PIPELINE_ROOT=/Users/ytt/Documents/startup/MissAV-Pipeline
JOBS_ROOT_MAC=/Users/ytt/MissAVJobs
JOBS_ROOT_WINDOWS=M:\
MAC_DOWNLOAD_CONCURRENCY=1
WORKER_LEASE_SECONDS=1800
MAX_DOWNLOAD_ATTEMPTS=3
MAX_WORKER_ATTEMPTS=3
```

Windows `.env`:

```text
MAC_API_BASE_URL=http://192.168.1.25:8000
WORKER_ID=windows-gpu-1
WINDOWS_JOBS_ROOT=M:\
WHISPER_MODEL=large-v3-turbo
WHISPER_DEVICE=cuda
WHISPER_COMPUTE_TYPE=float16
OPENAI_API_KEY=replace-with-key
TRANSLATE_SCRIPT_PATH=C:\Users\ytt\Documents\startup\E2E-download-subtitle-generation-translation-scripts\scripts\subtitle_translate.py
POLL_INTERVAL_SECONDS=10
HEARTBEAT_INTERVAL_SECONDS=60
```

## Transcription Strategy

Use `faster-whisper` on Windows with CUDA.

Recommended first model:

```text
large-v3-turbo
```

Reason:

- Faster than `large-v3`.
- Good enough for first production pipeline.
- Can be changed by env var if quality needs improve.

Transcription output:

```text
<movie>.Japanese.srt
```

Language should be forced to Japanese:

```text
language=ja
```

## Translation Strategy

Windows runs the existing SRT translation script after Japanese SRT is created.

Input:

```text
M:\ktb-096\ktb-096.Japanese.srt
```

Command shape:

```powershell
python C:\path\to\subtitle_translate.py `
  --input M:\ktb-096\ktb-096.Japanese.srt `
  --langs en `
  --output-dir M:\ktb-096
```

The worker should normalize or rename the script output to:

```text
M:\ktb-096\ktb-096.English.srt
```

## Security Model

First version is trusted LAN only.

- Mac API binds to LAN.
- SMB stays private on the home network.
- Cloudflare Tunnel may later expose only the Mac HTTP API, never SMB.
- API should use a shared bearer token before being exposed through Cloudflare Tunnel.
- Secrets stay in `.env` and are not committed.

## Operational Setup

Mac startup:

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
uvicorn orchestrator.api:app --host 0.0.0.0 --port 8000
python -m orchestrator.mac_worker
```

Windows startup:

```powershell
cd C:\Users\ytt\Documents\startup\JAV-Subtitle-Orchestrator
python -m orchestrator.windows_worker
```

Later, Windows worker can be registered with Task Scheduler so it starts after login.

## Error Handling

Download errors:

- Retry up to `MAX_DOWNLOAD_ATTEMPTS`.
- Keep error text in SQLite.
- Write detailed logs under the job folder.
- Mark terminal failures as `failed`.

Worker errors:

- Windows reports stage and error message.
- Retry up to `MAX_WORKER_ATTEMPTS`.
- Lease expiry handles worker crash or laptop sleep.
- Failed jobs remain queryable through the API.

Partial files:

- Workers write to deterministic temporary file names first: `audio.wav.tmp`, `<movie>.Japanese.srt.tmp`, and `<movie>.English.srt.tmp`.
- Final files are renamed into place only after successful completion.
- The API checks final file existence before marking `english_srt_ready`.

## Testing Strategy

Unit tests:

- Movie number normalization.
- Duplicate job submission.
- Batch submission with created, existing, and invalid IDs.
- SQLite claim behavior.
- Lease expiry behavior.
- Path mapping from Mac root to Windows root.
- Status transitions.

Integration tests on Mac:

- Submit a fake job.
- Mock MissAV adapter to write fake metadata and fake audio.
- Confirm job reaches `audio_ready`.

Integration tests on Windows:

- Use a tiny WAV fixture.
- Run transcription adapter in a dry-run or mocked mode.
- Run translation adapter against a small SRT fixture.
- Confirm English SRT output path is reported complete.

End-to-end LAN test:

- Submit one movie ID from Mac.
- Confirm Mac creates job folder.
- Confirm Windows sees audio through SMB.
- Confirm Windows writes Japanese and English SRT.
- Confirm Mac API reports `english_srt_ready`.

## Implementation Milestones

1. Create standalone repo scaffold and config files.
2. Add SQLite job store and tests.
3. Add FastAPI job submission and status endpoints.
4. Add worker claim, heartbeat, complete, and failed endpoints.
5. Add Mac worker with mocked MissAV adapter tests.
6. Connect Mac worker to existing MissAV audio download script.
7. Add Windows worker with mocked transcription and translation adapters.
8. Connect Windows worker to `faster-whisper`.
9. Connect Windows worker to existing `subtitle_translate.py`.
10. Add SMB setup and machine-specific run docs.
11. Run one real movie through the full Mac to Windows to Mac status flow.

## Open Decisions Resolved For Version 1

- Repo name: `JAV-Subtitle-Orchestrator`.
- Mac owns job storage.
- Windows runs both transcription and translation.
- SQLite is the queue database.
- SMB is the file transfer layer.
- Cloudflare Tunnel is optional later and only for API access.
- Concurrency is one Mac download and one Windows AI job.
- No web UI in version 1.
