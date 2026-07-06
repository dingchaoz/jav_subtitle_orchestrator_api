# JAV Subtitle Orchestrator API Client Guide

This guide is for trusted machines calling the private orchestrator API through
Cloudflare Access.

## Base URL and Authentication

```bash
export ORCHESTRATOR_BASE_URL="https://orchestrator.javsubtitle.com"
export CF_ACCESS_CLIENT_ID="replace-with-cloudflare-access-client-id"
export CF_ACCESS_CLIENT_SECRET="replace-with-cloudflare-access-client-secret"
```

Every API request through Cloudflare Access must include the service-token
headers:

```bash
-H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID" \
-H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET"
```

Do not commit real Cloudflare client IDs, client secrets, callback secrets, or
Supabase keys to this repository.

## Current Readiness Flow

Submit a job, save the returned `id`, then poll `GET /jobs/{job_id}` or
`GET /jobs/{job_id}/detail` until `status == "english_srt_ready"`.

```bash
JOB_ID=$(
  curl -sS "$ORCHESTRATOR_BASE_URL/jobs" \
    -H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID" \
    -H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET" \
    -H "Content-Type: application/json" \
    --json '{"movie_number":"KTB-096","priority":100,"force":false}' |
  python -c 'import json,sys; print(json.load(sys.stdin)["id"])'
)

curl -sS "$ORCHESTRATOR_BASE_URL/jobs/$JOB_ID/detail" \
  -H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID" \
  -H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET"
```

Job statuses:

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

## Callback Notifications

Callbacks are configured on the orchestrator server. API clients do not send
callback URLs in job requests.

Server configuration:

```bash
CALLBACK_CLIENTS_JSON='{"machine-a.access":{"url":"https://client.example.com/webhooks/subtitle-ready","secret":"replace-with-callback-hmac-secret"}}'
CALLBACK_TIMEOUT_SECONDS=10
```

When a request includes `CF-Access-Client-Id: machine-a.access`, new jobs are
associated with that configured callback client. After the Windows worker
completes the job and Supabase publishing succeeds, the orchestrator sends:

```json
{
  "event": "subtitle.ready",
  "job_id": "abc123",
  "movie_number": "ktb-096",
  "status": "english_srt_ready",
  "published_to_supabase": true,
  "ready_at": "2026-07-06T12:34:56+00:00"
}
```

Callback requests include:

```text
X-JSO-Timestamp: 2026-07-06T12:34:56+00:00
X-JSO-Signature: sha256=<hmac_sha256_hex>
```

The signature is HMAC SHA-256 over:

```text
<X-JSO-Timestamp>.<canonical-json-payload>
```

If callback delivery fails, job completion still succeeds. The latest callback
status is visible from `GET /jobs/{job_id}/detail`.

## Endpoint Reference

### GET /dashboard

Returns the operator dashboard HTML.

```bash
curl -sS "$ORCHESTRATOR_BASE_URL/dashboard" \
  -H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID" \
  -H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET"
```

### GET /dashboard/state

Returns dashboard state: API status, worker activity approximation, status
counts, latest jobs, and active errors.

```bash
curl -sS "$ORCHESTRATOR_BASE_URL/dashboard/state" \
  -H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID" \
  -H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET"
```

### POST /jobs

Creates one subtitle-generation job.

Request:

```json
{
  "movie_number": "KTB-096",
  "priority": 100,
  "force": false
}
```

Response:

```json
{
  "id": "job-id",
  "movie_number": "KTB-096",
  "status": "queued",
  "job_dir_mac": "/Users/ytt/MissAVJobs/ktb-096",
  "job_dir_windows": "M:\\ktb-096",
  "error": null
}
```

### POST /jobs/batch

Creates jobs for many movie numbers.

Request:

```json
{
  "movie_numbers": ["KTB-096", "KTB-097"],
  "priority": 100,
  "force": false
}
```

Response groups jobs into `created`, `existing`, and `invalid`.

### POST /jobs/import-subtitle-requests

Imports requested subtitles from Cloudflare D1 and queues those that do not
already have an `English_AI` subtitle in Supabase.

Request:

```json
{
  "min_count": 1,
  "limit": 500,
  "priority": 100,
  "force": false
}
```

The endpoint returns requested items, imported items, skipped available items,
and queue results.

### GET /jobs

Lists jobs ordered by priority and creation time. Optional query:
`status=<job-status>`.

```bash
curl -sS "$ORCHESTRATOR_BASE_URL/jobs?status=queued" \
  -H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID" \
  -H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET"
```

### GET /jobs/{job_id}

Returns the compact job response for one job.

### GET /jobs/{job_id}/detail

Returns the full operational job detail, including paths, attempts, timestamps,
worker claim state, errors, and latest callback status when available.

### GET /jobs/{job_id}/logs

Lists available allowlisted logs for a job. Allowed log names:

```text
mac-download.log
windows-worker.log
whisper.log
translate.log
```

### GET /jobs/{job_id}/logs/{log_name}

Returns a bounded tail from an allowlisted log. Optional query: `tail=200`.
The server bounds the tail size.

```bash
curl -sS "$ORCHESTRATOR_BASE_URL/jobs/$JOB_ID/logs/windows-worker.log?tail=200" \
  -H "CF-Access-Client-Id: $CF_ACCESS_CLIENT_ID" \
  -H "CF-Access-Client-Secret: $CF_ACCESS_CLIENT_SECRET"
```

### GET /worker/next-job

Worker-only endpoint. Claims the next `audio_ready` job for a Windows worker.

Query:

```text
worker_id=windows-gpu-1
```

Response:

```json
{
  "job": {
    "id": "job-id",
    "movie_number": "ktb-096",
    "audio_path_windows": "M:\\ktb-096\\audio.wav",
    "japanese_srt_path_windows": "M:\\ktb-096\\ktb-096.Japanese.srt",
    "english_srt_path_windows": "M:\\ktb-096\\ktb-096.English.srt"
  }
}
```

When no job is ready, `job` is `null`.

### POST /worker/jobs/{job_id}/heartbeat

Worker-only endpoint. Extends the worker lease and updates the current stage.

Request:

```json
{
  "worker_id": "windows-gpu-1",
  "stage": "transcribing"
}
```

### POST /worker/jobs/{job_id}/complete

Worker-only endpoint. Marks a claimed job ready after the final English SRT
exists on the Mac path. If Supabase publishing is configured, the API publishes
the English AI subtitle before sending any callback.

Request:

```json
{
  "worker_id": "windows-gpu-1",
  "japanese_srt_path_windows": "M:\\ktb-096\\ktb-096.Japanese.srt",
  "english_srt_path_windows": "M:\\ktb-096\\ktb-096.English.srt"
}
```

### POST /worker/jobs/{job_id}/failed

Worker-only endpoint. Records a worker failure for the current stage. The job
returns to `audio_ready` until `MAX_WORKER_ATTEMPTS` is reached, then becomes
`failed`.

Request:

```json
{
  "worker_id": "windows-gpu-1",
  "stage": "transcribing",
  "error": "Whisper failed"
}
```
