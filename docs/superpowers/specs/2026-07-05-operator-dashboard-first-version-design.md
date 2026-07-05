# Operator Dashboard First Version Design

## Purpose

Add a private operator dashboard to the existing Mac-hosted JAV Subtitle Orchestrator so the owner can open a GUI from another machine, submit jobs, inspect current queue state, view failures, and jump to Swagger for raw API calls.

This first version intentionally stays smaller than the full private API dashboard and variant-reuse design. It implements the daily operations surface only:

- Timeline-first dashboard at `GET /dashboard`.
- Non-destructive single and batch job submission.
- Latest job status, paths, attempts, timestamps, and concise errors.
- Read-only bounded log tails for known job log files.
- Link to the existing Swagger page at `GET /docs`.

## Non-Goals

- Do not implement variant subtitle reuse in this version.
- Do not add cancel, retry, delete, or force-rerun dashboard buttons.
- Do not build a separate React/Vite frontend.
- Do not expose SMB through Cloudflare.
- Do not add authentication inside the app in this version. External access should remain protected by Cloudflare Access or LAN-only routing.

## Recommended Approach

Use a single FastAPI-served dashboard with small in-page JavaScript and CSS.

Reasons:

- The project already runs FastAPI on the Mac.
- No frontend build process is needed on either Mac or Windows.
- It is enough for an operator console that mostly renders JSON state and submits existing API forms.
- The UI can be tested with FastAPI `TestClient` plus a browser smoke check.

## Routes

### `GET /dashboard`

Returns the dashboard HTML.

The page includes:

- Top navigation with links to `/dashboard` and `/docs`.
- Health/status cards.
- Single movie submit form.
- Batch movie submit form.
- Latest jobs list.
- Selected job detail panel.
- Log buttons for allowlisted logs.

### `GET /dashboard/state`

Returns the JSON needed to render the main page.

Payload fields:

- `api`: online status, current server time, configured jobs root paths.
- `counts`: count of jobs by status.
- `latest_jobs`: recent jobs ordered for operator usefulness.
- `active_errors`: failed jobs or jobs with non-empty `error`.

The first version can derive health from the current `jobs` table. A separate worker heartbeat table can be added later.

### `GET /jobs/{job_id}/detail`

Returns the full read model for one job.

Include:

- ID.
- Original and normalized movie number.
- Status.
- Priority.
- Download attempt count.
- Worker attempt count.
- Claimed worker ID.
- Lease expiry.
- Created and updated timestamps.
- Error.
- Mac and Windows job folders.
- Metadata path.
- Audio paths.
- Japanese SRT paths.
- English SRT paths.

### `GET /jobs/{job_id}/logs`

Returns available allowlisted logs for the job.

Allowed names:

- `mac-download.log`
- `windows-worker.log`
- `whisper.log`
- `translate.log`

The endpoint should only report logs inside the job's `logs` directory and should not accept arbitrary paths.

### `GET /jobs/{job_id}/logs/{log_name}?tail=200`

Returns the last `tail` lines for an allowlisted log.

Rules:

- Default `tail` is `200`.
- Maximum `tail` is `1000`.
- Unknown log names return `404`.
- Path traversal attempts return `404` or `422`.
- Missing log files return `404`.

## Dashboard Behavior

### Timeline-First Layout

The dashboard opens with the latest operational state, not a marketing or explanatory page.

Top row:

- API online card.
- Mac-side activity approximation.
- Windows-side activity approximation.
- Active error count.

Because there is no separate worker heartbeat table yet, worker cards should be derived from active jobs:

- Downloading metadata/audio means Mac worker activity.
- Transcription claimed/transcribing/transcription done/translating means Windows worker activity.
- No active stage means idle or unknown.

### Submit Controls

The dashboard supports:

- Single movie ID input.
- Batch textarea with one movie ID per line.
- Priority input defaulting to `100`.
- Submissions always use `force=false`.

The UI calls existing endpoints:

- `POST /jobs`
- `POST /jobs/batch`

Submission result should show a compact success/error message and refresh dashboard state.

Force submission remains available through Swagger for manual admin use. It should not appear in the first dashboard version because it can reset existing non-active jobs.

### Latest Jobs

Show recent jobs in a dense list. Each row includes:

- Movie number.
- Status.
- Updated time.
- Claimed worker when present.
- Concise error when present.

Clicking a row loads `/jobs/{job_id}/detail` into the detail panel.

### Job Details

The detail panel shows:

- Full job metadata.
- All Mac and Windows paths.
- Error text.
- Available logs.

Paths should be visible because this project often needs to debug SMB mapping and whether Windows wrote final files to `M:\`.

### Log Viewer

The log viewer is read-only.

Clicking a log name fetches `GET /jobs/{job_id}/logs/{log_name}?tail=200` and displays the tail in a monospace panel.

The main job list should not display large tracebacks. It should show a concise error summary and leave raw details to the log panel.

## Data Model

No database migration is required for the first version.

Use the existing `jobs` table and `JobRecord` fields:

- `id`
- `movie_number`
- `normalized_movie_number`
- `status`
- `priority`
- `attempt_count`
- `worker_attempt_count`
- `claimed_by`
- `lease_expires_at`
- `created_at`
- `updated_at`
- `error`
- `job_dir_mac`
- `job_dir_windows`
- `metadata_path_mac`
- `audio_path_mac`
- `audio_path_windows`
- `japanese_srt_path_mac`
- `japanese_srt_path_windows`
- `english_srt_path_mac`
- `english_srt_path_windows`

## Security

The dashboard exposes queue submission and operational paths. It should remain private.

For LAN testing:

- Use `http://<mac-lan-ip>:8010/dashboard`.
- Keep SMB LAN-only.

For domain access:

- Put the FastAPI app behind Cloudflare Tunnel.
- Protect `/dashboard`, `/docs`, `/openapi.json`, `/jobs`, `/jobs/*`, and `/dashboard/state` with Cloudflare Access.
- Do not expose SMB over Cloudflare.

The app itself should:

- Never return arbitrary filesystem paths beyond known job record paths.
- Only serve allowlisted logs from a job's own `logs` directory.
- Keep destructive operations out of the dashboard.

## Testing

Add focused tests for:

- `GET /dashboard` returns HTML with Dashboard and Swagger links.
- `GET /dashboard/state` returns counts, latest jobs, and active errors.
- `GET /jobs/{job_id}/detail` returns full paths and operational fields.
- `GET /jobs/{job_id}/logs` lists only existing allowlisted logs.
- `GET /jobs/{job_id}/logs/{log_name}` returns bounded tail output.
- Unknown log names and path traversal attempts are rejected.
- Dashboard single and batch submit paths continue to use existing API behavior.

Run:

```bash
.venv/bin/python -m pytest -q
```

After implementation, start the Mac API and smoke-test:

```bash
python -m orchestrator api
```

Then open:

```text
http://127.0.0.1:8010/dashboard
http://127.0.0.1:8010/docs
```

From another LAN machine, use:

```text
http://<mac-lan-ip>:8010/dashboard
```

## Acceptance Criteria

- `/dashboard` loads from the Mac and another LAN machine.
- Dashboard can submit one movie ID.
- Dashboard can submit a batch list.
- Dashboard shows latest jobs and active errors without needing Swagger.
- Clicking a job shows full detail paths and attempts.
- Log buttons show bounded tails for known logs.
- Swagger remains available at `/docs`.
- No destructive dashboard actions are present.
- Full test suite passes.
