# Private API Dashboard and Variant Subtitle Reuse Design

## Purpose

Expose the Mac-hosted JAV Subtitle Orchestrator as a private API service that can be opened from other machines through a domain, while keeping the existing Mac downloader, SQLite queue, SMB job folder, and Windows Whisper/translation worker architecture intact.

The exposed service should provide:

- Swagger/OpenAPI access for direct API calls.
- A private dashboard for job status, worker health, file paths, and errors.
- Variant-aware movie handling so IDs such as `ktb-012-uncensored` can reuse subtitles from `ktb-012` while still storing variant metadata when available.

## Scope

In scope:

- Cloudflare Tunnel and Cloudflare Access deployment design.
- Dashboard page served by the existing FastAPI app.
- API response additions needed by the dashboard.
- Worker health visibility.
- Log visibility for failed jobs.
- Variant ID normalization and subtitle reuse.
- SQLite schema changes and migration strategy.

Out of scope:

- Moving job execution off the Mac.
- Moving SQLite or SMB storage to the cloud.
- Making the Windows worker access SMB through Cloudflare.
- Public anonymous access.
- Building a full user-management system inside the app.
- Adding destructive retry, cancel, or force-rerun buttons to the first dashboard version.

## Deployment Design

The Mac continues to run the orchestrator API locally on port `8010`.

Recommended local binding:

```bash
python -m orchestrator api
```

The API should bind to `127.0.0.1:8010` for Cloudflare exposure, or `0.0.0.0:8010` when LAN Windows worker access is still needed directly. The current LAN workflow can continue to use:

```text
http://<mac-lan-ip>:8010
```

Cloudflare Tunnel runs on the Mac and forwards a private hostname to the local API:

```text
https://jav-api.<domain>
  -> http://127.0.0.1:8010
```

Cloudflare Access protects the hostname. Access policy should require the owner's approved email account or identity provider login.

Protected paths:

- `/`
- `/dashboard`
- `/docs`
- `/openapi.json`
- `/jobs`
- `/jobs/*`
- `/worker/*`
- `/logs/*`

The Windows worker should initially remain LAN-only with `MAC_API_BASE_URL=http://<mac-lan-ip>:8010` because it also needs the SMB share mapped to `M:\`. A later service-token design can allow the Windows worker to call through Cloudflare, but that is not needed for the current home-network setup.

## Dashboard Design

Add a private dashboard at:

```text
GET /dashboard
```

The dashboard is separate from Swagger. Swagger remains available for raw API testing and admin-only manual calls.

The dashboard uses a timeline-first layout.

### Top Health Row

Show compact status cards:

- API status: online, version, current time.
- Mac worker status: current job and stage, or idle.
- Windows worker status: current job and stage, last heartbeat age, or offline/stale.
- Active error count.

### Submit Controls

Show non-destructive submit controls:

- Single movie ID input.
- Batch paste box with one movie ID per line.
- Priority input with default `100`.
- `force=false` by default.

The dashboard should not include first-version buttons for destructive actions such as cancel, force rerun, or retry failed jobs. Those remain available through Swagger/admin API endpoints until the status dashboard is stable.

### Timeline List

The center of the dashboard shows latest jobs ordered by priority and update time.

Each job row should show:

- Requested movie number.
- Content movie number when different.
- Variant movie number when different.
- Current status.
- Stage timeline, for example:

```text
queued -> metadata -> downloading audio -> audio_ready -> transcribing -> translating -> ready
```

- Current useful progress detail when available:
  - in-progress audio file and size
  - selected worker
  - last heartbeat
  - subtitle reuse source
  - concise error summary

Filters:

- Active
- Ready
- Failed
- Reused subtitle

### Selected Job Details

The right panel shows full operational details for the selected job:

- Job ID.
- Requested movie number.
- Content movie number.
- Variant movie number.
- Status.
- Priority.
- Attempt count.
- Worker attempt count.
- Claimed worker ID.
- Lease expiry.
- Created and updated timestamps.
- Error summary.

File paths:

- Mac job folder.
- Windows job folder.
- Metadata path.
- Audio path.
- Japanese SRT path.
- English SRT path.
- Reused subtitle source path when applicable.

These paths should be shown because they are critical for debugging SMB mapping, download completion, and worker output.

### Error Display

Use concise errors in the normal timeline:

```text
metadata failed: Movie not found in MissAV catalog
```

Raw logs are available on demand through a details panel or route. Do not show large tracebacks directly in the main dashboard.

Log sources:

- `logs/mac-download.log`
- `logs/windows-worker.log`
- `logs/whisper.log`
- `logs/translate.log`

Add API support for reading bounded log tails:

```text
GET /jobs/{job_id}/logs
GET /jobs/{job_id}/logs/{log_name}?tail=200
```

Only allow known log names. Do not allow arbitrary filesystem paths.

## API Additions

Keep the existing job APIs and add dashboard-oriented read models.

### Dashboard Summary

```text
GET /dashboard/state
```

Returns:

- Health summary.
- Worker heartbeat summary.
- Status counts.
- Latest jobs.
- Active errors.

### Job Detail

Extend `GET /jobs/{job_id}` or add:

```text
GET /jobs/{job_id}/detail
```

The detail payload should include:

- Existing job response fields.
- Requested/content/variant movie numbers.
- Full file paths.
- worker and lease data.
- attempt counts.
- timestamps.
- subtitle reuse metadata.

### Worker Health

Add a worker heartbeat store separate from job claims:

```text
POST /worker/heartbeat
GET /workers
```

Workers should heartbeat while idle as well as while processing jobs. This lets the dashboard distinguish:

- Windows worker idle but alive.
- Windows worker busy.
- Windows worker stale/offline.
- Mac worker idle.
- Mac worker processing.

## Variant Subtitle Reuse Design

Current behavior is insufficient for `ktb-012-uncensored`:

- API validation only accepts IDs like `ktb-012` or `ktb012`.
- The job table dedupes by one `normalized_movie_number`.
- The MissAV adapter has partial suffix stripping for audio lookup, but the API rejects variant IDs before they reach the adapter.

Add explicit identity fields:

```text
requested_movie_number TEXT NOT NULL
content_movie_number TEXT NOT NULL
variant_movie_number TEXT NOT NULL
```

Definitions:

- `requested_movie_number`: the exact normalized user request, such as `ktb-012-uncensored`.
- `content_movie_number`: the base subtitle identity, such as `ktb-012`.
- `variant_movie_number`: the metadata/video identity, such as `ktb-012-uncensored`.

For base submissions:

```text
requested_movie_number = ktb-012
content_movie_number = ktb-012
variant_movie_number = ktb-012
```

For variant submissions:

```text
requested_movie_number = ktb-012-uncensored
content_movie_number = ktb-012
variant_movie_number = ktb-012-uncensored
```

### Accepted Variant Suffixes

Start with suffixes already known by the MissAV adapter:

- `-uncensored-leak`
- `-uncensored`
- `-english-subtitle`
- `-chinese-subtitle`
- `-subtitle`
- `-leak`

The parser should accept:

- `ktb-012`
- `ktb012`
- `ktb-012-uncensored`
- `ktb012-uncensored`

The parser should reject IDs with spaces, path separators, shell metacharacters, or unsupported suffixes.

### Subtitle Reuse Behavior

When a variant job is submitted:

1. Normalize the request into requested/content/variant identities.
2. Look for an existing completed subtitle for `content_movie_number`.
3. If the base English SRT exists, create a variant job record that references the reused subtitle and does not queue Windows transcription.
4. Still attempt to fetch or refresh metadata for `variant_movie_number` if available.
5. If variant metadata is unavailable but base metadata exists, store the base metadata as fallback and mark the metadata source explicitly.
6. If no base subtitle exists, queue a normal transcription job for `content_movie_number` once.

Subtitle readiness and metadata freshness are separate concerns. A variant can be usable for subtitles immediately through `subtitle_reused` while its metadata is still `variant`, `base_fallback`, or `missing`. The dashboard should show both facts instead of forcing metadata lookup to block subtitle reuse.

The dashboard should show this clearly:

```text
ktb-012-uncensored - subtitle_reused
subtitle source: ktb-012
variant metadata: saved when available
```

### New Status

Add a status for reuse completion:

```text
subtitle_reused
```

This status means the requested movie has a usable English SRT through `content_movie_number`, but no new transcription was run for the variant.

### SQLite Constraints

The current `UNIQUE(normalized_movie_number)` is too simple.

Recommended constraints:

- Unique job identity by `requested_movie_number` for direct duplicate submission handling.
- Reusable subtitle lookup by `content_movie_number`.
- At most one active transcription-producing job per `content_movie_number`.

Add fields:

```text
requested_movie_number TEXT
content_movie_number TEXT
variant_movie_number TEXT
subtitle_source_movie_number TEXT
metadata_source_movie_number TEXT
reused_english_srt_path_mac TEXT
reused_english_srt_path_windows TEXT
```

Migration rules:

- Existing rows should backfill all three identity fields from `normalized_movie_number`.
- Existing completed SRT rows become reusable subtitle sources for their `content_movie_number`.
- Existing API responses can keep `movie_number` as an alias for `requested_movie_number` until clients are updated.

## Metadata Behavior

For variant IDs, metadata lookup should try in this order:

1. Exact `variant_movie_number`.
2. Refresh MissAV catalog and retry exact variant.
3. Base `content_movie_number` fallback.

The metadata JSON should include an orchestrator wrapper field or companion fields indicating:

```json
{
  "orchestrator_metadata_source": "variant|base_fallback",
  "requested_movie_number": "ktb-012-uncensored",
  "content_movie_number": "ktb-012",
  "variant_movie_number": "ktb-012-uncensored"
}
```

This prevents the dashboard from pretending variant metadata exists when only base metadata was available.

## Security

Cloudflare Access is the main external protection boundary.

Additional app-level safeguards:

- Keep worker mutation endpoints under `/worker/*`.
- Avoid exposing arbitrary filesystem reads.
- Log APIs only read known log files under the job directory.
- Dashboard submit controls default to `force=false`.
- Do not expose cancel/force/rerun buttons in the first dashboard version.
- Keep SMB private to the LAN.

## Testing

Add unit tests for:

- Variant parser:
  - `ktb-012`
  - `ktb012`
  - `ktb-012-uncensored`
  - `ktb012-uncensored`
  - invalid suffixes and unsafe strings
- Store migration from old rows.
- Duplicate requested movie submission.
- Variant submission reuses existing base English SRT.
- Variant submission records metadata fallback when variant metadata is unavailable.
- Dashboard state response includes worker health and file paths.
- Log API rejects unknown log names and path traversal.

Add integration tests for:

- Submit base movie, mark English SRT ready, submit variant, verify no worker claim is created for the variant.
- Submit variant first with no base subtitle, verify only one content transcription job is queued.
- Worker heartbeat appears in `/dashboard/state`.

## Acceptance Criteria

- `https://jav-api.<domain>/docs` is reachable only after Cloudflare Access login.
- `/dashboard` is reachable only after Cloudflare Access login.
- Dashboard shows latest jobs, active stages, worker health, file paths, and concise errors.
- Raw logs can be opened on demand and are bounded to known job logs.
- `ktb-012-uncensored` is accepted by the API.
- `ktb-012-uncensored` reuses `ktb-012` English subtitles when they already exist.
- Variant metadata is stored when available.
- Base metadata fallback is explicit when variant metadata is not available.
- Windows worker is not asked to rerun Whisper for a variant when base subtitles are ready.
