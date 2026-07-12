# Dashboard Restoration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the established operator dashboard, current worker visibility, and read-only Supabase subtitle-audit views on the Windows-transcription/Mac-translation branch.

**Architecture:** Selectively port the known-good dashboard read models, bounded log helpers, HTML, and audit reader from `codex/subtitle-quality-audit-remediation`; adapt them to the current smaller store instead of merging the old branch. Keep Cloudflare Access as the external authentication layer, make audit failures independent from job-state rendering, and expose no remediation mutation path.

**Tech Stack:** Python 3.11, FastAPI, Pydantic, SQLite, server-rendered HTML/vanilla JavaScript, requests, pytest, Cloudflare Access/Tunnel.

---

## File responsibility map

- `orchestrator/dashboard.py`: dashboard-only state conversion, secure log reads, job browser queries, and HTML rendering.
- `orchestrator/models.py`: typed dashboard and read-only subtitle-audit response models.
- `orchestrator/store.py`: bounded dashboard job reads and worker telemetry persistence; no repair or publication operations.
- `orchestrator/api.py`: dashboard HTML/JSON routes and isolated read-only audit routes.
- `orchestrator/subtitle_audit_api.py`: bounded, server-side-only Supabase audit reader.
- `orchestrator/config.py`: optional audit visibility credentials and timeout; secrets remain server-side.
- `orchestrator/__main__.py`: construct the optional audit reader when starting the API.
- `tests/test_dashboard_state.py`: status, activity, browser, and detail behavior.
- `tests/test_dashboard_logs.py`: log allowlist, bounds, traversal, and symlink defenses.
- `tests/test_api_dashboard.py`: HTML and dashboard endpoint contract.
- `tests/test_api_subtitle_audits.py`: read-only audit validation, pagination, sanitization, and unavailable behavior.
- `docs/setup/cloudflare-tunnel.md`: preserve the existing public-login/local-direct access topology.

### Task 1: Restore dashboard read models and secure helpers

**Files:**
- Create: `orchestrator/dashboard.py`
- Modify: `orchestrator/models.py`
- Test: `tests/test_dashboard_state.py`
- Test: `tests/test_dashboard_logs.py`

- [ ] **Step 1: Add the old dashboard tests first**

Port the tests from the latest versions of:

```text
codex/subtitle-quality-audit-remediation:tests/test_dashboard_state.py
codex/subtitle-quality-audit-remediation:tests/test_dashboard_logs.py
```

Keep tests for status counts, deterministic recency, active/error filtering,
pagination, complete job paths, allowlisted log names, bounded tails, traversal,
directory substitution, and symlink rejection. Remove assertions for callback and
retry-policy fields that do not exist in the current store. Add these current-flow
assertions:

```python
assert state.activity["windows"]["status"] in {"transcribing", "idle"}
assert state.activity["translation"]["status"] in {"translating", "idle"}
assert state.counts["transcription_done"] == 1
assert state.counts["translating"] == 1
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
source .venv/bin/activate
pytest -q tests/test_dashboard_state.py tests/test_dashboard_logs.py
```

Expected: collection fails because the dashboard response models and
`orchestrator.dashboard` do not exist on this branch.

- [ ] **Step 3: Add the dashboard response models**

Add typed models equivalent to the old dashboard contract while using the current
`JobStatus` and `JobRecord` fields:

```python
class DashboardJobSummary(BaseModel):
    id: str
    movie_number: str
    status: JobStatus
    priority: int
    updated_at: str
    claimed_by: str | None = None
    error: str | None = None


class WorkerHealthSummary(BaseModel):
    worker_id: str
    role: str
    state: str
    status: str
    last_seen_at: str
    current_job_id: str | None = None
    current_movie_number: str | None = None
    stage: str | None = None
    last_error: str | None = None


class DashboardStateResponse(BaseModel):
    api: dict[str, str | bool]
    activity: dict[str, dict[str, str | None]]
    counts: dict[str, int]
    workers: list[WorkerHealthSummary] = []
    latest_jobs: list[DashboardJobSummary]
    active_errors: list[DashboardJobSummary]
```

Also add `JobBrowserItem`, `JobBrowserResponse`, `JobDetailResponse`,
`JobLogSummary`, `JobLogsResponse`, and `JobLogTailResponse` with only fields
present in the current `JobRecord`.

- [ ] **Step 4: Port the pure dashboard helpers**

Port the known-good implementations from the old branch, retaining these current
status groups:

```python
MAC_DOWNLOAD_STATUSES = {
    JobStatus.DOWNLOADING_METADATA,
    JobStatus.DOWNLOADING_AUDIO,
}
WINDOWS_TRANSCRIPTION_STATUSES = {
    JobStatus.TRANSCRIPTION_CLAIMED,
    JobStatus.TRANSCRIBING,
}
MAC_TRANSLATION_STATUSES = {
    JobStatus.TRANSLATING,
}
ACTIVE_BROWSER_STATUSES = {
    JobStatus.QUEUED,
    JobStatus.DOWNLOADING_METADATA,
    JobStatus.DOWNLOADING_AUDIO,
    JobStatus.AUDIO_READY,
    JobStatus.TRANSCRIPTION_CLAIMED,
    JobStatus.TRANSCRIBING,
    JobStatus.TRANSCRIPTION_DONE,
    JobStatus.TRANSLATING,
}
```

Retain the old `ALLOWED_JOB_LOGS`, path containment checks, symlink rejection,
regular-file validation, maximum tail, and UTF-8 replacement decoding. Never read
SRT files through a dashboard log route.

- [ ] **Step 5: Run helper tests and commit**

```bash
pytest -q tests/test_dashboard_state.py tests/test_dashboard_logs.py
git add orchestrator/dashboard.py orchestrator/models.py tests/test_dashboard_state.py tests/test_dashboard_logs.py
git commit -m "feat: restore dashboard read helpers"
```

Expected: all helper and log-security tests pass.

### Task 2: Restore dashboard routes and operator HTML

**Files:**
- Modify: `orchestrator/api.py`
- Modify: `orchestrator/dashboard.py`
- Test: `tests/test_api_dashboard.py`

- [ ] **Step 1: Add API tests before routes**

Port the current-branch-compatible portions of the old API tests. Require:

```python
assert client.get("/dashboard").status_code == 200
assert "JAV Subtitle Orchestrator" in client.get("/dashboard").text
assert client.get("/dashboard/state").status_code == 200
assert client.get(f"/jobs/{job.id}/detail").status_code == 200
assert client.get(f"/jobs/{job.id}/logs").status_code == 200
assert "force: true" not in client.get("/dashboard").text
```

Also assert that the HTML contains labels for Windows Transcription and Mac
Translation and includes `transcription_done` and `translating` filters.

- [ ] **Step 2: Run API tests and verify RED**

```bash
pytest -q tests/test_api_dashboard.py
```

Expected: `/dashboard` returns 404.

- [ ] **Step 3: Add the read-only dashboard routes**

Add routes without changing existing job/worker endpoints:

```python
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page() -> str:
    return dashboard_html()


@app.get("/dashboard/state", response_model=DashboardStateResponse)
def dashboard_state() -> DashboardStateResponse:
    return build_dashboard_state(store)


@app.get("/jobs/browser", response_model=JobBrowserResponse)
def jobs_browser(view: str = "active", search: str = "", page: int = 1,
                 page_size: int = 50) -> JobBrowserResponse:
    return build_job_browser(store, view=view, search=search,
                             page=page, page_size=page_size)
```

Restore detail/log routes with FastAPI 404/422 mapping. Do not add cancel, retry,
force-reset, delete, repair-apply, or upload routes.

- [ ] **Step 4: Port the existing HTML and adapt state labels**

Port `dashboard_html()` from the old branch. Preserve its CSP-safe same-origin API
calls and audit failure isolation. Replace the single processing panel with these
labels and state keys:

```javascript
renderActivity("mac-download-activity", state.activity.mac_download);
renderActivity("windows-activity", state.activity.windows);
renderActivity("translation-activity", state.activity.translation);
```

All submission payloads remain `force: false`. Remove any buttons that call import,
repair, requeue, overwrite, delete, or apply endpoints.

- [ ] **Step 5: Run dashboard API tests and commit**

```bash
pytest -q tests/test_api_dashboard.py tests/test_dashboard_state.py tests/test_dashboard_logs.py
git add orchestrator/api.py orchestrator/dashboard.py tests/test_api_dashboard.py
git commit -m "feat: restore operator dashboard"
```

Expected: `/dashboard` and all read routes pass their tests.

### Task 3: Add separate downloader, transcription, and translation worker telemetry

**Files:**
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/mac_worker.py`
- Modify: `orchestrator/windows_worker.py`
- Test: `tests/test_dashboard_state.py`
- Test: `tests/test_mac_worker.py`
- Test: `tests/test_windows_worker.py`

- [ ] **Step 1: Write worker telemetry tests**

Require each loop to publish a bounded status record and require dashboard state to
keep roles separate:

```python
assert {worker.role for worker in state.workers} >= {
    "mac_downloader", "windows_transcriber", "mac_translator"
}
assert state.activity["windows"]["worker_id"] == "windows-gpu-1"
assert state.activity["translation"]["worker_id"] == "mac-translation-1"
```

Verify status records contain no subtitle text and are updated to `idle` after a
poll with no job.

- [ ] **Step 2: Run telemetry tests and verify RED**

```bash
pytest -q tests/test_dashboard_state.py tests/test_mac_worker.py tests/test_windows_worker.py
```

Expected: `JobStore` has no worker status methods.

- [ ] **Step 3: Add the telemetry table and bounded store API**

Extend idempotent initialization and add upsert/list methods:

```sql
CREATE TABLE IF NOT EXISTS worker_statuses (
  worker_id TEXT PRIMARY KEY,
  role TEXT NOT NULL,
  state TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  current_job_id TEXT,
  current_movie_number TEXT,
  stage TEXT,
  last_error TEXT
)
```

`upsert_worker_status()` accepts only IDs, role/state/stage, job identifiers, and a
bounded error string. `list_worker_statuses()` returns typed records ordered by
worker id. It never stores subtitle content, tokens, paths, or environment values.

- [ ] **Step 4: Update worker loops**

Publish `processing` before work, `idle` after an empty poll or success, and
`error` with a bounded exception summary after failure. Use roles
`mac_downloader`, `windows_transcriber`, and `mac_translator`. Telemetry failure is
logged but must not change job state or stop the worker.

- [ ] **Step 5: Run worker/dashboard tests and commit**

```bash
pytest -q tests/test_dashboard_state.py tests/test_mac_worker.py tests/test_windows_worker.py
git add orchestrator/store.py orchestrator/mac_worker.py orchestrator/windows_worker.py tests/test_dashboard_state.py tests/test_mac_worker.py tests/test_windows_worker.py
git commit -m "feat: report split worker health"
```

### Task 4: Restore read-only Supabase audit API

**Files:**
- Create: `orchestrator/subtitle_audit_api.py`
- Modify: `orchestrator/models.py`
- Modify: `orchestrator/config.py`
- Modify: `orchestrator/api.py`
- Modify: `orchestrator/__main__.py`
- Test: `tests/test_api_subtitle_audits.py`

- [ ] **Step 1: Add the read-only audit tests**

Port the old tests that cover summary, filtered pagination, detail UUID validation,
unconfigured 503, secret sanitization, redirect rejection, response-size bounds,
Content-Range consistency, metric validation, and service shutdown. Exclude scanner,
persistence, migration, audit apply, and remediation mutation tests.

- [ ] **Step 2: Run audit tests and verify RED**

```bash
pytest -q tests/test_api_subtitle_audits.py
```

Expected: the audit service and response models do not exist.

- [ ] **Step 3: Add audit enums and response models without replacing the Mac gate**

Keep `validate_translation_quality()` unchanged. Add separate audit-only enums:

```python
class SubtitleAuditStatus(StrEnum):
    PASS = "pass"
    WARNING = "warning"
    REVIEW = "review"
    BAD = "bad"
    INVALID = "invalid"
    MISSING = "missing"
```

Add `SubtitleAuditSummaryResponse`, `SubtitleAuditItem`, and
`SubtitleAuditPageResponse`. Do not import the old branch's replacement
`subtitle_quality.py` because it would change production gate thresholds.

- [ ] **Step 4: Port the bounded audit reader and routes**

Port the latest hardened read-only request/validation logic from
`orchestrator/subtitle_audit_api.py`, mapping old audit enums to the new audit-only
types. Only these operations are allowed:

```text
POST /rest/v1/rpc/subtitle_quality_latest_summary
GET  /rest/v1/subtitle_quality_latest_catalog
GET  /rest/v1/movie_languages
```

Expose only:

```text
GET /subtitle-audits/summary
GET /subtitle-audits
GET /subtitle-audits/{subtitle_id}
```

Convert service/configuration errors to sanitized 503/502 responses without keys,
URLs containing credentials, or raw upstream bodies.

- [ ] **Step 5: Wire optional server-side configuration**

Add optional `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`,
`SUBTITLE_AUDIT_VISIBILITY_ENABLED`, and bounded timeout settings to `MacSettings`.
Construct the service only when visibility is enabled and both credentials exist.
Never serialize these settings into dashboard state.

- [ ] **Step 6: Run audit/API tests and commit**

```bash
pytest -q tests/test_api_subtitle_audits.py tests/test_api_dashboard.py tests/test_api_worker.py
git add orchestrator/subtitle_audit_api.py orchestrator/models.py orchestrator/config.py orchestrator/api.py orchestrator/__main__.py tests/test_api_subtitle_audits.py
git commit -m "feat: restore read-only subtitle audit api"
```

### Task 5: Restore audit cards and read-only repair visibility

**Files:**
- Modify: `orchestrator/dashboard.py`
- Modify: `tests/test_api_dashboard.py`
- Modify: `docs/setup/mac.md`
- Create: `docs/setup/cloudflare-tunnel.md`

- [ ] **Step 1: Add HTML safety and failure-isolation tests**

Assert the page contains separate subtitle-quality cards and filters, catches audit
refresh errors without clearing job state, never renders raw subtitle text, and has
no mutation controls:

```python
html = client.get("/dashboard").text
assert "Subtitle Quality" in html
assert "audit-unavailable" in html
for forbidden in ("force: true", "repair/apply", "requeue", "overwrite"):
    assert forbidden not in html
```

- [ ] **Step 2: Run the test and verify RED**

```bash
pytest -q tests/test_api_dashboard.py
```

Expected: subtitle-quality cards are absent.

- [ ] **Step 3: Port audit cards and retain read-only behavior**

Port the old summary cards, status/language filters, bounded findings table, and
detail view. Audit fetch failures update only the audit panel. Display a link or
copyable command template for the existing dry-run planner:

```text
python -m orchestrator plan-historical-subtitle-repair --allowlist abc-001 --limit 1
```

Do not invoke it from browser JavaScript and do not add an apply endpoint.

- [ ] **Step 4: Restore access documentation**

Document the verified topology:

```text
Browser -> Cloudflare Access login -> Cloudflare Tunnel
        -> http://127.0.0.1:8010/dashboard
```

State that local `127.0.0.1` access bypasses Cloudflare login, the tunnel exposes
only HTTP, and SMB/secrets must never be exposed.

- [ ] **Step 5: Run dashboard tests and commit**

```bash
pytest -q tests/test_api_dashboard.py tests/test_api_subtitle_audits.py
git add orchestrator/dashboard.py tests/test_api_dashboard.py docs/setup/mac.md docs/setup/cloudflare-tunnel.md
git commit -m "feat: restore dashboard audit visibility"
```

### Task 6: Full verification and live deployment

**Files:**
- Modify only if verification exposes a tested compatibility defect.

- [ ] **Step 1: Run static and full test verification**

```bash
source .venv/bin/activate
git diff --check
pytest -q
python -m orchestrator mac-translation-smoke-test
```

Expected: all tests pass; smoke reports 10 cues, unique ratio at least 0.5, and
known_bad 0.

- [ ] **Step 2: Restart only the API**

Stop the current `python -m orchestrator api` process gracefully and start the same
command from the current branch. Keep downloader, Windows worker, and Mac
translation worker running. Do not submit or requeue a job.

- [ ] **Step 3: Verify local HTTP contracts**

```bash
curl -fsS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8010/dashboard
curl -fsS http://127.0.0.1:8010/dashboard/state | jq '.api.online'
curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST http://127.0.0.1:8010/worker/jobs/verification-only/complete \
  -H 'Content-Type: application/json' \
  -d '{"worker_id":"verification-only","japanese_srt_path_windows":"M:\\\\none.Japanese.srt","english_srt_path_windows":"M:\\\\none.English.srt"}'
```

Expected: dashboard 200, API online true, legacy complete 409.

- [ ] **Step 4: Verify visually in the browser companion**

Open `http://127.0.0.1:8010/dashboard` and confirm page layout, current state
counts, recent jobs, Windows transcription activity, Mac translation activity, log
viewing, and independent audit-unavailable behavior. Do not click submission or
mutation controls.

- [ ] **Step 5: Verify Cloudflare Access**

```bash
curl -sS -I https://orchestrator.javsubtitle.com/dashboard
```

Expected: unauthenticated request returns a 302 to the existing Cloudflare Access
login. After the user logs in through the approved browser identity, the dashboard
renders from the Mac API.

- [ ] **Step 6: Commit any tested final compatibility fix, push, and update the draft PR**

```bash
git status --short
git push origin codex/windows-transcription-mac-translation
gh pr view 1 --json url,isDraft,state,headRefName,baseRefName
```

No production Supabase upload, audit persistence, remediation apply, requeue,
subtitle overwrite, audio deletion, or new canary submission is authorized by this
plan.
