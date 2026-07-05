# Operator Dashboard First Version Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a private timeline-first `/dashboard` GUI for submitting jobs, viewing latest job state, inspecting details, and reading bounded job logs, while keeping Swagger available at `/docs`.

**Architecture:** Add focused dashboard read models and helper functions, then expose them through the existing FastAPI app. Serve a single FastAPI-hosted HTML page with small in-page JavaScript that calls the new JSON endpoints and the existing `POST /jobs` and `POST /jobs/batch` APIs. No database migration or frontend build step is required.

**Tech Stack:** Python 3.11, FastAPI, Pydantic, SQLite-backed `JobStore`, plain HTML/CSS/JavaScript, pytest, FastAPI `TestClient`.

---

## Scope

Implement the approved first-version design in:

`docs/superpowers/specs/2026-07-05-operator-dashboard-first-version-design.md`

This plan intentionally does not implement variant subtitle reuse, internal authentication, cancel/retry/delete buttons, or a separate React/Vite frontend.

## File Structure

- Create `orchestrator/dashboard.py`: Pure dashboard helpers, job detail conversion, status counts, active job/activity derivation, allowlisted log listing, bounded log tail reading, and HTML rendering.
- Modify `orchestrator/models.py`: Add Pydantic response models for dashboard state, job detail, log list, and log tail responses.
- Modify `orchestrator/api.py`: Add `/dashboard`, `/dashboard/state`, `/jobs/{job_id}/detail`, `/jobs/{job_id}/logs`, and `/jobs/{job_id}/logs/{log_name}` routes.
- Create `tests/test_dashboard_state.py`: Unit tests for state summaries and detail serialization.
- Create `tests/test_dashboard_logs.py`: Unit tests for allowlisted logs, bounded tailing, missing logs, and path traversal rejection.
- Create `tests/test_api_dashboard.py`: API tests for dashboard HTML and new JSON routes.

Keep `JobStore` unchanged unless implementation discovers a real need. The current `list_jobs()` and `get_job()` methods already expose enough data for this first version.

## Task 1: Add Dashboard Response Models And State Helpers

**Files:**
- Modify: `orchestrator/models.py`
- Create: `orchestrator/dashboard.py`
- Test: `tests/test_dashboard_state.py`

- [ ] **Step 1: Write failing dashboard state tests**

Create `tests/test_dashboard_state.py`:

```python
from orchestrator.dashboard import build_dashboard_state, build_job_detail
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def test_build_dashboard_state_counts_latest_jobs_and_active_errors(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    queued = store.submit_job("ktb-096", priority=100, force=False).job
    failed = store.submit_job("ktb-095", priority=90, force=False).job
    ready = store.submit_job("ktb-094", priority=80, force=False).job

    store.mark_audio_ready(ready.id)
    store.record_download_failure(
        failed.id,
        JobStatus.FAILED,
        attempt_count=3,
        error="metadata failed: Movie not found in MissAV catalog",
    )

    state = build_dashboard_state(store)

    assert state.api["online"] is True
    assert state.api["jobs_root_mac"] == str(mac_jobs_root)
    assert state.api["jobs_root_windows"] == "M:\\"
    assert state.counts["queued"] == 1
    assert state.counts["audio_ready"] == 1
    assert state.counts["failed"] == 1
    assert [job.movie_number for job in state.latest_jobs] == ["ktb-094", "ktb-095", "ktb-096"]
    assert state.active_errors[0].movie_number == "ktb-095"
    assert state.active_errors[0].error == "metadata failed: Movie not found in MissAV catalog"


def test_build_dashboard_state_derives_worker_activity(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    downloading = store.submit_job("ktb-096", priority=100, force=False).job
    transcribing = store.submit_job("ktb-095", priority=100, force=False).job

    store.update_download_status(downloading.id, JobStatus.DOWNLOADING_AUDIO)
    store.mark_audio_ready(transcribing.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    store.heartbeat(claimed.id, "windows-gpu-1", JobStatus.TRANSCRIBING, lease_seconds=1800)

    state = build_dashboard_state(store)

    assert state.activity["mac"]["status"] == "downloading_audio"
    assert state.activity["mac"]["movie_number"] == "ktb-096"
    assert state.activity["windows"]["status"] == "transcribing"
    assert state.activity["windows"]["movie_number"] == "ktb-095"
    assert state.activity["windows"]["worker_id"] == "windows-gpu-1"


def test_build_job_detail_returns_full_operational_fields(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=50, force=False).job
    ready = store.mark_audio_ready(job.id)

    detail = build_job_detail(ready)

    assert detail.id == job.id
    assert detail.movie_number == "ktb-112"
    assert detail.normalized_movie_number == "ktb-112"
    assert detail.status == "audio_ready"
    assert detail.priority == 50
    assert detail.attempt_count == 0
    assert detail.worker_attempt_count == 0
    assert detail.claimed_by is None
    assert detail.job_dir_mac == str(mac_jobs_root / "ktb-112")
    assert detail.job_dir_windows == "M:\\ktb-112"
    assert detail.metadata_path_mac == str(mac_jobs_root / "ktb-112" / "metadata.json")
    assert detail.audio_path_mac == str(mac_jobs_root / "ktb-112" / "audio.wav")
    assert detail.audio_path_windows == "M:\\ktb-112\\audio.wav"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
.venv/bin/python -m pytest tests/test_dashboard_state.py -q
```

Expected: fail with `ModuleNotFoundError: No module named 'orchestrator.dashboard'`.

- [ ] **Step 3: Add dashboard Pydantic models**

Modify `orchestrator/models.py` by appending these models after `WorkerNextJobResponse`:

```python
class DashboardJobSummary(BaseModel):
    id: str
    movie_number: str
    status: JobStatus
    priority: int
    updated_at: str
    claimed_by: str | None = None
    error: str | None = None


class DashboardStateResponse(BaseModel):
    api: dict[str, str | bool]
    activity: dict[str, dict[str, str | None]]
    counts: dict[str, int]
    latest_jobs: list[DashboardJobSummary]
    active_errors: list[DashboardJobSummary]


class JobDetailResponse(BaseModel):
    id: str
    movie_number: str
    normalized_movie_number: str
    status: JobStatus
    priority: int
    attempt_count: int
    worker_attempt_count: int
    claimed_by: str | None = None
    lease_expires_at: str | None = None
    created_at: str
    updated_at: str
    error: str | None = None
    job_dir_mac: str
    job_dir_windows: str
    metadata_path_mac: str | None = None
    audio_path_mac: str | None = None
    audio_path_windows: str | None = None
    japanese_srt_path_mac: str | None = None
    japanese_srt_path_windows: str | None = None
    english_srt_path_mac: str | None = None
    english_srt_path_windows: str | None = None
```

- [ ] **Step 4: Add dashboard state helper implementation**

Create `orchestrator/dashboard.py`:

```python
from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

from orchestrator.models import DashboardJobSummary, DashboardStateResponse, JobDetailResponse, JobStatus
from orchestrator.store import JobRecord, JobStore


MAC_ACTIVE_STATUSES = {
    JobStatus.DOWNLOADING_METADATA,
    JobStatus.DOWNLOADING_AUDIO,
}

WINDOWS_ACTIVE_STATUSES = {
    JobStatus.TRANSCRIPTION_CLAIMED,
    JobStatus.TRANSCRIBING,
    JobStatus.TRANSCRIPTION_DONE,
    JobStatus.TRANSLATING,
}


def job_summary(job: JobRecord) -> DashboardJobSummary:
    return DashboardJobSummary(
        id=job.id,
        movie_number=job.normalized_movie_number,
        status=job.status,
        priority=job.priority,
        updated_at=job.updated_at,
        claimed_by=job.claimed_by,
        error=job.error,
    )


def build_job_detail(job: JobRecord) -> JobDetailResponse:
    return JobDetailResponse(
        id=job.id,
        movie_number=job.movie_number,
        normalized_movie_number=job.normalized_movie_number,
        status=job.status,
        priority=job.priority,
        attempt_count=job.attempt_count,
        worker_attempt_count=job.worker_attempt_count,
        claimed_by=job.claimed_by,
        lease_expires_at=job.lease_expires_at,
        created_at=job.created_at,
        updated_at=job.updated_at,
        error=job.error,
        job_dir_mac=job.job_dir_mac,
        job_dir_windows=job.job_dir_windows,
        metadata_path_mac=job.metadata_path_mac,
        audio_path_mac=job.audio_path_mac,
        audio_path_windows=job.audio_path_windows,
        japanese_srt_path_mac=job.japanese_srt_path_mac,
        japanese_srt_path_windows=job.japanese_srt_path_windows,
        english_srt_path_mac=job.english_srt_path_mac,
        english_srt_path_windows=job.english_srt_path_windows,
    )


def _latest_active_job(jobs: list[JobRecord], statuses: set[JobStatus]) -> JobRecord | None:
    candidates = [job for job in jobs if job.status in statuses]
    if not candidates:
        return None
    return sorted(candidates, key=lambda job: job.updated_at, reverse=True)[0]


def _activity_payload(job: JobRecord | None) -> dict[str, str | None]:
    if job is None:
        return {
            "status": "idle",
            "movie_number": None,
            "job_id": None,
            "worker_id": None,
            "updated_at": None,
        }
    return {
        "status": job.status.value,
        "movie_number": job.normalized_movie_number,
        "job_id": job.id,
        "worker_id": job.claimed_by,
        "updated_at": job.updated_at,
    }


def build_dashboard_state(store: JobStore, *, latest_limit: int = 50) -> DashboardStateResponse:
    jobs = store.list_jobs()
    counts = Counter(job.status.value for job in jobs)
    latest = sorted(jobs, key=lambda job: job.updated_at, reverse=True)[:latest_limit]
    errors = [
        job
        for job in sorted(jobs, key=lambda item: item.updated_at, reverse=True)
        if job_has_active_error(job)
    ]
    mac_job = _latest_active_job(jobs, MAC_ACTIVE_STATUSES)
    windows_job = _latest_active_job(jobs, WINDOWS_ACTIVE_STATUSES)
    return DashboardStateResponse(
        api={
            "online": True,
            "server_time": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "jobs_root_mac": str(store.jobs_root_mac),
            "jobs_root_windows": store.jobs_root_windows,
        },
        activity={
            "mac": _activity_payload(mac_job),
            "windows": _activity_payload(windows_job),
        },
        counts=dict(counts),
        latest_jobs=[job_summary(job) for job in latest],
        active_errors=[job_summary(job) for job in errors],
    )


def job_has_active_error(job: JobRecord) -> bool:
    return job.status == JobStatus.FAILED or bool(job.error)
```

- [ ] **Step 5: Run dashboard state tests**

Run:

```bash
.venv/bin/python -m pytest tests/test_dashboard_state.py -q
```

Expected: all tests pass.

- [ ] **Step 6: Run related existing tests**

Run:

```bash
.venv/bin/python -m pytest tests/test_api_jobs.py tests/test_store_worker_claims.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit Task 1**

Run:

```bash
git add orchestrator/models.py orchestrator/dashboard.py tests/test_dashboard_state.py
git commit -m "feat: add dashboard state models"
```

## Task 2: Add Job Log Listing And Bounded Tail Helpers

**Files:**
- Modify: `orchestrator/models.py`
- Modify: `orchestrator/dashboard.py`
- Test: `tests/test_dashboard_logs.py`

- [ ] **Step 1: Write failing log helper tests**

Create `tests/test_dashboard_logs.py`:

```python
import pytest

from orchestrator.dashboard import list_job_logs, read_job_log_tail
from orchestrator.store import JobStore


def test_list_job_logs_returns_existing_allowlisted_logs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "mac-download.log").write_text("download ok\n", encoding="utf-8")
    (logs_dir / "translate.log").write_text("translate ok\n", encoding="utf-8")
    (logs_dir / "secret.log").write_text("hidden\n", encoding="utf-8")

    response = list_job_logs(job)

    assert [log.name for log in response.logs] == ["mac-download.log", "translate.log"]
    assert response.logs[0].size_bytes == len("download ok\n")
    assert response.logs[0].available is True


def test_read_job_log_tail_returns_last_lines_and_caps_tail(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "translate.log").write_text(
        "\n".join(f"line {index}" for index in range(1, 1205)) + "\n",
        encoding="utf-8",
    )

    response = read_job_log_tail(job, "translate.log", tail=1200)

    assert response.log_name == "translate.log"
    assert response.tail == 1000
    assert response.lines[0] == "line 205"
    assert response.lines[-1] == "line 1204"


def test_read_job_log_tail_rejects_unknown_or_traversal_log_names(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job

    with pytest.raises(FileNotFoundError):
        read_job_log_tail(job, "secret.log")

    with pytest.raises(FileNotFoundError):
        read_job_log_tail(job, "../translate.log")


def test_read_job_log_tail_rejects_missing_allowlisted_log(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job

    with pytest.raises(FileNotFoundError):
        read_job_log_tail(job, "whisper.log")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
.venv/bin/python -m pytest tests/test_dashboard_logs.py -q
```

Expected: fail because `list_job_logs` and `read_job_log_tail` are not implemented.

- [ ] **Step 3: Add log response models**

Append these models to `orchestrator/models.py`:

```python
class JobLogSummary(BaseModel):
    name: str
    size_bytes: int
    available: bool


class JobLogsResponse(BaseModel):
    job_id: str
    logs: list[JobLogSummary]


class JobLogTailResponse(BaseModel):
    job_id: str
    log_name: str
    tail: int
    lines: list[str]
```

- [ ] **Step 4: Add allowlisted log helper implementation**

Modify the imports in `orchestrator/dashboard.py`:

```python
from pathlib import Path

from orchestrator.models import (
    DashboardJobSummary,
    DashboardStateResponse,
    JobDetailResponse,
    JobLogSummary,
    JobLogTailResponse,
    JobLogsResponse,
    JobStatus,
)
```

Append these helpers to `orchestrator/dashboard.py`:

```python
ALLOWED_LOG_NAMES = (
    "mac-download.log",
    "windows-worker.log",
    "whisper.log",
    "translate.log",
)


def _job_logs_dir(job: JobRecord) -> Path:
    return Path(job.job_dir_mac) / "logs"


def _resolve_allowed_log_path(job: JobRecord, log_name: str) -> Path:
    if log_name not in ALLOWED_LOG_NAMES:
        raise FileNotFoundError(log_name)
    logs_dir = _job_logs_dir(job).resolve()
    path = (logs_dir / log_name).resolve()
    if path.parent != logs_dir:
        raise FileNotFoundError(log_name)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(log_name)
    return path


def list_job_logs(job: JobRecord) -> JobLogsResponse:
    logs: list[JobLogSummary] = []
    logs_dir = _job_logs_dir(job)
    for log_name in ALLOWED_LOG_NAMES:
        path = logs_dir / log_name
        if path.exists() and path.is_file():
            logs.append(
                JobLogSummary(
                    name=log_name,
                    size_bytes=path.stat().st_size,
                    available=True,
                )
            )
    return JobLogsResponse(job_id=job.id, logs=logs)


def read_job_log_tail(job: JobRecord, log_name: str, tail: int = 200) -> JobLogTailResponse:
    bounded_tail = min(max(tail, 1), 1000)
    path = _resolve_allowed_log_path(job, log_name)
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return JobLogTailResponse(
        job_id=job.id,
        log_name=log_name,
        tail=bounded_tail,
        lines=lines[-bounded_tail:],
    )
```

- [ ] **Step 5: Run log helper tests**

Run:

```bash
.venv/bin/python -m pytest tests/test_dashboard_logs.py -q
```

Expected: all tests pass.

- [ ] **Step 6: Run dashboard state tests again**

Run:

```bash
.venv/bin/python -m pytest tests/test_dashboard_state.py tests/test_dashboard_logs.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit Task 2**

Run:

```bash
git add orchestrator/models.py orchestrator/dashboard.py tests/test_dashboard_logs.py
git commit -m "feat: add dashboard log helpers"
```

## Task 3: Add Dashboard And Detail API Routes

**Files:**
- Modify: `orchestrator/api.py`
- Test: `tests/test_api_dashboard.py`

- [ ] **Step 1: Write failing API route tests**

Create `tests/test_api_dashboard.py`:

```python
from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def test_dashboard_state_endpoint_returns_counts_latest_jobs_and_errors(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-096", priority=100, force=False)
    failed = store.submit_job("ktb-095", priority=100, force=False).job
    store.record_download_failure(failed.id, JobStatus.FAILED, 3, "download interrupted")
    client = TestClient(create_app(store))

    response = client.get("/dashboard/state")

    assert response.status_code == 200
    body = response.json()
    assert body["api"]["online"] is True
    assert body["counts"]["queued"] == 1
    assert body["counts"]["failed"] == 1
    assert [job["movie_number"] for job in body["active_errors"]] == ["ktb-095"]
    assert {job["movie_number"] for job in body["latest_jobs"]} == {"ktb-096", "ktb-095"}


def test_job_detail_endpoint_returns_full_paths(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=50, force=False).job
    store.mark_audio_ready(job.id)
    client = TestClient(create_app(store))

    response = client.get(f"/jobs/{job.id}/detail")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == job.id
    assert body["movie_number"] == "ktb-112"
    assert body["normalized_movie_number"] == "ktb-112"
    assert body["status"] == "audio_ready"
    assert body["priority"] == 50
    assert body["job_dir_mac"].endswith("/ktb-112")
    assert body["job_dir_windows"] == "M:\\ktb-112"
    assert body["audio_path_windows"] == "M:\\ktb-112\\audio.wav"


def test_log_endpoints_list_and_tail_allowlisted_logs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "translate.log").write_text("one\ntwo\nthree\n", encoding="utf-8")
    client = TestClient(create_app(store))

    list_response = client.get(f"/jobs/{job.id}/logs")
    tail_response = client.get(f"/jobs/{job.id}/logs/translate.log?tail=2")

    assert list_response.status_code == 200
    assert list_response.json()["logs"] == [
        {"name": "translate.log", "size_bytes": len("one\ntwo\nthree\n"), "available": True}
    ]
    assert tail_response.status_code == 200
    assert tail_response.json()["lines"] == ["two", "three"]


def test_log_tail_endpoint_rejects_unknown_and_traversal_names(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    client = TestClient(create_app(store))

    unknown = client.get(f"/jobs/{job.id}/logs/secret.log")
    traversal = client.get(f"/jobs/{job.id}/logs/..%2Ftranslate.log")

    assert unknown.status_code == 404
    assert traversal.status_code in {404, 422}


def test_dashboard_routes_return_404_for_missing_job(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    detail = client.get("/jobs/job_missing/detail")
    logs = client.get("/jobs/job_missing/logs")

    assert detail.status_code == 404
    assert logs.status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
.venv/bin/python -m pytest tests/test_api_dashboard.py -q
```

Expected: fail because the new routes do not exist.

- [ ] **Step 3: Import dashboard helpers and response models in `orchestrator/api.py`**

Modify `orchestrator/api.py` imports:

```python
from orchestrator.dashboard import (
    build_dashboard_state,
    build_job_detail,
    list_job_logs,
    read_job_log_tail,
)
from orchestrator.models import (
    BatchJobResponse,
    DashboardStateResponse,
    JobDetailResponse,
    JobLogTailResponse,
    JobLogsResponse,
    JobResponse,
    JobStatus,
    SubmitBatchRequest,
    SubmitJobRequest,
    WorkerCompleteRequest,
    WorkerFailedRequest,
    WorkerHeartbeatRequest,
    WorkerJobResponse,
    WorkerNextJobResponse,
)
```

- [ ] **Step 4: Add JSON API routes**

Inside `create_app()` in `orchestrator/api.py`, after `list_jobs()` and before `get_job()`, add:

```python
    @app.get("/dashboard/state", response_model=DashboardStateResponse)
    def dashboard_state() -> DashboardStateResponse:
        return build_dashboard_state(store)
```

After the existing `get_job()` route, add:

```python
    @app.get("/jobs/{job_id}/detail", response_model=JobDetailResponse)
    def get_job_detail(job_id: str) -> JobDetailResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return build_job_detail(job)

    @app.get("/jobs/{job_id}/logs", response_model=JobLogsResponse)
    def get_job_logs(job_id: str) -> JobLogsResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return list_job_logs(job)

    @app.get("/jobs/{job_id}/logs/{log_name}", response_model=JobLogTailResponse)
    def get_job_log_tail(job_id: str, log_name: str, tail: int = Query(default=200)) -> JobLogTailResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        try:
            return read_job_log_tail(job, log_name, tail)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="log not found") from exc
```

- [ ] **Step 5: Run API dashboard tests**

Run:

```bash
.venv/bin/python -m pytest tests/test_api_dashboard.py -q
```

Expected: all tests pass.

- [ ] **Step 6: Run route regression tests**

Run:

```bash
.venv/bin/python -m pytest tests/test_api_jobs.py tests/test_api_worker.py tests/test_api_publish.py tests/test_api_dashboard.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit Task 3**

Run:

```bash
git add orchestrator/api.py tests/test_api_dashboard.py
git commit -m "feat: add dashboard api routes"
```

## Task 4: Serve The Dashboard HTML Page

**Files:**
- Modify: `orchestrator/dashboard.py`
- Modify: `orchestrator/api.py`
- Modify: `tests/test_api_dashboard.py`

- [ ] **Step 1: Add failing dashboard HTML tests**

Append to `tests/test_api_dashboard.py`:

```python
def test_dashboard_page_returns_operator_html_without_force_controls(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    response = client.get("/dashboard")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    html = response.text
    assert "JAV Subtitle Orchestrator" in html
    assert 'href="/docs"' in html
    assert 'id="single-movie-form"' in html
    assert 'id="batch-movie-form"' in html
    assert 'id="jobs-list"' in html
    assert 'id="job-detail"' in html
    assert 'id="log-output"' in html
    assert 'id="force"' not in html.lower()
    assert 'name="force"' not in html.lower()
    assert 'type="checkbox"' not in html.lower()
    assert "force: false" in html
```

- [ ] **Step 2: Run HTML test to verify it fails**

Run:

```bash
.venv/bin/python -m pytest tests/test_api_dashboard.py::test_dashboard_page_returns_operator_html_without_force_controls -q
```

Expected: fail with `404 Not Found` for `/dashboard`.

- [ ] **Step 3: Add dashboard HTML renderer**

Append this function to `orchestrator/dashboard.py`:

```python
def dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>JAV Subtitle Orchestrator Dashboard</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #1f2933;
      --muted: #6b7280;
      --line: #d9dee7;
      --accent: #0f766e;
      --danger: #b42318;
      --ready: #176b3a;
      --active: #92400e;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 14px 20px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
      position: sticky;
      top: 0;
      z-index: 5;
    }
    h1 { font-size: 18px; margin: 0; letter-spacing: 0; }
    a { color: var(--accent); text-decoration: none; }
    main { padding: 18px 20px 28px; max-width: 1500px; margin: 0 auto; }
    .health {
      display: grid;
      grid-template-columns: repeat(4, minmax(150px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .card, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    .card { padding: 12px; min-height: 76px; }
    .label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .04em; }
    .value { margin-top: 4px; font-size: 18px; font-weight: 650; overflow-wrap: anywhere; }
    .sub { color: var(--muted); font-size: 12px; overflow-wrap: anywhere; }
    .submit-grid {
      display: grid;
      grid-template-columns: minmax(240px, 1fr) minmax(280px, 1.4fr);
      gap: 12px;
      margin-bottom: 14px;
    }
    form { padding: 12px; }
    form h2, .panel h2 { font-size: 14px; margin: 0 0 10px; }
    input, textarea, button {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      font: inherit;
    }
    input, textarea { padding: 9px 10px; background: white; color: var(--text); }
    textarea { min-height: 76px; resize: vertical; }
    button {
      padding: 9px 10px;
      background: var(--accent);
      color: white;
      border-color: var(--accent);
      cursor: pointer;
      font-weight: 650;
    }
    button.secondary { background: white; color: var(--text); border-color: var(--line); }
    .form-row { display: grid; grid-template-columns: 1fr 92px; gap: 8px; margin-bottom: 8px; }
    .message { min-height: 20px; margin-top: 8px; color: var(--muted); font-size: 12px; }
    .workspace { display: grid; grid-template-columns: minmax(360px, 1fr) minmax(420px, .9fr); gap: 12px; align-items: start; }
    .panel { min-height: 180px; overflow: hidden; }
    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 12px;
      border-bottom: 1px solid var(--line);
    }
    .jobs-list { display: grid; gap: 0; }
    .job-row {
      display: grid;
      grid-template-columns: 132px 150px 1fr;
      gap: 10px;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      cursor: pointer;
      align-items: center;
    }
    .job-row:hover { background: #f0fdfa; }
    .status {
      display: inline-block;
      padding: 3px 7px;
      border-radius: 999px;
      background: #eef2f7;
      color: var(--text);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .status.failed { background: #fee4e2; color: var(--danger); }
    .status.english_srt_ready { background: #dcfce7; color: var(--ready); }
    .status.transcribing, .status.translating, .status.downloading_audio, .status.downloading_metadata { background: #fef3c7; color: var(--active); }
    .detail-body { padding: 12px; }
    .kv { display: grid; grid-template-columns: 150px 1fr; gap: 8px; padding: 5px 0; border-bottom: 1px solid #eef2f7; }
    .kv div:last-child { overflow-wrap: anywhere; color: var(--text); }
    .logs { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; margin: 12px 0; }
    pre {
      margin: 0;
      padding: 10px;
      min-height: 160px;
      max-height: 420px;
      overflow: auto;
      background: #111827;
      color: #e5e7eb;
      border-radius: 6px;
      font-size: 12px;
      white-space: pre-wrap;
    }
    @media (max-width: 900px) {
      .health, .submit-grid, .workspace { grid-template-columns: 1fr; }
      .job-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <h1>JAV Subtitle Orchestrator</h1>
    <nav><a href="/dashboard">Dashboard</a> &nbsp; <a href="/docs">Swagger</a></nav>
  </header>
  <main>
    <section class="health">
      <div class="card"><div class="label">API</div><div class="value" id="api-status">Loading</div><div class="sub" id="server-time"></div></div>
      <div class="card"><div class="label">Mac Worker</div><div class="value" id="mac-status">Unknown</div><div class="sub" id="mac-detail"></div></div>
      <div class="card"><div class="label">Windows Worker</div><div class="value" id="windows-status">Unknown</div><div class="sub" id="windows-detail"></div></div>
      <div class="card"><div class="label">Active Errors</div><div class="value" id="error-count">0</div><div class="sub" id="queue-counts"></div></div>
    </section>

    <section class="submit-grid">
      <form id="single-movie-form" class="panel">
        <h2>Submit Job</h2>
        <div class="form-row">
          <input id="single-movie" placeholder="Movie ID, e.g. mtall-120" autocomplete="off">
          <input id="single-priority" type="number" value="100" min="0">
        </div>
        <button type="submit">Queue Movie</button>
        <div class="message" id="single-message"></div>
      </form>
      <form id="batch-movie-form" class="panel">
        <h2>Submit Batch</h2>
        <div class="form-row">
          <textarea id="batch-movies" placeholder="One movie ID per line"></textarea>
          <input id="batch-priority" type="number" value="100" min="0">
        </div>
        <button type="submit">Queue Batch</button>
        <div class="message" id="batch-message"></div>
      </form>
    </section>

    <section class="workspace">
      <div class="panel">
        <div class="panel-head"><h2>Latest Jobs</h2><button class="secondary" id="refresh-button" type="button">Refresh</button></div>
        <div class="jobs-list" id="jobs-list"></div>
      </div>
      <div class="panel">
        <div class="panel-head"><h2>Selected Job</h2><span class="sub" id="selected-job-label">None selected</span></div>
        <div class="detail-body">
          <div id="job-detail" class="sub">Select a job to view details.</div>
          <div class="logs" id="log-buttons"></div>
          <pre id="log-output">Log output appears here.</pre>
        </div>
      </div>
    </section>
  </main>
  <script>
    const statusClass = (status) => `status ${status || ""}`;
    const text = (value) => value === null || value === undefined || value === "" ? "-" : String(value);

    async function fetchJson(url, options = {}) {
      const response = await fetch(url, options);
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.detail || `HTTP ${response.status}`);
      }
      return body;
    }

    function renderActivity(prefix, activity) {
      document.getElementById(`${prefix}-status`).textContent = text(activity.status);
      document.getElementById(`${prefix}-detail`).textContent = activity.movie_number ? `${activity.movie_number} ${activity.worker_id || ""}` : "";
    }

    function renderJobs(jobs) {
      const list = document.getElementById("jobs-list");
      list.innerHTML = "";
      if (!jobs.length) {
        list.innerHTML = '<div class="job-row"><div>No jobs yet</div></div>';
        return;
      }
      for (const job of jobs) {
        const row = document.createElement("div");
        row.className = "job-row";
        row.innerHTML = `
          <strong>${text(job.movie_number)}</strong>
          <span class="${statusClass(job.status)}">${text(job.status)}</span>
          <div><div>${text(job.updated_at)}</div><div class="sub">${text(job.error || job.claimed_by)}</div></div>
        `;
        row.addEventListener("click", () => selectJob(job.id, job.movie_number));
        list.appendChild(row);
      }
    }

    async function refreshState() {
      const state = await fetchJson("/dashboard/state");
      document.getElementById("api-status").textContent = state.api.online ? "Online" : "Offline";
      document.getElementById("server-time").textContent = text(state.api.server_time);
      renderActivity("mac", state.activity.mac);
      renderActivity("windows", state.activity.windows);
      document.getElementById("error-count").textContent = state.active_errors.length;
      document.getElementById("queue-counts").textContent = Object.entries(state.counts).map(([key, value]) => `${key}: ${value}`).join(" | ");
      renderJobs(state.latest_jobs);
    }

    async function selectJob(jobId, movieNumber) {
      document.getElementById("selected-job-label").textContent = movieNumber;
      const detail = await fetchJson(`/jobs/${jobId}/detail`);
      const fields = [
        "id", "movie_number", "normalized_movie_number", "status", "priority",
        "attempt_count", "worker_attempt_count", "claimed_by", "lease_expires_at",
        "created_at", "updated_at", "error", "job_dir_mac", "job_dir_windows",
        "metadata_path_mac", "audio_path_mac", "audio_path_windows",
        "japanese_srt_path_mac", "japanese_srt_path_windows",
        "english_srt_path_mac", "english_srt_path_windows"
      ];
      document.getElementById("job-detail").innerHTML = fields.map((field) => `<div class="kv"><div class="label">${field}</div><div>${text(detail[field])}</div></div>`).join("");
      const logs = await fetchJson(`/jobs/${jobId}/logs`);
      const buttons = document.getElementById("log-buttons");
      buttons.innerHTML = "";
      for (const log of logs.logs) {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = log.name;
        button.addEventListener("click", () => loadLog(jobId, log.name));
        buttons.appendChild(button);
      }
      if (!logs.logs.length) {
        buttons.innerHTML = '<div class="sub">No logs available.</div>';
      }
      document.getElementById("log-output").textContent = "Log output appears here.";
    }

    async function loadLog(jobId, logName) {
      const log = await fetchJson(`/jobs/${jobId}/logs/${encodeURIComponent(logName)}?tail=200`);
      document.getElementById("log-output").textContent = log.lines.join("\\n") || "Log is empty.";
    }

    document.getElementById("single-movie-form").addEventListener("submit", async (event) => {
      event.preventDefault();
      const message = document.getElementById("single-message");
      try {
        const movie = document.getElementById("single-movie").value.trim();
        const priority = Number(document.getElementById("single-priority").value || 100);
        const result = await fetchJson("/jobs", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({movie_number: movie, priority, force: false})
        });
        message.textContent = `Queued ${result.movie_number}: ${result.status}`;
        await refreshState();
      } catch (error) {
        message.textContent = error.message;
      }
    });

    document.getElementById("batch-movie-form").addEventListener("submit", async (event) => {
      event.preventDefault();
      const message = document.getElementById("batch-message");
      try {
        const movies = document.getElementById("batch-movies").value.split(/\\r?\\n/).map((item) => item.trim()).filter(Boolean);
        const priority = Number(document.getElementById("batch-priority").value || 100);
        const result = await fetchJson("/jobs/batch", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({movie_numbers: movies, priority, force: false})
        });
        message.textContent = `Created ${result.created.length}, existing ${result.existing.length}, invalid ${result.invalid.length}`;
        await refreshState();
      } catch (error) {
        message.textContent = error.message;
      }
    });

    document.getElementById("refresh-button").addEventListener("click", refreshState);
    refreshState().catch((error) => {
      document.getElementById("jobs-list").innerHTML = `<div class="job-row">${error.message}</div>`;
    });
  </script>
</body>
</html>"""
```

- [ ] **Step 4: Add `/dashboard` route**

Modify imports in `orchestrator/api.py`:

```python
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
```

Include `dashboard_html` in the dashboard imports:

```python
from orchestrator.dashboard import (
    build_dashboard_state,
    build_job_detail,
    dashboard_html,
    list_job_logs,
    read_job_log_tail,
)
```

Inside `create_app()` before the `/jobs` routes, add:

```python
    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard_page() -> str:
        return dashboard_html()
```

- [ ] **Step 5: Run dashboard HTML test**

Run:

```bash
.venv/bin/python -m pytest tests/test_api_dashboard.py::test_dashboard_page_returns_operator_html_without_force_controls -q
```

Expected: test passes.

- [ ] **Step 6: Run all dashboard API tests**

Run:

```bash
.venv/bin/python -m pytest tests/test_dashboard_state.py tests/test_dashboard_logs.py tests/test_api_dashboard.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit Task 4**

Run:

```bash
git add orchestrator/dashboard.py orchestrator/api.py tests/test_api_dashboard.py
git commit -m "feat: serve operator dashboard"
```

## Task 5: Final Verification And LAN Smoke Test

**Files:**
- Modify: none unless verification finds a defect.

- [ ] **Step 1: Run the full test suite**

Run:

```bash
.venv/bin/python -m pytest -q
```

Expected: all tests pass.

- [ ] **Step 2: Start the Mac API on port 8010**

Run:

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
source .venv/bin/activate
python -m orchestrator api
```

Expected output includes:

```text
Uvicorn running on http://0.0.0.0:8010
```

Leave this process running while performing the smoke checks.

- [ ] **Step 3: Verify dashboard and Swagger locally**

In another terminal, run:

```bash
curl -I http://127.0.0.1:8010/dashboard
curl -I http://127.0.0.1:8010/docs
curl http://127.0.0.1:8010/dashboard/state
```

Expected:

- `/dashboard` returns HTTP `200` with `content-type: text/html`.
- `/docs` returns HTTP `200`.
- `/dashboard/state` returns JSON with `api`, `activity`, `counts`, `latest_jobs`, and `active_errors`.

- [ ] **Step 4: Open the dashboard in a browser**

Open:

```text
http://127.0.0.1:8010/dashboard
```

Verify:

- Top health cards render.
- Latest jobs render.
- Clicking an existing job shows detail fields.
- If that job has logs, clicking `translate.log` or another available log shows log tail output.
- The Swagger link opens `/docs`.

- [ ] **Step 5: Submit a harmless test job through the dashboard**

Use the dashboard single submit form with a movie ID that is safe to queue for testing.

Recommended:

```text
dashboard-smoke-001
```

Expected:

- The dashboard shows a validation error because this is not a valid movie ID.
- No destructive action runs.

Then use a real movie ID only if the owner wants to actually queue work.

- [ ] **Step 6: Verify from another LAN machine**

Find the Mac LAN IP:

```bash
ipconfig getifaddr en0
```

From another machine on the same network, open:

```text
http://<mac-lan-ip>:8010/dashboard
```

Expected:

- Dashboard loads.
- `/docs` link works.
- Submitting through the dashboard reaches the Mac API.

- [ ] **Step 7: Commit verification fix if needed**

If verification required a code fix, run the relevant test again and commit:

```bash
git add orchestrator/dashboard.py orchestrator/api.py orchestrator/models.py tests/test_dashboard_state.py tests/test_dashboard_logs.py tests/test_api_dashboard.py
git commit -m "fix: polish operator dashboard"
```

If no fix was needed, do not create an empty commit.

## Completion Checklist

- [ ] `/dashboard` serves HTML.
- [ ] `/dashboard/state` serves API health, activity, status counts, latest jobs, and active errors.
- [ ] `/jobs/{job_id}/detail` returns all operational fields and paths.
- [ ] `/jobs/{job_id}/logs` lists only allowlisted existing logs.
- [ ] `/jobs/{job_id}/logs/{log_name}?tail=200` returns bounded read-only log tails.
- [ ] Unknown logs and path traversal are rejected.
- [ ] Dashboard single submit calls `POST /jobs` with `force=false`.
- [ ] Dashboard batch submit calls `POST /jobs/batch` with `force=false`.
- [ ] No force, cancel, retry, delete, or rerun controls appear in the dashboard HTML.
- [ ] Swagger remains available at `/docs`.
- [ ] Full test suite passes.
- [ ] LAN smoke test succeeds or any LAN/network issue is documented separately from code.
