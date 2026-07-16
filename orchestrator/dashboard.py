from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from math import ceil
from pathlib import Path

from orchestrator.models import (
    AudioCleanupStatus,
    DashboardJobSummary,
    DashboardStateResponse,
    HistoricalRepairDashboardCounts,
    HistoricalRepairDashboardCurrent,
    HistoricalRepairDashboardProgress,
    JobBrowserItem,
    JobBrowserResponse,
    JobDetailResponse,
    JobLogSummary,
    JobLogTailResponse,
    JobLogsResponse,
    JobStatus,
    WorkerHealthSummary,
)
from orchestrator.store import (
    NORMAL_TRANSLATION_ORIGIN,
    HistoricalRepairDashboardSnapshot,
    JobRecord,
    JobStore,
    WorkerStatusRecord,
)


MAC_ACTIVE_STATUSES = {
    JobStatus.DOWNLOADING_METADATA,
    JobStatus.DOWNLOADING_AUDIO,
}

WINDOWS_TRANSCRIPTION_STATUSES = {
    JobStatus.TRANSCRIPTION_CLAIMED,
    JobStatus.TRANSCRIBING,
}

MAC_TRANSLATION_STATUSES = {
    JobStatus.TRANSLATING,
    JobStatus.PUBLISH_PENDING,
    JobStatus.PUBLISHING,
    JobStatus.CATALOG_SYNC_PENDING,
    JobStatus.CATALOG_SYNCING,
}

SUBTITLE_PROCESSING_STATUSES = {
    *WINDOWS_TRANSCRIPTION_STATUSES,
    JobStatus.TRANSCRIPTION_DONE,
    *MAC_TRANSLATION_STATUSES,
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
    JobStatus.PUBLISH_PENDING,
    JobStatus.PUBLISHING,
    JobStatus.CATALOG_SYNC_PENDING,
    JobStatus.CATALOG_SYNCING,
}

IN_PROGRESS_BROWSER_STATUSES = ACTIVE_BROWSER_STATUSES - {JobStatus.QUEUED}
JOB_BROWSER_VIEWS = {"active", "queued", "ready", "failed", "all"}

SAFE_HISTORICAL_DASHBOARD_REASONS = frozenset(
    {
        "allowlist_changed",
        "catalog_auth_failed",
        "catalog_redirect_rejected",
        "catalog_response_invalid",
        "catalog_response_mismatch",
        "catalog_sync_failed",
        "historical_controller_state_unavailable",
        "historical_lane_paused",
        "historical_orphaned_terminal_state",
        "historical_orphaned_transient_retry",
        "operator_pause",
        "plan_digest_changed",
        "preservation_hash_changed",
        "publication_attempts_exhausted",
        "publication_configuration_missing",
        "public_visibility_failed",
        "public_visibility_mismatch",
        "public_visibility_redirect_rejected",
        "public_visibility_response_invalid",
        "quality_failure_limit",
        "quarantine_failed",
        "supabase_verification_failed",
        "translation_worker_count_mismatch",
        "translation_attempts_exhausted",
        "catalog_receipt_invalid",
        "catalog_sync_attempts_exhausted",
    }
)
SAFE_HISTORICAL_REPAIR_STATES = frozenset(
    {"planned", "pending", "running", "retry_wait", "paused"}
)


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


def job_browser_item(job: JobRecord) -> JobBrowserItem:
    return JobBrowserItem(
        id=job.id,
        movie_number=job.normalized_movie_number,
        status=job.status,
        priority=job.priority,
        created_at=job.created_at,
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
        translation_attempt_count=job.translation_attempt_count,
        publish_attempt_count=job.publish_attempt_count,
        next_publish_attempt_at=job.next_publish_attempt_at,
        artifact_status=job.artifact_status,
        catalog_sync_status=job.catalog_sync_status,
        catalog_sync_warning_code=job.catalog_sync_warning_code,
        catalog_sync_warning_message=job.catalog_sync_warning_message,
        catalog_sync_attempt_count=job.catalog_sync_attempt_count,
        next_catalog_sync_attempt_at=job.next_catalog_sync_attempt_at,
        catalog_sync_last_http_status=job.catalog_sync_last_http_status,
        catalog_sync_last_response_json=job.catalog_sync_last_response_json,
        catalog_sync_last_attempt_at=job.catalog_sync_last_attempt_at,
        catalog_movie_uuid=job.catalog_movie_uuid,
        metadata_status=job.metadata_status,
        metadata_source=job.metadata_source,
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


def dashboard_recency_key(job: JobRecord) -> tuple[str, str, str]:
    return (job.updated_at, job.created_at, job.id)


def _latest_active_job(
    jobs: list[JobRecord],
    statuses: set[JobStatus],
    *,
    origin: str | None = None,
) -> JobRecord | None:
    candidates = [
        job
        for job in jobs
        if job.status in statuses
        and (origin is None or job.translation_origin == origin)
    ]
    if not candidates:
        return None
    return sorted(candidates, key=dashboard_recency_key, reverse=True)[0]


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


def _worker_liveness(last_seen_at: str, now: datetime) -> str:
    try:
        last_seen = datetime.fromisoformat(last_seen_at)
    except ValueError:
        return "unknown"
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=UTC)
    age_seconds = (now - last_seen).total_seconds()
    if age_seconds <= 180:
        return "online"
    if age_seconds <= 1800:
        return "stale"
    return "offline"


def worker_health_summary(
    worker: WorkerStatusRecord,
    *,
    now: datetime | None = None,
) -> WorkerHealthSummary:
    current_time = now or datetime.now(UTC)
    return WorkerHealthSummary(
        worker_id=worker.worker_id,
        role=worker.role,
        state=worker.state,
        status=_worker_liveness(worker.last_seen_at, current_time),
        last_seen_at=worker.last_seen_at,
        last_poll_at=worker.last_poll_at,
        last_ip=worker.last_ip,
        current_job_id=worker.current_job_id,
        current_movie_number=worker.current_movie_number,
        stage=worker.stage,
        updated_at=worker.updated_at,
        last_error=worker.last_error,
    )


def _windows_activity_payload(
    processing_job: JobRecord | None,
    workers: list[WorkerHealthSummary],
) -> dict[str, str | None]:
    windows_workers = [
        worker for worker in workers
        if worker.role in {"windows", "windows_transcriber"}
    ]
    processing_workers = [
        worker
        for worker in windows_workers
        if worker.state == "processing" and worker.current_job_id is not None
    ]
    if processing_workers:
        worker = processing_workers[0]
        return {
            "status": worker.stage or worker.state,
            "movie_number": worker.current_movie_number,
            "job_id": worker.current_job_id,
            "worker_id": worker.worker_id,
            "updated_at": worker.last_seen_at,
        }
    if processing_job is not None:
        return _activity_payload(processing_job)
    if windows_workers:
        worker = windows_workers[0]
        return {
            "status": worker.state,
            "movie_number": worker.current_movie_number,
            "job_id": worker.current_job_id,
            "worker_id": worker.worker_id,
            "updated_at": worker.last_seen_at,
        }
    return _activity_payload(None)


def _role_activity_payload(
    fallback_job: JobRecord | None,
    workers: list[WorkerHealthSummary],
    role: str,
    *,
    allowed_job_ids: set[str] | None = None,
) -> dict[str, str | None]:
    matching = [
        worker
        for worker in workers
        if worker.role == role
        and (
            allowed_job_ids is None
            or worker.current_job_id is None
            or worker.current_job_id in allowed_job_ids
        )
    ]
    processing = [
        worker
        for worker in matching
        if worker.state == "processing"
        and (
            allowed_job_ids is None
            or worker.current_job_id in allowed_job_ids
        )
    ]
    if processing:
        worker = processing[0]
        return {
            "status": worker.stage or worker.state,
            "movie_number": worker.current_movie_number,
            "job_id": worker.current_job_id,
            "worker_id": worker.worker_id,
            "updated_at": worker.last_seen_at,
        }
    if fallback_job is not None:
        return _activity_payload(fallback_job)
    if matching:
        worker = matching[0]
        return {
            "status": worker.state,
            "movie_number": worker.current_movie_number,
            "job_id": worker.current_job_id,
            "worker_id": worker.worker_id,
            "updated_at": worker.last_seen_at,
        }
    return _activity_payload(None)


def _safe_historical_reason(reason_code: str | None) -> str | None:
    if reason_code is None:
        return None
    if reason_code in SAFE_HISTORICAL_DASHBOARD_REASONS:
        return reason_code
    return "historical_error"


def _safe_historical_state(state: str) -> str:
    if state in SAFE_HISTORICAL_REPAIR_STATES:
        return state
    return "unknown"


def _historical_progress(
    snapshot: HistoricalRepairDashboardSnapshot,
) -> HistoricalRepairDashboardProgress:
    current = snapshot.current
    return HistoricalRepairDashboardProgress(
        counts=HistoricalRepairDashboardCounts(
            total=snapshot.total,
            planned=snapshot.planned,
            pending=snapshot.pending,
            running=snapshot.running,
            retry_wait=snapshot.retry_wait,
            paused=snapshot.paused,
            succeeded=snapshot.succeeded,
            permanent_failed=snapshot.permanent_failed,
            unknown=snapshot.unknown,
        ),
        current=(
            HistoricalRepairDashboardCurrent(
                batch_id=current.batch_id,
                repair_id=current.repair_id,
                job_id=current.job_id,
                movie_number=current.movie_number,
                stage=current.stage,
                state=_safe_historical_state(current.state),
                reason_code=(
                    "historical_error"
                    if current.state not in SAFE_HISTORICAL_REPAIR_STATES
                    else _safe_historical_reason(current.reason_code)
                ),
                updated_at=current.updated_at,
            )
            if current is not None
            else None
        ),
        lane_paused=snapshot.lane_paused,
        reason_code=_safe_historical_reason(snapshot.reason_code),
        consecutive_quality_failures=snapshot.consecutive_quality_failures,
        updated_at=snapshot.updated_at,
    )


def _historical_activity_payload(
    snapshot: HistoricalRepairDashboardSnapshot,
) -> dict[str, str | None]:
    current = snapshot.current
    if current is None:
        return {
            "status": "paused" if snapshot.lane_paused else "idle",
            "movie_number": None,
            "job_id": None,
            "worker_id": None,
            "updated_at": snapshot.updated_at,
            "stage": None,
            "state": None,
        }
    return {
        "status": current.stage,
        "movie_number": current.movie_number,
        "job_id": current.job_id,
        "worker_id": current.worker_id,
        "updated_at": current.updated_at,
        "stage": current.stage,
        "state": _safe_historical_state(current.state),
    }


def build_dashboard_state(
    store: JobStore,
    *,
    latest_limit: int = 50,
    delete_audio_after_publish: bool = True,
) -> DashboardStateResponse:
    jobs = store.list_jobs()
    counts = Counter(job.status.value for job in jobs)
    latest = sorted(jobs, key=dashboard_recency_key, reverse=True)[:latest_limit]
    errors = [
        job
        for job in sorted(jobs, key=dashboard_recency_key, reverse=True)
        if job_has_active_error(job)
    ]
    mac_job = _latest_active_job(
        jobs, MAC_ACTIVE_STATUSES, origin=NORMAL_TRANSLATION_ORIGIN
    )
    processing_job = _latest_active_job(
        jobs, SUBTITLE_PROCESSING_STATUSES, origin=NORMAL_TRANSLATION_ORIGIN
    )
    windows_job = _latest_active_job(
        jobs, WINDOWS_TRANSCRIPTION_STATUSES, origin=NORMAL_TRANSLATION_ORIGIN
    )
    translation_job = _latest_active_job(
        jobs, MAC_TRANSLATION_STATUSES, origin=NORMAL_TRANSLATION_ORIGIN
    )
    normal_job_ids = {
        job.id for job in jobs if job.translation_origin == NORMAL_TRANSLATION_ORIGIN
    }
    historical_snapshot = store.historical_repair_dashboard_snapshot()
    now = datetime.now(UTC).replace(microsecond=0)
    workers = [
        worker_health_summary(worker, now=now)
        for worker in store.list_worker_statuses()
    ]
    return DashboardStateResponse(
        api={
            "online": True,
            "server_time": now.isoformat(),
            "jobs_root_mac": str(store.jobs_root_mac),
            "jobs_root_windows": store.jobs_root_windows,
        },
        activity={
            "mac": _activity_payload(mac_job),
            "processing": _activity_payload(processing_job),
            "mac_download": _role_activity_payload(mac_job, workers, "mac_downloader"),
            "windows": _windows_activity_payload(windows_job, workers),
            "translation": _role_activity_payload(
                translation_job,
                workers,
                "mac_translator",
                allowed_job_ids=normal_job_ids,
            ),
            "mac_translation": _role_activity_payload(
                translation_job,
                workers,
                "mac_translator",
                allowed_job_ids=normal_job_ids,
            ),
            "historical_translation": _historical_activity_payload(
                historical_snapshot
            ),
        },
        counts=dict(counts),
        audio_cleanup=AudioCleanupStatus(enabled=delete_audio_after_publish),
        historical_repairs=_historical_progress(historical_snapshot),
        workers=workers,
        latest_jobs=[job_summary(job) for job in latest],
        active_errors=[job_summary(job) for job in errors],
    )


def build_job_browser(
    store: JobStore,
    *,
    view: str = "active",
    page: int = 1,
    page_size: int = 50,
    q: str = "",
) -> JobBrowserResponse:
    selected_view = view if view in JOB_BROWSER_VIEWS else "active"
    bounded_page_size = min(max(page_size, 1), 100)
    current_page = max(page, 1)
    query = q.strip().lower()
    jobs = store.list_jobs()

    if selected_view == "active":
        jobs = [job for job in jobs if job.status in ACTIVE_BROWSER_STATUSES]
    elif selected_view == "queued":
        jobs = [job for job in jobs if job.status == JobStatus.QUEUED]
    elif selected_view == "ready":
        jobs = [job for job in jobs if job.status == JobStatus.ENGLISH_SRT_READY]
    elif selected_view == "failed":
        jobs = [job for job in jobs if job.status == JobStatus.FAILED]

    if query:
        jobs = [
            job
            for job in jobs
            if query in job.normalized_movie_number.lower()
            or query in job.movie_number.lower()
        ]

    if selected_view == "active":
        in_progress = sorted(
            [job for job in jobs if job.status in IN_PROGRESS_BROWSER_STATUSES],
            key=dashboard_recency_key,
            reverse=True,
        )
        queued = sorted(
            [job for job in jobs if job.status == JobStatus.QUEUED],
            key=_queued_browser_sort_key,
        )
        jobs = in_progress + queued
    elif selected_view == "queued":
        jobs = sorted(jobs, key=_queued_browser_sort_key)
    else:
        jobs = sorted(jobs, key=dashboard_recency_key, reverse=True)

    total = len(jobs)
    pages = max(ceil(total / bounded_page_size), 1)
    current_page = min(current_page, pages)
    start = (current_page - 1) * bounded_page_size
    end = start + bounded_page_size
    return JobBrowserResponse(
        items=[job_browser_item(job) for job in jobs[start:end]],
        total=total,
        page=current_page,
        page_size=bounded_page_size,
        pages=pages,
        view=selected_view,
    )


def _queued_browser_sort_key(job: JobRecord) -> tuple[int, str, str]:
    return (job.priority, job.created_at, job.id)


def job_has_active_error(job: JobRecord) -> bool:
    return job.status == JobStatus.FAILED or bool(job.error)


ALLOWED_LOG_NAMES = (
    "audio-cleanup.log",
    "mac-download.log",
    "mac-translation.log",
    "quality.log",
    "translate-batches.log",
    "local-worker.log",
    "reazon.log",
    "windows-worker.log",
    "whisper.log",
    "translate.log",
)


def _job_logs_dir(job: JobRecord) -> Path:
    return Path(job.job_dir_mac) / "logs"


def _resolve_allowed_log_path(job: JobRecord, log_name: str) -> Path:
    if log_name not in ALLOWED_LOG_NAMES:
        raise FileNotFoundError(log_name)
    job_dir = Path(job.job_dir_mac)
    if job_dir.is_symlink():
        raise FileNotFoundError(log_name)
    logs_dir_raw = _job_logs_dir(job)
    if logs_dir_raw.is_symlink():
        raise FileNotFoundError(log_name)
    logs_dir = logs_dir_raw.resolve()
    path = (logs_dir / log_name).resolve()
    if path.parent != logs_dir:
        raise FileNotFoundError(log_name)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(log_name)
    return path


def list_job_logs(job: JobRecord) -> JobLogsResponse:
    logs: list[JobLogSummary] = []
    for log_name in ALLOWED_LOG_NAMES:
        try:
            path = _resolve_allowed_log_path(job, log_name)
        except FileNotFoundError:
            continue
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

def dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>JAV Subtitle Orchestrator</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --panel-soft: #f0f4f8;
      --text: #16202a;
      --muted: #5d6b78;
      --border: #d7dee7;
      --accent: #1463ff;
      --accent-dark: #0b4fd0;
      --danger: #b42318;
      --ok: #067647;
      --warn: #b54708;
      --shadow: 0 1px 2px rgba(16, 24, 40, 0.08);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-width: 320px;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.45;
    }

    a { color: var(--accent); text-decoration: none; }
    a:hover { text-decoration: underline; }

    header {
      position: sticky;
      top: 0;
      z-index: 10;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px clamp(16px, 3vw, 32px);
      border-bottom: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.96);
      backdrop-filter: blur(10px);
    }

    h1, h2, h3, p { margin: 0; }

    h1 {
      font-size: 20px;
      line-height: 1.2;
      overflow-wrap: anywhere;
    }

    h2 { font-size: 16px; }
    h3 { font-size: 14px; }

    nav {
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      font-size: 14px;
    }

    .dashboard-tabs {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 8px;
    }

    .dashboard-tab {
      width: auto;
      min-height: 36px;
      border-color: var(--border);
      background: #ffffff;
      color: var(--text);
    }

    .dashboard-tab.active {
      border-color: var(--accent-dark);
      background: var(--accent);
      color: #ffffff;
    }

    .dashboard-view {
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 18px;
    }

    .dashboard-view[hidden] {
      display: none;
    }

    main {
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 18px;
      width: min(1440px, 100%);
      margin: 0 auto;
      padding: 18px clamp(16px, 3vw, 32px) 32px;
    }

    .health-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
    }

    .health-card,
    .panel {
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel);
      box-shadow: var(--shadow);
    }

    .health-card {
      min-height: 112px;
      padding: 14px;
      display: grid;
      gap: 8px;
      align-content: start;
    }

    .subtitle-quality-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
      padding: 14px;
    }

    .subtitle-quality-card {
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel-soft);
      padding: 12px;
    }

    .subtitle-quality-card strong {
      display: block;
      margin-top: 4px;
      font-size: 20px;
    }

    .subtitle-quality-toolbar {
      display: grid;
      grid-template-columns: minmax(160px, 220px) minmax(160px, 1fr);
      gap: 10px;
      padding: 12px 14px;
      border-top: 1px solid var(--border);
      border-bottom: 1px solid var(--border);
    }

    .subtitle-quality-row {
      display: grid;
      grid-template-columns: minmax(120px, 1fr) minmax(100px, .8fr) 90px 90px minmax(180px, 1.5fr);
      gap: 10px;
      padding: 10px 14px;
      border-bottom: 1px solid var(--border);
      align-items: center;
      font-size: 13px;
    }

    .health-title {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0;
      text-transform: uppercase;
    }

    .health-value {
      font-size: 20px;
      font-weight: 700;
      overflow-wrap: anywhere;
    }

    .health-meta {
      color: var(--muted);
      font-size: 13px;
      overflow-wrap: anywhere;
    }

    .status-ok { color: var(--ok); }
    .status-warn { color: var(--warn); }
    .status-error { color: var(--danger); }

    .content-grid {
      display: grid;
      grid-template-columns: minmax(300px, 380px) minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }

    .side-stack,
    .main-stack {
      display: grid;
      gap: 18px;
      min-width: 0;
    }

    .panel-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-height: 48px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
    }

    .panel-header button { width: auto; }

    .panel-body {
      padding: 14px;
      min-width: 0;
    }

    form {
      display: grid;
      gap: 12px;
    }

    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 600;
      min-width: 0;
    }

    input,
    textarea,
    select,
    button {
      width: 100%;
      min-width: 0;
      border-radius: 8px;
      font: inherit;
    }

    input,
    textarea,
    select {
      border: 1px solid var(--border);
      background: #ffffff;
      color: var(--text);
      padding: 9px 10px;
    }

    textarea {
      min-height: 118px;
      resize: vertical;
    }

    button {
      min-height: 40px;
      border: 1px solid var(--accent-dark);
      background: var(--accent);
      color: #ffffff;
      padding: 9px 12px;
      font-weight: 700;
      cursor: pointer;
    }

    button:hover { background: var(--accent-dark); }
    button:disabled { cursor: not-allowed; opacity: 0.62; }

    .message {
      min-height: 20px;
      color: var(--muted);
      font-size: 13px;
      overflow-wrap: anywhere;
    }

    .jobs-list {
      display: grid;
      gap: 0;
    }

    .jobs-scroll {
      max-height: 560px;
      overflow: auto;
      border-bottom: 1px solid var(--border);
    }

    .browser-toolbar {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
    }

    .view-tabs,
    .pager {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 8px;
    }

    .view-tab,
    .pager button {
      width: auto;
      min-height: 34px;
      border-color: var(--border);
      background: #ffffff;
      color: var(--text);
    }

    .view-tab.active {
      border-color: var(--accent-dark);
      background: var(--accent);
      color: #ffffff;
    }

    .search-controls {
      display: grid;
      grid-template-columns: minmax(160px, 1fr) 110px;
      gap: 8px;
    }

    .browser-footer {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-height: 48px;
      padding: 10px 14px;
      color: var(--muted);
      font-size: 13px;
    }

    .import-details {
      display: grid;
      gap: 8px;
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
    }

    .import-details details {
      border: 1px solid var(--border);
      border-radius: 8px;
      background: #ffffff;
      padding: 8px 10px;
    }

    .import-details summary {
      color: var(--text);
      cursor: pointer;
      font-weight: 700;
    }

    .import-details-list {
      margin: 8px 0 0;
      padding-left: 18px;
    }

    .job-row {
      display: grid;
      grid-template-columns:
        minmax(120px, 1.2fr)
        minmax(112px, 1fr)
        72px
        minmax(120px, 1fr)
        minmax(150px, 1.2fr)
        minmax(150px, 1.2fr);
      gap: 12px;
      align-items: center;
      width: 100%;
      padding: 11px 14px;
      border: 0;
      border-bottom: 1px solid var(--border);
      border-radius: 0;
      background: #ffffff;
      color: var(--text);
      text-align: left;
      font-weight: 400;
    }

    .job-row:hover,
    .job-row:focus {
      background: var(--panel-soft);
      outline: none;
    }

    .job-row:last-child { border-bottom: 0; }

    .job-code,
    .job-status,
    .job-priority,
    .job-worker,
    .job-error,
    .job-updated {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .job-code { font-weight: 700; }
    .job-status, .job-priority, .job-worker, .job-error, .job-updated { color: var(--muted); font-size: 13px; }
    .job-error { color: var(--danger); }

    .detail-grid {
      display: grid;
      grid-template-columns: minmax(120px, 180px) minmax(0, 1fr);
      gap: 8px 12px;
      font-size: 13px;
    }

    .detail-grid dt {
      color: var(--muted);
      font-weight: 700;
    }

    .detail-grid dd {
      margin: 0;
      min-width: 0;
      overflow-wrap: anywhere;
    }

    .log-buttons {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
    }

    .log-buttons button {
      width: auto;
      min-height: 34px;
      max-width: 100%;
      border-color: var(--border);
      background: #ffffff;
      color: var(--text);
      overflow-wrap: anywhere;
    }

    .log-buttons button:hover { background: var(--panel-soft); }

    #log-output {
      min-height: 220px;
      max-height: 520px;
      overflow: auto;
      border-radius: 8px;
      background: #111827;
      color: #f9fafb;
      padding: 12px;
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
      line-height: 1.5;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }

    .empty {
      color: var(--muted);
      padding: 12px 0;
    }

    @media (max-width: 980px) {
      .health-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .subtitle-quality-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .content-grid { grid-template-columns: minmax(0, 1fr); }
    }

    @media (max-width: 640px) {
      header {
        align-items: flex-start;
        flex-direction: column;
      }

      .health-grid { grid-template-columns: minmax(0, 1fr); }
      .subtitle-quality-grid,
      .subtitle-quality-toolbar,
      .subtitle-quality-row { grid-template-columns: minmax(0, 1fr); }

      .job-row {
        grid-template-columns: minmax(0, 1fr);
        gap: 4px;
      }

      .browser-toolbar {
        grid-template-columns: minmax(0, 1fr);
      }

      .search-controls {
        grid-template-columns: minmax(0, 1fr);
      }

      .job-code,
      .job-status,
      .job-priority,
      .job-worker,
      .job-error,
      .job-updated {
        white-space: normal;
      }

      .detail-grid { grid-template-columns: minmax(0, 1fr); }
    }
  </style>
</head>
<body>
  <header>
    <h1>JAV Subtitle Orchestrator</h1>
    <nav aria-label="Primary">
      <a href="/dashboard">Dashboard</a>
      <a href="/docs">Swagger</a>
    </nav>
  </header>
  <main>
    <section class="dashboard-tabs" role="tablist" aria-label="Dashboard sections">
      <button type="button" class="dashboard-tab active" id="dashboard-tab-operations" data-dashboard-tab="operations" role="tab" aria-selected="true" aria-controls="dashboard-view-operations">Operations</button>
      <button type="button" class="dashboard-tab" id="dashboard-tab-subtitle-quality" data-dashboard-tab="subtitle-quality" role="tab" aria-selected="false" aria-controls="dashboard-view-subtitle-quality">Subtitle Quality</button>
    </section>

    <section class="dashboard-view" id="dashboard-view-operations" role="tabpanel" aria-labelledby="dashboard-tab-operations">
      <section class="health-grid" aria-label="Health">
        <article class="health-card">
          <div class="health-title">API</div>
          <div class="health-value" id="api-status">Loading</div>
          <div class="health-meta" id="api-meta">Fetching state</div>
        </article>
        <article class="health-card">
          <div class="health-title">Mac Downloader</div>
          <div class="health-value" id="mac-download-status">Loading</div>
          <div class="health-meta" id="mac-download-meta">Fetching state</div>
        </article>
        <article class="health-card">
          <div class="health-title">Windows Transcription</div>
          <div class="health-value" id="windows-status">Loading</div>
          <div class="health-meta" id="windows-meta">Fetching state</div>
        </article>
        <article class="health-card">
          <div class="health-title">Mac Translation</div>
          <div class="health-value" id="translation-status">Loading</div>
          <div class="health-meta" id="translation-meta">Fetching state</div>
        </article>
        <article class="health-card">
          <div class="health-title">History repair</div>
          <div class="health-value" id="history-repair-status">Loading</div>
          <div class="health-meta" id="history-repair-meta">Fetching state</div>
          <div class="health-meta" id="history-repair-counts">No counts yet</div>
          <div class="health-meta" id="history-repair-current">No current repair</div>
          <div class="health-meta" id="history-repair-pause">Pause state unknown</div>
        </article>
        <article class="health-card">
          <div class="health-title">Active errors</div>
          <div class="health-value" id="errors-status">Loading</div>
          <div class="health-meta" id="errors-meta">Fetching state</div>
        </article>
        <article class="health-card">
          <div class="health-title">Audio cleanup</div>
          <div class="health-value" id="audio-cleanup-status">Loading</div>
          <div class="health-meta" id="audio-cleanup-meta">Fetching configuration</div>
        </article>
      </section>

      <section class="content-grid">
      <div class="side-stack">
        <section class="panel" aria-labelledby="single-submit-title">
          <div class="panel-header">
            <h2 id="single-submit-title">Single movie</h2>
          </div>
          <div class="panel-body">
            <form id="single-movie-form">
              <label>
                Movie number
                <input id="single-movie-number" name="movie_number" autocomplete="off" required>
              </label>
              <label>
                Priority
                <input id="single-priority" name="priority" type="number" value="100" min="0" max="9999" required>
              </label>
              <button type="submit">Submit movie</button>
              <div class="message" id="single-message" role="status"></div>
            </form>
          </div>
        </section>

        <section class="panel" aria-labelledby="batch-submit-title">
          <div class="panel-header">
            <h2 id="batch-submit-title">Batch movies</h2>
          </div>
          <div class="panel-body">
            <form id="batch-movie-form">
              <label>
                Movie numbers
                <textarea id="batch-movie-numbers" name="movie_numbers" required></textarea>
              </label>
              <label>
                Priority
                <input id="batch-priority" name="priority" type="number" value="100" min="0" max="9999" required>
              </label>
              <button type="submit">Submit batch</button>
              <div class="message" id="batch-message" role="status"></div>
            </form>
          </div>
        </section>

        <section class="panel" aria-labelledby="import-requested-title">
          <div class="panel-header">
            <h2 id="import-requested-title">Requested subtitles</h2>
          </div>
          <div class="panel-body">
            <form id="import-requested-form">
              <label>
                Minimum requests
                <input id="import-requested-min-count" name="min_count" type="number" value="1" min="1" max="9999" required>
              </label>
              <label>
                Limit
                <input id="import-requested-limit" name="limit" type="number" value="500" min="1" max="500" required>
              </label>
              <label>
                Priority
                <input id="import-requested-priority" name="priority" type="number" value="100" min="0" max="9999" required>
              </label>
              <button type="submit">Import requested subtitles</button>
              <div class="message" id="import-requested-message" role="status"></div>
            </form>
          </div>
        </section>

      </div>

      <div class="main-stack">
        <section class="panel" aria-labelledby="job-browser-title">
          <div class="panel-header">
            <h2 id="job-browser-title">Job browser</h2>
            <button type="button" id="refresh-button">Refresh</button>
          </div>
          <div class="browser-toolbar">
            <div class="view-tabs" role="tablist" aria-label="Job browser views">
              <button type="button" class="view-tab active" id="browser-view-active" data-view="active">Active</button>
              <button type="button" class="view-tab" id="browser-view-queued" data-view="queued">Queued</button>
              <button type="button" class="view-tab" id="browser-view-ready" data-view="ready">Ready</button>
              <button type="button" class="view-tab" id="browser-view-failed" data-view="failed">Failed</button>
              <button type="button" class="view-tab" id="browser-view-all" data-view="all">All</button>
            </div>
            <div class="search-controls">
              <input id="job-search" type="search" placeholder="Search movie ID" autocomplete="off" aria-label="Search movie ID">
              <select id="page-size" aria-label="Page size">
                <option value="25">25 rows</option>
                <option value="50" selected>50 rows</option>
                <option value="100">100 rows</option>
              </select>
            </div>
          </div>
          <div class="jobs-scroll">
            <div id="browser-jobs-list" class="jobs-list"></div>
          </div>
          <div class="browser-footer">
            <div id="browser-count">Showing 0 of 0</div>
            <div class="pager">
              <button type="button" id="browser-prev">Prev</button>
              <span id="browser-page">Page 1 of 1</span>
              <button type="button" id="browser-next">Next</button>
            </div>
          </div>
        </section>

        <section class="panel" aria-labelledby="latest-jobs-title">
          <div class="panel-header">
            <h2 id="latest-jobs-title">Recent activity</h2>
          </div>
          <div id="jobs-list" class="jobs-list"></div>
        </section>

        <section class="panel" aria-labelledby="job-detail-title">
          <div class="panel-header">
            <h2 id="job-detail-title">Selected job</h2>
          </div>
          <div class="panel-body">
            <div id="job-detail" class="empty">Select a job from the job browser or recent activity list.</div>
          </div>
        </section>

        <section class="panel" aria-labelledby="logs-title">
          <div class="panel-header">
            <h2 id="logs-title">Logs</h2>
          </div>
          <div class="panel-body">
            <pre id="log-output">Select a job log.</pre>
          </div>
        </section>
      </div>
    </section>
    </section>

    <section class="dashboard-view" id="dashboard-view-subtitle-quality" role="tabpanel" aria-labelledby="dashboard-tab-subtitle-quality" hidden>
      <section class="panel" aria-labelledby="subtitle-quality-title">
        <div class="panel-header">
          <h2 id="subtitle-quality-title">Subtitle Quality</h2>
          <div class="health-meta" id="subtitle-quality-latest">Loading latest scan</div>
        </div>
        <div class="subtitle-quality-grid">
          <article class="subtitle-quality-card"><span>Bad</span><strong id="subtitle-quality-bad">—</strong></article>
          <article class="subtitle-quality-card"><span>Invalid</span><strong id="subtitle-quality-invalid">—</strong></article>
          <article class="subtitle-quality-card"><span>Missing</span><strong id="subtitle-quality-missing">—</strong></article>
          <article class="subtitle-quality-card"><span>Review</span><strong id="subtitle-quality-review">—</strong></article>
          <article class="subtitle-quality-card"><span>Audited / catalog</span><strong id="subtitle-quality-progress">—</strong></article>
          <article class="subtitle-quality-card"><span>Top reasons</span><strong id="subtitle-quality-reasons">—</strong></article>
        </div>
        <div class="subtitle-quality-toolbar">
          <label>Status
            <select id="subtitle-quality-status-filter">
              <option value="">All statuses</option>
              <option value="bad">Bad</option>
              <option value="invalid">Invalid</option>
              <option value="missing">Missing</option>
              <option value="review">Review</option>
              <option value="warning">Warning</option>
              <option value="pass">Pass</option>
            </select>
          </label>
          <label>Language
            <input id="subtitle-quality-language-filter" maxlength="128" autocomplete="off" placeholder="For example English_AI">
          </label>
        </div>
        <div id="subtitle-quality-findings" aria-live="polite"></div>
        <div class="browser-footer">
          <div id="subtitle-quality-page">Page 1 of 1</div>
          <div class="pager">
            <button type="button" id="subtitle-quality-prev">Prev</button>
            <button type="button" id="subtitle-quality-next">Next</button>
          </div>
        </div>
        <p class="health-meta">
          Historical repair planning is dry-run only. Run on the Mac:
          <code>python -m orchestrator plan-historical-subtitle-repair --allowlist abc-001 --limit 1</code>
        </p>
      </section>
    </section>
  </main>

  <script>
    let selectedJobId = null;
    let browserState = {
      view: "active",
      page: 1,
      pageSize: 50,
      q: ""
    };
    let subtitleQualityState = {page: 1, pages: 1, accessiblePages: 1, pageSize: 50};
    let subtitleAuditRequestGeneration = 0;
    let subtitleAuditAbortController = null;

    function selectDashboardTab(tab) {
      const selected = tab === "subtitle-quality" ? "subtitle-quality" : "operations";
      for (const button of document.querySelectorAll(".dashboard-tab")) {
        const active = button.dataset.dashboardTab === selected;
        button.classList.toggle("active", active);
        button.setAttribute("aria-selected", active ? "true" : "false");
      }
      for (const view of document.querySelectorAll(".dashboard-view")) {
        view.hidden = view.id !== `dashboard-view-${selected}`;
      }
      window.location.hash = tab === "operations" ? "" : `#${tab}`;
    }

    function restoreDashboardTabFromHash() {
      const requested = window.location.hash.replace(/^#/, "");
      selectDashboardTab(requested === "subtitle-quality" ? requested : "operations");
    }

    async function fetchJson(url, options = {}) {
      const response = await fetch(url, {
        headers: {
          "Accept": "application/json",
          ...(options.body ? {"Content-Type": "application/json"} : {})
        },
        ...options
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        const message = typeof body.detail === "string" ? body.detail : response.statusText;
        throw new Error(message || `Request failed: ${response.status}`);
      }
      return body;
    }

    function text(value, fallback = "None") {
      return value === null || value === undefined || value === "" ? fallback : String(value);
    }

    function concise(value, limit = 120) {
      const rendered = text(value, "");
      return rendered.length > limit ? `${rendered.slice(0, limit - 3)}...` : rendered;
    }

    function formatDate(value) {
      if (!value) {
        return "No timestamp";
      }
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) {
        return value;
      }
      return date.toLocaleString();
    }

    function setHealth(id, value, meta, className) {
      const valueEl = document.getElementById(`${id}-status`);
      const metaEl = document.getElementById(`${id}-meta`);
      valueEl.textContent = value;
      valueEl.className = `health-value ${className || ""}`.trim();
      metaEl.textContent = meta;
    }

    function workerStatusClass(status) {
      return status && status !== "idle" ? "status-warn" : "status-ok";
    }

    function windowsHealthClass(worker, activity) {
      if (worker && worker.status === "offline") {
        return "status-error";
      }
      if (worker && worker.status === "stale") {
        return "status-warn";
      }
      return workerStatusClass(activity.status);
    }

    function renderHistoricalRepairs(state) {
      const progress = state.historical_repairs || {};
      const counts = progress.counts || {};
      const current = progress.current || null;
      const activity = state.activity.historical_translation || {};
      const paused = Boolean(progress.lane_paused);
      const status = paused
        ? "Paused"
        : (current ? text(activity.stage, current.stage) : "Idle");
      const statusElement = document.getElementById("history-repair-status");
      statusElement.textContent = status;
      statusElement.className = `health-value ${paused ? "status-error" : workerStatusClass(activity.status)}`;
      document.getElementById("history-repair-meta").textContent =
        `Updated ${formatDate(progress.updated_at)}`;
      document.getElementById("history-repair-counts").textContent =
        `Total ${text(counts.total, "0")}; planned ${text(counts.planned, "0")}; pending ${text(counts.pending, "0")}; running ${text(counts.running, "0")}; retry ${text(counts.retry_wait, "0")}; paused ${text(counts.paused, "0")}; succeeded ${text(counts.succeeded, "0")}; failed ${text(counts.permanent_failed, "0")}; unknown ${text(counts.unknown, "0")}`;
      document.getElementById("history-repair-current").textContent = current
        ? `${current.movie_number}; state ${current.state}; stage ${current.stage}; batch ${current.batch_id}; job ${current.job_id}; reason ${text(current.reason_code, "none")}`
        : "No current historical repair";
      document.getElementById("history-repair-pause").textContent = paused
        ? `Paused: ${text(progress.reason_code, "historical_error")}; consecutive quality failures ${text(progress.consecutive_quality_failures, "0")}`
        : `Lane active; consecutive quality failures ${text(progress.consecutive_quality_failures, "0")}`;
    }

    function renderHealth(state) {
      setHealth(
        "api",
        state.api.online ? "Online" : "Offline",
        `Server time ${formatDate(state.api.server_time)}`,
        state.api.online ? "status-ok" : "status-error"
      );

      const audioCleanup = state.audio_cleanup || {};
      setHealth(
        "audio-cleanup",
        audioCleanup.enabled ? "Enabled" : "Disabled",
        audioCleanup.enabled
          ? "Deletes WAV after verified Supabase publication"
          : "Published WAV files are being retained",
        audioCleanup.enabled ? "status-ok" : "status-warn"
      );

      const mac = state.activity.mac_download || state.activity.mac || {};
      const windowsActivity = state.activity.windows || {};
      const translationActivity = state.activity.mac_translation || state.activity.translation || {};
      const windowsWorkers = (state.workers || []).filter(
        (worker) => worker.role === "windows" || worker.role === "windows_transcriber"
      );
      const windowsWorker = windowsWorkers[0] || null;
      const translationWorker = (state.workers || []).find(
        (worker) => worker.role === "mac_translator"
      ) || null;
      const errors = state.active_errors || [];

      setHealth(
        "mac-download",
        text(mac.status, "Idle"),
        mac.movie_number ? `${mac.movie_number} updated ${formatDate(mac.updated_at)}` : "No active Mac job",
        workerStatusClass(mac.status)
      );
      setHealth(
        "windows",
        windowsWorker
          ? `${text(windowsWorker.status, "unknown")} ${text(windowsActivity.status, "idle")}`
          : text(windowsActivity.status, "Idle"),
        windowsWorker
          ? (
              windowsActivity.movie_number
                ? `${windowsActivity.movie_number} on ${windowsWorker.worker_id}; stage ${text(windowsWorker.stage, windowsActivity.status)}; seen ${formatDate(windowsWorker.last_seen_at)}`
                : `${windowsWorker.worker_id}; ${text(windowsWorker.last_ip, "no ip")}; seen ${formatDate(windowsWorker.last_seen_at)}`
            )
          : "No Windows worker heartbeat yet",
        windowsHealthClass(windowsWorker, windowsActivity)
      );
      setHealth(
        "translation",
        translationWorker
          ? `${text(translationWorker.status, "unknown")} ${text(translationActivity.status, "idle")}`
          : text(translationActivity.status, "Idle"),
        translationActivity.movie_number
          ? `${translationActivity.movie_number} on ${text(translationActivity.worker_id, "Mac")}; updated ${formatDate(translationActivity.updated_at)}`
          : "No active Mac translation job",
        windowsHealthClass(translationWorker, translationActivity)
      );
      renderHistoricalRepairs(state);
      setHealth(
        "errors",
        String(errors.length),
        errors.length ? errors.map((job) => job.movie_number).slice(0, 3).join(", ") : "No active errors",
        errors.length ? "status-error" : "status-ok"
      );
    }

    function renderSubtitleQualitySummary(summary) {
      const counts = summary.status_counts || {};
      for (const status of ["bad", "invalid", "missing", "review"]) {
        document.getElementById(`subtitle-quality-${status}`).textContent = text(counts[status], "0");
      }
      const percent = Math.round(Number(summary.progress_ratio || 0) * 100);
      document.getElementById("subtitle-quality-progress").textContent = `${summary.total_audited} / ${summary.catalog_total} (${percent}%)`;
      document.getElementById("subtitle-quality-latest").textContent = `Latest ${formatDate(summary.latest_scanned_at)}`;
      const reasons = Object.entries(summary.reason_counts || {})
        .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
        .slice(0, 3)
        .map(([reason, count]) => `${reason} ${count}`);
      document.getElementById("subtitle-quality-reasons").textContent = reasons.length ? reasons.join(", ") : "None";
    }

    function renderSubtitleQualityFindings(payload) {
      const list = document.getElementById("subtitle-quality-findings");
      list.replaceChildren();
      if (!(payload.items || []).length) {
        const empty = document.createElement("div");
        empty.className = "empty panel-body";
        empty.textContent = "No subtitle findings match these filters.";
        list.append(empty);
      }
      for (const item of payload.items || []) {
        const subtitleRow = document.createElement("div");
        subtitleRow.className = "subtitle-quality-row";
        subtitleRow.replaceChildren();
        const subtitleCode = document.createElement("a");
        const subtitleLanguage = document.createElement("span");
        const subtitleStatus = document.createElement("span");
        const subtitleMetrics = document.createElement("span");
        const subtitleReasons = document.createElement("span");
        subtitleCode.href = `/subtitle-audits/${encodeURIComponent(item.subtitle_id)}`;
        subtitleCode.textContent = text(item.canonical_code, item.subtitle_id);
        subtitleLanguage.textContent = text(item.language);
        subtitleStatus.textContent = text(item.status);
        const cueCount = item.metrics && item.metrics.cue_count;
        const coverage = item.metrics && item.metrics.coverage_ratio;
        subtitleMetrics.textContent = `cues ${text(cueCount, "—")}; coverage ${coverage === null || coverage === undefined ? "—" : `${Math.round(Number(coverage) * 100)}%`}`;
        subtitleReasons.textContent = (item.reason_codes || []).join(", ") || "No reasons";
        subtitleRow.append(subtitleCode, subtitleLanguage, subtitleStatus, subtitleMetrics, subtitleReasons);
        list.append(subtitleRow);
      }
      subtitleQualityState.page = payload.page;
      subtitleQualityState.pages = payload.pages;
      subtitleQualityState.accessiblePages = payload.accessible_pages;
      const limitNote = payload.accessible_pages < payload.pages
        ? `; API offset limit exposes ${payload.accessible_pages} pages`
        : "";
      document.getElementById("subtitle-quality-page").textContent = `Page ${payload.page} of ${payload.pages}; ${payload.total} findings${limitNote}`;
      document.getElementById("subtitle-quality-prev").disabled = payload.page <= 1;
      document.getElementById("subtitle-quality-next").disabled = payload.page >= payload.accessible_pages;
    }

    function renderSubtitleQualityUnavailable() {
      for (const status of ["bad", "invalid", "missing", "review"]) {
        document.getElementById(`subtitle-quality-${status}`).textContent = "Unavailable";
      }
      document.getElementById("subtitle-quality-progress").textContent = "Unavailable";
      document.getElementById("subtitle-quality-latest").textContent = "Audit data unavailable";
      document.getElementById("subtitle-quality-reasons").textContent = "Unavailable";
      const list = document.getElementById("subtitle-quality-findings");
      list.replaceChildren();
      const unavailable = document.createElement("div");
      unavailable.className = "empty panel-body";
      unavailable.textContent = "Subtitle audit visibility is unavailable.";
      list.append(unavailable);
    }

    async function refreshSubtitleQuality() {
      const generation = ++subtitleAuditRequestGeneration;
      if (subtitleAuditAbortController) {
        subtitleAuditAbortController.abort();
      }
      const controller = new AbortController();
      subtitleAuditAbortController = controller;
      const params = new URLSearchParams({
        page: String(subtitleQualityState.page),
        page_size: String(subtitleQualityState.pageSize)
      });
      const status = document.getElementById("subtitle-quality-status-filter").value;
      const language = document.getElementById("subtitle-quality-language-filter").value.trim();
      if (status) params.set("status", status);
      if (language) params.set("language", language);
      try {
        const [summary, findings] = await Promise.all([
          fetchJson("/subtitle-audits/summary", {signal: controller.signal}),
          fetchJson(`/subtitle-audits?${params.toString()}`, {signal: controller.signal})
        ]);
        if (generation !== subtitleAuditRequestGeneration) return;
        renderSubtitleQualitySummary(summary);
        if (generation !== subtitleAuditRequestGeneration) return;
        renderSubtitleQualityFindings(findings);
      } catch (error) {
        if (generation !== subtitleAuditRequestGeneration) return;
        if (error.name === "AbortError") return;
        renderSubtitleQualityUnavailable(error);
      } finally {
        if (
          generation === subtitleAuditRequestGeneration
          && subtitleAuditAbortController === controller
        ) {
          subtitleAuditAbortController = null;
        }
      }
    }

    function renderJobRows(list, jobs, emptyText) {
      list.replaceChildren();

      if (!jobs.length) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = emptyText;
        list.append(empty);
        return;
      }

      for (const job of jobs) {
        const row = document.createElement("button");
        row.type = "button";
        row.className = "job-row";
        row.dataset.jobId = job.id;
        const code = document.createElement("span");
        const status = document.createElement("span");
        const priority = document.createElement("span");
        const worker = document.createElement("span");
        const error = document.createElement("span");
        const updated = document.createElement("span");
        code.className = "job-code";
        status.className = "job-status";
        priority.className = "job-priority";
        worker.className = "job-worker";
        error.className = "job-error";
        updated.className = "job-updated";
        row.append(code, status, priority, worker, error, updated);
        row.querySelector(".job-code").textContent = job.movie_number;
        row.querySelector(".job-status").textContent = job.status;
        row.querySelector(".job-priority").textContent = `P${job.priority}`;
        row.querySelector(".job-worker").textContent = job.claimed_by ? `Claimed ${job.claimed_by}` : "";
        row.querySelector(".job-error").textContent = job.error ? `Error ${concise(job.error)}` : "";
        row.querySelector(".job-updated").textContent = formatDate(job.updated_at);
        row.addEventListener("click", () => selectJob(job.id));
        list.append(row);
      }
    }

    function renderJobs(jobs) {
      renderJobRows(document.getElementById("jobs-list"), jobs, "No recent activity.");
    }

    function setActiveBrowserTab(view) {
      for (const tab of document.querySelectorAll(".view-tab")) {
        tab.classList.toggle("active", tab.dataset.view === view);
      }
    }

    function renderBrowser(payload) {
      const list = document.getElementById("browser-jobs-list");
      renderJobRows(list, payload.items || [], "No jobs match this view.");
      const start = payload.total === 0 ? 0 : ((payload.page - 1) * payload.page_size) + 1;
      const end = payload.total === 0 ? 0 : start + (payload.items || []).length - 1;
      document.getElementById("browser-count").textContent = `Showing ${start}-${end} of ${payload.total}`;
      document.getElementById("browser-page").textContent = `Page ${payload.page} of ${payload.pages}`;
      document.getElementById("browser-prev").disabled = payload.page <= 1;
      document.getElementById("browser-next").disabled = payload.page >= payload.pages;
      setActiveBrowserTab(payload.view);
      browserState.view = payload.view;
      browserState.page = payload.page;
      browserState.pageSize = payload.page_size;
    }

    async function refreshBrowser() {
      const params = new URLSearchParams({
        view: browserState.view,
        page: String(browserState.page),
        page_size: String(browserState.pageSize),
        q: browserState.q
      });
      const payload = await fetchJson(`/jobs/browser?${params.toString()}`);
      renderBrowser(payload);
    }

    function detailRows(detail) {
      const fields = [
        ["Job ID", detail.id],
        ["Original movie", detail.movie_number],
        ["Normalized movie", detail.normalized_movie_number],
        ["Status", detail.status],
        ["Priority", detail.priority],
        ["Requests", detail.request_count],
        ["Latest request action", detail.latest_request_action],
        ["Failure class", detail.failure_class],
        ["Failure stage", detail.failure_stage],
        ["Next attempt", formatDate(detail.next_attempt_at)],
        ["Retry eligible", formatDate(detail.retry_eligible_at)],
        ["Retry cycle", detail.retry_cycle],
        ["Mac attempts", detail.attempt_count],
        ["Worker attempts", detail.worker_attempt_count],
        ["Translation attempts", detail.translation_attempt_count],
        ["Publish attempts", detail.publish_attempt_count],
        ["Next publish attempt", formatDate(detail.next_publish_attempt_at)],
        ["Artifact status", detail.artifact_status],
        ["Catalog sync status", detail.catalog_sync_status],
        ["Catalog warning code", detail.catalog_sync_warning_code],
        ["Catalog warning", detail.catalog_sync_warning_message],
        ["Catalog sync attempts", detail.catalog_sync_attempt_count],
        ["Next catalog sync attempt", formatDate(detail.next_catalog_sync_attempt_at)],
        ["Catalog last HTTP status", detail.catalog_sync_last_http_status],
        ["Catalog last response", detail.catalog_sync_last_response_json],
        ["Catalog last attempt", formatDate(detail.catalog_sync_last_attempt_at)],
        ["Catalog movie UUID", detail.catalog_movie_uuid],
        ["Metadata status", detail.metadata_status],
        ["Metadata source", detail.metadata_source],
        ["Claimed by", detail.claimed_by],
        ["Lease expires", formatDate(detail.lease_expires_at)],
        ["Created", formatDate(detail.created_at)],
        ["Updated", formatDate(detail.updated_at)],
        ["Job dir Mac", detail.job_dir_mac],
        ["Job dir Windows", detail.job_dir_windows],
        ["Metadata Mac", detail.metadata_path_mac],
        ["Audio Mac", detail.audio_path_mac],
        ["Audio Windows", detail.audio_path_windows],
        ["Japanese SRT Mac", detail.japanese_srt_path_mac],
        ["Japanese SRT Windows", detail.japanese_srt_path_windows],
        ["English SRT Mac", detail.english_srt_path_mac],
        ["English SRT Windows", detail.english_srt_path_windows],
        ["Error", detail.error]
      ];
      return fields.map(([name, value]) => `<dt>${name}</dt><dd>${escapeHtml(text(value))}</dd>`).join("");
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    async function selectJob(jobId) {
      selectedJobId = jobId;
      const detailEl = document.getElementById("job-detail");
      const logOutput = document.getElementById("log-output");
      detailEl.textContent = "Loading job detail...";
      logOutput.textContent = "Loading logs...";

      try {
        const [detail, logsResponse] = await Promise.all([
          fetchJson(`/jobs/${jobId}/detail`),
          fetchJson(`/jobs/${jobId}/logs`)
        ]);
        detailEl.className = "";
        detailEl.innerHTML = `<dl class="detail-grid">${detailRows(detail)}</dl><div class="log-buttons"></div>`;
        const logButtons = detailEl.querySelector(".log-buttons");
        if (!logsResponse.logs.length) {
          logButtons.textContent = "No logs available.";
          logOutput.textContent = "No logs available.";
          return;
        }
        for (const log of logsResponse.logs) {
          const button = document.createElement("button");
          button.type = "button";
          button.textContent = `${log.name} (${log.size_bytes} bytes)`;
          button.addEventListener("click", () => loadLog(jobId, log.name));
          logButtons.append(button);
        }
        await loadLog(jobId, logsResponse.logs[0].name);
      } catch (error) {
        detailEl.className = "empty";
        detailEl.textContent = error.message;
        logOutput.textContent = "";
      }
    }

    async function loadLog(jobId, logName) {
      const logOutput = document.getElementById("log-output");
      logOutput.textContent = `Loading ${logName}...`;
      try {
        const payload = await fetchJson(`/jobs/${jobId}/logs/${encodeURIComponent(logName)}?tail=200`);
        logOutput.textContent = payload.lines.length ? payload.lines.join("\\n") : `${logName} is empty.`;
      } catch (error) {
        logOutput.textContent = error.message;
      }
    }

    async function refreshState() {
      const list = document.getElementById("jobs-list");
      const browserList = document.getElementById("browser-jobs-list");
      try {
        const [state] = await Promise.all([
          fetchJson("/dashboard/state"),
          refreshBrowser()
        ]);
        renderHealth(state);
        renderJobs(state.latest_jobs || []);
      } catch (error) {
        list.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
        browserList.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
        setHealth("api", "Error", error.message, "status-error");
      }
    }

    async function submitSingle(event) {
      event.preventDefault();
      const form = event.currentTarget;
      const message = document.getElementById("single-message");
      const movie = document.getElementById("single-movie-number").value.trim();
      const priority = Number(document.getElementById("single-priority").value);
      message.textContent = "Submitting...";
      try {
        const result = await fetchJson("/jobs", {
          method: "POST",
          body: JSON.stringify({movie_number: movie, priority, force: false})
        });
        message.textContent = `Submitted ${result.movie_number}`;
        form.reset();
        document.getElementById("single-priority").value = "100";
        await refreshState();
      } catch (error) {
        message.textContent = error.message;
      }
    }

    async function submitBatch(event) {
      event.preventDefault();
      const form = event.currentTarget;
      const message = document.getElementById("batch-message");
      const movies = document.getElementById("batch-movie-numbers").value
        .split(/[\\s,]+/)
        .map((item) => item.trim())
        .filter(Boolean);
      const priority = Number(document.getElementById("batch-priority").value);
      message.textContent = "Submitting...";
      try {
        const result = await fetchJson("/jobs/batch", {
          method: "POST",
          body: JSON.stringify({movie_numbers: movies, priority, force: false})
        });
        message.textContent = `Created ${result.created.length}, existing ${result.existing.length}, invalid ${result.invalid.length}`;
        form.reset();
        document.getElementById("batch-priority").value = "100";
        await refreshState();
      } catch (error) {
        message.textContent = error.message;
      }
    }

    function importRequestRange(requested) {
      const counts = (requested || []).map((item) => Number(item.request_count || 0));
      if (!counts.length) return "no request counts";
      const min = Math.min(...counts);
      const max = Math.max(...counts);
      return min === max ? `request count ${min}` : `request counts ${min}-${max}`;
    }

    async function importRequestedSubtitles(event) {
      event.preventDefault();
      const message = document.getElementById("import-requested-message");
      const minCount = Number(document.getElementById("import-requested-min-count").value || "1");
      const limit = Number(document.getElementById("import-requested-limit").value || "500");
      const priority = Number(document.getElementById("import-requested-priority").value || "100");
      message.textContent = "Importing requested subtitles...";
      try {
        const result = await fetchJson("/jobs/import-subtitle-requests", {
          method: "POST",
          body: JSON.stringify({
            min_count: minCount,
            limit,
            priority,
            force: false
          })
        });
        message.textContent = `Requested ${result.requested.length} (${importRequestRange(result.requested)}), imported ${result.imported.length}, skipped available ${result.skipped_available.length}, created ${result.created.length}, existing ${result.existing.length}, invalid ${result.invalid.length}`;
        await refreshState();
      } catch (error) {
        message.textContent = error.message;
      }
    }

    function selectBrowserView(view) {
      browserState.view = view;
      browserState.page = 1;
      refreshState();
    }

    function updateBrowserSearch() {
      browserState.q = document.getElementById("job-search").value.trim();
      browserState.page = 1;
      refreshState();
    }

    document.getElementById("refresh-button").addEventListener("click", refreshState);
    for (const tab of document.querySelectorAll(".view-tab")) {
      tab.addEventListener("click", () => selectBrowserView(tab.dataset.view));
    }
    document.getElementById("job-search").addEventListener("change", updateBrowserSearch);
    document.getElementById("job-search").addEventListener("search", updateBrowserSearch);
    document.getElementById("page-size").addEventListener("change", (event) => {
      browserState.pageSize = Number(event.currentTarget.value);
      browserState.page = 1;
      refreshState();
    });
    document.getElementById("browser-prev").addEventListener("click", () => {
      browserState.page = Math.max(browserState.page - 1, 1);
      refreshState();
    });
    document.getElementById("browser-next").addEventListener("click", () => {
      browserState.page += 1;
      refreshState();
    });
    document.getElementById("single-movie-form").addEventListener("submit", submitSingle);
    document.getElementById("batch-movie-form").addEventListener("submit", submitBatch);
    document.getElementById("import-requested-form").addEventListener("submit", importRequestedSubtitles);
    for (const tab of document.querySelectorAll(".dashboard-tab")) {
      tab.addEventListener("click", () => selectDashboardTab(tab.dataset.dashboardTab));
    }
    window.addEventListener("hashchange", restoreDashboardTabFromHash);
    document.getElementById("subtitle-quality-status-filter").addEventListener("change", () => {
      subtitleQualityState.page = 1;
      refreshSubtitleQuality();
    });
    document.getElementById("subtitle-quality-language-filter").addEventListener("change", () => {
      subtitleQualityState.page = 1;
      refreshSubtitleQuality();
    });
    document.getElementById("subtitle-quality-prev").addEventListener("click", () => {
      subtitleQualityState.page = Math.max(1, subtitleQualityState.page - 1);
      refreshSubtitleQuality();
    });
    document.getElementById("subtitle-quality-next").addEventListener("click", () => {
      subtitleQualityState.page = Math.min(subtitleQualityState.accessiblePages, subtitleQualityState.page + 1);
      refreshSubtitleQuality();
    });
    window.addEventListener("load", () => {
      restoreDashboardTabFromHash();
      refreshState();
      refreshSubtitleQuality();
    });
  </script>
</body>
</html>
"""
