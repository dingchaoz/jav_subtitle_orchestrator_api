from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from math import ceil
from pathlib import Path

from orchestrator.models import (
    DashboardJobSummary,
    DashboardStateResponse,
    JobBrowserItem,
    JobBrowserResponse,
    JobDetailResponse,
    JobLogSummary,
    JobLogTailResponse,
    JobLogsResponse,
    JobStatus,
    WorkerHealthSummary,
)
from orchestrator.store import JobRecord, JobStore, WorkerStatusRecord


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
}

IN_PROGRESS_BROWSER_STATUSES = ACTIVE_BROWSER_STATUSES - {JobStatus.QUEUED}
JOB_BROWSER_VIEWS = {"active", "queued", "ready", "failed", "all"}


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


def _latest_active_job(jobs: list[JobRecord], statuses: set[JobStatus]) -> JobRecord | None:
    candidates = [job for job in jobs if job.status in statuses]
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
) -> dict[str, str | None]:
    matching = [worker for worker in workers if worker.role == role]
    processing = [worker for worker in matching if worker.state == "processing"]
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


def build_dashboard_state(store: JobStore, *, latest_limit: int = 50) -> DashboardStateResponse:
    jobs = store.list_jobs()
    counts = Counter(job.status.value for job in jobs)
    latest = sorted(jobs, key=dashboard_recency_key, reverse=True)[:latest_limit]
    errors = [
        job
        for job in sorted(jobs, key=dashboard_recency_key, reverse=True)
        if job_has_active_error(job)
    ]
    mac_job = _latest_active_job(jobs, MAC_ACTIVE_STATUSES)
    processing_job = _latest_active_job(jobs, SUBTITLE_PROCESSING_STATUSES)
    windows_job = _latest_active_job(jobs, WINDOWS_TRANSCRIPTION_STATUSES)
    translation_job = _latest_active_job(jobs, MAC_TRANSLATION_STATUSES)
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
                translation_job, workers, "mac_translator"
            ),
        },
        counts=dict(counts),
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


