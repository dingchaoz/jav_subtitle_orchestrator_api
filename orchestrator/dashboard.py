from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

from orchestrator.models import (
    DashboardJobSummary,
    DashboardStateResponse,
    JobDetailResponse,
    JobStatus,
)
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
