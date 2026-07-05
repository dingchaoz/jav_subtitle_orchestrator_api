import sqlite3
from contextlib import closing

from orchestrator.dashboard import build_dashboard_state, build_job_detail
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def set_job_recency(sqlite_path, job_id, *, created_at, updated_at):
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE jobs SET created_at = ?, updated_at = ? WHERE id = ?",
            (created_at, updated_at, job_id),
        )
        conn.commit()


def test_build_dashboard_state_counts_latest_jobs_and_active_errors(
    sqlite_path,
    mac_jobs_root,
):
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
    set_job_recency(
        sqlite_path,
        queued.id,
        created_at="2026-07-05T11:00:01+00:00",
        updated_at="2026-07-05T12:00:01+00:00",
    )
    set_job_recency(
        sqlite_path,
        failed.id,
        created_at="2026-07-05T11:00:02+00:00",
        updated_at="2026-07-05T12:00:02+00:00",
    )
    set_job_recency(
        sqlite_path,
        ready.id,
        created_at="2026-07-05T11:00:03+00:00",
        updated_at="2026-07-05T12:00:03+00:00",
    )

    state = build_dashboard_state(store)

    assert state.api["online"] is True
    assert state.api["jobs_root_mac"] == str(mac_jobs_root)
    assert state.api["jobs_root_windows"] == "M:\\"
    assert state.counts["queued"] == 1
    assert state.counts["audio_ready"] == 1
    assert state.counts["failed"] == 1
    assert [job.movie_number for job in state.latest_jobs] == [
        "ktb-094",
        "ktb-095",
        "ktb-096",
    ]
    assert state.active_errors[0].movie_number == "ktb-095"
    assert (
        state.active_errors[0].error
        == "metadata failed: Movie not found in MissAV catalog"
    )


def test_build_dashboard_state_derives_worker_activity(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    downloading = store.submit_job("ktb-096", priority=100, force=False).job
    transcribing = store.submit_job("ktb-095", priority=100, force=False).job

    store.update_download_status(downloading.id, JobStatus.DOWNLOADING_AUDIO)
    store.mark_audio_ready(transcribing.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    store.heartbeat(
        claimed.id,
        "windows-gpu-1",
        JobStatus.TRANSCRIBING,
        lease_seconds=1800,
    )

    state = build_dashboard_state(store)

    assert state.activity["mac"]["status"] == "downloading_audio"
    assert state.activity["mac"]["movie_number"] == "ktb-096"
    assert state.activity["windows"]["status"] == "transcribing"
    assert state.activity["windows"]["movie_number"] == "ktb-095"
    assert state.activity["windows"]["worker_id"] == "windows-gpu-1"


def test_build_dashboard_state_uses_deterministic_recency_for_same_second_ties(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    failed_old = store.submit_job("ktb-101", priority=10, force=False).job
    failed_new = store.submit_job("ktb-102", priority=90, force=False).job
    mac_old = store.submit_job("ktb-103", priority=10, force=False).job
    mac_new = store.submit_job("ktb-104", priority=90, force=False).job
    windows_old = store.submit_job("ktb-105", priority=10, force=False).job
    windows_new = store.submit_job("ktb-106", priority=90, force=False).job

    store.record_download_failure(
        failed_old.id,
        JobStatus.FAILED,
        attempt_count=3,
        error="old failure",
    )
    store.record_download_failure(
        failed_new.id,
        JobStatus.FAILED,
        attempt_count=3,
        error="new failure",
    )
    store.update_download_status(mac_old.id, JobStatus.DOWNLOADING_AUDIO)
    store.update_download_status(mac_new.id, JobStatus.DOWNLOADING_AUDIO)
    store.mark_audio_ready(windows_old.id)
    store.mark_audio_ready(windows_new.id)
    claimed_old = store.claim_next_worker_job("windows-old", lease_seconds=1800)
    claimed_new = store.claim_next_worker_job("windows-new", lease_seconds=1800)
    store.heartbeat(
        claimed_old.id,
        "windows-old",
        JobStatus.TRANSCRIBING,
        lease_seconds=1800,
    )
    store.heartbeat(
        claimed_new.id,
        "windows-new",
        JobStatus.TRANSCRIBING,
        lease_seconds=1800,
    )

    same_updated_at = "2026-07-05T12:00:00+00:00"
    set_job_recency(
        sqlite_path,
        failed_old.id,
        created_at="2026-07-05T11:00:01+00:00",
        updated_at=same_updated_at,
    )
    set_job_recency(
        sqlite_path,
        failed_new.id,
        created_at="2026-07-05T11:00:02+00:00",
        updated_at=same_updated_at,
    )
    set_job_recency(
        sqlite_path,
        mac_old.id,
        created_at="2026-07-05T11:00:03+00:00",
        updated_at=same_updated_at,
    )
    set_job_recency(
        sqlite_path,
        mac_new.id,
        created_at="2026-07-05T11:00:04+00:00",
        updated_at=same_updated_at,
    )
    set_job_recency(
        sqlite_path,
        windows_old.id,
        created_at="2026-07-05T11:00:05+00:00",
        updated_at=same_updated_at,
    )
    set_job_recency(
        sqlite_path,
        windows_new.id,
        created_at="2026-07-05T11:00:06+00:00",
        updated_at=same_updated_at,
    )

    state = build_dashboard_state(store)

    assert [job.movie_number for job in state.latest_jobs] == [
        "ktb-106",
        "ktb-105",
        "ktb-104",
        "ktb-103",
        "ktb-102",
        "ktb-101",
    ]
    assert [job.movie_number for job in state.active_errors] == ["ktb-102", "ktb-101"]
    assert state.activity["mac"]["movie_number"] == "ktb-104"
    assert state.activity["windows"]["movie_number"] == "ktb-106"
    assert state.activity["windows"]["worker_id"] == "windows-new"


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
    assert detail.lease_expires_at is None
    assert detail.created_at == ready.created_at
    assert detail.updated_at == ready.updated_at
    assert detail.error is None
    assert detail.job_dir_mac == str(mac_jobs_root / "ktb-112")
    assert detail.job_dir_windows == "M:\\ktb-112"
    assert detail.metadata_path_mac == str(mac_jobs_root / "ktb-112" / "metadata.json")
    assert detail.audio_path_mac == str(mac_jobs_root / "ktb-112" / "audio.wav")
    assert detail.audio_path_windows == "M:\\ktb-112\\audio.wav"
    assert detail.japanese_srt_path_mac is None
    assert detail.japanese_srt_path_windows is None
    assert detail.english_srt_path_mac is None
    assert detail.english_srt_path_windows is None
