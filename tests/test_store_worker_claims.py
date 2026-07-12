import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def test_claim_next_download_job_is_atomic_and_ordered(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    slow = store.submit_job("ktb-096", priority=100, force=False).job
    fast = store.submit_job("ktb-095", priority=10, force=False).job

    claimed = store.claim_next_download_job()
    second_claim = store.claim_next_download_job()

    assert claimed.id == fast.id
    assert claimed.status == JobStatus.DOWNLOADING_METADATA
    assert second_claim.id == slow.id
    assert second_claim.status == JobStatus.DOWNLOADING_METADATA


def test_claim_next_download_job_removes_claimed_job_from_queue(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job

    claimed = store.claim_next_download_job()
    second_claim = store.claim_next_download_job()

    assert claimed.id == job.id
    assert store.list_jobs(JobStatus.QUEUED) == []
    assert second_claim is None


def test_claim_next_audio_ready_job_is_atomic_and_ordered(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    slow = store.submit_job("ktb-096", priority=100, force=False).job
    fast = store.submit_job("ktb-095", priority=10, force=False).job
    store.mark_audio_ready(slow.id)
    store.mark_audio_ready(fast.id)

    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    second_claim = store.claim_next_worker_job("windows-gpu-2", lease_seconds=1800)

    assert claimed.id == fast.id
    assert claimed.status == JobStatus.TRANSCRIPTION_CLAIMED
    assert claimed.claimed_by == "windows-gpu-1"
    assert second_claim.id == slow.id


def test_claim_next_worker_job_returns_none_quickly_when_no_audio_ready_and_db_is_write_locked(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    lock_conn = sqlite3.connect(sqlite_path)
    try:
        lock_conn.execute("PRAGMA journal_mode = WAL")
        lock_conn.execute("BEGIN IMMEDIATE")
        started = time.perf_counter()

        claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)

        elapsed = time.perf_counter() - started
        assert claimed is None
        assert elapsed < 1.0
    finally:
        lock_conn.rollback()
        lock_conn.close()


def test_heartbeat_extends_lease_and_updates_stage(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=60)

    updated = store.heartbeat(
        claimed.id,
        "windows-gpu-1",
        JobStatus.TRANSCRIBING,
        lease_seconds=1800,
    )

    assert updated.status == JobStatus.TRANSCRIBING
    assert updated.claimed_by == "windows-gpu-1"
    assert updated.lease_expires_at > claimed.lease_expires_at


def test_worker_complete_requires_final_files(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=60)

    completed = store.complete_worker_job(
        claimed.id,
        "windows-gpu-1",
        japanese_srt_path_windows="M:\\ktb-096\\ktb-096.Japanese.srt",
        english_srt_path_windows="M:\\ktb-096\\ktb-096.English.srt",
        final_file_exists=lambda path: path.endswith(".English.srt"),
    )

    assert completed.status == JobStatus.ENGLISH_SRT_READY
    assert completed.japanese_srt_path_windows == "M:\\ktb-096\\ktb-096.Japanese.srt"
    assert completed.english_srt_path_windows == "M:\\ktb-096\\ktb-096.English.srt"
    assert completed.claimed_by is None
    assert completed.lease_expires_at is None


def test_expired_worker_lease_returns_job_to_audio_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1)
    expired = (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    store.force_lease_expiry_for_test(claimed.id, expired)

    recovered = store.recover_expired_worker_leases(max_worker_attempts=3)

    assert recovered == 1
    refreshed = store.get_job(claimed.id)
    assert refreshed.status == JobStatus.AUDIO_READY
    assert refreshed.worker_attempt_count == 1
    assert refreshed.claimed_by is None


def test_expired_transcription_done_worker_lease_returns_job_to_audio_ready(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1)
    heartbeat = store.heartbeat(
        claimed.id,
        "windows-gpu-1",
        JobStatus.TRANSCRIPTION_DONE,
        lease_seconds=1,
    )
    expired = (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    store.force_lease_expiry_for_test(heartbeat.id, expired)

    recovered = store.recover_expired_worker_leases(max_worker_attempts=3)

    assert recovered == 1
    refreshed = store.get_job(heartbeat.id)
    assert refreshed.status == JobStatus.AUDIO_READY
    assert refreshed.worker_attempt_count == 1
    assert refreshed.claimed_by is None


def test_complete_worker_transcription_records_japanese_and_releases_lease(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=60)
    japanese = mac_jobs_root / "ktb-096" / "ktb-096.Japanese.srt"
    japanese.parent.mkdir(parents=True, exist_ok=True)
    japanese.write_text("valid", encoding="utf-8")

    completed = store.complete_worker_transcription(
        claimed.id,
        "windows-gpu-1",
        "M:\\ktb-096\\ktb-096.Japanese.srt",
        lambda path: Path(path).exists(),
    )

    assert completed.status == JobStatus.TRANSCRIPTION_DONE
    assert completed.claimed_by is None
    assert completed.lease_expires_at is None
    assert completed.japanese_srt_path_mac == str(japanese)
    assert completed.japanese_srt_path_windows == "M:\\ktb-096\\ktb-096.Japanese.srt"
