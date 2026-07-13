import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore


def _prepare_translation_job(store, root: Path, movie: str):
    job = store.submit_job(movie, priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-test", lease_seconds=60)
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    paths.japanese_srt_path_mac.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nsource\n",
        encoding="utf-8",
    )
    return store.complete_worker_transcription(
        claimed.id,
        "windows-gpu-test",
        paths.japanese_srt_path_windows,
        lambda path: Path(path).exists(),
    )


def _write_historical_files(root: Path, movie: str):
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    japanese = b"1\n00:00:00,000 --> 00:00:01,000\nsource\n"
    english = b"1\n00:00:00,000 --> 00:00:01,000\nCannot translate\n"
    paths.audio_path_mac.write_bytes(b"synthetic-audio")
    paths.japanese_srt_path_mac.write_bytes(japanese)
    paths.english_srt_path_mac.write_bytes(english)
    return paths, japanese, english


def _prepare_publication_job(store, root: Path, movie: str, *, worker_id="mac-quality"):
    transcription = _prepare_translation_job(store, root, movie)
    claimed = store.claim_translation_job(transcription.id, worker_id, lease_seconds=60)
    paths = build_job_paths(movie, root, "M:\\")
    paths.english_srt_path_mac.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\ntranslated\n",
        encoding="utf-8",
    )
    return store.complete_mac_translation_quality(
        claimed.id,
        worker_id,
        lambda path: Path(path).exists(),
    )


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


def test_historical_reset_preserves_windows_attempts_and_paths(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("abc-001", priority=100, force=False).job
    paths, japanese, english = _write_historical_files(mac_jobs_root, "abc-001")
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, worker_attempt_count = 2, "
            "translation_attempt_count = 2, publish_attempt_count = 2, "
            "next_publish_attempt_at = ?, catalog_movie_uuid = ?, "
            "metadata_status = ?, metadata_source = ?, audio_path_mac = ?, "
            "japanese_srt_path_mac = ?, english_srt_path_mac = ? WHERE id = ?",
            (
                JobStatus.ENGLISH_SRT_READY.value,
                "2026-07-12T12:00:00+00:00",
                "f1bd9932-5697-4f16-865a-c56edc73d491",
                "complete",
                "public",
                str(paths.audio_path_mac),
                str(paths.japanese_srt_path_mac),
                str(paths.english_srt_path_mac),
                job.id,
            ),
        )

    reset = store.prepare_historical_translation_repair(
        job.id, expected_status=JobStatus.ENGLISH_SRT_READY
    )

    assert reset.status is JobStatus.TRANSCRIPTION_DONE
    assert reset.worker_attempt_count == 2
    assert reset.translation_attempt_count == 0
    assert reset.publish_attempt_count == 0
    assert reset.next_publish_attempt_at is None
    assert reset.catalog_movie_uuid is None
    assert reset.metadata_status is None
    assert reset.metadata_source is None
    assert reset.audio_path_mac == str(paths.audio_path_mac)
    assert reset.japanese_srt_path_mac == str(paths.japanese_srt_path_mac)
    assert reset.english_srt_path_mac is None
    assert paths.audio_path_mac.read_bytes() == b"synthetic-audio"
    assert paths.japanese_srt_path_mac.read_bytes() == japanese
    assert paths.english_srt_path_mac.read_bytes() == english


def test_prepare_catalog_publication_preserves_translation_and_artifacts(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("abc-021", priority=100, force=False).job
    paths = build_job_paths("abc-021", mac_jobs_root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    audio_before = b"publication-repair-audio"
    japanese_before = b"1\n00:00:00,000 --> 00:00:01,000\nsource\n"
    english_before = b"1\n00:00:00,000 --> 00:00:01,000\ntranslated\n"
    paths.audio_path_mac.write_bytes(audio_before)
    paths.japanese_srt_path_mac.write_bytes(japanese_before)
    paths.english_srt_path_mac.write_bytes(english_before)
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, attempt_count = 5, "
            "worker_attempt_count = 2, translation_attempt_count = 3, "
            "publish_attempt_count = 4, next_publish_attempt_at = ?, "
            "catalog_movie_uuid = ?, metadata_status = ?, metadata_source = ?, "
            "error = ?, audio_path_mac = ?, audio_path_windows = ?, "
            "japanese_srt_path_mac = ?, japanese_srt_path_windows = ?, "
            "english_srt_path_mac = ?, english_srt_path_windows = ? WHERE id = ?",
            (
                JobStatus.FAILED.value,
                "2026-07-12T12:00:00+00:00",
                "f1bd9932-5697-4f16-865a-c56edc73d491",
                "stale",
                "stale",
                "publishing: catalog lookup failed",
                str(paths.audio_path_mac),
                paths.audio_path_windows,
                str(paths.japanese_srt_path_mac),
                paths.japanese_srt_path_windows,
                "/tmp/stale.English.srt",
                "M:\\stale\\stale.English.srt",
                job.id,
            ),
        )

    prepared = store.prepare_catalog_publication_repair(
        job.id,
        expected_status=JobStatus.FAILED,
        expected_movie="abc-021",
    )

    assert prepared.status is JobStatus.PUBLISH_PENDING
    assert prepared.attempt_count == 5
    assert prepared.worker_attempt_count == 2
    assert prepared.translation_attempt_count == 3
    assert prepared.publish_attempt_count == 0
    assert prepared.next_publish_attempt_at is None
    assert prepared.catalog_movie_uuid is None
    assert prepared.metadata_status is None
    assert prepared.metadata_source is None
    assert prepared.claimed_by is None
    assert prepared.lease_expires_at is None
    assert prepared.error is None
    assert prepared.audio_path_mac == str(paths.audio_path_mac)
    assert prepared.audio_path_windows == paths.audio_path_windows
    assert prepared.japanese_srt_path_mac == str(paths.japanese_srt_path_mac)
    assert prepared.japanese_srt_path_windows == paths.japanese_srt_path_windows
    assert prepared.english_srt_path_mac == str(paths.english_srt_path_mac)
    assert prepared.english_srt_path_windows == paths.english_srt_path_windows
    assert paths.audio_path_mac.read_bytes() == audio_before
    assert paths.japanese_srt_path_mac.read_bytes() == japanese_before
    assert paths.english_srt_path_mac.read_bytes() == english_before


def _prepare_catalog_publication_candidate(
    store,
    root: Path,
    movie: str,
    *,
    status: JobStatus = JobStatus.FAILED,
    claimed_by: str | None = None,
    catalog_movie_uuid: str | None = None,
    metadata_status: str | None = None,
    metadata_source: str | None = None,
    english_state: str = "present",
):
    job = store.submit_job(movie, priority=100, force=False).job
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    paths.audio_path_mac.write_bytes(b"synthetic-audio")
    paths.japanese_srt_path_mac.write_bytes(
        b"1\n00:00:00,000 --> 00:00:01,000\nsource\n"
    )
    if english_state != "missing":
        english = (
            b"1\n00:00:00,000 --> 00:00:01,000\ntranslated\n"
            if english_state == "present"
            else b""
        )
        paths.english_srt_path_mac.write_bytes(english)
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = ?, lease_expires_at = ?, "
            "translation_attempt_count = 3, publish_attempt_count = 4, "
            "next_publish_attempt_at = ?, catalog_movie_uuid = ?, "
            "metadata_status = ?, metadata_source = ?, error = ?, "
            "audio_path_mac = ?, audio_path_windows = ?, "
            "japanese_srt_path_mac = ?, japanese_srt_path_windows = ?, "
            "english_srt_path_mac = ?, english_srt_path_windows = ? WHERE id = ?",
            (
                status.value,
                claimed_by,
                "2026-07-12T12:30:00+00:00" if claimed_by else None,
                "2026-07-12T12:00:00+00:00",
                catalog_movie_uuid,
                metadata_status,
                metadata_source,
                "stale repair error",
                str(paths.audio_path_mac),
                paths.audio_path_windows,
                str(paths.japanese_srt_path_mac),
                paths.japanese_srt_path_windows,
                str(paths.english_srt_path_mac),
                paths.english_srt_path_windows,
                job.id,
            ),
        )
    return job.id, paths


def _catalog_artifact_snapshot(paths):
    return {
        path: path.read_bytes() if path.exists() else None
        for path in (
            paths.audio_path_mac,
            paths.japanese_srt_path_mac,
            paths.english_srt_path_mac,
        )
    }


def test_prepare_catalog_publication_rejects_expected_movie_mismatch_atomically(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store, mac_jobs_root, "abc-022"
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)

    with pytest.raises(ValueError, match="movie"):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-023",
        )

    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before


def test_prepare_catalog_publication_rejects_claimed_row_atomically(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store,
        mac_jobs_root,
        "abc-024",
        claimed_by="publisher",
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)

    with pytest.raises(RuntimeError, match="state changed"):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-024",
        )

    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before


@pytest.mark.parametrize(
    "status",
    [
        JobStatus.QUEUED,
        JobStatus.TRANSCRIPTION_DONE,
        JobStatus.TRANSLATING,
        JobStatus.PUBLISHING,
    ],
)
def test_prepare_catalog_publication_rejects_ineligible_status_atomically(
    sqlite_path, mac_jobs_root, status
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store, mac_jobs_root, "abc-025", status=status
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)

    with pytest.raises(ValueError, match="status"):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=status,
            expected_movie="abc-025",
        )

    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before


@pytest.mark.parametrize(
    ("metadata_status", "metadata_source"),
    [
        ("complete", "public"),
        ("partial", "missav"),
        ("placeholder", "local"),
        ("complete", "placeholder"),
    ],
)
def test_prepare_catalog_publication_rejects_verified_ready_atomically(
    sqlite_path,
    mac_jobs_root,
    metadata_status,
    metadata_source,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store,
        mac_jobs_root,
        "abc-026",
        status=JobStatus.ENGLISH_SRT_READY,
        catalog_movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status=metadata_status,
        metadata_source=metadata_source,
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)

    with pytest.raises(ValueError, match="verified publication"):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.ENGLISH_SRT_READY,
            expected_movie="abc-026",
        )

    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before


@pytest.mark.parametrize("english_state", ["missing", "empty"])
def test_prepare_catalog_publication_rejects_invalid_canonical_english_atomically(
    sqlite_path, mac_jobs_root, english_state
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store,
        mac_jobs_root,
        "abc-027",
        english_state=english_state,
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)

    with pytest.raises(FileNotFoundError):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-027",
        )

    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before


def test_exact_translation_claim_cannot_claim_another_job(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = _prepare_translation_job(store, mac_jobs_root, "abc-001")
    second = _prepare_translation_job(store, mac_jobs_root, "abc-002")

    claimed = store.claim_translation_job(
        second.id, "mac-translation-canary", lease_seconds=60
    )

    assert claimed.id == second.id
    assert store.get_job(first.id).status is JobStatus.TRANSCRIPTION_DONE
    assert store.get_job(second.id).status is JobStatus.TRANSLATING


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


def test_translation_quality_success_moves_to_publish_pending_and_preserves_counters(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    transcription = _prepare_translation_job(store, mac_jobs_root, "abc-010")
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET translation_attempt_count = 2, publish_attempt_count = 1 "
            "WHERE id = ?",
            (transcription.id,),
        )
    claimed = store.claim_translation_job(
        transcription.id, "mac-quality", lease_seconds=60
    )
    paths = build_job_paths("abc-010", mac_jobs_root, "M:\\")
    paths.english_srt_path_mac.write_text("valid", encoding="utf-8")

    completed = store.complete_mac_translation_quality(
        claimed.id, "mac-quality", lambda path: Path(path).exists()
    )

    assert completed.status is JobStatus.PUBLISH_PENDING
    assert completed.translation_attempt_count == 2
    assert completed.publish_attempt_count == 1
    assert completed.english_srt_path_mac == str(paths.english_srt_path_mac)
    assert completed.english_srt_path_windows == paths.english_srt_path_windows
    assert completed.claimed_by is None
    assert completed.lease_expires_at is None
    assert completed.error is None


def test_translation_quality_rejects_empty_english_file_without_partial_update(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    transcription = _prepare_translation_job(store, mac_jobs_root, "abc-011")
    claimed = store.claim_translation_job(
        transcription.id, "mac-quality", lease_seconds=60
    )
    paths = build_job_paths("abc-011", mac_jobs_root, "M:\\")
    paths.english_srt_path_mac.write_bytes(b"")

    with pytest.raises(FileNotFoundError, match="empty"):
        store.complete_mac_translation_quality(
            claimed.id, "mac-quality", lambda path: Path(path).exists()
        )

    unchanged = store.get_job(claimed.id)
    assert unchanged.status is JobStatus.TRANSLATING
    assert unchanged.claimed_by == "mac-quality"
    assert unchanged.english_srt_path_mac is None


def test_publication_claim_respects_retry_time_and_exact_job(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    future_job = _prepare_publication_job(store, mac_jobs_root, "abc-012")
    ready_job = _prepare_publication_job(store, mac_jobs_root, "abc-013")
    future = (datetime.now(UTC) + timedelta(hours=1)).replace(microsecond=0).isoformat()
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET next_publish_attempt_at = ?, priority = 1 WHERE id = ?",
            (future, future_job.id),
        )

    assert store.claim_publication_job(
        "publisher", 60, job_id=future_job.id
    ) is None
    exact = store.claim_publication_job("publisher", 60, job_id=ready_job.id)

    assert exact.id == ready_job.id
    assert exact.status is JobStatus.PUBLISHING
    assert exact.claimed_by == "publisher"
    assert store.get_job(future_job.id).status is JobStatus.PUBLISH_PENDING


def test_publication_failure_uses_independent_counter_and_preserves_english(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-014")
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)

    failed = store.fail_publication(
        claimed.id,
        "publisher",
        "catalog unavailable",
        max_publish_attempts=3,
        retry_seconds=120,
    )

    assert failed.status is JobStatus.PUBLISH_PENDING
    assert failed.publish_attempt_count == 1
    assert failed.translation_attempt_count == 0
    assert failed.next_publish_attempt_at is not None
    assert failed.next_publish_attempt_at > failed.updated_at
    assert failed.error == "publishing: catalog unavailable"
    assert failed.english_srt_path_mac == pending.english_srt_path_mac
    assert failed.english_srt_path_windows == pending.english_srt_path_windows
    assert failed.claimed_by is None


def test_publication_final_attempt_fails_job(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-015")
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)

    failed = store.fail_publication(
        claimed.id,
        "publisher",
        "catalog unavailable",
        max_publish_attempts=1,
        retry_seconds=120,
    )

    assert failed.status is JobStatus.FAILED
    assert failed.publish_attempt_count == 1
    assert failed.translation_attempt_count == 0
    assert failed.next_publish_attempt_at is None


def test_permanent_publication_failure_fails_immediately_without_translation_attempt(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-020")
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)

    failed = store.fail_publication(
        claimed.id,
        "publisher",
        "quality_gate_failed:known_bad_collapse",
        max_publish_attempts=10,
        retry_seconds=120,
        permanent=True,
    )

    assert failed.status is JobStatus.FAILED
    assert failed.publish_attempt_count == 1
    assert failed.translation_attempt_count == 0
    assert failed.next_publish_attempt_at is None
    assert failed.error == "publishing: quality_gate_failed:known_bad_collapse"
    assert failed.english_srt_path_mac == pending.english_srt_path_mac
    assert failed.english_srt_path_windows == pending.english_srt_path_windows


def test_publication_success_is_only_from_claim_and_records_catalog_metadata(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-016")
    movie_uuid = "f1bd9932-5697-4f16-865a-c56edc73d491"

    with pytest.raises(PermissionError):
        store.complete_publication(
            pending.id,
            "publisher",
            movie_uuid=movie_uuid,
            metadata_status="placeholder",
            metadata_source="placeholder",
        )
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)
    completed = store.complete_publication(
        claimed.id,
        "publisher",
        movie_uuid=movie_uuid,
        metadata_status="placeholder",
        metadata_source="placeholder",
    )

    assert completed.status is JobStatus.ENGLISH_SRT_READY
    assert completed.catalog_movie_uuid == movie_uuid
    assert completed.metadata_status == "placeholder"
    assert completed.metadata_source == "placeholder"
    assert completed.next_publish_attempt_at is None
    assert completed.error is None
    assert completed.claimed_by is None
    assert completed.lease_expires_at is None


@pytest.mark.parametrize(
    ("movie_uuid", "metadata_status", "metadata_source"),
    [
        ("not-a-uuid", "complete", "public"),
        ("f1bd9932-5697-4f16-865a-c56edc73d491", "unknown", "public"),
        ("f1bd9932-5697-4f16-865a-c56edc73d491", "complete", "unknown"),
    ],
)
def test_publication_success_rejects_invalid_catalog_values_atomically(
    sqlite_path,
    mac_jobs_root,
    movie_uuid,
    metadata_status,
    metadata_source,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-017")
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)

    with pytest.raises(ValueError):
        store.complete_publication(
            claimed.id,
            "publisher",
            movie_uuid=movie_uuid,
            metadata_status=metadata_status,
            metadata_source=metadata_source,
        )

    unchanged = store.get_job(claimed.id)
    assert unchanged.status is JobStatus.PUBLISHING
    assert unchanged.claimed_by == "publisher"
    assert unchanged.catalog_movie_uuid is None


def test_expired_publication_lease_uses_publication_counter(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-018")
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)
    expired = (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    store.force_lease_expiry_for_test(claimed.id, expired)

    recovered = store.recover_expired_publication_leases(
        max_publish_attempts=3, retry_seconds=30
    )

    assert recovered == 1
    refreshed = store.get_job(claimed.id)
    assert refreshed.status is JobStatus.PUBLISH_PENDING
    assert refreshed.publish_attempt_count == 1
    assert refreshed.translation_attempt_count == 0
    assert refreshed.next_publish_attempt_at is not None
    assert refreshed.error == "publishing: publication lease expired"
    assert refreshed.english_srt_path_mac == pending.english_srt_path_mac


def test_publication_failure_rejects_wrong_worker_without_partial_update(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-019")
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)

    with pytest.raises(PermissionError):
        store.fail_publication(
            claimed.id,
            "intruder",
            "bad",
            max_publish_attempts=3,
            retry_seconds=30,
        )

    unchanged = store.get_job(claimed.id)
    assert unchanged.status is JobStatus.PUBLISHING
    assert unchanged.publish_attempt_count == 0
    assert unchanged.claimed_by == "publisher"


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
