import hashlib
import os
import sqlite3
import time
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import orchestrator.store as store_module
from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore


def test_historical_repair_state_and_record_contract():
    state_type = getattr(store_module, "HistoricalRepairState", None)
    record_type = getattr(store_module, "HistoricalRepairRecord", None)

    assert state_type is not None
    assert [state.value for state in state_type] == [
        "planned",
        "pending",
        "running",
        "retry_wait",
        "succeeded",
        "permanent_failed",
        "paused",
    ]
    assert record_type is not None
    assert record_type.__dataclass_params__.frozen is True
    assert [field.name for field in fields(record_type)] == [
        "id",
        "batch_id",
        "job_id",
        "movie_code",
        "allowlist_sha256",
        "state",
        "attempt_count",
        "next_attempt_at",
        "reason_code",
        "japanese_sha256",
        "audio_sha256",
        "english_sha256",
        "created_at",
        "updated_at",
    ]
    assert record_type.__annotations__ == {
        "id": str,
        "batch_id": str,
        "job_id": str,
        "movie_code": str,
        "allowlist_sha256": str,
        "state": state_type,
        "attempt_count": int,
        "next_attempt_at": str | None,
        "reason_code": str | None,
        "japanese_sha256": str,
        "audio_sha256": str | None,
        "english_sha256": str | None,
        "created_at": str,
        "updated_at": str,
    }


def test_initialize_adds_catalog_and_historical_repair_schema(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    with store.connection() as conn:
        job_columns = {
            row["name"]: row
            for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        repair_columns = {
            row["name"]: row
            for row in conn.execute(
                "PRAGMA table_info(historical_translation_repairs)"
            ).fetchall()
        }
        repair_foreign_keys = conn.execute(
            "PRAGMA foreign_key_list(historical_translation_repairs)"
        ).fetchall()
        repair_indexes = conn.execute(
            "PRAGMA index_list(historical_translation_repairs)"
        ).fetchall()

    assert {
        "translation_origin",
        "published_subtitle_id",
        "published_storage_path",
        "published_content_sha256",
        "published_file_size",
        "catalog_sync_attempt_count",
        "next_catalog_sync_attempt_at",
        "catalog_lease_token",
    } <= job_columns.keys()
    assert job_columns["translation_origin"]["dflt_value"] == (
        f"'{store_module.NORMAL_TRANSLATION_ORIGIN}'"
    )
    assert job_columns["catalog_sync_attempt_count"]["dflt_value"] == "0"
    assert {
        "id",
        "batch_id",
        "job_id",
        "movie_code",
        "allowlist_sha256",
        "state",
        "attempt_count",
        "next_attempt_at",
        "reason_code",
        "japanese_sha256",
        "audio_sha256",
        "english_sha256",
        "created_at",
        "updated_at",
    } <= repair_columns.keys()
    assert any(
        row["table"] == "jobs" and row["from"] == "job_id" and row["to"] == "id"
        for row in repair_foreign_keys
    )
    index_names = {row["name"] for row in repair_indexes}
    assert "idx_historical_translation_repairs_state_created_at" in index_names


def test_job_record_reads_durable_catalog_fields_without_fallbacks(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id = store.submit_job("abc-099", priority=100, force=False).job.id
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET translation_origin = ?, published_subtitle_id = ?, "
            "published_storage_path = ?, published_content_sha256 = ?, "
            "published_file_size = ?, catalog_sync_attempt_count = ?, "
            "next_catalog_sync_attempt_at = ?, catalog_lease_token = ? WHERE id = ?",
            (
                store_module.HISTORICAL_TRANSLATION_ORIGIN,
                "subtitle-uuid",
                "subtitles/abc-099.srt",
                "a" * 64,
                1234,
                3,
                "2026-07-13T12:00:00+00:00",
                "catalog-lease-token",
                job_id,
            ),
        )

    job = store.get_job(job_id)

    assert job.translation_origin == store_module.HISTORICAL_TRANSLATION_ORIGIN
    assert job.published_subtitle_id == "subtitle-uuid"
    assert job.published_storage_path == "subtitles/abc-099.srt"
    assert job.published_content_sha256 == "a" * 64
    assert job.published_file_size == 1234
    assert job.catalog_sync_attempt_count == 3
    assert job.next_catalog_sync_attempt_at == "2026-07-13T12:00:00+00:00"
    assert job.catalog_lease_token == "catalog-lease-token"


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
        **_catalog_subtitle_hashes(paths),
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
    english_target: Path | None = None,
):
    job = store.submit_job(movie, priority=100, force=False).job
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    paths.audio_path_mac.write_bytes(b"synthetic-audio")
    paths.japanese_srt_path_mac.write_bytes(
        b"1\n00:00:00,000 --> 00:00:01,000\nsource\n"
    )
    if english_state == "symlink":
        assert english_target is not None
        paths.english_srt_path_mac.symlink_to(english_target)
    elif english_state != "missing":
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


def _catalog_subtitle_hashes(paths):
    return {
        "expected_japanese_sha256": hashlib.sha256(
            paths.japanese_srt_path_mac.read_bytes()
        ).hexdigest(),
        "expected_english_sha256": hashlib.sha256(
            paths.english_srt_path_mac.read_bytes()
        ).hexdigest(),
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
            **_catalog_subtitle_hashes(paths),
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
            **_catalog_subtitle_hashes(paths),
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
            **_catalog_subtitle_hashes(paths),
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
            **_catalog_subtitle_hashes(paths),
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
            expected_japanese_sha256="0" * 64,
            expected_english_sha256="0" * 64,
        )

    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before


def test_prepare_catalog_publication_rejects_symlinked_canonical_english_atomically(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    external_english = mac_jobs_root.parent / "outside.English.srt"
    external_before = b"external subtitle must not be published"
    external_english.write_bytes(external_before)
    job_id, paths = _prepare_catalog_publication_candidate(
        store,
        mac_jobs_root,
        "abc-028",
        english_state="symlink",
        english_target=external_english,
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)
    link_stat_before = paths.english_srt_path_mac.lstat()
    link_target_before = paths.english_srt_path_mac.readlink()

    with pytest.raises(FileNotFoundError, match="regular"):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-028",
            expected_japanese_sha256="0" * 64,
            expected_english_sha256="0" * 64,
        )

    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before
    assert paths.english_srt_path_mac.is_symlink()
    assert paths.english_srt_path_mac.lstat() == link_stat_before
    assert paths.english_srt_path_mac.readlink() == link_target_before
    assert external_english.read_bytes() == external_before


@pytest.mark.parametrize("changed_language", ["japanese", "english"])
def test_prepare_catalog_publication_rejects_subtitle_snapshot_mismatch_atomically(
    sqlite_path, mac_jobs_root, changed_language
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store, mac_jobs_root, "abc-030"
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)
    expected_hashes = _catalog_subtitle_hashes(paths)
    expected_hashes[f"expected_{changed_language}_sha256"] = "0" * 64

    with pytest.raises(
        RuntimeError,
        match="^subtitle_snapshot_changed_before_prepare$",
    ):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-030",
            **expected_hashes,
        )

    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before


@pytest.mark.parametrize("invalid_hash", ["0" * 63, "A" * 64, "g" * 64])
def test_prepare_catalog_publication_rejects_invalid_snapshot_hash_atomically(
    sqlite_path, mac_jobs_root, invalid_hash
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store, mac_jobs_root, "abc-032"
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)
    expected_hashes = _catalog_subtitle_hashes(paths)
    expected_hashes["expected_english_sha256"] = invalid_hash

    with pytest.raises(ValueError, match="expected_english_sha256"):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-032",
            **expected_hashes,
        )

    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before


def test_prepare_catalog_publication_rejects_symlinked_job_directory_atomically(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("abc-031", priority=100, force=False).job
    external_root = tmp_path / "outside-jobs"
    external_store = JobStore(tmp_path / "external.sqlite3", external_root, "M:\\")
    external_store.initialize()
    _, external_paths = _prepare_catalog_publication_candidate(
        external_store,
        external_root,
        "abc-031",
    )
    canonical_paths = build_job_paths("abc-031", mac_jobs_root, "M:\\")
    mac_jobs_root.mkdir(parents=True, exist_ok=True)
    canonical_paths.job_dir_mac.symlink_to(
        external_paths.job_dir_mac,
        target_is_directory=True,
    )
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, translation_attempt_count = 3, "
            "publish_attempt_count = 4, error = ? WHERE id = ?",
            (JobStatus.FAILED.value, "stale repair error", job.id),
        )
    row_before = store.get_job(job.id)
    files_before = _catalog_artifact_snapshot(canonical_paths)
    expected_hashes = _catalog_subtitle_hashes(canonical_paths)

    with pytest.raises(ValueError, match="job_directory"):
        store.prepare_catalog_publication_repair(
            job.id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-031",
            **expected_hashes,
        )

    assert store.get_job(job.id) == row_before
    assert _catalog_artifact_snapshot(canonical_paths) == files_before


def test_prepare_catalog_publication_rejects_replaced_english_basename_during_hash(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store, mac_jobs_root, "abc-033"
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)
    expected_hashes = _catalog_subtitle_hashes(paths)
    english_inode = paths.english_srt_path_mac.stat().st_ino
    replacement = b"replacement canonical english subtitle\n"
    replacement_path = tmp_path / "replacement.English.srt"
    replacement_path.write_bytes(replacement)
    real_read = os.read
    replaced = False

    def read_then_replace(file_fd, byte_count):
        nonlocal replaced
        chunk = real_read(file_fd, byte_count)
        if (
            not replaced
            and chunk
            and os.fstat(file_fd).st_ino == english_inode
        ):
            os.replace(replacement_path, paths.english_srt_path_mac)
            replaced = True
        return chunk

    monkeypatch.setattr(store_module.os, "read", read_then_replace)

    with pytest.raises(
        RuntimeError,
        match="^subtitle_snapshot_changed_before_prepare$",
    ):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-033",
            **expected_hashes,
        )

    assert replaced is True
    assert store.get_job(job_id) == row_before
    expected_files = {
        **files_before,
        paths.english_srt_path_mac: replacement,
    }
    assert _catalog_artifact_snapshot(paths) == expected_files


def test_prepare_catalog_publication_rolls_back_if_english_replaced_after_update(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store, mac_jobs_root, "abc-036"
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)
    expected_hashes = _catalog_subtitle_hashes(paths)
    replacement = b"replacement after conditional update\n"
    replacement_path = tmp_path / "post-update.English.srt"
    replacement_path.write_bytes(replacement)
    replaced = False

    class ReplaceAfterUpdateConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            nonlocal replaced
            cursor = super().execute(sql, parameters)
            if (
                not replaced
                and "UPDATE jobs" in sql
                and parameters
                and parameters[0] == JobStatus.PUBLISH_PENDING.value
            ):
                os.replace(replacement_path, paths.english_srt_path_mac)
                replaced = True
            return cursor

    def connect_with_replacement():
        connection = sqlite3.connect(
            sqlite_path,
            factory=ReplaceAfterUpdateConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    monkeypatch.setattr(store, "connect", connect_with_replacement)

    with pytest.raises(
        RuntimeError,
        match="^subtitle_snapshot_changed_before_prepare$",
    ):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-036",
            **expected_hashes,
        )

    assert replaced is True
    assert store.get_job(job_id) == row_before
    expected_files = {
        **files_before,
        paths.english_srt_path_mac: replacement,
    }
    assert _catalog_artifact_snapshot(paths) == expected_files


def test_prepare_catalog_publication_rolls_back_if_job_directory_replaced_after_update(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store, mac_jobs_root, "abc-037"
    )
    row_before = store.get_job(job_id)
    expected_hashes = _catalog_subtitle_hashes(paths)
    replacement_directory = tmp_path / "replacement-job"
    replacement_directory.mkdir()
    replacement_files = {
        paths.audio_path_mac.name: b"replacement-audio",
        paths.japanese_srt_path_mac.name: b"replacement-japanese\n",
        paths.english_srt_path_mac.name: b"replacement-english\n",
        "rejected/existing.srt": b"replacement-rejected",
    }
    for relative_name, contents in replacement_files.items():
        replacement_path = replacement_directory / relative_name
        replacement_path.parent.mkdir(parents=True, exist_ok=True)
        replacement_path.write_bytes(contents)
    original_backup = tmp_path / "original-job-backup"
    replaced = False

    class ReplaceDirectoryAfterUpdateConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            nonlocal replaced
            cursor = super().execute(sql, parameters)
            if (
                not replaced
                and "UPDATE jobs" in sql
                and parameters
                and parameters[0] == JobStatus.PUBLISH_PENDING.value
            ):
                paths.job_dir_mac.rename(original_backup)
                replacement_directory.rename(paths.job_dir_mac)
                replaced = True
            return cursor

    def connect_with_directory_replacement():
        connection = sqlite3.connect(
            sqlite_path,
            factory=ReplaceDirectoryAfterUpdateConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    monkeypatch.setattr(store, "connect", connect_with_directory_replacement)

    with pytest.raises(
        RuntimeError,
        match="^subtitle_snapshot_changed_before_prepare$",
    ):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-037",
            **expected_hashes,
        )

    assert replaced is True
    assert store.get_job(job_id) == row_before
    assert {
        str(path.relative_to(paths.job_dir_mac)): path.read_bytes()
        for path in paths.job_dir_mac.rglob("*")
        if path.is_file()
    } == replacement_files


def test_prepare_catalog_publication_rejects_oversized_sparse_subtitle_atomically(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store, mac_jobs_root, "abc-034"
    )
    maximum_size = 32 * 1024 * 1024
    assert store_module.MAX_PUBLICATION_CANARY_SRT_BYTES == maximum_size
    paths.english_srt_path_mac.write_bytes(b"x")
    os.truncate(paths.english_srt_path_mac, maximum_size + 1)
    row_before = store.get_job(job_id)
    stat_before = paths.english_srt_path_mac.stat()
    japanese_before = paths.japanese_srt_path_mac.read_bytes()
    audio_before = paths.audio_path_mac.read_bytes()

    with pytest.raises(FileNotFoundError, match="size limit"):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-034",
            expected_japanese_sha256="0" * 64,
            expected_english_sha256="0" * 64,
        )

    assert store.get_job(job_id) == row_before
    assert paths.english_srt_path_mac.stat().st_size == stat_before.st_size
    assert paths.japanese_srt_path_mac.read_bytes() == japanese_before
    assert paths.audio_path_mac.read_bytes() == audio_before


def test_prepare_catalog_publication_rejects_append_truncate_race_atomically(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store, mac_jobs_root, "abc-035"
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)
    expected_hashes = _catalog_subtitle_hashes(paths)
    english_before = paths.english_srt_path_mac.stat()
    real_read = os.read
    raced = False

    def read_then_append_and_restore(file_fd, byte_count):
        nonlocal raced
        chunk = real_read(file_fd, byte_count)
        if (
            not raced
            and chunk
            and os.fstat(file_fd).st_ino == english_before.st_ino
        ):
            with paths.english_srt_path_mac.open("ab") as subtitle:
                subtitle.write(b"transient-extra-byte")
            os.truncate(paths.english_srt_path_mac, english_before.st_size)
            os.utime(
                paths.english_srt_path_mac,
                ns=(english_before.st_atime_ns, english_before.st_mtime_ns),
            )
            raced = True
        return chunk

    monkeypatch.setattr(
        store_module.os,
        "read",
        read_then_append_and_restore,
    )

    with pytest.raises(
        RuntimeError,
        match="^subtitle_snapshot_changed_before_prepare$",
    ):
        store.prepare_catalog_publication_repair(
            job_id,
            expected_status=JobStatus.FAILED,
            expected_movie="abc-035",
            **expected_hashes,
        )

    assert raced is True
    assert (
        paths.english_srt_path_mac.stat().st_ctime_ns
        != english_before.st_ctime_ns
    )
    assert store.get_job(job_id) == row_before
    assert _catalog_artifact_snapshot(paths) == files_before


def test_prepare_catalog_publication_accepts_unverified_legacy_ready(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job_id, paths = _prepare_catalog_publication_candidate(
        store,
        mac_jobs_root,
        "abc-029",
        status=JobStatus.ENGLISH_SRT_READY,
        catalog_movie_uuid=None,
        metadata_status="complete",
        metadata_source="public",
    )
    row_before = store.get_job(job_id)
    files_before = _catalog_artifact_snapshot(paths)

    prepared = store.prepare_catalog_publication_repair(
        job_id,
        expected_status=JobStatus.ENGLISH_SRT_READY,
        expected_movie="abc-029",
        **_catalog_subtitle_hashes(paths),
    )

    assert prepared.status is JobStatus.PUBLISH_PENDING
    assert prepared.translation_attempt_count == row_before.translation_attempt_count
    assert prepared.publish_attempt_count == 0
    assert prepared.catalog_movie_uuid is None
    assert prepared.metadata_status is None
    assert prepared.metadata_source is None
    assert prepared.audio_path_mac == row_before.audio_path_mac
    assert prepared.audio_path_windows == row_before.audio_path_windows
    assert prepared.japanese_srt_path_mac == row_before.japanese_srt_path_mac
    assert prepared.japanese_srt_path_windows == row_before.japanese_srt_path_windows
    assert prepared.english_srt_path_mac == row_before.english_srt_path_mac
    assert prepared.english_srt_path_windows == row_before.english_srt_path_windows
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


def test_supabase_success_is_only_from_claim_and_records_verified_receipt(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-016")
    movie_uuid = "f1bd9932-5697-4f16-865a-c56edc73d491"

    with pytest.raises(PermissionError):
        store.complete_supabase_publication(
            pending.id,
            "publisher",
            movie_uuid=movie_uuid,
            metadata_status="placeholder",
            metadata_source="placeholder",
            subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
            storage_path="abc/abc-016/abc-016-English_AI.srt",
            content_sha256="a" * 64,
            file_size=123,
        )
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)
    completed = store.complete_supabase_publication(
        claimed.id,
        "publisher",
        movie_uuid=movie_uuid,
        metadata_status="placeholder",
        metadata_source="placeholder",
        subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
        storage_path="abc/abc-016/abc-016-English_AI.srt",
        content_sha256="a" * 64,
        file_size=123,
    )

    assert completed.status is JobStatus.CATALOG_SYNC_PENDING
    assert completed.catalog_movie_uuid == movie_uuid
    assert completed.metadata_status == "placeholder"
    assert completed.metadata_source == "placeholder"
    assert completed.published_subtitle_id == "f1bd9932-5697-4f16-865a-c56edc73d492"
    assert completed.published_storage_path == "abc/abc-016/abc-016-English_AI.srt"
    assert completed.published_content_sha256 == "a" * 64
    assert completed.published_file_size == 123
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
def test_supabase_success_rejects_invalid_receipt_values_atomically(
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
        store.complete_supabase_publication(
            claimed.id,
            "publisher",
            movie_uuid=movie_uuid,
            metadata_status=metadata_status,
            metadata_source=metadata_source,
            subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
            storage_path="abc/abc-017/abc-017-English_AI.srt",
            content_sha256="a" * 64,
            file_size=123,
        )

    unchanged = store.get_job(claimed.id)
    assert unchanged.status is JobStatus.PUBLISHING
    assert unchanged.claimed_by == "publisher"
    assert unchanged.catalog_movie_uuid is None


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("subtitle_id", 123),
        ("storage_path", "wrong/path.srt"),
        ("content_sha256", "A" * 64),
        ("file_size", True),
    ],
)
def test_supabase_success_uses_strict_shared_receipt_validator(
    sqlite_path, mac_jobs_root, field, invalid_value
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-017")
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)
    receipt = {
        "movie_uuid": "f1bd9932-5697-4f16-865a-c56edc73d491",
        "metadata_status": "complete",
        "metadata_source": "public",
        "subtitle_id": "f1bd9932-5697-4f16-865a-c56edc73d492",
        "storage_path": "abc/abc-017/abc-017-English_AI.srt",
        "content_sha256": "a" * 64,
        "file_size": 123,
    }
    receipt[field] = invalid_value

    with pytest.raises(ValueError, match="verified Supabase receipt is invalid"):
        store.complete_supabase_publication(
            claimed.id,
            "publisher",
            **receipt,
        )

    unchanged = store.get_job(claimed.id)
    assert unchanged.status is JobStatus.PUBLISHING
    assert unchanged.catalog_movie_uuid is None


def test_catalog_sync_claim_failure_retry_and_exact_success(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-023")
    claimed = store.claim_publication_job("publisher", 60, job_id=pending.id)
    receipt = store.complete_supabase_publication(
        claimed.id,
        "publisher",
        movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status="complete",
        metadata_source="public",
        subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
        storage_path="abc/abc-023/abc-023-English_AI.srt",
        content_sha256="b" * 64,
        file_size=456,
    )

    with pytest.raises(PermissionError):
        store.complete_catalog_sync(
            receipt.id,
            "catalog-worker",
            lease_token="not-a-valid-lease",
            canonical_code="abc-023",
            d1_rows_updated=1,
            subtitle_count=1,
            kv_keys_deleted=("movie:full:abc-023", "movie:light:abc-023"),
        )
    syncing = store.claim_catalog_sync_job(
        "catalog-worker", 60, job_id=receipt.id
    )
    assert syncing.status is JobStatus.CATALOG_SYNCING
    failed = store.fail_catalog_sync(
        syncing.id,
        "catalog-worker",
        "catalog_fetch_failed",
        lease_token=syncing.catalog_lease_token,
        max_catalog_sync_attempts=3,
        retry_seconds=0,
    )
    assert failed.status is JobStatus.CATALOG_SYNC_PENDING
    assert failed.catalog_sync_attempt_count == 1
    assert failed.publish_attempt_count == 0
    assert failed.next_catalog_sync_attempt_at is not None
    assert failed.published_subtitle_id == receipt.published_subtitle_id
    assert failed.error == "catalog_sync: catalog_fetch_failed"

    syncing = store.claim_catalog_sync_job(
        "catalog-worker", 60, job_id=receipt.id
    )
    ready = store.complete_catalog_sync(
        syncing.id,
        "catalog-worker",
        lease_token=syncing.catalog_lease_token,
        canonical_code="abc-023",
        d1_rows_updated=1,
        subtitle_count=1,
        kv_keys_deleted=("movie:full:abc-023", "movie:light:abc-023"),
    )
    assert ready.status is JobStatus.ENGLISH_SRT_READY
    assert ready.claimed_by is None
    assert ready.lease_expires_at is None
    assert ready.error is None


def test_expired_catalog_sync_lease_uses_only_catalog_counter(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-024")
    publishing = store.claim_publication_job("publisher", 60, job_id=pending.id)
    receipt = store.complete_supabase_publication(
        publishing.id,
        "publisher",
        movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status="complete",
        metadata_source="public",
        subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
        storage_path="abc/abc-024/abc-024-English_AI.srt",
        content_sha256="c" * 64,
        file_size=789,
    )
    syncing = store.claim_catalog_sync_job("catalog-worker", 60, job_id=receipt.id)
    expired = (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    store.force_lease_expiry_for_test(syncing.id, expired)

    assert store.recover_expired_catalog_sync_leases(3, 0) == 1

    recovered = store.get_job(receipt.id)
    assert recovered.status is JobStatus.CATALOG_SYNC_PENDING
    assert recovered.catalog_sync_attempt_count == 1
    assert recovered.publish_attempt_count == 0
    assert recovered.translation_attempt_count == 0
    assert recovered.error == "catalog_sync: catalog_sync_lease_expired"


def test_catalog_fencing_rejects_stale_same_worker_complete_and_fail(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-027")
    publishing = store.claim_publication_job("publisher", 60, job_id=pending.id)
    receipt = store.complete_supabase_publication(
        publishing.id,
        "publisher",
        movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status="complete",
        metadata_source="public",
        subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
        storage_path="abc/abc-027/abc-027-English_AI.srt",
        content_sha256="e" * 64,
        file_size=654,
    )
    first = store.claim_catalog_sync_job("same-worker", 60, job_id=receipt.id)
    first_token = first.catalog_lease_token
    assert first_token
    expired = (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    store.force_lease_expiry_for_test(first.id, expired)
    assert store.recover_expired_catalog_sync_leases(3, 0) == 1
    second = store.claim_catalog_sync_job("same-worker", 60, job_id=receipt.id)
    assert second.catalog_lease_token
    assert second.catalog_lease_token != first_token

    with pytest.raises(PermissionError):
        store.complete_catalog_sync(
            first.id,
            "same-worker",
            lease_token=first_token,
            canonical_code="abc-027",
            d1_rows_updated=1,
            subtitle_count=1,
            kv_keys_deleted=("movie:full:abc-027", "movie:light:abc-027"),
        )
    unchanged = store.get_job(receipt.id)
    assert unchanged.status is JobStatus.CATALOG_SYNCING
    assert unchanged.catalog_lease_token == second.catalog_lease_token
    assert unchanged.catalog_sync_attempt_count == 1

    with pytest.raises(PermissionError):
        store.fail_catalog_sync(
            first.id,
            "same-worker",
            "catalog_fetch_failed",
            lease_token=first_token,
            max_catalog_sync_attempts=3,
            retry_seconds=0,
        )
    unchanged = store.get_job(receipt.id)
    assert unchanged.status is JobStatus.CATALOG_SYNCING
    assert unchanged.catalog_lease_token == second.catalog_lease_token
    assert unchanged.catalog_sync_attempt_count == 1


def test_catalog_fencing_rejects_different_worker_and_expired_lease(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-032")
    publishing = store.claim_publication_job("publisher", 60, job_id=pending.id)
    receipt = store.complete_supabase_publication(
        publishing.id,
        "publisher",
        movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status="complete",
        metadata_source="public",
        subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
        storage_path="abc/abc-032/abc-032-English_AI.srt",
        content_sha256="2" * 64,
        file_size=222,
    )
    syncing = store.claim_catalog_sync_job("owner", 60, job_id=receipt.id)

    for action in ("complete", "fail"):
        with pytest.raises(PermissionError):
            if action == "complete":
                store.complete_catalog_sync(
                    syncing.id,
                    "intruder",
                    lease_token=syncing.catalog_lease_token,
                    canonical_code="abc-032",
                    d1_rows_updated=1,
                    subtitle_count=1,
                    kv_keys_deleted=(
                        "movie:full:abc-032",
                        "movie:light:abc-032",
                    ),
                )
            else:
                store.fail_catalog_sync(
                    syncing.id,
                    "intruder",
                    "catalog_fetch_failed",
                    lease_token=syncing.catalog_lease_token,
                    max_catalog_sync_attempts=3,
                    retry_seconds=0,
                )

    expired = (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    store.force_lease_expiry_for_test(syncing.id, expired)
    for action in ("complete", "fail"):
        with pytest.raises(PermissionError):
            if action == "complete":
                store.complete_catalog_sync(
                    syncing.id,
                    "owner",
                    lease_token=syncing.catalog_lease_token,
                    canonical_code="abc-032",
                    d1_rows_updated=1,
                    subtitle_count=1,
                    kv_keys_deleted=(
                        "movie:full:abc-032",
                        "movie:light:abc-032",
                    ),
                )
            else:
                store.fail_catalog_sync(
                    syncing.id,
                    "owner",
                    "catalog_fetch_failed",
                    lease_token=syncing.catalog_lease_token,
                    max_catalog_sync_attempts=3,
                    retry_seconds=0,
                )
    unchanged = store.get_job(receipt.id)
    assert unchanged.status is JobStatus.CATALOG_SYNCING
    assert unchanged.catalog_sync_attempt_count == 0


@pytest.mark.parametrize(
    ("column", "invalid_value"),
    [
        ("catalog_movie_uuid", "not-a-uuid"),
        ("metadata_status", "unknown"),
        ("metadata_source", "unknown"),
        ("published_subtitle_id", "not-a-uuid"),
        ("published_storage_path", "wrong/path.srt"),
        ("published_content_sha256", "A" * 64),
        ("published_file_size", 0),
    ],
)
def test_catalog_claim_refuses_strictly_invalid_verified_receipt(
    sqlite_path, mac_jobs_root, column, invalid_value
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-028")
    publishing = store.claim_publication_job("publisher", 60, job_id=pending.id)
    receipt = store.complete_supabase_publication(
        publishing.id,
        "publisher",
        movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status="complete",
        metadata_source="public",
        subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
        storage_path="abc/abc-028/abc-028-English_AI.srt",
        content_sha256="f" * 64,
        file_size=987,
    )
    with store.connection() as conn:
        conn.execute(f"UPDATE jobs SET {column} = ? WHERE id = ?", (invalid_value, receipt.id))

    assert store.claim_catalog_sync_job("catalog-worker", 60, job_id=receipt.id) is None
    unchanged = store.get_job(receipt.id)
    assert unchanged.status is JobStatus.CATALOG_SYNC_PENDING
    assert unchanged.claimed_by is None


def test_catalog_complete_revalidates_receipt_after_claim(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-029")
    publishing = store.claim_publication_job("publisher", 60, job_id=pending.id)
    receipt = store.complete_supabase_publication(
        publishing.id,
        "publisher",
        movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status="complete",
        metadata_source="public",
        subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
        storage_path="abc/abc-029/abc-029-English_AI.srt",
        content_sha256="1" * 64,
        file_size=111,
    )
    syncing = store.claim_catalog_sync_job("catalog-worker", 60, job_id=receipt.id)
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET published_storage_path = ? WHERE id = ?",
            ("wrong/path.srt", receipt.id),
        )

    with pytest.raises(ValueError, match="verified Supabase receipt is invalid"):
        store.complete_catalog_sync(
            syncing.id,
            "catalog-worker",
            lease_token=syncing.catalog_lease_token,
            canonical_code="abc-029",
            d1_rows_updated=1,
            subtitle_count=1,
            kv_keys_deleted=("movie:full:abc-029", "movie:light:abc-029"),
        )
    unchanged = store.get_job(receipt.id)
    assert unchanged.status is JobStatus.CATALOG_SYNCING
    assert unchanged.catalog_lease_token == syncing.catalog_lease_token


def test_catalog_sync_claim_refuses_pending_row_without_verified_receipt(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("abc-025", priority=100, force=False).job
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ? WHERE id = ?",
            (JobStatus.CATALOG_SYNC_PENDING.value, job.id),
        )

    assert store.claim_catalog_sync_job("catalog-worker", 60) is None
    unchanged = store.get_job(job.id)
    assert unchanged.status is JobStatus.CATALOG_SYNC_PENDING
    assert unchanged.claimed_by is None


def test_catalog_sync_failure_rejects_unrecognized_reason_text(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = _prepare_publication_job(store, mac_jobs_root, "abc-026")
    publishing = store.claim_publication_job("publisher", 60, job_id=pending.id)
    receipt = store.complete_supabase_publication(
        publishing.id,
        "publisher",
        movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status="complete",
        metadata_source="public",
        subtitle_id="f1bd9932-5697-4f16-865a-c56edc73d492",
        storage_path="abc/abc-026/abc-026-English_AI.srt",
        content_sha256="d" * 64,
        file_size=321,
    )
    syncing = store.claim_catalog_sync_job("catalog-worker", 60, job_id=receipt.id)

    failed = store.fail_catalog_sync(
        syncing.id,
        "catalog-worker",
        "admin_token_secret",
        lease_token=syncing.catalog_lease_token,
        max_catalog_sync_attempts=3,
        retry_seconds=0,
    )

    assert failed.error == "catalog_sync: catalog_sync_failed"


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
