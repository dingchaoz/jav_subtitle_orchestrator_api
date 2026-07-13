from __future__ import annotations

import hashlib
from pathlib import Path
from datetime import UTC, datetime, timedelta

import pytest

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import (
    HISTORICAL_TRANSLATION_ORIGIN,
    NORMAL_TRANSLATION_ORIGIN,
    HistoricalRepairState,
    JobStore,
    StageLeaseLostError,
)


def _store(sqlite_path: Path, mac_jobs_root: Path) -> JobStore:
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    return store


def _content_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _repair_candidate(
    store: JobStore,
    root: Path,
    movie: str,
    *,
    state: HistoricalRepairState = HistoricalRepairState.PENDING,
):
    job = store.submit_job(movie, priority=100, force=False).job
    assert job is not None
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True)
    paths.japanese_srt_path_mac.write_text("Japanese source\n", encoding="utf-8")
    paths.english_srt_path_mac.write_text("rejected old English\n", encoding="utf-8")
    paths.audio_path_mac.write_bytes(b"RIFF-preserved-audio")
    japanese_sha256 = _content_sha256(paths.japanese_srt_path_mac)
    audio_sha256 = _content_sha256(paths.audio_path_mac)
    english_sha256 = _content_sha256(paths.english_srt_path_mac)
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, japanese_srt_path_mac = ?, "
            "english_srt_path_mac = ?, audio_path_mac = ? WHERE id = ?",
            (
                JobStatus.ENGLISH_SRT_READY.value,
                str(paths.japanese_srt_path_mac),
                str(paths.english_srt_path_mac),
                str(paths.audio_path_mac),
                job.id,
            ),
        )
        conn.execute(
            """
            INSERT INTO historical_translation_repairs (
              id, batch_id, job_id, movie_code, allowlist_sha256, state,
              attempt_count, next_attempt_at, reason_code, japanese_sha256,
              audio_probe_snapshot_sha256, audio_sha256,
              source_english_sha256, english_sha256, created_at, updated_at
            ) VALUES (?, 'batch-test', ?, ?, ?, ?, 0, NULL, NULL, ?, ?, ?, ?,
                      NULL, '2026-07-13T00:00:00+00:00',
                      '2026-07-13T00:00:00+00:00')
            """,
            (
                f"repair-{movie}",
                job.id,
                movie,
                "a" * 64,
                state.value,
                japanese_sha256,
                "b" * 64,
                audio_sha256,
                english_sha256,
            ),
        )
    return store.get_job(job.id), paths


def test_normal_claim_never_takes_historical_origin(sqlite_path, mac_jobs_root):
    store = _store(sqlite_path, mac_jobs_root)
    historical, _ = _repair_candidate(store, mac_jobs_root, "old-001")
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = ? WHERE id = ?",
            (
                JobStatus.TRANSCRIPTION_DONE.value,
                HISTORICAL_TRANSLATION_ORIGIN,
                historical.id,
            ),
        )

    assert (
        store.claim_next_translation_job(
            "mac-1", 60, origin=NORMAL_TRANSLATION_ORIGIN
        )
        is None
    )


def test_historical_claim_yields_to_claimable_normal_work(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    historical, _ = _repair_candidate(store, mac_jobs_root, "old-001")
    normal = store.submit_job("new-001", priority=100, force=False).job
    assert normal is not None
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ? WHERE id = ?",
            (JobStatus.TRANSCRIPTION_DONE.value, normal.id),
        )

    assert store.claim_next_historical_repair("mac-1", 60) is None
    claimed = store.claim_next_translation_job("mac-1", 60, origin="normal")
    assert claimed is not None and claimed.id == normal.id
    assert store.get_job(historical.id).status is JobStatus.ENGLISH_SRT_READY


def test_historical_claim_atomically_activates_exact_job(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _repair_candidate(store, mac_jobs_root, "old-001")

    claimed = store.claim_next_historical_repair("mac-1", 60)

    assert claimed is not None and claimed.id == job.id
    assert claimed.status is JobStatus.TRANSLATING
    assert claimed.claimed_by == "mac-1"
    assert claimed.translation_origin == HISTORICAL_TRANSLATION_ORIGIN
    repair = store.get_historical_repair(job.id)
    assert repair is not None
    assert repair.state is HistoricalRepairState.RUNNING
    assert repair.attempt_count == 1


def test_historical_lane_pause_is_durable_and_does_not_hide_normal_work(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    normal = store.submit_job("new-001", priority=100, force=False).job
    assert normal is not None
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ? WHERE id = ?",
            (JobStatus.TRANSCRIPTION_DONE.value, normal.id),
        )

    paused = store.pause_historical_lane("quality_failure_limit")

    assert paused.paused is True
    assert paused.reason_code == "quality_failure_limit"
    assert store.historical_lane_state() == paused
    assert store.has_claimable_normal_work() is True
    resumed = store.resume_historical_lane()
    assert resumed.paused is False
    assert resumed.reason_code is None


def test_expired_historical_translation_returns_only_to_repair_retry_queue(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _repair_candidate(store, mac_jobs_root, "old-201")
    claimed = store.claim_next_historical_repair("mac-1", 60)
    assert claimed is not None
    expired = (datetime.now(UTC) - timedelta(seconds=1)).replace(
        microsecond=0
    ).isoformat()
    store.force_lease_expiry_for_test(job.id, expired)

    assert store.recover_expired_translation_leases(3) == 1

    refreshed = store.get_job(job.id)
    repair = store.get_historical_repair(job.id)
    assert refreshed.status is JobStatus.FAILED
    assert refreshed.translation_origin == HISTORICAL_TRANSLATION_ORIGIN
    assert repair.state is HistoricalRepairState.RETRY_WAIT
    assert store.claim_next_translation_job("mac-2", 60, origin="normal") is None
    reclaimed = store.claim_next_historical_repair("mac-2", 60)
    assert reclaimed is not None and reclaimed.id == job.id


def test_same_worker_old_translation_success_and_failure_cannot_touch_new_claim(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _repair_candidate(store, mac_jobs_root, "old-202")
    old = store.claim_next_historical_repair("mac-same", 60)
    assert old is not None and old.stage_lease_token
    expired = (datetime.now(UTC) - timedelta(seconds=1)).replace(
        microsecond=0
    ).isoformat()
    store.force_lease_expiry_for_test(job.id, expired)
    assert store.recover_expired_translation_leases(3) == 1
    new = store.claim_next_historical_repair("mac-same", 60)
    assert new is not None and new.stage_lease_token != old.stage_lease_token
    before = store.get_job(job.id)
    repair_before = store.get_historical_repair(job.id)

    with pytest.raises(StageLeaseLostError):
        store.complete_mac_translation_quality(
            job.id,
            "mac-same",
            lambda _path: True,
            lease_token=old.stage_lease_token,
        )
    with pytest.raises(StageLeaseLostError):
        store.mark_historical_retry(
            job.id,
            "translation_failed",
            0,
            worker_id="mac-same",
            lease_token=old.stage_lease_token,
        )

    assert store.get_job(job.id) == before
    assert store.get_historical_repair(job.id) == repair_before


def test_same_worker_old_publication_success_and_failure_cannot_touch_new_claim(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _repair_candidate(store, mac_jobs_root, "old-203")
    activated = store.claim_next_historical_repair("mac-same", 60)
    assert activated is not None
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = NULL, lease_expires_at = NULL, "
            "stage_lease_token = NULL, english_srt_path_mac = ? WHERE id = ?",
            (JobStatus.PUBLISH_PENDING.value, str(paths.english_srt_path_mac), job.id),
        )
    old = store.claim_publication_job("mac-same", 60, job_id=job.id)
    assert old is not None and old.stage_lease_token
    expired = (datetime.now(UTC) - timedelta(seconds=1)).replace(
        microsecond=0
    ).isoformat()
    store.force_lease_expiry_for_test(job.id, expired)
    assert store.recover_expired_publication_leases(3, 0) == 1
    new = store.claim_publication_job("mac-same", 60, job_id=job.id)
    assert new is not None and new.stage_lease_token != old.stage_lease_token
    before = store.get_job(job.id)

    with pytest.raises(StageLeaseLostError):
        store.fail_publication(
            job.id,
            "mac-same",
            "offline",
            max_publish_attempts=3,
            retry_seconds=0,
            lease_token=old.stage_lease_token,
        )
    with pytest.raises(StageLeaseLostError):
        store.complete_supabase_publication(
            job.id,
            "mac-same",
            movie_uuid="00000000-0000-0000-0000-000000000001",
            metadata_status="complete",
            metadata_source="missav",
            subtitle_id="00000000-0000-0000-0000-000000000002",
            storage_path="old/old-203/old-203-English_AI.srt",
            content_sha256="a" * 64,
            file_size=1,
            lease_token=old.stage_lease_token,
        )

    assert store.get_job(job.id) == before


def test_normal_arriving_after_historical_file_check_still_wins_transaction(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = _store(sqlite_path, mac_jobs_root)
    historical, _ = _repair_candidate(store, mac_jobs_root, "old-204")
    normal = store.submit_job("new-204", priority=100, force=False).job
    assert normal is not None
    real_validate = store._validate_historical_source_files

    def validate_then_arrive(repair):
        real_validate(repair)
        with store.connection() as conn:
            conn.execute(
                "UPDATE jobs SET status = ? WHERE id = ?",
                (JobStatus.TRANSCRIPTION_DONE.value, normal.id),
            )

    monkeypatch.setattr(store, "_validate_historical_source_files", validate_then_arrive)

    assert store.claim_next_historical_repair("mac-1", 60) is None
    assert store.get_job(historical.id).status is JobStatus.ENGLISH_SRT_READY
    claimed = store.claim_next_translation_job("mac-1", 60, origin="normal")
    assert claimed is not None and claimed.id == normal.id
