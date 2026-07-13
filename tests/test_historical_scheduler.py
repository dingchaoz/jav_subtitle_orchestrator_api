from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
from pathlib import Path
from datetime import UTC, datetime, timedelta

import pytest

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import (
    HISTORICAL_TRANSLATION_ORIGIN,
    NORMAL_TRANSLATION_ORIGIN,
    HistoricalRepairActivationError,
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


@pytest.mark.parametrize(
    ("status", "retry_column"),
    [
        (JobStatus.PUBLISH_PENDING, "next_publish_attempt_at"),
        (JobStatus.CATALOG_SYNC_PENDING, "next_catalog_sync_attempt_at"),
        (JobStatus.FAILED, None),
    ],
)
def test_running_historical_unit_blocks_activation_of_second_repair(
    sqlite_path, mac_jobs_root, status, retry_column
):
    store = _store(sqlite_path, mac_jobs_root)
    first, _ = _repair_candidate(store, mac_jobs_root, "old-011")
    second, _ = _repair_candidate(store, mac_jobs_root, "old-012")
    future = (datetime.now(UTC) + timedelta(hours=1)).replace(microsecond=0).isoformat()
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ? WHERE job_id = ?",
            (HistoricalRepairState.RUNNING.value, first.id),
        )
        if retry_column is None:
            conn.execute(
                "UPDATE jobs SET status = ?, translation_origin = ? WHERE id = ?",
                (status.value, HISTORICAL_TRANSLATION_ORIGIN, first.id),
            )
        else:
            conn.execute(
                f"UPDATE jobs SET status = ?, translation_origin = ?, "
                f"{retry_column} = ? WHERE id = ?",
                (status.value, HISTORICAL_TRANSLATION_ORIGIN, future, first.id),
            )

    assert store.claim_next_historical_repair("mac-1", 60) is None
    assert store.get_historical_repair(second.id).state is HistoricalRepairState.PENDING


def test_historical_lane_pause_is_durable_and_does_not_hide_normal_work(sqlite_path, mac_jobs_root):
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


def test_historical_quality_failure_counter_is_durable_atomic_and_resets(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)

    first = store.record_historical_quality_failure(3)
    restarted = _store(sqlite_path, mac_jobs_root)
    second = restarted.record_historical_quality_failure(3)
    third = restarted.record_historical_quality_failure(3)

    assert first.consecutive_quality_failures == 1
    assert second.consecutive_quality_failures == 2
    assert third.consecutive_quality_failures == 3
    assert third.paused is True
    assert third.reason_code == "quality_failure_limit"
    reset = restarted.reset_historical_quality_failures()
    assert reset.consecutive_quality_failures == 0


def test_initialize_migrates_legacy_historical_control_counter(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    with store.connection() as conn:
        conn.execute(
            "ALTER TABLE historical_repair_control "
            "DROP COLUMN consecutive_quality_failures"
        )

    restarted = _store(sqlite_path, mac_jobs_root)

    assert restarted.historical_lane_state().consecutive_quality_failures == 0


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

    def validate_then_arrive(repair, job_fd):
        real_validate(repair, job_fd)
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


def test_historical_job_lock_is_still_held_inside_activation_transaction(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = _store(sqlite_path, mac_jobs_root)
    _repair_candidate(store, mac_jobs_root, "old-205")
    real_check = store._has_claimable_normal_work_conn
    observed = []

    def check_while_transaction_is_open(conn, now):
        script = (
            "import fcntl, os, sys; "
            "fd=os.open(sys.argv[1], os.O_RDONLY|os.O_DIRECTORY); "
            "\ntry: fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
            "except BlockingIOError: sys.exit(17)\n"
            "else: sys.exit(0)"
        )
        result = subprocess.run(
            [sys.executable, "-c", script, str(mac_jobs_root / "old-205")],
            check=False,
        )
        observed.append(result.returncode)
        return real_check(conn, now)

    monkeypatch.setattr(store, "_has_claimable_normal_work_conn", check_while_transaction_is_open)

    assert store.claim_next_historical_repair("mac-1", 60) is not None
    assert observed == [17]


def test_invalid_files_are_not_rejected_if_normal_work_arrives_before_transaction(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = _store(sqlite_path, mac_jobs_root)
    historical, _ = _repair_candidate(store, mac_jobs_root, "old-206")
    normal = store.submit_job("new-206", priority=100, force=False).job
    assert normal is not None

    def fail_after_normal_arrives(_repair, _job_fd):
        with store.connection() as conn:
            conn.execute(
                "UPDATE jobs SET status = ? WHERE id = ?",
                (JobStatus.TRANSCRIPTION_DONE.value, normal.id),
            )
        raise RuntimeError("preservation_hash_changed")

    monkeypatch.setattr(store, "_validate_historical_source_files", fail_after_normal_arrives)

    assert store.claim_next_historical_repair("mac-1", 60) is None
    assert store.get_job(historical.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_historical_repair(historical.id).state is HistoricalRepairState.PENDING


def test_activation_rejects_symlinked_rejected_directory(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _repair_candidate(store, mac_jobs_root, "old-207")
    repair = store.get_historical_repair(job.id)
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_file = outside / store.historical_source_quarantine_path(repair).name
    outside_file.write_bytes(paths.english_srt_path_mac.read_bytes())
    paths.english_srt_path_mac.unlink()
    (paths.job_dir_mac / "rejected").symlink_to(outside, target_is_directory=True)

    with pytest.raises(
        HistoricalRepairActivationError,
        match="preservation_hash_changed",
    ):
        store.claim_next_historical_repair("mac-1", 60)

    assert outside_file.exists()
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.PERMANENT_FAILED


def test_quality_terminal_and_counter_rollback_together_on_injected_failure(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _repair_candidate(store, mac_jobs_root, "old-208")
    claimed = store.claim_next_historical_repair("mac-1", 60)
    assert claimed is not None
    with store.connection() as conn:
        conn.execute(
            """
            CREATE TRIGGER reject_quality_terminal
            BEFORE UPDATE OF state ON historical_translation_repairs
            WHEN NEW.state = 'permanent_failed'
            BEGIN
              SELECT RAISE(ABORT, 'injected quality failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected quality failure"):
        store.fail_historical_translation_permanent(
            job.id,
            "mac-1",
            lease_token=claimed.stage_lease_token,
            reason_code="quality_gate_failed:collapsed_output",
            quality_failure_limit=3,
        )

    assert store.get_job(job.id).status is JobStatus.TRANSLATING
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.RUNNING
    assert store.historical_lane_state().consecutive_quality_failures == 0


def test_reconciler_fails_closed_for_unknown_running_failed_repair(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _repair_candidate(store, mac_jobs_root, "old-209")
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ? WHERE job_id = ?",
            (HistoricalRepairState.RUNNING.value, job.id),
        )
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = ?, error = ? "
            "WHERE id = ?",
            (
                JobStatus.FAILED.value,
                HISTORICAL_TRANSLATION_ORIGIN,
                "publishing: transient legacy failure",
                job.id,
            ),
        )

    assert store.reconcile_orphaned_historical_repairs() == 1
    repair = store.get_historical_repair(job.id)
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code == "historical_orphaned_terminal_state"
    assert store.claim_next_historical_repair("mac-1", 60) is None


def test_reconciler_preserves_receipts_for_exhausted_publication_orphan(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _repair_candidate(store, mac_jobs_root, "old-211")
    receipt = {
        "published_subtitle_id": "subtitle-existing",
        "published_storage_path": "subtitles/old-211/English_AI.srt",
        "published_content_sha256": "c" * 64,
        "published_file_size": 123,
        "catalog_movie_uuid": "movie-existing",
    }
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ? WHERE job_id = ?",
            (HistoricalRepairState.RUNNING.value, job.id),
        )
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = ?, error = ?, "
            "publish_attempt_count = 3, published_subtitle_id = ?, "
            "published_storage_path = ?, published_content_sha256 = ?, "
            "published_file_size = ?, catalog_movie_uuid = ? WHERE id = ?",
            (
                JobStatus.FAILED.value,
                HISTORICAL_TRANSLATION_ORIGIN,
                "publishing: publication_failed",
                receipt["published_subtitle_id"],
                receipt["published_storage_path"],
                receipt["published_content_sha256"],
                receipt["published_file_size"],
                receipt["catalog_movie_uuid"],
                job.id,
            ),
        )

    assert store.reconcile_orphaned_historical_repairs(
        max_publish_attempts=3
    ) == 1

    repair = store.get_historical_repair(job.id)
    refreshed = store.get_job(job.id)
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code == "publication_attempts_exhausted"
    assert refreshed.status is JobStatus.FAILED
    for field, expected in receipt.items():
        assert getattr(refreshed, field) == expected
    assert store.claim_next_historical_repair("mac-1", 60) is None


def test_reconciler_counts_legacy_quality_failure_exactly_once(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _repair_candidate(store, mac_jobs_root, "old-212")
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ? WHERE job_id = ?",
            (HistoricalRepairState.RUNNING.value, job.id),
        )
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = ?, error = ? "
            "WHERE id = ?",
            (
                JobStatus.FAILED.value,
                HISTORICAL_TRANSLATION_ORIGIN,
                "historical_repair: quality_gate_failed:dominant_text_collapse",
                job.id,
            ),
        )

    assert store.reconcile_orphaned_historical_repairs(
        quality_failure_limit=1
    ) == 1
    assert store.reconcile_orphaned_historical_repairs(
        quality_failure_limit=1
    ) == 0

    repair = store.get_historical_repair(job.id)
    lane = store.historical_lane_state()
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code == (
        "quality_gate_failed:dominant_text_collapse"
    )
    assert lane.consecutive_quality_failures == 1
    assert lane.paused is True
    assert lane.reason_code == "quality_failure_limit"


def test_reconciler_quality_terminal_and_counter_rollback_together(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _repair_candidate(store, mac_jobs_root, "old-214")
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ? WHERE job_id = ?",
            (HistoricalRepairState.RUNNING.value, job.id),
        )
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = ?, error = ? "
            "WHERE id = ?",
            (
                JobStatus.FAILED.value,
                HISTORICAL_TRANSLATION_ORIGIN,
                "historical_repair: quality_gate_failed:collapsed_output",
                job.id,
            ),
        )
        conn.execute(
            """
            CREATE TRIGGER reject_reconciled_quality_counter
            BEFORE UPDATE OF consecutive_quality_failures
            ON historical_repair_control
            BEGIN
              SELECT RAISE(ABORT, 'injected reconciler counter failure');
            END
            """
        )

    with pytest.raises(
        sqlite3.IntegrityError,
        match="injected reconciler counter failure",
    ):
        store.reconcile_orphaned_historical_repairs()

    assert store.get_job(job.id).status is JobStatus.FAILED
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.RUNNING
    assert store.historical_lane_state().consecutive_quality_failures == 0


def test_reconciler_retries_only_explicit_unexhausted_lease_orphan(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _repair_candidate(store, mac_jobs_root, "old-213")
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ?, "
            "attempt_count = 1 WHERE job_id = ?",
            (HistoricalRepairState.RUNNING.value, job.id),
        )
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = ?, error = ? "
            "WHERE id = ?",
            (
                JobStatus.FAILED.value,
                HISTORICAL_TRANSLATION_ORIGIN,
                "historical_repair: translation_lease_expired",
                job.id,
            ),
        )

    assert store.reconcile_orphaned_historical_repairs(
        max_translation_attempts=3
    ) == 1
    repair = store.get_historical_repair(job.id)
    assert repair.state is HistoricalRepairState.RETRY_WAIT
    assert repair.reason_code == "historical_orphaned_transient_retry"


def test_paused_lane_blocks_inflight_historical_claim(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _repair_candidate(store, mac_jobs_root, "old-210")
    claimed = store.claim_next_historical_repair("setup", 60)
    assert claimed is not None
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = NULL, "
            "lease_expires_at = NULL, stage_lease_token = NULL, "
            "english_srt_path_mac = ? WHERE id = ?",
            (JobStatus.PUBLISH_PENDING.value, str(paths.english_srt_path_mac), job.id),
        )
    store.pause_historical_lane("publication_configuration_missing")

    assert store.claim_inflight_historical_stage("mac-1", 60) is None
    assert store.claim_publication_job(
        "mac-1",
        60,
        job_id=job.id,
        origin=HISTORICAL_TRANSLATION_ORIGIN,
    ) is None
    assert store.claim_publication_job(
        "mac-1", 60, job_id=job.id
    ) is None
