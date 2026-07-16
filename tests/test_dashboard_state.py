import json
import sqlite3
from contextlib import closing

import pytest

from orchestrator.dashboard import build_dashboard_state, build_job_browser, build_job_detail
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


_TEST_SHA256 = "1" * 64


def insert_historical_repair(
    store,
    sqlite_path,
    movie_code,
    state,
    *,
    job_status=JobStatus.ENGLISH_SRT_READY,
    created_at="2026-07-05T10:00:00+00:00",
    reason_code=None,
):
    job = store.submit_job(movie_code, priority=100, force=False).job
    repair_id = f"repair_{movie_code.replace('-', '_')}"
    batch_id = f"batch_{movie_code.replace('-', '_')}"
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = 'historical', "
            "claimed_by = ?, updated_at = ? WHERE id = ?",
            (
                job_status.value,
                "mac-translation-1" if state == "running" else None,
                created_at,
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
            ) VALUES (?, ?, ?, ?, ?, ?, 1, NULL, ?, ?, ?, ?, ?, NULL, ?, ?)
            """,
            (
                repair_id,
                batch_id,
                job.id,
                movie_code,
                _TEST_SHA256,
                state,
                reason_code,
                _TEST_SHA256,
                _TEST_SHA256,
                _TEST_SHA256,
                _TEST_SHA256,
                created_at,
                created_at,
            ),
        )
        conn.commit()
    return job, repair_id, batch_id


def set_job_recency(sqlite_path, job_id, *, created_at, updated_at):
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE jobs SET created_at = ?, updated_at = ? WHERE id = ?",
            (created_at, updated_at, job_id),
        )
        conn.commit()


def set_job_status_and_recency(sqlite_path, job_id, *, status, created_at, updated_at):
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, created_at = ?, updated_at = ? WHERE id = ?",
            (status.value, created_at, updated_at, job_id),
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
    assert state.audio_cleanup.enabled is True
    assert state.audio_cleanup.trigger == "verified_supabase_publication"
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


def test_dashboard_state_reports_disabled_audio_cleanup(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    state = build_dashboard_state(store, delete_audio_after_publish=False)

    assert state.audio_cleanup.enabled is False


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
    assert state.activity["processing"]["status"] == "transcribing"
    assert state.activity["processing"]["movie_number"] == "ktb-095"
    assert state.activity["processing"]["worker_id"] == "windows-gpu-1"
    assert state.activity["windows"]["status"] == "transcribing"
    assert state.activity["windows"]["movie_number"] == "ktb-095"
    assert state.activity["windows"]["worker_id"] == "windows-gpu-1"


def test_build_dashboard_state_includes_idle_windows_worker_health(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    store.record_worker_idle(
        "windows-gpu-1",
        role="windows",
        last_ip="192.168.1.201",
        stage="polling",
    )

    state = build_dashboard_state(store)

    assert len(state.workers) == 1
    worker = state.workers[0]
    assert worker.worker_id == "windows-gpu-1"
    assert worker.role == "windows"
    assert worker.state == "idle"
    assert worker.status == "online"
    assert worker.last_ip == "192.168.1.201"
    assert worker.current_job_id is None
    assert state.activity["windows"]["status"] == "idle"
    assert state.activity["windows"]["worker_id"] == "windows-gpu-1"
    assert state.activity["windows"]["updated_at"] == worker.last_seen_at


def test_dashboard_state_separates_windows_and_mac_translation_workers(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    windows_job = store.submit_job("abc-201", priority=100, force=False).job
    translation_job = store.submit_job("abc-202", priority=100, force=False).job

    store.record_worker_processing(
        "windows-gpu-1",
        role="windows_transcriber",
        job=windows_job,
        stage="transcribing",
    )
    store.record_worker_processing(
        "mac-translation-1",
        role="mac_translator",
        job=translation_job,
        stage="translating",
    )

    state = build_dashboard_state(store)

    assert state.activity["windows"]["worker_id"] == "windows-gpu-1"
    assert state.activity["windows"]["status"] == "transcribing"
    assert state.activity["translation"]["worker_id"] == "mac-translation-1"
    assert state.activity["translation"]["status"] == "translating"
    assert {worker.role for worker in state.workers} == {
        "windows_transcriber",
        "mac_translator",
    }


def test_dashboard_separates_normal_and_historical_translation_activity(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    normal = store.submit_job("new-001", priority=100, force=False).job
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = 'normal', "
            "claimed_by = ?, updated_at = ? WHERE id = ?",
            (
                JobStatus.CATALOG_SYNCING.value,
                "mac-translation-1",
                "2026-07-05T12:00:00+00:00",
                normal.id,
            ),
        )
        conn.commit()
    historical, repair_id, batch_id = insert_historical_repair(
        store,
        sqlite_path,
        "old-001",
        "running",
        job_status=JobStatus.PUBLISHING,
        created_at="2026-07-05T11:00:00+00:00",
    )

    state = build_dashboard_state(store)

    assert state.activity["mac_translation"]["movie_number"] == "new-001"
    assert state.activity["mac_translation"]["job_id"] == normal.id
    assert state.activity["historical_translation"]["movie_number"] == "old-001"
    assert state.activity["historical_translation"]["job_id"] == historical.id
    assert state.activity["historical_translation"]["stage"] == "publishing"
    assert state.activity["historical_translation"]["state"] == "running"
    assert state.historical_repairs.current is not None
    assert state.historical_repairs.current.repair_id == repair_id
    assert state.historical_repairs.current.batch_id == batch_id


def test_historical_worker_heartbeat_never_appears_as_normal_translation(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    historical, _, _ = insert_historical_repair(
        store,
        sqlite_path,
        "old-002",
        "running",
        job_status=JobStatus.TRANSLATING,
    )
    store.record_worker_processing(
        "mac-translation-1",
        role="mac_translator",
        job=store.get_job(historical.id),
        stage="translating",
    )

    state = build_dashboard_state(store)

    assert state.activity["mac_translation"]["status"] == "idle"
    assert state.activity["mac_translation"]["movie_number"] is None
    assert state.activity["historical_translation"]["movie_number"] == "old-002"


def test_dashboard_historical_repair_counts_cover_each_reported_state(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    states = (
        "pending",
        "running",
        "retry_wait",
        "succeeded",
        "permanent_failed",
        "planned",
        "paused",
    )
    for index, repair_state in enumerate(states):
        insert_historical_repair(
            store,
            sqlite_path,
            f"old-{index + 10:03d}",
            repair_state,
            job_status=(
                JobStatus.TRANSLATING
                if repair_state == "running"
                else JobStatus.ENGLISH_SRT_READY
            ),
            created_at=f"2026-07-05T10:00:{index:02d}+00:00",
        )

    progress = build_dashboard_state(store).historical_repairs

    assert progress.counts.model_dump() == {
        "total": 7,
        "planned": 1,
        "pending": 1,
        "running": 1,
        "retry_wait": 1,
        "paused": 1,
        "succeeded": 1,
        "permanent_failed": 1,
        "unknown": 0,
    }
    assert sum(
        count
        for name, count in progress.counts.model_dump().items()
        if name != "total"
    ) == progress.counts.total


@pytest.mark.parametrize(
    ("repair_state", "job_status", "reason_code"),
    (
        ("pending", JobStatus.ENGLISH_SRT_READY, None),
        (
            "retry_wait",
            JobStatus.FAILED,
            "historical_orphaned_transient_retry",
        ),
        ("paused", JobStatus.FAILED, "preservation_hash_changed"),
    ),
)
def test_dashboard_reports_nonrunning_nonterminal_repair_as_current(
    sqlite_path,
    mac_jobs_root,
    repair_state,
    job_status,
    reason_code,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, repair_id, batch_id = insert_historical_repair(
        store,
        sqlite_path,
        "old-050",
        repair_state,
        job_status=job_status,
        reason_code=reason_code,
    )

    state = build_dashboard_state(store)
    current = state.historical_repairs.current

    assert current is not None
    assert current.batch_id == batch_id
    assert current.repair_id == repair_id
    assert current.job_id == job.id
    assert current.movie_number == "old-050"
    assert current.state == repair_state
    assert current.stage == job_status.value
    assert current.reason_code == reason_code
    assert state.activity["historical_translation"]["movie_number"] == "old-050"
    assert state.activity["historical_translation"]["state"] == repair_state
    assert state.activity["historical_translation"]["stage"] == job_status.value


def test_dashboard_current_repair_uses_state_priority_before_creation_time(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    insert_historical_repair(
        store, sqlite_path, "old-060", "planned",
        created_at="2026-07-05T10:00:00+00:00",
    )
    insert_historical_repair(
        store, sqlite_path, "old-061", "pending",
        created_at="2026-07-05T10:00:01+00:00",
    )
    retry_job, _, _ = insert_historical_repair(
        store, sqlite_path, "old-062", "retry_wait",
        job_status=JobStatus.FAILED,
        created_at="2026-07-05T10:00:02+00:00",
    )
    insert_historical_repair(
        store, sqlite_path, "old-063", "paused",
        job_status=JobStatus.FAILED,
        created_at="2026-07-05T10:00:03+00:00",
    )
    running_job, _, _ = insert_historical_repair(
        store, sqlite_path, "old-064", "running",
        job_status=JobStatus.PUBLISHING,
        created_at="2026-07-05T10:00:04+00:00",
    )

    first = build_dashboard_state(store).historical_repairs.current
    assert first is not None
    assert first.job_id == running_job.id

    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET state = 'succeeded' "
            "WHERE job_id = ?",
            (running_job.id,),
        )
        conn.commit()

    second = build_dashboard_state(store).historical_repairs.current
    assert second is not None
    assert second.job_id == retry_job.id


def test_dashboard_historical_pause_is_structured_and_unknown_reason_is_safe(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE historical_repair_control SET paused = 1, "
            "reason_code = 'quality_failure_limit', "
            "consecutive_quality_failures = 3, updated_at = ? WHERE singleton = 1",
            ("2026-07-05T13:00:00+00:00",),
        )
        conn.commit()

    progress = build_dashboard_state(store).historical_repairs

    assert progress.lane_paused is True
    assert progress.reason_code == "quality_failure_limit"
    assert progress.consecutive_quality_failures == 3
    assert progress.updated_at == "2026-07-05T13:00:00+00:00"

    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE historical_repair_control SET reason_code = ? WHERE singleton = 1",
            ("Authorization: Bearer SECRET-TOKEN /Users/private/subtitle.srt",),
        )
        conn.commit()

    assert build_dashboard_state(store).historical_repairs.reason_code == "historical_error"


def test_dashboard_multiple_running_repairs_reports_count_and_earliest_active(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    oldest, oldest_repair_id, _ = insert_historical_repair(
        store,
        sqlite_path,
        "old-101",
        "running",
        job_status=JobStatus.CATALOG_SYNC_PENDING,
        created_at="2026-07-05T10:00:00+00:00",
    )
    insert_historical_repair(
        store,
        sqlite_path,
        "old-102",
        "running",
        job_status=JobStatus.TRANSLATING,
        created_at="2026-07-05T10:00:01+00:00",
    )

    progress = build_dashboard_state(store).historical_repairs

    assert progress.counts.running == 2
    assert progress.current is not None
    assert progress.current.job_id == oldest.id
    assert progress.current.repair_id == oldest_repair_id
    assert progress.current.stage == "catalog_sync_pending"


def test_dashboard_historical_payload_omits_sensitive_record_fields(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _, _ = insert_historical_repair(
        store,
        sqlite_path,
        "old-201",
        "running",
        job_status=JobStatus.CATALOG_SYNCING,
        reason_code="quality_gate_failed:KNOWN_BAD_TRANSLATION",
    )
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE jobs SET published_storage_path = ?, "
            "published_content_sha256 = ?, stage_lease_token = ?, error = ? WHERE id = ?",
            (
                "/Users/private/old-201-English_AI.srt",
                "a" * 64,
                "LEASE-SECRET",
                "adult subtitle text SERVICE_ROLE_SECRET",
                job.id,
            ),
        )
        conn.commit()

    state = build_dashboard_state(store)
    payload = {
        "historical_repairs": state.historical_repairs.model_dump(mode="json"),
        "historical_translation": state.activity["historical_translation"],
    }
    rendered = json.dumps(payload)

    for forbidden_key in (
        "allowlist_sha256",
        "japanese_sha256",
        "audio_sha256",
        "source_english_sha256",
        "english_sha256",
        "storage_path",
        "lease_token",
        "subtitle_text",
        "metadata",
        "description",
    ):
        assert forbidden_key not in rendered
    for forbidden_value in (
        "/Users/private",
        "SERVICE_ROLE_SECRET",
        "LEASE-SECRET",
        "quality_gate_failed",
        "KNOWN_BAD_TRANSLATION",
        "a" * 64,
        _TEST_SHA256,
    ):
        assert forbidden_value not in rendered


def test_historical_dashboard_snapshot_uses_at_most_two_selects(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    insert_historical_repair(store, sqlite_path, "old-301", "pending")
    traced_selects = []
    original_connect = store.connect

    def traced_connect():
        conn = original_connect()
        conn.set_trace_callback(
            lambda sql: traced_selects.append(sql)
            if sql.lstrip().upper().startswith("SELECT")
            else None
        )
        return conn

    store.connect = traced_connect

    snapshot = store.historical_repair_dashboard_snapshot()

    assert snapshot.current is not None
    assert len(traced_selects) <= 2


def test_dashboard_missing_historical_control_row_fails_safe(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute("DELETE FROM historical_repair_control WHERE singleton = 1")
        conn.commit()

    progress = build_dashboard_state(store).historical_repairs

    assert progress.lane_paused is True
    assert progress.reason_code == "historical_controller_state_unavailable"
    assert progress.consecutive_quality_failures == 0
    assert progress.updated_at is None


def test_dashboard_orphaned_nonterminal_repair_still_has_safe_current(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, repair_id, _ = insert_historical_repair(
        store,
        sqlite_path,
        "old-399",
        "pending",
    )
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute("DELETE FROM jobs WHERE id = ?", (job.id,))
        conn.commit()

    progress = build_dashboard_state(store).historical_repairs

    assert progress.counts.pending == 1
    assert progress.current is not None
    assert progress.current.repair_id == repair_id
    assert progress.current.job_id == job.id
    assert progress.current.stage == "historical_error"


def test_dashboard_unknown_legacy_repair_state_fails_closed_in_counts(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    insert_historical_repair(
        store,
        sqlite_path,
        "old-398",
        "legacy_unknown",
    )

    state = build_dashboard_state(store)
    counts = state.historical_repairs.counts

    assert counts.total == 1
    assert counts.permanent_failed == 0
    assert counts.unknown == 1
    assert sum(
        count
        for name, count in counts.model_dump().items()
        if name != "total"
    ) == counts.total
    assert state.historical_repairs.current is not None
    assert state.historical_repairs.current.state == "unknown"
    assert state.historical_repairs.current.reason_code == "historical_error"
    assert state.activity["historical_translation"]["state"] == "unknown"
    assert "legacy_unknown" not in json.dumps(
        {
            "historical_repairs": state.historical_repairs.model_dump(mode="json"),
            "activity": state.activity["historical_translation"],
        }
    )


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
    assert state.activity["processing"]["movie_number"] == "ktb-106"
    assert state.activity["processing"]["worker_id"] == "windows-new"
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
    assert detail.translation_attempt_count == 0
    assert detail.publish_attempt_count == 0
    assert detail.next_publish_attempt_at is None
    assert detail.catalog_movie_uuid is None
    assert detail.metadata_status is None
    assert detail.metadata_source is None
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


def test_publication_states_are_active_processing_and_browser_jobs(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = store.submit_job("abc-021", priority=100, force=False).job
    publishing = store.submit_job("abc-022", priority=100, force=False).job
    set_job_status_and_recency(
        sqlite_path,
        pending.id,
        status=JobStatus.PUBLISH_PENDING,
        created_at="2026-07-05T11:00:01+00:00",
        updated_at="2026-07-05T12:00:01+00:00",
    )

    pending_state = build_dashboard_state(store)

    assert pending_state.counts["publish_pending"] == 1
    assert pending_state.activity["processing"]["status"] == "publish_pending"
    assert pending_state.activity["translation"]["status"] == "publish_pending"

    set_job_status_and_recency(
        sqlite_path,
        publishing.id,
        status=JobStatus.PUBLISHING,
        created_at="2026-07-05T11:00:02+00:00",
        updated_at="2026-07-05T12:00:02+00:00",
    )

    publishing_state = build_dashboard_state(store)
    browser = build_job_browser(store, view="active")

    assert publishing_state.counts["publish_pending"] == 1
    assert publishing_state.counts["publishing"] == 1
    assert publishing_state.activity["processing"]["status"] == "publishing"
    assert publishing_state.activity["translation"]["status"] == "publishing"
    assert browser.total == 2
    assert [item.status for item in browser.items] == [
        JobStatus.PUBLISHING,
        JobStatus.PUBLISH_PENDING,
    ]


def test_catalog_sync_states_are_active_processing_and_browser_jobs(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = store.submit_job("abc-031", priority=100, force=False).job
    syncing = store.submit_job("abc-032", priority=100, force=False).job
    set_job_status_and_recency(
        sqlite_path,
        pending.id,
        status=JobStatus.CATALOG_SYNC_PENDING,
        created_at="2026-07-05T11:00:01+00:00",
        updated_at="2026-07-05T12:00:01+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        syncing.id,
        status=JobStatus.CATALOG_SYNCING,
        created_at="2026-07-05T11:00:02+00:00",
        updated_at="2026-07-05T12:00:02+00:00",
    )

    state = build_dashboard_state(store)
    browser = build_job_browser(store, view="active")

    assert state.counts["catalog_sync_pending"] == 1
    assert state.counts["catalog_syncing"] == 1
    assert state.activity["processing"]["status"] == "catalog_syncing"
    assert state.activity["mac_translation"]["status"] == "catalog_syncing"
    assert [item.status for item in browser.items] == [
        JobStatus.CATALOG_SYNCING,
        JobStatus.CATALOG_SYNC_PENDING,
    ]


def test_build_job_browser_defaults_to_active_with_in_progress_before_queued(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    queued_high = store.submit_job("abc-001", priority=10, force=False).job
    queued_low = store.submit_job("abc-002", priority=90, force=False).job
    downloading_old = store.submit_job("abc-003", priority=100, force=False).job
    downloading_new = store.submit_job("abc-004", priority=100, force=False).job
    ready = store.submit_job("abc-005", priority=100, force=False).job
    failed = store.submit_job("abc-006", priority=100, force=False).job

    set_job_status_and_recency(
        sqlite_path,
        queued_high.id,
        status=JobStatus.QUEUED,
        created_at="2026-07-05T11:00:02+00:00",
        updated_at="2026-07-05T11:00:02+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        queued_low.id,
        status=JobStatus.QUEUED,
        created_at="2026-07-05T11:00:01+00:00",
        updated_at="2026-07-05T11:00:01+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        downloading_old.id,
        status=JobStatus.DOWNLOADING_AUDIO,
        created_at="2026-07-05T11:00:03+00:00",
        updated_at="2026-07-05T12:00:01+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        downloading_new.id,
        status=JobStatus.TRANSCRIBING,
        created_at="2026-07-05T11:00:04+00:00",
        updated_at="2026-07-05T12:00:02+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        ready.id,
        status=JobStatus.ENGLISH_SRT_READY,
        created_at="2026-07-05T11:00:05+00:00",
        updated_at="2026-07-05T12:00:03+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        failed.id,
        status=JobStatus.FAILED,
        created_at="2026-07-05T11:00:06+00:00",
        updated_at="2026-07-05T12:00:04+00:00",
    )

    browser = build_job_browser(store)

    assert browser.view == "active"
    assert browser.total == 4
    assert browser.pages == 1
    assert [job.movie_number for job in browser.items] == [
        "abc-004",
        "abc-003",
        "abc-001",
        "abc-002",
    ]


def test_build_job_browser_filters_sorts_and_paginates(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = store.submit_job("abc-001", priority=50, force=False).job
    second = store.submit_job("abc-002", priority=10, force=False).job
    third = store.submit_job("abc-003", priority=10, force=False).job
    ready_old = store.submit_job("abc-101", priority=100, force=False).job
    ready_new = store.submit_job("abc-102", priority=100, force=False).job

    set_job_status_and_recency(
        sqlite_path,
        first.id,
        status=JobStatus.QUEUED,
        created_at="2026-07-05T11:00:03+00:00",
        updated_at="2026-07-05T11:00:03+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        second.id,
        status=JobStatus.QUEUED,
        created_at="2026-07-05T11:00:02+00:00",
        updated_at="2026-07-05T11:00:02+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        third.id,
        status=JobStatus.QUEUED,
        created_at="2026-07-05T11:00:01+00:00",
        updated_at="2026-07-05T11:00:01+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        ready_old.id,
        status=JobStatus.ENGLISH_SRT_READY,
        created_at="2026-07-05T11:00:04+00:00",
        updated_at="2026-07-05T12:00:01+00:00",
    )
    set_job_status_and_recency(
        sqlite_path,
        ready_new.id,
        status=JobStatus.ENGLISH_SRT_READY,
        created_at="2026-07-05T11:00:05+00:00",
        updated_at="2026-07-05T12:00:02+00:00",
    )

    queued_page = build_job_browser(store, view="queued", page=1, page_size=2)
    ready_page = build_job_browser(store, view="ready")
    search_page = build_job_browser(store, view="all", q="102")

    assert queued_page.total == 3
    assert queued_page.pages == 2
    assert [job.movie_number for job in queued_page.items] == ["abc-003", "abc-002"]
    assert [job.movie_number for job in ready_page.items] == ["abc-102", "abc-101"]
    assert [job.movie_number for job in search_page.items] == ["abc-102"]


def test_ready_artifact_with_catalog_warning_stays_ready_and_exposes_diagnostics(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-104", priority=100, force=False).job
    with store.connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'english_srt_ready', artifact_status = 'ready',
                catalog_sync_status = 'failed',
                catalog_sync_warning_code = 'catalog_response_mismatch',
                catalog_sync_warning_message = 'Catalog synchronization failed.',
                catalog_sync_last_http_status = 207,
                catalog_sync_last_response_json = '{"success":false}',
                error = NULL
            WHERE id = ?
            """,
            (job.id,),
        )
    current = store.get_job(job.id)

    ready = build_job_browser(store, view="ready")
    failed = build_job_browser(store, view="failed")
    detail = build_job_detail(current)

    assert [item.id for item in ready.items] == [job.id]
    assert failed.items == []
    assert detail.status is JobStatus.ENGLISH_SRT_READY
    assert detail.artifact_status == "ready"
    assert detail.catalog_sync_status == "failed"
    assert detail.catalog_sync_warning_code == "catalog_response_mismatch"
    assert detail.catalog_sync_warning_message == "Catalog synchronization failed."
    assert detail.catalog_sync_last_http_status == 207
    assert detail.catalog_sync_last_response_json == '{"success":false}'
    assert detail.error is None
