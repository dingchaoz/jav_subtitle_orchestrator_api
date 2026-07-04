import sqlite3
from contextlib import closing

import pytest

from orchestrator.models import JobStatus
from orchestrator.store import JobStore


class TrackingJobStore(JobStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connections = []

    def connect(self):
        conn = super().connect()
        self.connections.append(conn)
        return conn


class TracingJobStore(JobStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.statements = []

    def connect(self):
        conn = super().connect()
        conn.set_trace_callback(self.statements.append)
        return conn


def assert_connection_closed(conn):
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")


def set_job_fields(sqlite_path, job_id, **fields):
    assignments = ", ".join(f"{field} = ?" for field in fields)
    values = list(fields.values())
    with closing(sqlite3.connect(sqlite_path)) as conn:
        with conn:
            conn.execute(f"UPDATE jobs SET {assignments} WHERE id = ?", (*values, job_id))


def test_submit_job_creates_sqlite_row(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    result = store.submit_job("KTB-096", priority=100, force=False)

    assert result.kind == "created"
    assert result.job.movie_number == "KTB-096"
    assert result.job.normalized_movie_number == "ktb-096"
    assert result.job.status == JobStatus.QUEUED
    assert result.job.priority == 100
    assert result.job.job_dir_mac == str(mac_jobs_root / "ktb-096")
    assert result.job.job_dir_windows == "M:\\ktb-096"


def test_initialize_closes_connection(sqlite_path, mac_jobs_root):
    store = TrackingJobStore(sqlite_path, mac_jobs_root, "M:\\")

    store.initialize()

    assert len(store.connections) == 1
    assert_connection_closed(store.connections[0])


def test_submit_job_closes_connection_after_write(sqlite_path, mac_jobs_root):
    store = TrackingJobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.connections.clear()

    store.submit_job("KTB-096", priority=100, force=False)

    assert len(store.connections) == 1
    assert_connection_closed(store.connections[0])


def test_submit_job_starts_immediate_transaction(sqlite_path, mac_jobs_root):
    store = TracingJobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.statements.clear()

    store.submit_job("KTB-096", priority=100, force=False)

    assert "BEGIN IMMEDIATE" in store.statements


def test_duplicate_submit_returns_existing(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    created = store.submit_job("ktb-096", priority=100, force=False)

    existing = store.submit_job("KTB096", priority=10, force=False)

    assert existing.kind == "existing"
    assert existing.job.id == created.job.id
    assert existing.job.priority == 100


def test_force_submit_resets_existing_job_and_clears_outputs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    created = store.submit_job("ktb-096", priority=100, force=False)
    set_job_fields(
        sqlite_path,
        created.job.id,
        status=JobStatus.FAILED.value,
        claimed_by="worker-1",
        lease_expires_at="2026-07-04T12:00:00+00:00",
        error="transcription failed",
        metadata_path_mac="/tmp/metadata.json",
        audio_path_mac="/tmp/audio.wav",
        audio_path_windows="M:\\ktb-096\\audio.wav",
        japanese_srt_path_mac="/tmp/ktb-096.Japanese.srt",
        japanese_srt_path_windows="M:\\ktb-096\\ktb-096.Japanese.srt",
        english_srt_path_mac="/tmp/ktb-096.English.srt",
        english_srt_path_windows="M:\\ktb-096\\ktb-096.English.srt",
    )

    result = store.submit_job("KTB096", priority=10, force=True)

    assert result.kind == "created"
    assert result.job.id == created.job.id
    assert result.job.status == JobStatus.QUEUED
    assert result.job.claimed_by is None
    assert result.job.lease_expires_at is None
    assert result.job.error is None
    assert result.job.metadata_path_mac is None
    assert result.job.audio_path_mac is None
    assert result.job.audio_path_windows is None
    assert result.job.japanese_srt_path_mac is None
    assert result.job.japanese_srt_path_windows is None
    assert result.job.english_srt_path_mac is None
    assert result.job.english_srt_path_windows is None


@pytest.mark.parametrize(
    "active_status",
    [
        JobStatus.TRANSCRIPTION_CLAIMED,
        JobStatus.TRANSCRIBING,
        JobStatus.TRANSLATING,
    ],
)
def test_force_submit_returns_conflict_for_active_worker_statuses(
    sqlite_path,
    mac_jobs_root,
    active_status,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    created = store.submit_job("ktb-096", priority=100, force=False)
    set_job_fields(sqlite_path, created.job.id, status=active_status.value, claimed_by="worker-1")

    result = store.submit_job("KTB096", priority=10, force=True)

    assert result.kind == "conflict"
    assert result.job.id == created.job.id
    assert result.job.status == active_status
    assert result.job.claimed_by == "worker-1"


def test_submit_invalid_movie_number(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    result = store.submit_job("bad id", priority=100, force=False)

    assert result.kind == "invalid"
    assert result.job is None
    assert result.movie_number == "bad id"


def test_batch_submission_groups_created_existing_and_invalid(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-095", priority=100, force=False)

    result = store.submit_batch(["ktb-096", "ktb-095", "bad id"], priority=100, force=False)

    assert [item.job.normalized_movie_number for item in result.created] == ["ktb-096"]
    assert [item.job.normalized_movie_number for item in result.existing] == ["ktb-095"]
    assert [item.movie_number for item in result.invalid] == ["bad id"]
