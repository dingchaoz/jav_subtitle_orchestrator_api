import sqlite3
from contextlib import closing

import pytest

import orchestrator.store as store_module
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


def test_translation_origin_constants_are_stable():
    assert getattr(store_module, "NORMAL_TRANSLATION_ORIGIN", None) == "normal"
    assert getattr(store_module, "HISTORICAL_TRANSLATION_ORIGIN", None) == "historical"


def test_initialize_migrates_legacy_row_idempotently(
    sqlite_path, mac_jobs_root
):
    with closing(sqlite3.connect(sqlite_path)) as conn:
        with conn:
            conn.execute(
                """
                CREATE TABLE jobs (
                  id TEXT PRIMARY KEY,
                  movie_number TEXT NOT NULL,
                  normalized_movie_number TEXT NOT NULL UNIQUE,
                  status TEXT NOT NULL,
                  priority INTEGER NOT NULL DEFAULT 100,
                  attempt_count INTEGER NOT NULL DEFAULT 0,
                  worker_attempt_count INTEGER NOT NULL DEFAULT 0,
                  translation_attempt_count INTEGER NOT NULL DEFAULT 0,
                  claimed_by TEXT,
                  lease_expires_at TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  error TEXT,
                  job_dir_mac TEXT NOT NULL,
                  job_dir_windows TEXT NOT NULL,
                  metadata_path_mac TEXT,
                  audio_path_mac TEXT,
                  audio_path_windows TEXT,
                  japanese_srt_path_mac TEXT,
                  japanese_srt_path_windows TEXT,
                  english_srt_path_mac TEXT,
                  english_srt_path_windows TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO jobs (
                  id, movie_number, normalized_movie_number, status, priority,
                  attempt_count, worker_attempt_count, translation_attempt_count,
                  created_at, updated_at, error, job_dir_mac, job_dir_windows
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "legacy-job",
                    "KTB-096",
                    "ktb-096",
                    JobStatus.FAILED.value,
                    7,
                    5,
                    3,
                    2,
                    "2026-07-01T00:00:00+00:00",
                    "2026-07-02T00:00:00+00:00",
                    "legacy failure",
                    "/tmp/legacy-job",
                    "M:\\legacy-job",
                ),
            )

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.row_factory = sqlite3.Row
        columns = {
            row["name"]: (row["type"].upper(), row["notnull"], row["dflt_value"])
            for row in conn.execute("PRAGMA table_info(jobs)")
        }
        migrated = conn.execute(
            "SELECT * FROM jobs WHERE id = ?", ("legacy-job",)
        ).fetchone()
        first_schema = conn.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE tbl_name = 'historical_translation_repairs' "
            "ORDER BY type, name"
        ).fetchall()

    store.initialize()

    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.row_factory = sqlite3.Row
        remigrated = conn.execute(
            "SELECT * FROM jobs WHERE id = ?", ("legacy-job",)
        ).fetchone()
        second_schema = conn.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE tbl_name = 'historical_translation_repairs' "
            "ORDER BY type, name"
        ).fetchall()
        foreign_key_violations = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert migrated["movie_number"] == "KTB-096"
    assert migrated["priority"] == 7
    assert migrated["attempt_count"] == 5
    assert migrated["error"] == "legacy failure"
    assert dict(remigrated) == dict(migrated)
    assert columns["publish_attempt_count"] == ("INTEGER", 1, "0")
    assert columns["next_publish_attempt_at"] == ("TEXT", 0, None)
    assert columns["catalog_movie_uuid"] == ("TEXT", 0, None)
    assert columns["metadata_status"] == ("TEXT", 0, None)
    assert columns["metadata_source"] == ("TEXT", 0, None)
    assert migrated["translation_origin"] == store_module.NORMAL_TRANSLATION_ORIGIN
    assert migrated["published_subtitle_id"] is None
    assert migrated["published_storage_path"] is None
    assert migrated["published_content_sha256"] is None
    assert migrated["published_file_size"] is None
    assert migrated["catalog_sync_attempt_count"] == 0
    assert migrated["next_catalog_sync_attempt_at"] is None
    assert migrated["catalog_lease_token"] is None
    assert any(row["type"] == "table" for row in first_schema)
    assert [tuple(row) for row in second_schema] == [tuple(row) for row in first_schema]
    assert foreign_key_violations == []


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


def test_submit_finds_legacy_unpadded_normalized_alias(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    created = store.submit_job("abc-7", priority=100, force=False)
    set_job_fields(
        sqlite_path,
        created.job.id,
        normalized_movie_number="abc-7",
    )

    existing = store.submit_job("abc-007", priority=10, force=False)

    assert existing.kind == "existing"
    assert existing.job.id == created.job.id
    assert existing.job.normalized_movie_number == "abc-7"


def test_force_submit_resets_existing_job_and_clears_outputs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    created = store.submit_job("ktb-096", priority=100, force=False)
    set_job_fields(
        sqlite_path,
        created.job.id,
        status=JobStatus.FAILED.value,
        translation_attempt_count=2,
        publish_attempt_count=2,
        next_publish_attempt_at="2026-07-12T12:00:00+00:00",
        catalog_movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status="complete",
        metadata_source="public",
        translation_origin=store_module.HISTORICAL_TRANSLATION_ORIGIN,
        published_subtitle_id="published-subtitle-id",
        published_storage_path="subtitles/ktb-096.English.srt",
        published_content_sha256="a" * 64,
        published_file_size=1234,
        catalog_sync_attempt_count=3,
        next_catalog_sync_attempt_at="2026-07-12T13:00:00+00:00",
        catalog_lease_token="stale-catalog-lease-token",
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
    assert result.job.translation_attempt_count == 0
    assert result.job.publish_attempt_count == 0
    assert result.job.next_publish_attempt_at is None
    assert result.job.catalog_movie_uuid is None
    assert result.job.metadata_status is None
    assert result.job.metadata_source is None
    assert result.job.translation_origin == store_module.NORMAL_TRANSLATION_ORIGIN
    assert result.job.published_subtitle_id is None
    assert result.job.published_storage_path is None
    assert result.job.published_content_sha256 is None
    assert result.job.published_file_size is None
    assert result.job.catalog_sync_attempt_count == 0
    assert result.job.next_catalog_sync_attempt_at is None
    assert result.job.catalog_lease_token is None
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
        JobStatus.TRANSCRIPTION_DONE,
        JobStatus.TRANSLATING,
        JobStatus.PUBLISHING,
        JobStatus.CATALOG_SYNCING,
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
