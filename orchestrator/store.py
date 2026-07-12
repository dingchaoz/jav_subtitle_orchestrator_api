import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths, new_job_id, normalize_movie_number


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class JobRecord:
    id: str
    movie_number: str
    normalized_movie_number: str
    status: JobStatus
    priority: int
    attempt_count: int
    worker_attempt_count: int
    translation_attempt_count: int
    claimed_by: str | None
    lease_expires_at: str | None
    created_at: str
    updated_at: str
    error: str | None
    job_dir_mac: str
    job_dir_windows: str
    metadata_path_mac: str | None
    audio_path_mac: str | None
    audio_path_windows: str | None
    japanese_srt_path_mac: str | None
    japanese_srt_path_windows: str | None
    english_srt_path_mac: str | None
    english_srt_path_windows: str | None


@dataclass(frozen=True)
class WorkerStatusRecord:
    worker_id: str
    role: str
    state: str
    last_seen_at: str
    last_poll_at: str | None
    last_ip: str | None
    current_job_id: str | None
    current_movie_number: str | None
    stage: str | None
    updated_at: str
    last_error: str | None


@dataclass(frozen=True)
class SubmitResult:
    kind: Literal["created", "existing", "invalid", "conflict"]
    movie_number: str
    job: JobRecord | None = None


@dataclass(frozen=True)
class BatchSubmitResult:
    created: list[SubmitResult]
    existing: list[SubmitResult]
    invalid: list[SubmitResult]


class JobStore:
    def __init__(self, db_path: Path, jobs_root_mac: Path, jobs_root_windows: str) -> None:
        self.db_path = db_path
        self.jobs_root_mac = jobs_root_mac
        self.jobs_root_windows = jobs_root_windows

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        with closing(self.connect()) as conn:
            with conn:
                yield conn

    def initialize(self) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
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
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
            }
            if "translation_attempt_count" not in columns:
                conn.execute(
                    "ALTER TABLE jobs ADD COLUMN translation_attempt_count "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_status_priority_created "
                "ON jobs(status, priority, created_at)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS worker_statuses (
                  worker_id TEXT PRIMARY KEY,
                  role TEXT NOT NULL,
                  state TEXT NOT NULL,
                  last_seen_at TEXT NOT NULL,
                  last_poll_at TEXT,
                  last_ip TEXT,
                  current_job_id TEXT,
                  current_movie_number TEXT,
                  stage TEXT,
                  updated_at TEXT NOT NULL,
                  last_error TEXT
                )
                """
            )

    def submit_job(self, movie_number: str, priority: int, force: bool) -> SubmitResult:
        normalized = normalize_movie_number(movie_number)
        if normalized is None:
            return SubmitResult(kind="invalid", movie_number=movie_number)

        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = self._get_by_normalized(conn, normalized)
            if existing and not force:
                return SubmitResult(kind="existing", movie_number=movie_number, job=existing)
            if existing and force:
                return self._force_reset(conn, existing, movie_number)

            now = utc_now_iso()
            paths = build_job_paths(normalized, self.jobs_root_mac, self.jobs_root_windows)
            job_id = new_job_id()
            conn.execute(
                """
                INSERT INTO jobs (
                  id, movie_number, normalized_movie_number, status, priority,
                  attempt_count, worker_attempt_count, translation_attempt_count,
                  claimed_by, lease_expires_at,
                  created_at, updated_at, error, job_dir_mac, job_dir_windows,
                  metadata_path_mac, audio_path_mac, audio_path_windows,
                  japanese_srt_path_mac, japanese_srt_path_windows,
                  english_srt_path_mac, english_srt_path_windows
                )
                VALUES (
                  ?, ?, ?, ?, ?, 0, 0, 0, NULL, NULL, ?, ?, NULL, ?, ?,
                  NULL, NULL, NULL, NULL, NULL, NULL, NULL
                )
                """,
                (
                    job_id,
                    movie_number,
                    normalized,
                    JobStatus.QUEUED.value,
                    priority,
                    now,
                    now,
                    str(paths.job_dir_mac),
                    paths.job_dir_windows,
                ),
            )
            job = self.get_job(job_id, conn=conn)
            return SubmitResult(kind="created", movie_number=movie_number, job=job)

    def submit_batch(
        self,
        movie_numbers: list[str],
        priority: int,
        force: bool,
    ) -> BatchSubmitResult:
        created: list[SubmitResult] = []
        existing: list[SubmitResult] = []
        invalid: list[SubmitResult] = []
        for movie_number in movie_numbers:
            result = self.submit_job(movie_number, priority=priority, force=force)
            if result.kind == "created":
                created.append(result)
            elif result.kind in {"existing", "conflict"}:
                existing.append(result)
            else:
                invalid.append(result)
        return BatchSubmitResult(created=created, existing=existing, invalid=invalid)

    def get_job(self, job_id: str, conn: sqlite3.Connection | None = None) -> JobRecord | None:
        if conn is not None:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return self._row_to_job(row) if row else None

        with self.connection() as active_conn:
            row = active_conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return self._row_to_job(row) if row else None

    def list_jobs(self, status: JobStatus | None = None) -> list[JobRecord]:
        with self.connection() as conn:
            if status is None:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY priority ASC, created_at ASC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE status = ? ORDER BY priority ASC, created_at ASC",
                    (status.value,),
                ).fetchall()
            return [self._row_to_job(row) for row in rows]

    def record_worker_idle(
        self,
        worker_id: str,
        *,
        role: str = "windows",
        last_ip: str | None = None,
        stage: str | None = None,
        last_error: str | None = None,
        update_poll: bool = True,
    ) -> WorkerStatusRecord:
        now = utc_now_iso()
        last_error = last_error[:1000] if last_error else None
        last_poll_at = now if update_poll else None
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO worker_statuses (
                  worker_id, role, state, last_seen_at, last_poll_at, last_ip,
                  current_job_id, current_movie_number, stage, updated_at, last_error
                )
                VALUES (?, ?, 'idle', ?, ?, ?, NULL, NULL, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                  role = excluded.role, state = 'idle',
                  last_seen_at = excluded.last_seen_at,
                  last_poll_at = COALESCE(excluded.last_poll_at, worker_statuses.last_poll_at),
                  last_ip = COALESCE(excluded.last_ip, worker_statuses.last_ip),
                  current_job_id = NULL, current_movie_number = NULL,
                  stage = excluded.stage, updated_at = excluded.updated_at,
                  last_error = excluded.last_error
                """,
                (worker_id, role, now, last_poll_at, last_ip, stage, now, last_error),
            )
            status = self.get_worker_status(worker_id, conn=conn)
            assert status is not None
            return status

    def record_worker_processing(
        self,
        worker_id: str,
        *,
        role: str,
        job: JobRecord,
        stage: str,
        last_ip: str | None = None,
    ) -> WorkerStatusRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO worker_statuses (
                  worker_id, role, state, last_seen_at, last_poll_at, last_ip,
                  current_job_id, current_movie_number, stage, updated_at, last_error
                )
                VALUES (?, ?, 'processing', ?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(worker_id) DO UPDATE SET
                  role = excluded.role, state = 'processing',
                  last_seen_at = excluded.last_seen_at,
                  last_poll_at = excluded.last_poll_at,
                  last_ip = COALESCE(excluded.last_ip, worker_statuses.last_ip),
                  current_job_id = excluded.current_job_id,
                  current_movie_number = excluded.current_movie_number,
                  stage = excluded.stage, updated_at = excluded.updated_at,
                  last_error = NULL
                """,
                (
                    worker_id,
                    role,
                    now,
                    now,
                    last_ip,
                    job.id,
                    job.normalized_movie_number,
                    stage,
                    now,
                ),
            )
            status = self.get_worker_status(worker_id, conn=conn)
            assert status is not None
            return status

    def get_worker_status(
        self,
        worker_id: str,
        conn: sqlite3.Connection | None = None,
    ) -> WorkerStatusRecord | None:
        if conn is not None:
            row = conn.execute(
                "SELECT * FROM worker_statuses WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            return self._row_to_worker_status(row) if row else None
        with self.connection() as active_conn:
            row = active_conn.execute(
                "SELECT * FROM worker_statuses WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            return self._row_to_worker_status(row) if row else None

    def list_worker_statuses(self) -> list[WorkerStatusRecord]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM worker_statuses ORDER BY last_seen_at DESC, worker_id ASC"
            ).fetchall()
            return [self._row_to_worker_status(row) for row in rows]

    def update_download_status(
        self,
        job_id: str,
        status: JobStatus,
        *,
        metadata_path_mac: str | None = None,
        audio_path_mac: str | None = None,
        audio_path_windows: str | None = None,
        error: str | None = None,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?, metadata_path_mac = COALESCE(?, metadata_path_mac),
                    audio_path_mac = COALESCE(?, audio_path_mac),
                    audio_path_windows = COALESCE(?, audio_path_windows),
                    error = ?
                WHERE id = ?
                """,
                (
                    status.value,
                    now,
                    metadata_path_mac,
                    audio_path_mac,
                    audio_path_windows,
                    error,
                    job_id,
                ),
            )
            if cursor.rowcount == 0:
                raise KeyError(job_id)
            job = self.get_job(job_id, conn=conn)
            assert job is not None
            return job

    def mark_audio_ready(self, job_id: str) -> JobRecord:
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        paths = build_job_paths(
            job.normalized_movie_number,
            self.jobs_root_mac,
            self.jobs_root_windows,
        )
        return self.update_download_status(
            job_id,
            JobStatus.AUDIO_READY,
            metadata_path_mac=str(paths.metadata_path_mac),
            audio_path_mac=str(paths.audio_path_mac),
            audio_path_windows=paths.audio_path_windows,
        )

    def record_download_failure(
        self,
        job_id: str,
        status: JobStatus,
        attempt_count: int,
        error: str,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, attempt_count = ?, updated_at = ?, error = ?
                WHERE id = ?
                """,
                (status.value, attempt_count, now, error, job_id),
            )
            if cursor.rowcount == 0:
                raise KeyError(job_id)
            job = self.get_job(job_id, conn=conn)
            assert job is not None
            return job

    def claim_next_download_job(self) -> JobRecord | None:
        now = utc_now_iso()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT id FROM jobs
                WHERE status = ?
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                """,
                (JobStatus.QUEUED.value,),
            ).fetchone()
            if row is None:
                return None
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status = ?
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                """,
                (JobStatus.QUEUED.value,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?, error = NULL
                WHERE id = ? AND status = ?
                """,
                (
                    JobStatus.DOWNLOADING_METADATA.value,
                    now,
                    row["id"],
                    JobStatus.QUEUED.value,
                ),
            )
            return self.get_job(row["id"], conn=conn)

    def recover_interrupted_downloads(self, max_download_attempts: int) -> int:
        now = utc_now_iso()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM jobs
                WHERE status IN (?, ?)
                LIMIT 1
                """,
                (
                    JobStatus.DOWNLOADING_METADATA.value,
                    JobStatus.DOWNLOADING_AUDIO.value,
                ),
            ).fetchone()
            if row is None:
                return 0
        recovered = 0
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status IN (?, ?)
                """,
                (
                    JobStatus.DOWNLOADING_METADATA.value,
                    JobStatus.DOWNLOADING_AUDIO.value,
                ),
            ).fetchall()
            for row in rows:
                job = self._row_to_job(row)
                paths = build_job_paths(
                    job.normalized_movie_number,
                    self.jobs_root_mac,
                    self.jobs_root_windows,
                )
                if (
                    job.status == JobStatus.DOWNLOADING_AUDIO
                    and paths.audio_path_mac.exists()
                ):
                    metadata_path_mac = (
                        str(paths.metadata_path_mac) if paths.metadata_path_mac.exists() else None
                    )
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status = ?, updated_at = ?, error = NULL,
                            metadata_path_mac = COALESCE(?, metadata_path_mac),
                            audio_path_mac = ?, audio_path_windows = ?
                        WHERE id = ?
                        """,
                        (
                            JobStatus.AUDIO_READY.value,
                            now,
                            metadata_path_mac,
                            str(paths.audio_path_mac),
                            paths.audio_path_windows,
                            row["id"],
                        ),
                    )
                    recovered += 1
                    continue
                attempts = row["attempt_count"] + 1
                next_status = (
                    JobStatus.FAILED if attempts >= max_download_attempts else JobStatus.QUEUED
                )
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, attempt_count = ?, updated_at = ?, error = ?
                    WHERE id = ?
                    """,
                    (
                        next_status.value,
                        attempts,
                        now,
                        "download interrupted",
                        row["id"],
                    ),
                )
                recovered += 1
        return recovered

    def claim_next_worker_job(self, worker_id: str, lease_seconds: int) -> JobRecord | None:
        now = utc_now_iso()
        lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).replace(
            microsecond=0
        ).isoformat()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT id FROM jobs
                WHERE status = ?
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                """,
                (JobStatus.AUDIO_READY.value,),
            ).fetchone()
            if row is None:
                return None
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status = ?
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                """,
                (JobStatus.AUDIO_READY.value,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = ?, lease_expires_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (JobStatus.TRANSCRIPTION_CLAIMED.value, worker_id, lease, now, row["id"]),
            )
            return self.get_job(row["id"], conn=conn)

    def heartbeat(
        self,
        job_id: str,
        worker_id: str,
        stage: JobStatus,
        lease_seconds: int,
    ) -> JobRecord:
        lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).replace(
            microsecond=0
        ).isoformat()
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND claimed_by = ?
                """,
                (stage.value, lease, now, job_id, worker_id),
            )
            if cursor.rowcount == 0:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            job = self.get_job(job_id, conn=conn)
            assert job is not None
            return job

    def complete_worker_job(
        self,
        job_id: str,
        worker_id: str,
        japanese_srt_path_windows: str,
        english_srt_path_windows: str,
        final_file_exists,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if job.claimed_by != worker_id:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            paths = build_job_paths(
                job.normalized_movie_number,
                self.jobs_root_mac,
                self.jobs_root_windows,
            )
            if not final_file_exists(str(paths.english_srt_path_mac)):
                raise FileNotFoundError(paths.english_srt_path_mac)
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                    updated_at = ?, error = NULL,
                    japanese_srt_path_mac = ?, japanese_srt_path_windows = ?,
                    english_srt_path_mac = ?, english_srt_path_windows = ?
                WHERE id = ? AND claimed_by = ?
                """,
                (
                    JobStatus.ENGLISH_SRT_READY.value,
                    now,
                    str(paths.japanese_srt_path_mac),
                    japanese_srt_path_windows,
                    str(paths.english_srt_path_mac),
                    english_srt_path_windows,
                    job_id,
                    worker_id,
                ),
            )
            completed = self.get_job(job_id, conn=conn)
            assert completed is not None
            return completed

    def complete_worker_transcription(
        self,
        job_id: str,
        worker_id: str,
        japanese_srt_path_windows: str,
        final_file_exists,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if job.claimed_by != worker_id:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            paths = build_job_paths(
                job.normalized_movie_number,
                self.jobs_root_mac,
                self.jobs_root_windows,
            )
            if not final_file_exists(str(paths.japanese_srt_path_mac)):
                raise FileNotFoundError(paths.japanese_srt_path_mac)
            if paths.japanese_srt_path_mac.stat().st_size == 0:
                raise FileNotFoundError(
                    f"final file empty: {paths.japanese_srt_path_mac}"
                )
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                    updated_at = ?, error = NULL,
                    japanese_srt_path_mac = ?, japanese_srt_path_windows = ?,
                    english_srt_path_mac = NULL, english_srt_path_windows = NULL
                WHERE id = ? AND claimed_by = ?
                """,
                (
                    JobStatus.TRANSCRIPTION_DONE.value,
                    now,
                    str(paths.japanese_srt_path_mac),
                    japanese_srt_path_windows,
                    job_id,
                    worker_id,
                ),
            )
            completed = self.get_job(job_id, conn=conn)
            assert completed is not None
            return completed

    def claim_next_translation_job(
        self,
        worker_id: str,
        lease_seconds: int,
    ) -> JobRecord | None:
        now = utc_now_iso()
        lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).replace(
            microsecond=0
        ).isoformat()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status = ? AND claimed_by IS NULL
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                """,
                (JobStatus.TRANSCRIPTION_DONE.value,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = ?, lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND status = ? AND claimed_by IS NULL
                """,
                (
                    JobStatus.TRANSLATING.value,
                    worker_id,
                    lease,
                    now,
                    row["id"],
                    JobStatus.TRANSCRIPTION_DONE.value,
                ),
            )
            return self.get_job(row["id"], conn=conn)

    def claim_translation_job(
        self,
        job_id: str,
        worker_id: str,
        lease_seconds: int,
    ) -> JobRecord | None:
        now = utc_now_iso()
        lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).replace(
            microsecond=0
        ).isoformat()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = ?, lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND status = ? AND claimed_by IS NULL
                """,
                (
                    JobStatus.TRANSLATING.value,
                    worker_id,
                    lease,
                    now,
                    job_id,
                    JobStatus.TRANSCRIPTION_DONE.value,
                ),
            )
            return (
                self.get_job(job_id, conn=conn)
                if cursor.rowcount == 1
                else None
            )

    def prepare_historical_translation_repair(
        self,
        job_id: str,
        *,
        expected_status: JobStatus,
    ) -> JobRecord:
        if expected_status not in {
            JobStatus.QUEUED,
            JobStatus.FAILED,
            JobStatus.ENGLISH_SRT_READY,
        }:
            raise ValueError("historical repair status is not eligible")
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                    translation_attempt_count = 0, updated_at = ?, error = NULL,
                    english_srt_path_mac = NULL, english_srt_path_windows = NULL
                WHERE id = ? AND status = ? AND claimed_by IS NULL
                """,
                (
                    JobStatus.TRANSCRIPTION_DONE.value,
                    now,
                    job_id,
                    expected_status.value,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    "historical repair state changed before prepare"
                )
            prepared = self.get_job(job_id, conn=conn)
            assert prepared is not None
            return prepared

    def complete_mac_translation(
        self,
        job_id: str,
        worker_id: str,
        final_file_exists,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if job.claimed_by != worker_id or job.status != JobStatus.TRANSLATING:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            paths = build_job_paths(
                job.normalized_movie_number,
                self.jobs_root_mac,
                self.jobs_root_windows,
            )
            if not final_file_exists(str(paths.english_srt_path_mac)):
                raise FileNotFoundError(paths.english_srt_path_mac)
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                    updated_at = ?, error = NULL,
                    english_srt_path_mac = ?, english_srt_path_windows = ?
                WHERE id = ? AND claimed_by = ?
                """,
                (
                    JobStatus.ENGLISH_SRT_READY.value,
                    now,
                    str(paths.english_srt_path_mac),
                    paths.english_srt_path_windows,
                    job_id,
                    worker_id,
                ),
            )
            completed = self.get_job(job_id, conn=conn)
            assert completed is not None
            return completed

    def fail_mac_translation(
        self,
        job_id: str,
        worker_id: str,
        error: str,
        max_translation_attempts: int,
        *,
        permanent: bool = False,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if job.claimed_by != worker_id:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            attempts = job.translation_attempt_count + 1
            next_status = (
                JobStatus.FAILED
                if permanent or attempts >= max_translation_attempts
                else JobStatus.TRANSCRIPTION_DONE
            )
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, translation_attempt_count = ?, claimed_by = NULL,
                    lease_expires_at = NULL, updated_at = ?, error = ?
                WHERE id = ? AND claimed_by = ?
                """,
                (
                    next_status.value,
                    attempts,
                    now,
                    f"translating: {error}",
                    job_id,
                    worker_id,
                ),
            )
            failed = self.get_job(job_id, conn=conn)
            assert failed is not None
            return failed

    def recover_expired_translation_leases(self, max_translation_attempts: int) -> int:
        now = utc_now_iso()
        recovered = 0
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status = ? AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < ?
                """,
                (JobStatus.TRANSLATING.value, now),
            ).fetchall()
            for row in rows:
                attempts = row["translation_attempt_count"] + 1
                next_status = (
                    JobStatus.FAILED
                    if attempts >= max_translation_attempts
                    else JobStatus.TRANSCRIPTION_DONE
                )
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, translation_attempt_count = ?, claimed_by = NULL,
                        lease_expires_at = NULL, updated_at = ?, error = ?
                    WHERE id = ?
                    """,
                    (
                        next_status.value,
                        attempts,
                        now,
                        "translating: translation lease expired",
                        row["id"],
                    ),
                )
                recovered += 1
        return recovered

    def fail_worker_job(
        self,
        job_id: str,
        worker_id: str,
        stage: JobStatus,
        error: str,
        max_worker_attempts: int,
        *,
        permanent: bool = False,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if job.claimed_by != worker_id:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            next_attempts = job.worker_attempt_count + 1
            next_status = (
                JobStatus.FAILED
                if permanent or next_attempts >= max_worker_attempts
                else JobStatus.AUDIO_READY
            )
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, worker_attempt_count = ?, claimed_by = NULL,
                    lease_expires_at = NULL, updated_at = ?, error = ?
                WHERE id = ? AND claimed_by = ?
                """,
                (
                    next_status.value,
                    next_attempts,
                    now,
                    f"{stage.value}: {error}",
                    job_id,
                    worker_id,
                ),
            )
            failed = self.get_job(job_id, conn=conn)
            assert failed is not None
            return failed

    def recover_expired_worker_leases(self, max_worker_attempts: int) -> int:
        now = utc_now_iso()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM jobs
                WHERE status IN (?, ?, ?) AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < ?
                LIMIT 1
                """,
                (
                    JobStatus.TRANSCRIPTION_CLAIMED.value,
                    JobStatus.TRANSCRIBING.value,
                    JobStatus.TRANSCRIPTION_DONE.value,
                    now,
                ),
            ).fetchone()
            if row is None:
                return 0
        recovered = 0
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status IN (?, ?, ?) AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < ?
                """,
                (
                    JobStatus.TRANSCRIPTION_CLAIMED.value,
                    JobStatus.TRANSCRIBING.value,
                    JobStatus.TRANSCRIPTION_DONE.value,
                    now,
                ),
            ).fetchall()
            for row in rows:
                attempts = row["worker_attempt_count"] + 1
                next_status = (
                    JobStatus.FAILED if attempts >= max_worker_attempts else JobStatus.AUDIO_READY
                )
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, worker_attempt_count = ?, claimed_by = NULL,
                        lease_expires_at = NULL, updated_at = ?, error = ?
                    WHERE id = ?
                    """,
                    (
                        next_status.value,
                        attempts,
                        now,
                        "worker lease expired",
                        row["id"],
                    ),
                )
                recovered += 1
        return recovered

    def force_lease_expiry_for_test(self, job_id: str, lease_expires_at: str) -> None:
        with self.connection() as conn:
            conn.execute(
                "UPDATE jobs SET lease_expires_at = ? WHERE id = ?",
                (lease_expires_at, job_id),
            )

    def _get_by_normalized(self, conn: sqlite3.Connection, normalized: str) -> JobRecord | None:
        row = conn.execute(
            "SELECT * FROM jobs WHERE normalized_movie_number = ?",
            (normalized,),
        ).fetchone()
        return self._row_to_job(row) if row else None

    def _force_reset(
        self,
        conn: sqlite3.Connection,
        existing: JobRecord,
        movie_number: str,
    ) -> SubmitResult:
        if existing.status in {
            JobStatus.TRANSCRIPTION_CLAIMED,
            JobStatus.TRANSCRIBING,
            JobStatus.TRANSCRIPTION_DONE,
            JobStatus.TRANSLATING,
        }:
            return SubmitResult(kind="conflict", movie_number=movie_number, job=existing)
        now = utc_now_iso()
        active_statuses = (
            JobStatus.TRANSCRIPTION_CLAIMED.value,
            JobStatus.TRANSCRIBING.value,
            JobStatus.TRANSCRIPTION_DONE.value,
            JobStatus.TRANSLATING.value,
        )
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = ?, claimed_by = NULL, lease_expires_at = NULL, updated_at = ?,
                error = NULL, translation_attempt_count = 0,
                metadata_path_mac = NULL, audio_path_mac = NULL,
                audio_path_windows = NULL, japanese_srt_path_mac = NULL,
                japanese_srt_path_windows = NULL, english_srt_path_mac = NULL,
                english_srt_path_windows = NULL
            WHERE id = ?
              AND status NOT IN (?, ?, ?, ?)
            """,
            (JobStatus.QUEUED.value, now, existing.id, *active_statuses),
        )
        if cursor.rowcount == 0:
            job = self.get_job(existing.id, conn=conn)
            return SubmitResult(kind="conflict", movie_number=movie_number, job=job)
        job = self.get_job(existing.id, conn=conn)
        return SubmitResult(kind="created", movie_number=movie_number, job=job)

    def _row_to_job(self, row: sqlite3.Row) -> JobRecord:
        return JobRecord(
            id=row["id"],
            movie_number=row["movie_number"],
            normalized_movie_number=row["normalized_movie_number"],
            status=JobStatus(row["status"]),
            priority=row["priority"],
            attempt_count=row["attempt_count"],
            worker_attempt_count=row["worker_attempt_count"],
            translation_attempt_count=row["translation_attempt_count"],
            claimed_by=row["claimed_by"],
            lease_expires_at=row["lease_expires_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            error=row["error"],
            job_dir_mac=row["job_dir_mac"],
            job_dir_windows=row["job_dir_windows"],
            metadata_path_mac=row["metadata_path_mac"],
            audio_path_mac=row["audio_path_mac"],
            audio_path_windows=row["audio_path_windows"],
            japanese_srt_path_mac=row["japanese_srt_path_mac"],
            japanese_srt_path_windows=row["japanese_srt_path_windows"],
            english_srt_path_mac=row["english_srt_path_mac"],
            english_srt_path_windows=row["english_srt_path_windows"],
        )

    @staticmethod
    def _row_to_worker_status(row: sqlite3.Row) -> WorkerStatusRecord:
        return WorkerStatusRecord(
            worker_id=row["worker_id"],
            role=row["role"],
            state=row["state"],
            last_seen_at=row["last_seen_at"],
            last_poll_at=row["last_poll_at"],
            last_ip=row["last_ip"],
            current_job_id=row["current_job_id"],
            current_movie_number=row["current_movie_number"],
            stage=row["stage"],
            updated_at=row["updated_at"],
            last_error=row["last_error"],
        )
