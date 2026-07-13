import hashlib
import os
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from orchestrator.movie_code import canonical_movie_code
from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths, new_job_id, normalize_movie_number


_VERIFIED_METADATA_STATUSES = frozenset({"complete", "partial", "placeholder"})
_VERIFIED_METADATA_SOURCES = frozenset(
    {"public", "missav", "local", "placeholder"}
)
_LOWERCASE_HEX_DIGITS = frozenset("0123456789abcdef")
MAX_PUBLICATION_CANARY_SRT_BYTES = 32 * 1024 * 1024
NORMAL_TRANSLATION_ORIGIN = "normal"
HISTORICAL_TRANSLATION_ORIGIN = "historical"
_CATALOG_SYNC_FAILURE_REASON_CODES = frozenset(
    {
        "catalog_fetch_failed",
        "catalog_redirect_rejected",
        "catalog_auth_failed",
        "catalog_sync_failed",
        "catalog_response_invalid",
        "catalog_response_mismatch",
    }
)


class CatalogLeaseLostError(PermissionError):
    """Raised when a catalog mutation no longer owns its fenced lease."""


def _validate_expected_sha256(value: str, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _LOWERCASE_HEX_DIGITS for character in value)
    ):
        raise ValueError(f"{label} must be 64 lowercase hexadecimal characters")


def _validate_verified_supabase_receipt(
    *,
    movie_code: str,
    movie_uuid: object,
    metadata_status: object,
    metadata_source: object,
    subtitle_id: object,
    storage_path: object,
    content_sha256: object,
    file_size: object,
) -> None:
    try:
        canonical = canonical_movie_code(movie_code)
        movie_uuid_valid = (
            isinstance(movie_uuid, str) and str(UUID(movie_uuid)) == movie_uuid
        )
        subtitle_id_valid = (
            isinstance(subtitle_id, str) and str(UUID(subtitle_id)) == subtitle_id
        )
    except (AttributeError, TypeError, ValueError):
        raise ValueError("verified Supabase receipt is invalid") from None
    expected_storage_path = (
        f"{canonical.split('-', 1)[0]}/{canonical}/{canonical}-English_AI.srt"
    )
    valid_sha256 = (
        isinstance(content_sha256, str)
        and len(content_sha256) == 64
        and all(character in _LOWERCASE_HEX_DIGITS for character in content_sha256)
    )
    if not (
        movie_uuid_valid
        and subtitle_id_valid
        and metadata_status in _VERIFIED_METADATA_STATUSES
        and metadata_source in _VERIFIED_METADATA_SOURCES
        and isinstance(storage_path, str)
        and storage_path == expected_storage_path
        and valid_sha256
        and isinstance(file_size, int)
        and not isinstance(file_size, bool)
        and file_size > 0
    ):
        raise ValueError("verified Supabase receipt is invalid")


def _same_file_snapshot(before: os.stat_result, after: os.stat_result) -> bool:
    return (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
        before.st_mode,
        before.st_nlink,
    ) == (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
        after.st_mode,
        after.st_nlink,
    )


def _open_regular_nonempty_file_at(
    directory_fd: int,
    basename: str,
    label: str,
    stack: ExitStack,
) -> tuple[int, os.stat_result]:
    try:
        path_stat = os.stat(
            basename,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise FileNotFoundError(
            f"final {label} file unavailable before prepare"
        ) from exc
    if not stat.S_ISREG(path_stat.st_mode):
        raise FileNotFoundError(
            f"final {label} file is not regular before prepare"
        )
    if path_stat.st_size <= 0:
        raise FileNotFoundError(f"final {label} file empty before prepare")
    if path_stat.st_size > MAX_PUBLICATION_CANARY_SRT_BYTES:
        raise FileNotFoundError(
            f"final {label} file exceeds publication canary size limit"
        )
    try:
        file_fd = os.open(
            basename,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=directory_fd,
        )
    except OSError as exc:
        raise FileNotFoundError(
            f"final {label} file unavailable before prepare"
        ) from exc
    stack.callback(os.close, file_fd)
    before = os.fstat(file_fd)
    if not stat.S_ISREG(before.st_mode):
        raise FileNotFoundError(
            f"final {label} file is not regular before prepare"
        )
    if before.st_size <= 0:
        raise FileNotFoundError(f"final {label} file empty before prepare")
    if before.st_size > MAX_PUBLICATION_CANARY_SRT_BYTES:
        raise FileNotFoundError(
            f"final {label} file exceeds publication canary size limit"
        )
    if not _same_file_snapshot(path_stat, before):
        raise RuntimeError("subtitle_snapshot_changed_before_prepare")
    return file_fd, before


def _sha256_open_file(file_fd: int, before: os.stat_result) -> str:
    try:
        digest = hashlib.sha256()
        remaining = before.st_size
        while remaining:
            chunk = os.read(file_fd, min(1024 * 1024, remaining))
            if not chunk:
                raise RuntimeError("subtitle_snapshot_changed_before_prepare")
            digest.update(chunk)
            remaining -= len(chunk)
        if os.read(file_fd, 1):
            raise RuntimeError("subtitle_snapshot_changed_before_prepare")
        after = os.fstat(file_fd)
        if not _same_file_snapshot(before, after):
            raise RuntimeError("subtitle_snapshot_changed_before_prepare")
        return digest.hexdigest()
    except OSError as exc:
        raise RuntimeError("subtitle_snapshot_changed_before_prepare") from exc


def _require_basename_matches_open_file(
    directory_fd: int,
    basename: str,
    file_fd: int,
    expected_stat: os.stat_result,
) -> None:
    try:
        path_stat = os.stat(
            basename,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        opened_stat = os.fstat(file_fd)
    except OSError as exc:
        raise RuntimeError("subtitle_snapshot_changed_before_prepare") from exc
    if (
        not stat.S_ISREG(path_stat.st_mode)
        or (path_stat.st_dev, path_stat.st_ino)
        != (opened_stat.st_dev, opened_stat.st_ino)
        or not _same_file_snapshot(expected_stat, opened_stat)
    ):
        raise RuntimeError("subtitle_snapshot_changed_before_prepare")


def _require_path_matches_open_directory(
    job_directory: Path,
    configured_root: Path,
    directory_fd: int,
) -> None:
    try:
        path_stat = job_directory.lstat()
        opened_stat = os.fstat(directory_fd)
        canonical_job_directory = job_directory.resolve(strict=True)
    except OSError as exc:
        raise RuntimeError("subtitle_snapshot_changed_before_prepare") from exc
    if (
        stat.S_ISLNK(path_stat.st_mode)
        or not stat.S_ISDIR(path_stat.st_mode)
        or not stat.S_ISDIR(opened_stat.st_mode)
        or canonical_job_directory.parent != configured_root
        or (path_stat.st_dev, path_stat.st_ino)
        != (opened_stat.st_dev, opened_stat.st_ino)
    ):
        raise RuntimeError("subtitle_snapshot_changed_before_prepare")


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class HistoricalRepairState(StrEnum):
    PLANNED = "planned"
    PENDING = "pending"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    PERMANENT_FAILED = "permanent_failed"
    PAUSED = "paused"


@dataclass(frozen=True)
class HistoricalRepairRecord:
    id: str
    batch_id: str
    job_id: str
    movie_code: str
    allowlist_sha256: str
    state: HistoricalRepairState
    attempt_count: int
    next_attempt_at: str | None
    reason_code: str | None
    japanese_sha256: str
    audio_sha256: str | None
    english_sha256: str | None
    created_at: str
    updated_at: str


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
    publish_attempt_count: int
    next_publish_attempt_at: str | None
    translation_origin: str
    published_subtitle_id: str | None
    published_storage_path: str | None
    published_content_sha256: str | None
    published_file_size: int | None
    catalog_sync_attempt_count: int
    next_catalog_sync_attempt_at: str | None
    catalog_lease_token: str | None
    catalog_movie_uuid: str | None
    metadata_status: str | None
    metadata_source: str | None
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
                f"""
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY,
                  movie_number TEXT NOT NULL,
                  normalized_movie_number TEXT NOT NULL UNIQUE,
                  status TEXT NOT NULL,
                  priority INTEGER NOT NULL DEFAULT 100,
                  attempt_count INTEGER NOT NULL DEFAULT 0,
                  worker_attempt_count INTEGER NOT NULL DEFAULT 0,
                  translation_attempt_count INTEGER NOT NULL DEFAULT 0,
                  publish_attempt_count INTEGER NOT NULL DEFAULT 0,
                  next_publish_attempt_at TEXT,
                  translation_origin TEXT NOT NULL DEFAULT '{NORMAL_TRANSLATION_ORIGIN}',
                  published_subtitle_id TEXT,
                  published_storage_path TEXT,
                  published_content_sha256 TEXT,
                  published_file_size INTEGER,
                  catalog_sync_attempt_count INTEGER NOT NULL DEFAULT 0,
                  next_catalog_sync_attempt_at TEXT,
                  catalog_lease_token TEXT,
                  catalog_movie_uuid TEXT,
                  metadata_status TEXT,
                  metadata_source TEXT,
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
            publication_columns = {
                "publish_attempt_count": "INTEGER NOT NULL DEFAULT 0",
                "next_publish_attempt_at": "TEXT",
                "catalog_movie_uuid": "TEXT",
                "metadata_status": "TEXT",
                "metadata_source": "TEXT",
            }
            for column, definition in publication_columns.items():
                if column not in columns:
                    conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")
            durable_state_columns = {
                "translation_origin": (
                    f"TEXT NOT NULL DEFAULT '{NORMAL_TRANSLATION_ORIGIN}'"
                ),
                "published_subtitle_id": "TEXT",
                "published_storage_path": "TEXT",
                "published_content_sha256": "TEXT",
                "published_file_size": "INTEGER",
                "catalog_sync_attempt_count": "INTEGER NOT NULL DEFAULT 0",
                "next_catalog_sync_attempt_at": "TEXT",
                "catalog_lease_token": "TEXT",
            }
            for column, definition in durable_state_columns.items():
                if column not in columns:
                    conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS historical_translation_repairs (
                  id TEXT PRIMARY KEY,
                  batch_id TEXT NOT NULL,
                  job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
                  movie_code TEXT NOT NULL,
                  allowlist_sha256 TEXT NOT NULL,
                  state TEXT NOT NULL,
                  attempt_count INTEGER NOT NULL DEFAULT 0,
                  next_attempt_at TEXT,
                  reason_code TEXT,
                  japanese_sha256 TEXT NOT NULL,
                  audio_sha256 TEXT,
                  english_sha256 TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS "
                "idx_historical_translation_repairs_state_created_at "
                "ON historical_translation_repairs(state, created_at)"
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
                  publish_attempt_count, next_publish_attempt_at, catalog_movie_uuid,
                  metadata_status, metadata_source,
                  claimed_by, lease_expires_at,
                  created_at, updated_at, error, job_dir_mac, job_dir_windows,
                  metadata_path_mac, audio_path_mac, audio_path_windows,
                  japanese_srt_path_mac, japanese_srt_path_windows,
                  english_srt_path_mac, english_srt_path_windows
                )
                VALUES (
                  ?, ?, ?, ?, ?, 0, 0, 0, 0, NULL, NULL, NULL, NULL,
                  NULL, NULL, ?, ?, NULL, ?, ?,
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

    def finalize_interrupted_audio(
        self,
        job_id: str,
        *,
        expected_movie_code: str,
        expected_job_dir_mac: str,
        expected_job_dir_windows: str,
        expected_audio_path_mac: str,
        expected_audio_path_windows: str,
        audio_snapshot_check: Callable[[], None],
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            paths = build_job_paths(
                expected_movie_code,
                self.jobs_root_mac,
                self.jobs_root_windows,
            )
            if (
                expected_job_dir_mac != str(paths.job_dir_mac)
                or expected_job_dir_windows != paths.job_dir_windows
                or expected_audio_path_mac != str(paths.audio_path_mac)
                or expected_audio_path_windows != paths.audio_path_windows
                or job.normalized_movie_number != expected_movie_code
                or job.job_dir_mac != expected_job_dir_mac
                or job.job_dir_windows != expected_job_dir_windows
            ):
                raise RuntimeError("audio_recovery_state_changed")
            exact_optional_paths = (
                (job.metadata_path_mac, str(paths.metadata_path_mac)),
                (job.audio_path_mac, str(paths.audio_path_mac)),
                (job.audio_path_windows, paths.audio_path_windows),
                (job.japanese_srt_path_mac, str(paths.japanese_srt_path_mac)),
                (job.japanese_srt_path_windows, paths.japanese_srt_path_windows),
                (job.english_srt_path_mac, str(paths.english_srt_path_mac)),
                (job.english_srt_path_windows, paths.english_srt_path_windows),
            )
            if any(
                actual is not None and actual != expected
                for actual, expected in exact_optional_paths
            ):
                raise RuntimeError("audio_recovery_state_changed")
            metadata_path_mac = (
                str(paths.metadata_path_mac)
                if paths.metadata_path_mac.exists()
                else None
            )
            audio_snapshot_check()
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?,
                    metadata_path_mac = COALESCE(?, metadata_path_mac),
                    audio_path_mac = ?, audio_path_windows = ?, error = NULL
                WHERE id = ? AND status = ? AND claimed_by IS NULL
                  AND lease_expires_at IS NULL
                  AND normalized_movie_number = ?
                  AND job_dir_mac = ? AND job_dir_windows = ?
                """,
                (
                    JobStatus.AUDIO_READY.value,
                    now,
                    metadata_path_mac,
                    str(paths.audio_path_mac),
                    paths.audio_path_windows,
                    job_id,
                    JobStatus.DOWNLOADING_AUDIO.value,
                    expected_movie_code,
                    expected_job_dir_mac,
                    expected_job_dir_windows,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("audio_recovery_state_changed")
            audio_snapshot_check()
            finalized = self.get_job(job_id, conn=conn)
            assert finalized is not None
            return finalized

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
                    translation_attempt_count = 0, publish_attempt_count = 0,
                    next_publish_attempt_at = NULL, catalog_movie_uuid = NULL,
                    metadata_status = NULL, metadata_source = NULL,
                    updated_at = ?, error = NULL,
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

    def enqueue_historical_repairs(
        self,
        plan: object,
        allowlist_path: Path,
        *,
        confirm_plan_sha256: str,
    ) -> list[HistoricalRepairRecord]:
        from orchestrator.historical_batch import (
            HistoricalBatchPlan,
            _enqueue_historical_repairs_transaction,
            _prehold_plan_job_files,
            find_idempotent_historical_enqueue,
        )
        from orchestrator.job_files_lock import exclusive_jobs_root_lock

        if not isinstance(plan, HistoricalBatchPlan):
            raise ValueError("historical_plan_changed")
        existing = find_idempotent_historical_enqueue(
            self,
            plan,
            Path(allowlist_path),
            confirm_plan_sha256=confirm_plan_sha256,
        )
        if existing is not None:
            return existing
        with ExitStack() as descriptor_stack:
            root_lock = descriptor_stack.enter_context(
                exclusive_jobs_root_lock(
                    self.jobs_root_mac,
                    blocking=True,
                )
            )
            preheld_jobs = _prehold_plan_job_files(
                self,
                plan,
                Path(allowlist_path),
                confirm_plan_sha256=confirm_plan_sha256,
                root_lock=root_lock,
                descriptor_stack=descriptor_stack,
            )
            with self.connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                return _enqueue_historical_repairs_transaction(
                    self,
                    conn,
                    plan,
                    Path(allowlist_path),
                    confirm_plan_sha256=confirm_plan_sha256,
                    root_lock=root_lock,
                    preheld_jobs=preheld_jobs,
                )

    def prepare_catalog_publication_repair(
        self,
        job_id: str,
        *,
        expected_status: JobStatus,
        expected_movie: str,
        expected_japanese_sha256: str,
        expected_english_sha256: str,
    ) -> JobRecord:
        if expected_status not in {
            JobStatus.FAILED,
            JobStatus.ENGLISH_SRT_READY,
        }:
            raise ValueError("catalog publication status is not eligible")
        _validate_expected_sha256(
            expected_japanese_sha256,
            "expected_japanese_sha256",
        )
        _validate_expected_sha256(
            expected_english_sha256,
            "expected_english_sha256",
        )
        expected_canonical = canonical_movie_code(expected_movie)
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if canonical_movie_code(job.normalized_movie_number) != expected_canonical:
                raise ValueError("confirmed job movie changed before prepare")
            if job.status is not expected_status or job.claimed_by is not None:
                raise RuntimeError("catalog publication state changed before prepare")
            if (
                job.status is JobStatus.ENGLISH_SRT_READY
                and job.catalog_movie_uuid
                and job.metadata_status in _VERIFIED_METADATA_STATUSES
                and job.metadata_source in _VERIFIED_METADATA_SOURCES
            ):
                raise ValueError("verified publication is not eligible")
            paths = build_job_paths(
                job.normalized_movie_number,
                self.jobs_root_mac,
                self.jobs_root_windows,
            )
            try:
                job_directory_stat = paths.job_dir_mac.lstat()
            except OSError as exc:
                raise ValueError(
                    "catalog_publication_job_directory_unavailable"
                ) from exc
            if stat.S_ISLNK(job_directory_stat.st_mode):
                raise ValueError("catalog_publication_job_directory_symlink")
            if not stat.S_ISDIR(job_directory_stat.st_mode):
                raise ValueError("catalog_publication_job_directory_not_directory")
            try:
                configured_root = self.jobs_root_mac.resolve(strict=True)
                canonical_job_directory = paths.job_dir_mac.resolve(strict=True)
            except OSError as exc:
                raise ValueError(
                    "catalog_publication_job_directory_unavailable"
                ) from exc
            if canonical_job_directory.parent != configured_root:
                raise ValueError(
                    "catalog_publication_job_directory_not_direct_child"
                )
            if (
                paths.japanese_srt_path_mac.parent != paths.job_dir_mac
                or paths.english_srt_path_mac.parent != paths.job_dir_mac
            ):
                raise ValueError(
                    "catalog_publication_subtitle_not_direct_child"
                )
            with ExitStack() as stack:
                try:
                    directory_fd = os.open(
                        paths.job_dir_mac,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    )
                except OSError as exc:
                    raise ValueError(
                        "catalog_publication_job_directory_unavailable"
                    ) from exc
                stack.callback(os.close, directory_fd)
                opened_directory_stat = os.fstat(directory_fd)
                if not stat.S_ISDIR(opened_directory_stat.st_mode) or (
                    opened_directory_stat.st_dev,
                    opened_directory_stat.st_ino,
                ) != (
                    job_directory_stat.st_dev,
                    job_directory_stat.st_ino,
                ):
                    raise RuntimeError(
                        "subtitle_snapshot_changed_before_prepare"
                    )
                japanese_fd, japanese_stat = _open_regular_nonempty_file_at(
                    directory_fd,
                    paths.japanese_srt_path_mac.name,
                    "japanese",
                    stack,
                )
                english_fd, english_stat = _open_regular_nonempty_file_at(
                    directory_fd,
                    paths.english_srt_path_mac.name,
                    "english",
                    stack,
                )
                japanese_sha256 = _sha256_open_file(
                    japanese_fd,
                    japanese_stat,
                )
                english_sha256 = _sha256_open_file(
                    english_fd,
                    english_stat,
                )
                if (
                    japanese_sha256 != expected_japanese_sha256
                    or english_sha256 != expected_english_sha256
                ):
                    raise RuntimeError(
                        "subtitle_snapshot_changed_before_prepare"
                    )
                _require_path_matches_open_directory(
                    paths.job_dir_mac,
                    configured_root,
                    directory_fd,
                )
                _require_basename_matches_open_file(
                    directory_fd,
                    paths.japanese_srt_path_mac.name,
                    japanese_fd,
                    japanese_stat,
                )
                _require_basename_matches_open_file(
                    directory_fd,
                    paths.english_srt_path_mac.name,
                    english_fd,
                    english_stat,
                )
                cursor = conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                        publish_attempt_count = 0, next_publish_attempt_at = NULL,
                        catalog_lease_token = NULL,
                        catalog_movie_uuid = NULL, metadata_status = NULL,
                        metadata_source = NULL, updated_at = ?, error = NULL,
                        english_srt_path_mac = ?, english_srt_path_windows = ?
                    WHERE id = ? AND status = ? AND claimed_by IS NULL
                    """,
                    (
                        JobStatus.PUBLISH_PENDING.value,
                        now,
                        str(paths.english_srt_path_mac),
                        paths.english_srt_path_windows,
                        job_id,
                        expected_status.value,
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "catalog publication state changed before prepare"
                    )
                prepared = self.get_job(job_id, conn=conn)
                assert prepared is not None
                _require_path_matches_open_directory(
                    paths.job_dir_mac,
                    configured_root,
                    directory_fd,
                )
                _require_basename_matches_open_file(
                    directory_fd,
                    paths.japanese_srt_path_mac.name,
                    japanese_fd,
                    japanese_stat,
                )
                _require_basename_matches_open_file(
                    directory_fd,
                    paths.english_srt_path_mac.name,
                    english_fd,
                    english_stat,
                )
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
                    catalog_lease_token = NULL, updated_at = ?, error = NULL,
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

    def complete_mac_translation_quality(
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
            if job.status != JobStatus.TRANSLATING or job.claimed_by != worker_id:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            paths = build_job_paths(
                job.normalized_movie_number,
                self.jobs_root_mac,
                self.jobs_root_windows,
            )
            if not final_file_exists(str(paths.english_srt_path_mac)):
                raise FileNotFoundError(paths.english_srt_path_mac)
            if paths.english_srt_path_mac.stat().st_size == 0:
                raise FileNotFoundError(f"final file empty: {paths.english_srt_path_mac}")
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                    updated_at = ?, error = NULL,
                    english_srt_path_mac = ?, english_srt_path_windows = ?
                WHERE id = ? AND status = ? AND claimed_by = ?
                """,
                (
                    JobStatus.PUBLISH_PENDING.value,
                    now,
                    str(paths.english_srt_path_mac),
                    paths.english_srt_path_windows,
                    job_id,
                    JobStatus.TRANSLATING.value,
                    worker_id,
                ),
            )
            if cursor.rowcount != 1:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            completed = self.get_job(job_id, conn=conn)
            assert completed is not None
            return completed

    def claim_publication_job(
        self,
        worker_id: str,
        lease_seconds: int,
        *,
        job_id: str | None = None,
    ) -> JobRecord | None:
        now_dt = datetime.now(UTC).replace(microsecond=0)
        now = now_dt.isoformat()
        lease = (now_dt + timedelta(seconds=lease_seconds)).isoformat()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            parameters: list[str] = [JobStatus.PUBLISH_PENDING.value, now]
            job_filter = ""
            if job_id is not None:
                job_filter = " AND id = ?"
                parameters.append(job_id)
            row = conn.execute(
                f"""
                SELECT id FROM jobs
                WHERE status = ? AND claimed_by IS NULL
                  AND (next_publish_attempt_at IS NULL OR next_publish_attempt_at <= ?)
                  {job_filter}
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                """,
                parameters,
            ).fetchone()
            if row is None:
                return None
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = ?, lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND status = ? AND claimed_by IS NULL
                  AND (next_publish_attempt_at IS NULL OR next_publish_attempt_at <= ?)
                """,
                (
                    JobStatus.PUBLISHING.value,
                    worker_id,
                    lease,
                    now,
                    row["id"],
                    JobStatus.PUBLISH_PENDING.value,
                    now,
                ),
            )
            if cursor.rowcount != 1:
                return None
            return self.get_job(row["id"], conn=conn)

    def fail_publication(
        self,
        job_id: str,
        worker_id: str,
        error: str,
        *,
        max_publish_attempts: int,
        retry_seconds: int,
        permanent: bool = False,
    ) -> JobRecord:
        now_dt = datetime.now(UTC).replace(microsecond=0)
        now = now_dt.isoformat()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if job.status != JobStatus.PUBLISHING or job.claimed_by != worker_id:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            attempts = job.publish_attempt_count + 1
            exhausted = permanent or attempts >= max_publish_attempts
            next_status = JobStatus.FAILED if exhausted else JobStatus.PUBLISH_PENDING
            next_attempt_at = (
                None
                if exhausted
                else (now_dt + timedelta(seconds=retry_seconds)).isoformat()
            )
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, publish_attempt_count = ?, next_publish_attempt_at = ?,
                    claimed_by = NULL, lease_expires_at = NULL, updated_at = ?, error = ?
                WHERE id = ? AND status = ? AND claimed_by = ?
                """,
                (
                    next_status.value,
                    attempts,
                    next_attempt_at,
                    now,
                    f"publishing: {error}",
                    job_id,
                    JobStatus.PUBLISHING.value,
                    worker_id,
                ),
            )
            if cursor.rowcount != 1:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            failed = self.get_job(job_id, conn=conn)
            assert failed is not None
            return failed

    def complete_supabase_publication(
        self,
        job_id: str,
        worker_id: str,
        *,
        movie_uuid: str,
        metadata_status: str,
        metadata_source: str,
        subtitle_id: str,
        storage_path: str,
        content_sha256: str,
        file_size: int,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if job.status != JobStatus.PUBLISHING or job.claimed_by != worker_id:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            _validate_verified_supabase_receipt(
                movie_code=job.normalized_movie_number,
                movie_uuid=movie_uuid,
                metadata_status=metadata_status,
                metadata_source=metadata_source,
                subtitle_id=subtitle_id,
                storage_path=storage_path,
                content_sha256=content_sha256,
                file_size=file_size,
            )
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, catalog_movie_uuid = ?, metadata_status = ?,
                    metadata_source = ?, published_subtitle_id = ?,
                    published_storage_path = ?, published_content_sha256 = ?,
                    published_file_size = ?, next_publish_attempt_at = NULL,
                    catalog_sync_attempt_count = 0,
                    next_catalog_sync_attempt_at = NULL, catalog_lease_token = NULL,
                    claimed_by = NULL, lease_expires_at = NULL,
                    updated_at = ?, error = NULL
                WHERE id = ? AND status = ? AND claimed_by = ?
                """,
                (
                    JobStatus.CATALOG_SYNC_PENDING.value,
                    movie_uuid,
                    metadata_status,
                    metadata_source,
                    subtitle_id,
                    storage_path,
                    content_sha256,
                    file_size,
                    now,
                    job_id,
                    JobStatus.PUBLISHING.value,
                    worker_id,
                ),
            )
            if cursor.rowcount != 1:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            completed = self.get_job(job_id, conn=conn)
            assert completed is not None
            return completed

    def claim_catalog_sync_job(
        self,
        worker_id: str,
        lease_seconds: int,
        *,
        job_id: str | None = None,
    ) -> JobRecord | None:
        now_dt = datetime.now(UTC).replace(microsecond=0)
        now = now_dt.isoformat()
        lease = (now_dt + timedelta(seconds=lease_seconds)).isoformat()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            parameters: list[str] = [JobStatus.CATALOG_SYNC_PENDING.value, now]
            job_filter = ""
            if job_id is not None:
                job_filter = " AND id = ?"
                parameters.append(job_id)
            rows = conn.execute(
                f"""
                SELECT * FROM jobs
                WHERE status = ? AND claimed_by IS NULL
                  AND (next_catalog_sync_attempt_at IS NULL
                       OR next_catalog_sync_attempt_at <= ?)
                  AND catalog_movie_uuid IS NOT NULL
                  AND metadata_status IS NOT NULL
                  AND metadata_source IS NOT NULL
                  AND published_subtitle_id IS NOT NULL
                  AND published_storage_path IS NOT NULL
                  AND published_content_sha256 IS NOT NULL
                  AND published_file_size > 0
                  {job_filter}
                ORDER BY priority ASC, created_at ASC
                """,
                parameters,
            ).fetchall()
            candidate = None
            for row in rows:
                job = self._row_to_job(row)
                try:
                    _validate_verified_supabase_receipt(
                        movie_code=job.normalized_movie_number,
                        movie_uuid=job.catalog_movie_uuid,
                        metadata_status=job.metadata_status,
                        metadata_source=job.metadata_source,
                        subtitle_id=job.published_subtitle_id,
                        storage_path=job.published_storage_path,
                        content_sha256=job.published_content_sha256,
                        file_size=job.published_file_size,
                    )
                except ValueError:
                    continue
                candidate = job
                break
            if candidate is None:
                return None
            lease_token = uuid4().hex
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = ?, lease_expires_at = ?,
                    catalog_lease_token = ?, updated_at = ?
                WHERE id = ? AND status = ? AND claimed_by IS NULL
                  AND (next_catalog_sync_attempt_at IS NULL
                       OR next_catalog_sync_attempt_at <= ?)
                  AND catalog_movie_uuid IS NOT NULL
                  AND metadata_status IS NOT NULL
                  AND metadata_source IS NOT NULL
                  AND published_subtitle_id IS NOT NULL
                  AND published_storage_path IS NOT NULL
                  AND published_content_sha256 IS NOT NULL
                  AND published_file_size > 0
                """,
                (
                    JobStatus.CATALOG_SYNCING.value,
                    worker_id,
                    lease,
                    lease_token,
                    now,
                    candidate.id,
                    JobStatus.CATALOG_SYNC_PENDING.value,
                    now,
                ),
            )
            if cursor.rowcount != 1:
                return None
            return self.get_job(candidate.id, conn=conn)

    def fail_catalog_sync(
        self,
        job_id: str,
        worker_id: str,
        reason_code: str,
        *,
        lease_token: str,
        max_catalog_sync_attempts: int,
        retry_seconds: int,
    ) -> JobRecord:
        if reason_code not in _CATALOG_SYNC_FAILURE_REASON_CODES:
            reason_code = "catalog_sync_failed"
        now_dt = datetime.now(UTC).replace(microsecond=0)
        now = now_dt.isoformat()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if (
                job.status is not JobStatus.CATALOG_SYNCING
                or job.claimed_by != worker_id
                or job.catalog_lease_token != lease_token
                or not job.lease_expires_at
                or job.lease_expires_at <= now
            ):
                raise CatalogLeaseLostError(
                    f"job {job_id} catalog lease is no longer owned"
                )
            attempts = job.catalog_sync_attempt_count + 1
            exhausted = attempts >= max_catalog_sync_attempts
            next_status = JobStatus.FAILED if exhausted else JobStatus.CATALOG_SYNC_PENDING
            next_attempt_at = (
                None
                if exhausted
                else (now_dt + timedelta(seconds=retry_seconds)).isoformat()
            )
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, catalog_sync_attempt_count = ?,
                    next_catalog_sync_attempt_at = ?, claimed_by = NULL,
                    lease_expires_at = NULL, catalog_lease_token = NULL,
                    updated_at = ?, error = ?
                WHERE id = ? AND status = ? AND claimed_by = ?
                  AND catalog_lease_token = ?
                  AND lease_expires_at > ?
                """,
                (
                    next_status.value,
                    attempts,
                    next_attempt_at,
                    now,
                    f"catalog_sync: {reason_code}",
                    job_id,
                    JobStatus.CATALOG_SYNCING.value,
                    worker_id,
                    lease_token,
                    now,
                ),
            )
            if cursor.rowcount != 1:
                raise CatalogLeaseLostError(
                    f"job {job_id} catalog lease is no longer owned"
                )
            failed = self.get_job(job_id, conn=conn)
            assert failed is not None
            return failed

    def complete_catalog_sync(
        self,
        job_id: str,
        worker_id: str,
        *,
        lease_token: str,
        canonical_code: str,
        d1_rows_updated: int,
        subtitle_count: int,
        kv_keys_deleted: tuple[str, ...],
    ) -> JobRecord:
        canonical = canonical_movie_code(canonical_code)
        if canonical != canonical_code:
            raise ValueError("catalog canonical code is not canonical")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in (d1_rows_updated, subtitle_count)
        ):
            raise ValueError("catalog result counts must be positive integers")
        expected_keys = {
            f"movie:full:{canonical}",
            f"movie:light:{canonical}",
        }
        if (
            not isinstance(kv_keys_deleted, tuple)
            or len(kv_keys_deleted) != 2
            or set(kv_keys_deleted) != expected_keys
        ):
            raise ValueError("catalog result cache keys do not match")

        now = utc_now_iso()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id, conn=conn)
            if job is None:
                raise KeyError(job_id)
            if (
                job.status is not JobStatus.CATALOG_SYNCING
                or job.claimed_by != worker_id
                or job.catalog_lease_token != lease_token
                or not job.lease_expires_at
                or job.lease_expires_at <= now
            ):
                raise CatalogLeaseLostError(
                    f"job {job_id} catalog lease is no longer owned"
                )
            if canonical_movie_code(job.normalized_movie_number) != canonical:
                raise ValueError("catalog canonical code does not match job")
            _validate_verified_supabase_receipt(
                movie_code=job.normalized_movie_number,
                movie_uuid=job.catalog_movie_uuid,
                metadata_status=job.metadata_status,
                metadata_source=job.metadata_source,
                subtitle_id=job.published_subtitle_id,
                storage_path=job.published_storage_path,
                content_sha256=job.published_content_sha256,
                file_size=job.published_file_size,
            )
            cursor = conn.execute(
                """
                UPDATE jobs
                SET status = ?, next_catalog_sync_attempt_at = NULL,
                    claimed_by = NULL, lease_expires_at = NULL,
                    catalog_lease_token = NULL, updated_at = ?, error = NULL
                WHERE id = ? AND status = ? AND claimed_by = ?
                  AND catalog_lease_token = ?
                  AND lease_expires_at > ?
                """,
                (
                    JobStatus.ENGLISH_SRT_READY.value,
                    now,
                    job_id,
                    JobStatus.CATALOG_SYNCING.value,
                    worker_id,
                    lease_token,
                    now,
                ),
            )
            if cursor.rowcount != 1:
                raise CatalogLeaseLostError(
                    f"job {job_id} catalog lease is no longer owned"
                )
            completed = self.get_job(job_id, conn=conn)
            assert completed is not None
            return completed

    def recover_expired_catalog_sync_leases(
        self,
        max_catalog_sync_attempts: int,
        retry_seconds: int,
    ) -> int:
        now_dt = datetime.now(UTC).replace(microsecond=0)
        now = now_dt.isoformat()
        recovered = 0
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT id, claimed_by, catalog_lease_token,
                       catalog_sync_attempt_count FROM jobs
                WHERE status = ? AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?
                """,
                (JobStatus.CATALOG_SYNCING.value, now),
            ).fetchall()
            for row in rows:
                attempts = row["catalog_sync_attempt_count"] + 1
                exhausted = attempts >= max_catalog_sync_attempts
                next_status = (
                    JobStatus.FAILED
                    if exhausted
                    else JobStatus.CATALOG_SYNC_PENDING
                )
                next_attempt_at = (
                    None
                    if exhausted
                    else (now_dt + timedelta(seconds=retry_seconds)).isoformat()
                )
                cursor = conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, catalog_sync_attempt_count = ?,
                        next_catalog_sync_attempt_at = ?, claimed_by = NULL,
                        lease_expires_at = NULL, catalog_lease_token = NULL,
                        updated_at = ?, error = ?
                    WHERE id = ? AND status = ? AND claimed_by = ?
                      AND catalog_lease_token IS ?
                    """,
                    (
                        next_status.value,
                        attempts,
                        next_attempt_at,
                        now,
                        "catalog_sync: catalog_sync_lease_expired",
                        row["id"],
                        JobStatus.CATALOG_SYNCING.value,
                        row["claimed_by"],
                        row["catalog_lease_token"],
                    ),
                )
                recovered += cursor.rowcount
        return recovered

    def recover_expired_publication_leases(
        self,
        max_publish_attempts: int,
        retry_seconds: int,
    ) -> int:
        now_dt = datetime.now(UTC).replace(microsecond=0)
        now = now_dt.isoformat()
        recovered = 0
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT id, claimed_by, publish_attempt_count FROM jobs
                WHERE status = ? AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < ?
                """,
                (JobStatus.PUBLISHING.value, now),
            ).fetchall()
            for row in rows:
                attempts = row["publish_attempt_count"] + 1
                exhausted = attempts >= max_publish_attempts
                next_status = JobStatus.FAILED if exhausted else JobStatus.PUBLISH_PENDING
                next_attempt_at = (
                    None
                    if exhausted
                    else (now_dt + timedelta(seconds=retry_seconds)).isoformat()
                )
                cursor = conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, publish_attempt_count = ?,
                        next_publish_attempt_at = ?, claimed_by = NULL,
                        lease_expires_at = NULL, updated_at = ?, error = ?
                    WHERE id = ? AND status = ? AND claimed_by = ?
                    """,
                    (
                        next_status.value,
                        attempts,
                        next_attempt_at,
                        now,
                        "publishing: publication lease expired",
                        row["id"],
                        JobStatus.PUBLISHING.value,
                        row["claimed_by"],
                    ),
                )
                recovered += cursor.rowcount
        return recovered

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
        if row is not None:
            return self._row_to_job(row)

        canonical = canonical_movie_code(normalized)
        legacy_rows = conn.execute(
            "SELECT * FROM jobs ORDER BY created_at ASC, id ASC"
        ).fetchall()
        for legacy_row in legacy_rows:
            try:
                legacy_canonical = canonical_movie_code(
                    legacy_row["normalized_movie_number"]
                )
            except (AttributeError, TypeError, ValueError):
                continue
            if legacy_canonical == canonical:
                return self._row_to_job(legacy_row)
        return None

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
            JobStatus.PUBLISHING,
            JobStatus.CATALOG_SYNCING,
        }:
            return SubmitResult(kind="conflict", movie_number=movie_number, job=existing)
        now = utc_now_iso()
        active_statuses = (
            JobStatus.TRANSCRIPTION_CLAIMED.value,
            JobStatus.TRANSCRIBING.value,
            JobStatus.TRANSCRIPTION_DONE.value,
            JobStatus.TRANSLATING.value,
            JobStatus.PUBLISHING.value,
            JobStatus.CATALOG_SYNCING.value,
        )
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = ?, claimed_by = NULL, lease_expires_at = NULL, updated_at = ?,
                error = NULL, translation_attempt_count = 0,
                publish_attempt_count = 0, next_publish_attempt_at = NULL,
                translation_origin = ?, published_subtitle_id = NULL,
                published_storage_path = NULL, published_content_sha256 = NULL,
                published_file_size = NULL, catalog_sync_attempt_count = 0,
                next_catalog_sync_attempt_at = NULL, catalog_lease_token = NULL,
                catalog_movie_uuid = NULL, metadata_status = NULL,
                metadata_source = NULL,
                metadata_path_mac = NULL, audio_path_mac = NULL,
                audio_path_windows = NULL, japanese_srt_path_mac = NULL,
                japanese_srt_path_windows = NULL, english_srt_path_mac = NULL,
                english_srt_path_windows = NULL
            WHERE id = ?
              AND status NOT IN (?, ?, ?, ?, ?, ?)
            """,
            (
                JobStatus.QUEUED.value,
                now,
                NORMAL_TRANSLATION_ORIGIN,
                existing.id,
                *active_statuses,
            ),
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
            publish_attempt_count=row["publish_attempt_count"],
            next_publish_attempt_at=row["next_publish_attempt_at"],
            translation_origin=row["translation_origin"],
            published_subtitle_id=row["published_subtitle_id"],
            published_storage_path=row["published_storage_path"],
            published_content_sha256=row["published_content_sha256"],
            published_file_size=row["published_file_size"],
            catalog_sync_attempt_count=row["catalog_sync_attempt_count"],
            next_catalog_sync_attempt_at=row["next_catalog_sync_attempt_at"],
            catalog_lease_token=row["catalog_lease_token"],
            catalog_movie_uuid=row["catalog_movie_uuid"],
            metadata_status=row["metadata_status"],
            metadata_source=row["metadata_source"],
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
