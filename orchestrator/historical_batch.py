from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from contextlib import ExitStack
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from orchestrator.historical_repair import ELIGIBLE_STATUSES
from orchestrator.job_files_lock import (
    JobFilesLock,
    JobFilesLockError,
    JobsRootLock,
    exclusive_jobs_root_lock,
    open_job_directory_from_root,
    shared_job_files_lock_from_root,
)
from orchestrator.models import JobStatus
from orchestrator.movie_code import canonical_movie_code
from orchestrator.paths import normalize_movie_number
from orchestrator.store import HistoricalRepairRecord, HistoricalRepairState, utc_now_iso
from orchestrator.subtitle_quality import validate_translation_quality_snapshots

if TYPE_CHECKING:
    from orchestrator.store import JobStore


PLAN_VERSION = 1
MAX_BATCH_LIMIT = 20
MAX_ALLOWLIST_BYTES = 1024 * 1024
MAX_ALLOWLIST_ENTRIES = 10_000
MAX_SUBTITLE_BYTES = 32 * 1024 * 1024
_LOWER_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_REASON_RE = re.compile(r"^[a-z0-9_]+$")


def canonical_plan_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class HistoricalBatchItem:
    job_id: str
    movie_code: str
    path_movie_number: str
    expected_status: str
    expected_updated_at: str
    reason_codes: tuple[str, ...]
    japanese_sha256: str
    japanese_size: int
    japanese_mtime_ns: int
    audio_sha256: str
    audio_size: int
    audio_mtime_ns: int
    english_sha256: str
    english_size: int
    english_mtime_ns: int

    def to_payload(self) -> dict[str, object]:
        payload = asdict(self)
        payload["reason_codes"] = list(self.reason_codes)
        return payload


@dataclass(frozen=True, slots=True)
class HistoricalBatchPlan:
    version: int
    allowlist_path: str
    allowlist_sha256: str
    allowlist_entry_count: int
    limit: int
    eligible_total: int
    already_repaired: int
    ineligible: int
    blocked: int
    scan_sha256: str
    items: tuple[HistoricalBatchItem, ...]
    plan_sha256: str

    @property
    def batch_id(self) -> str:
        return f"batch_{self.plan_sha256[:32]}"

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "allowlist_path": self.allowlist_path,
            "allowlist_sha256": self.allowlist_sha256,
            "allowlist_entry_count": self.allowlist_entry_count,
            "limit": self.limit,
            "eligible_total": self.eligible_total,
            "already_repaired": self.already_repaired,
            "ineligible": self.ineligible,
            "blocked": self.blocked,
            "scan_sha256": self.scan_sha256,
            "items": [item.to_payload() for item in self.items],
        }

    def recalculate_sha256(self) -> str:
        return hashlib.sha256(canonical_plan_bytes(self._digest_payload())).hexdigest()

    def to_payload(self) -> dict[str, object]:
        return {**self._digest_payload(), "plan_sha256": self.plan_sha256}

    def to_json_bytes(self) -> bytes:
        return canonical_plan_bytes(self.to_payload())

    @classmethod
    def build(
        cls,
        *,
        allowlist_path: str,
        allowlist_sha256: str,
        allowlist_entry_count: int,
        limit: int,
        eligible_total: int,
        already_repaired: int,
        ineligible: int,
        blocked: int,
        scan_sha256: str,
        items: tuple[HistoricalBatchItem, ...],
    ) -> HistoricalBatchPlan:
        plan = cls(
            version=PLAN_VERSION,
            allowlist_path=allowlist_path,
            allowlist_sha256=allowlist_sha256,
            allowlist_entry_count=allowlist_entry_count,
            limit=limit,
            eligible_total=eligible_total,
            already_repaired=already_repaired,
            ineligible=ineligible,
            blocked=blocked,
            scan_sha256=scan_sha256,
            items=items,
            plan_sha256="",
        )
        return replace(plan, plan_sha256=plan.recalculate_sha256())

    @classmethod
    def from_json_bytes(cls, snapshot: bytes) -> HistoricalBatchPlan:
        try:
            payload = json.loads(snapshot.decode("utf-8"), object_pairs_hook=_strict_object)
            plan = _plan_from_payload(payload)
        except (AttributeError, KeyError, TypeError, UnicodeError, ValueError):
            raise ValueError("historical_plan_invalid") from None
        return plan


@dataclass(frozen=True, slots=True)
class _FileSnapshot:
    sha256: str
    size: int
    mtime_ns: int
    content: bytes | None
    basename: str | None = None


@dataclass(frozen=True, slots=True)
class _HeldJobSnapshot:
    lock: JobFilesLock
    files: tuple[_FileSnapshot, _FileSnapshot, _FileSnapshot]

    def require_unchanged(self) -> None:
        self.lock.require_bound()
        for snapshot in self.files:
            if snapshot.basename is None:
                raise OSError("snapshot basename unavailable")
            current = _open_stable_regular_file_at(
                self.lock.job_fd,
                snapshot.basename,
                keep_content=False,
            )
            if (
                current.sha256 != snapshot.sha256
                or current.size != snapshot.size
                or current.mtime_ns != snapshot.mtime_ns
            ):
                raise OSError("snapshot path changed")


@dataclass(frozen=True, slots=True)
class _PreheldJobFiles:
    lock: JobFilesLock

    def snapshots(
        self,
    ) -> tuple[_FileSnapshot, _FileSnapshot, _FileSnapshot]:
        return _snapshot_three_job_files(
            self.lock.job_fd,
            self.lock.movie_code,
        )


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _require_exact_keys(payload: object, expected: frozenset[str]) -> dict[str, Any]:
    if not isinstance(payload, dict) or frozenset(payload) != expected:
        raise ValueError("invalid keys")
    return payload


def _require_int(value: object, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError("invalid integer")
    return value


def _require_digest(value: object) -> str:
    if not isinstance(value, str) or _LOWER_HEX_RE.fullmatch(value) is None:
        raise ValueError("invalid digest")
    return value


_PLAN_KEYS = frozenset(
    {
        "version",
        "allowlist_path",
        "allowlist_sha256",
        "allowlist_entry_count",
        "limit",
        "eligible_total",
        "already_repaired",
        "ineligible",
        "blocked",
        "scan_sha256",
        "items",
        "plan_sha256",
    }
)
_ITEM_KEYS = frozenset(
    {
        "job_id",
        "movie_code",
        "path_movie_number",
        "expected_status",
        "expected_updated_at",
        "reason_codes",
        "japanese_sha256",
        "japanese_size",
        "japanese_mtime_ns",
        "audio_sha256",
        "audio_size",
        "audio_mtime_ns",
        "english_sha256",
        "english_size",
        "english_mtime_ns",
    }
)


def _item_from_payload(value: object) -> HistoricalBatchItem:
    payload = _require_exact_keys(value, _ITEM_KEYS)
    job_id = payload["job_id"]
    movie_code = payload["movie_code"]
    path_movie_number = payload["path_movie_number"]
    expected_status = payload["expected_status"]
    expected_updated_at = payload["expected_updated_at"]
    raw_reasons = payload["reason_codes"]
    if (
        not isinstance(job_id, str)
        or not job_id
        or not isinstance(movie_code, str)
        or canonical_movie_code(movie_code) != movie_code
        or not isinstance(path_movie_number, str)
        or normalize_movie_number(path_movie_number) != movie_code
        or not isinstance(expected_status, str)
        or JobStatus(expected_status) not in ELIGIBLE_STATUSES
        or not isinstance(expected_updated_at, str)
        or not expected_updated_at
        or not isinstance(raw_reasons, list)
        or not raw_reasons
        or any(
            not isinstance(reason, str)
            or _SAFE_REASON_RE.fullmatch(reason) is None
            for reason in raw_reasons
        )
        or len(set(raw_reasons)) != len(raw_reasons)
    ):
        raise ValueError("invalid item")
    return HistoricalBatchItem(
        job_id=job_id,
        movie_code=movie_code,
        path_movie_number=path_movie_number,
        expected_status=expected_status,
        expected_updated_at=expected_updated_at,
        reason_codes=tuple(raw_reasons),
        japanese_sha256=_require_digest(payload["japanese_sha256"]),
        japanese_size=_require_int(payload["japanese_size"], minimum=1),
        japanese_mtime_ns=_require_int(payload["japanese_mtime_ns"]),
        audio_sha256=_require_digest(payload["audio_sha256"]),
        audio_size=_require_int(payload["audio_size"], minimum=1),
        audio_mtime_ns=_require_int(payload["audio_mtime_ns"]),
        english_sha256=_require_digest(payload["english_sha256"]),
        english_size=_require_int(payload["english_size"], minimum=1),
        english_mtime_ns=_require_int(payload["english_mtime_ns"]),
    )


def _plan_from_payload(value: object) -> HistoricalBatchPlan:
    payload = _require_exact_keys(value, _PLAN_KEYS)
    version = _require_int(payload["version"], minimum=1)
    limit = _require_int(payload["limit"], minimum=1)
    allowlist_path = payload["allowlist_path"]
    raw_items = payload["items"]
    if (
        version != PLAN_VERSION
        or limit > MAX_BATCH_LIMIT
        or not isinstance(allowlist_path, str)
        or not Path(allowlist_path).is_absolute()
        or not isinstance(raw_items, list)
    ):
        raise ValueError("invalid plan")
    items = tuple(_item_from_payload(item) for item in raw_items)
    eligible_total = _require_int(payload["eligible_total"])
    already_repaired = _require_int(payload["already_repaired"])
    ineligible = _require_int(payload["ineligible"])
    blocked = _require_int(payload["blocked"])
    entry_count = _require_int(payload["allowlist_entry_count"], minimum=1)
    if (
        len(items) > limit
        or len(items) > eligible_total
        or entry_count != eligible_total + already_repaired + ineligible + blocked
        or tuple(item.movie_code for item in items)
        != tuple(sorted(item.movie_code for item in items))
        or len({item.movie_code for item in items}) != len(items)
    ):
        raise ValueError("invalid counts")
    plan = HistoricalBatchPlan(
        version=version,
        allowlist_path=allowlist_path,
        allowlist_sha256=_require_digest(payload["allowlist_sha256"]),
        allowlist_entry_count=entry_count,
        limit=limit,
        eligible_total=eligible_total,
        already_repaired=already_repaired,
        ineligible=ineligible,
        blocked=blocked,
        scan_sha256=_require_digest(payload["scan_sha256"]),
        items=items,
        plan_sha256=_require_digest(payload["plan_sha256"]),
    )
    if plan.recalculate_sha256() != plan.plan_sha256:
        raise ValueError("digest mismatch")
    return plan


def _open_stable_regular_file(
    path: Path,
    *,
    keep_content: bool,
    max_bytes: int | None = None,
) -> _FileSnapshot:
    path = Path(path)
    before_path = path.lstat()
    if not stat.S_ISREG(before_path.st_mode) or before_path.st_size <= 0:
        raise OSError("not a nonempty regular file")
    if max_bytes is not None and before_path.st_size > max_bytes:
        raise OSError("file too large")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or (before.st_dev, before.st_ino) != (before_path.st_dev, before_path.st_ino)
        ):
            raise OSError("file changed")
        digest = hashlib.sha256()
        content = bytearray() if keep_content else None
        remaining = before.st_size
        while remaining:
            chunk = os.read(fd, min(1024 * 1024, remaining))
            if not chunk:
                raise OSError("file changed")
            digest.update(chunk)
            if content is not None:
                content.extend(chunk)
            remaining -= len(chunk)
        if os.read(fd, 1):
            raise OSError("file changed")
        after = os.fstat(fd)
        after_path = path.lstat()
        snapshot_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if (
            any(getattr(before, key) != getattr(after, key) for key in snapshot_fields)
            or (after_path.st_dev, after_path.st_ino, after_path.st_size, after_path.st_mtime_ns)
            != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        ):
            raise OSError("file changed")
        return _FileSnapshot(
            sha256=digest.hexdigest(),
            size=before.st_size,
            mtime_ns=before.st_mtime_ns,
            content=bytes(content) if content is not None else None,
        )
    finally:
        os.close(fd)


def _open_stable_regular_file_at(
    directory_fd: int,
    basename: str,
    *,
    keep_content: bool,
    max_bytes: int | None = None,
) -> _FileSnapshot:
    before_path = os.stat(basename, dir_fd=directory_fd, follow_symlinks=False)
    if not stat.S_ISREG(before_path.st_mode) or before_path.st_size <= 0:
        raise OSError("not a nonempty regular file")
    if max_bytes is not None and before_path.st_size > max_bytes:
        raise OSError("file too large")
    fd = os.open(
        basename,
        os.O_RDONLY | os.O_NOFOLLOW,
        dir_fd=directory_fd,
    )
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or (before.st_dev, before.st_ino) != (before_path.st_dev, before_path.st_ino)
        ):
            raise OSError("file changed")
        digest = hashlib.sha256()
        content = bytearray() if keep_content else None
        remaining = before.st_size
        while remaining:
            chunk = os.read(fd, min(1024 * 1024, remaining))
            if not chunk:
                raise OSError("file changed")
            digest.update(chunk)
            if content is not None:
                content.extend(chunk)
            remaining -= len(chunk)
        if os.read(fd, 1):
            raise OSError("file changed")
        after = os.fstat(fd)
        after_path = os.stat(basename, dir_fd=directory_fd, follow_symlinks=False)
        snapshot_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if (
            any(getattr(before, key) != getattr(after, key) for key in snapshot_fields)
            or (after_path.st_dev, after_path.st_ino, after_path.st_size, after_path.st_mtime_ns)
            != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        ):
            raise OSError("file changed")
        return _FileSnapshot(
            sha256=digest.hexdigest(),
            size=before.st_size,
            mtime_ns=before.st_mtime_ns,
            content=bytes(content) if content is not None else None,
            basename=basename,
        )
    finally:
        os.close(fd)


def _snapshot_three_job_files(
    directory_fd: int,
    path_movie_number: str,
) -> tuple[_FileSnapshot, _FileSnapshot, _FileSnapshot]:
    return (
        _open_stable_regular_file_at(
            directory_fd,
            f"{path_movie_number}.Japanese.srt",
            keep_content=True,
            max_bytes=MAX_SUBTITLE_BYTES,
        ),
        _open_stable_regular_file_at(
            directory_fd,
            f"{path_movie_number}.English.srt",
            keep_content=True,
            max_bytes=MAX_SUBTITLE_BYTES,
        ),
        _open_stable_regular_file_at(
            directory_fd,
            "audio.wav",
            keep_content=False,
        ),
    )


def _prehold_plan_job_files(
    store: JobStore,
    plan: HistoricalBatchPlan,
    allowlist_path: Path,
    *,
    confirm_plan_sha256: str,
    root_lock: JobsRootLock,
    descriptor_stack: ExitStack,
) -> dict[str, _PreheldJobFiles]:
    try:
        _validate_plan_confirmation(
            plan,
            allowlist_path,
            confirm_plan_sha256=confirm_plan_sha256,
        )
        result: dict[str, _PreheldJobFiles] = {}
        for item in sorted(plan.items, key=lambda candidate: candidate.path_movie_number):
            held = descriptor_stack.enter_context(
                shared_job_files_lock_from_root(
                    root_lock,
                    item.path_movie_number,
                    blocking=True,
                )
            )
            result[item.path_movie_number] = _PreheldJobFiles(held)
        return result
    except (JobFilesLockError, OSError, TypeError, ValueError):
        raise ValueError("historical_plan_changed") from None


def _snapshot_job_files(
    root_lock: JobsRootLock,
    path_movie_number: str,
    *,
    held_snapshots: list[_HeldJobSnapshot] | None = None,
    preheld: _PreheldJobFiles | None = None,
) -> tuple[_FileSnapshot, _FileSnapshot, _FileSnapshot]:
    if (
        normalize_movie_number(path_movie_number) is None
        or Path(path_movie_number).name != path_movie_number
    ):
        raise OSError("unsafe job directory")
    if preheld is not None:
        snapshots = preheld.snapshots()
        held_snapshot = _HeldJobSnapshot(preheld.lock, snapshots)
        held_snapshot.require_unchanged()
        if held_snapshots is not None:
            held_snapshots.append(held_snapshot)
        return snapshots
    with open_job_directory_from_root(root_lock, path_movie_number) as job_fd:
        return _snapshot_three_job_files(job_fd, path_movie_number)


def _read_allowlist(path: Path) -> tuple[tuple[str, ...], str, str]:
    try:
        snapshot = _open_stable_regular_file(
            path,
            keep_content=True,
            max_bytes=MAX_ALLOWLIST_BYTES,
        )
        assert snapshot.content is not None
        text = snapshot.content.decode("utf-8")
        raw_lines = text.splitlines()
        if not raw_lines or len(raw_lines) > MAX_ALLOWLIST_ENTRIES:
            raise ValueError
        movies: list[str] = []
        seen: set[str] = set()
        for raw_line in raw_lines:
            value = raw_line.strip()
            normalized = normalize_movie_number(value)
            if not value or normalized is None or normalized in seen:
                raise ValueError
            seen.add(normalized)
            movies.append(normalized)
        absolute_path = str(Path(path).absolute())
        return tuple(sorted(movies)), snapshot.sha256, absolute_path
    except (OSError, UnicodeError, ValueError):
        raise ValueError("allowlist_invalid") from None


def load_repair_allowlist(path: Path) -> frozenset[str]:
    movies, _, _ = _read_allowlist(path)
    return frozenset(movies)


def _read_only_connection(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(Path(db_path).absolute()), safe='/')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _scan_snapshot_entry(
    *,
    movie: str,
    classification: str,
    row: sqlite3.Row,
    japanese: _FileSnapshot,
    english: _FileSnapshot,
    audio: _FileSnapshot,
    reason_codes: tuple[str, ...],
) -> dict[str, object]:
    return {
        "movie_code": movie,
        "classification": classification,
        "job_id": row["id"],
        "path_movie_number": row["normalized_movie_number"],
        "status": row["status"],
        "updated_at": row["updated_at"],
        "reason_codes": list(reason_codes),
        "japanese": {
            "sha256": japanese.sha256,
            "size": japanese.size,
            "mtime_ns": japanese.mtime_ns,
        },
        "english": {
            "sha256": english.sha256,
            "size": english.size,
            "mtime_ns": english.mtime_ns,
        },
        "audio": {
            "sha256": audio.sha256,
            "size": audio.size,
            "mtime_ns": audio.mtime_ns,
        },
    }


def _build_plan(
    store: JobStore,
    allowlist_path: Path,
    *,
    limit: int,
    conn: sqlite3.Connection,
    root_lock: JobsRootLock,
    ignore_batch_id: str | None = None,
    held_snapshots: list[_HeldJobSnapshot] | None = None,
    preheld_jobs: dict[str, _PreheldJobFiles] | None = None,
) -> HistoricalBatchPlan:
    movies, allowlist_sha256, absolute_allowlist_path = _read_allowlist(allowlist_path)
    jobs_by_movie: dict[str, list[Any]] = {}
    for row in conn.execute("SELECT * FROM jobs ORDER BY created_at ASC, id ASC"):
        try:
            canonical = canonical_movie_code(row["normalized_movie_number"])
        except (TypeError, ValueError):
            continue
        jobs_by_movie.setdefault(canonical, []).append(row)
    repair_rows = conn.execute(
        "SELECT job_id, batch_id FROM historical_translation_repairs"
    ).fetchall()
    repair_by_job = {
        row["job_id"]: row["batch_id"]
        for row in repair_rows
        if ignore_batch_id is None or row["batch_id"] != ignore_batch_id
    }
    repair_jobs = {
        job_id for job_id in repair_by_job
    }
    eligible: list[HistoricalBatchItem] = []
    scan_entries: list[dict[str, object]] = []
    already_repaired = 0
    ineligible = 0
    blocked = 0
    for movie in movies:
        matching = jobs_by_movie.get(movie, [])
        if len(matching) != 1:
            blocked += 1
            scan_entries.append(
                {
                    "movie_code": movie,
                    "classification": "blocked",
                    "reason": "job_missing" if not matching else "job_ambiguous",
                    "job_ids": sorted(row["id"] for row in matching),
                }
            )
            continue
        row = matching[0]
        if row["id"] in repair_jobs:
            already_repaired += 1
            scan_entries.append(
                {
                    "movie_code": movie,
                    "classification": "already_repaired",
                    "job_id": row["id"],
                    "batch_id": repair_by_job[row["id"]],
                    "updated_at": row["updated_at"],
                }
            )
            continue
        try:
            status_value = JobStatus(row["status"])
        except ValueError:
            blocked += 1
            scan_entries.append(
                {
                    "movie_code": movie,
                    "classification": "blocked",
                    "reason": "job_status_invalid",
                    "job_id": row["id"],
                    "updated_at": row["updated_at"],
                }
            )
            continue
        if status_value not in ELIGIBLE_STATUSES or row["claimed_by"] is not None:
            blocked += 1
            scan_entries.append(
                {
                    "movie_code": movie,
                    "classification": "blocked",
                    "reason": "job_not_idle_eligible",
                    "job_id": row["id"],
                    "status": status_value.value,
                    "updated_at": row["updated_at"],
                    "claimed": row["claimed_by"] is not None,
                }
            )
            continue
        path_movie_number = row["normalized_movie_number"]
        try:
            japanese, english, audio = _snapshot_job_files(
                root_lock,
                path_movie_number,
                held_snapshots=held_snapshots,
                preheld=(preheld_jobs or {}).get(path_movie_number),
            )
        except (JobFilesLockError, OSError):
            blocked += 1
            scan_entries.append(
                {
                    "movie_code": movie,
                    "classification": "blocked",
                    "reason": "snapshot_unavailable",
                    "job_id": row["id"],
                    "status": status_value.value,
                    "updated_at": row["updated_at"],
                }
            )
            continue
        assert japanese.content is not None and english.content is not None
        report = validate_translation_quality_snapshots(
            japanese.content,
            english.content,
        )
        if report.passed:
            ineligible += 1
            scan_entries.append(
                _scan_snapshot_entry(
                    movie=movie,
                    classification="ineligible",
                    row=row,
                    japanese=japanese,
                    english=english,
                    audio=audio,
                    reason_codes=(),
                )
            )
            continue
        scan_entries.append(
            _scan_snapshot_entry(
                movie=movie,
                classification="eligible",
                row=row,
                japanese=japanese,
                english=english,
                audio=audio,
                reason_codes=tuple(report.reason_codes),
            )
        )
        eligible.append(
            HistoricalBatchItem(
                job_id=row["id"],
                movie_code=movie,
                path_movie_number=path_movie_number,
                expected_status=status_value.value,
                expected_updated_at=row["updated_at"],
                reason_codes=tuple(report.reason_codes),
                japanese_sha256=japanese.sha256,
                japanese_size=japanese.size,
                japanese_mtime_ns=japanese.mtime_ns,
                audio_sha256=audio.sha256,
                audio_size=audio.size,
                audio_mtime_ns=audio.mtime_ns,
                english_sha256=english.sha256,
                english_size=english.size,
                english_mtime_ns=english.mtime_ns,
            )
        )
    eligible.sort(key=lambda item: (item.movie_code, item.path_movie_number, item.job_id))
    return HistoricalBatchPlan.build(
        allowlist_path=absolute_allowlist_path,
        allowlist_sha256=allowlist_sha256,
        allowlist_entry_count=len(movies),
        limit=limit,
        eligible_total=len(eligible),
        already_repaired=already_repaired,
        ineligible=ineligible,
        blocked=blocked,
        scan_sha256=hashlib.sha256(
            canonical_plan_bytes({"entries": scan_entries})
        ).hexdigest(),
        items=tuple(eligible[:limit]),
    )


def plan_historical_batch(
    store: JobStore,
    allowlist_path: Path,
    *,
    limit: int,
) -> HistoricalBatchPlan:
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= MAX_BATCH_LIMIT
    ):
        raise ValueError("historical batch limit must be between 1 and 20")
    with exclusive_jobs_root_lock(
        store.jobs_root_mac,
        blocking=True,
    ) as root_lock:
        conn = _read_only_connection(store.db_path)
        try:
            return _build_plan(
                store,
                Path(allowlist_path),
                limit=limit,
                conn=conn,
                root_lock=root_lock,
            )
        finally:
            conn.close()


def render_historical_batch_report(plan: HistoricalBatchPlan) -> str:
    lines = [
        f"planned=true batch_id={plan.batch_id} plan_sha256={plan.plan_sha256} "
        f"allowlist_sha256={plan.allowlist_sha256} "
        f"scan_sha256={plan.scan_sha256} "
        f"allowlist_entries={plan.allowlist_entry_count} "
        f"eligible_total={plan.eligible_total} selected={len(plan.items)} "
        f"already_repaired={plan.already_repaired} "
        f"ineligible={plan.ineligible} blocked={plan.blocked}"
    ]
    lines.extend(
        f"item job_id={item.job_id} movie={item.movie_code} "
        "actions=quarantine_english,reset_translation_stage,upsert_english_subtitle"
        for item in plan.items
    )
    return "\n".join(lines)


def _open_directory_chain(path: Path, *, create: bool) -> int:
    absolute = Path(path).absolute()
    if not absolute.is_absolute():
        raise OSError("directory path is not absolute")
    components = absolute.parts[1:]
    if any(component in {"", ".", ".."} for component in components):
        raise OSError("unsafe directory component")
    current_fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for component in components:
            try:
                entry = os.stat(
                    component,
                    dir_fd=current_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(component, mode=0o700, dir_fd=current_fd)
                entry = os.stat(
                    component,
                    dir_fd=current_fd,
                    follow_symlinks=False,
                )
            if not stat.S_ISDIR(entry.st_mode) or stat.S_ISLNK(entry.st_mode):
                raise OSError("unsafe directory component")
            next_fd = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=current_fd,
            )
            opened = os.fstat(next_fd)
            if (entry.st_dev, entry.st_ino) != (opened.st_dev, opened.st_ino):
                os.close(next_fd)
                raise OSError("directory component changed")
            os.close(current_fd)
            current_fd = next_fd
        return current_fd
    except BaseException:
        os.close(current_fd)
        raise


def _require_parent_path_bound(parent: Path, expected_stat: os.stat_result) -> None:
    reopened_fd = _open_directory_chain(parent, create=False)
    try:
        reopened = os.fstat(reopened_fd)
        if (reopened.st_dev, reopened.st_ino) != (
            expected_stat.st_dev,
            expected_stat.st_ino,
        ):
            raise OSError("plan parent changed")
    finally:
        os.close(reopened_fd)


def _unlink_if_same_inode(
    parent_fd: int,
    basename: str,
    expected_inode: tuple[int, int],
) -> None:
    try:
        current = os.stat(basename, dir_fd=parent_fd, follow_symlinks=False)
    except OSError:
        return
    if (current.st_dev, current.st_ino) == expected_inode:
        try:
            os.unlink(basename, dir_fd=parent_fd)
        except OSError:
            pass


def write_private_plan(path: Path, plan: HistoricalBatchPlan) -> None:
    absolute = Path(path).absolute()
    parent = absolute.parent
    basename = absolute.name
    if not basename or basename in {".", ".."}:
        raise ValueError("plan_output_unsafe")
    parent_fd: int | None = None
    temporary_fd: int | None = None
    temporary_name = f".{basename}.{os.getpid()}.{os.urandom(8).hex()}.tmp"
    temporary_inode: tuple[int, int] | None = None
    final_inode: tuple[int, int] | None = None
    try:
        parent_fd = _open_directory_chain(parent, create=True)
        parent_stat = os.fstat(parent_fd)
        try:
            os.stat(basename, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise OSError("plan output exists")
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent_fd,
        )
        os.fchmod(temporary_fd, 0o600)
        temporary_stat = os.fstat(temporary_fd)
        temporary_inode = (temporary_stat.st_dev, temporary_stat.st_ino)
        snapshot = plan.to_json_bytes()
        written = 0
        while written < len(snapshot):
            written += os.write(temporary_fd, snapshot[written:])
        os.fsync(temporary_fd)
        _require_parent_path_bound(parent, parent_stat)
        os.link(
            temporary_name,
            basename,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
            follow_symlinks=False,
        )
        final_inode = temporary_inode
        _require_parent_path_bound(parent, parent_stat)
        os.unlink(temporary_name, dir_fd=parent_fd)
        temporary_inode = None
        os.fsync(parent_fd)
        final_inode = None
    except OSError:
        raise ValueError("plan_output_unsafe") from None
    finally:
        if temporary_fd is not None:
            os.close(temporary_fd)
        if parent_fd is not None:
            if final_inode is not None:
                _unlink_if_same_inode(parent_fd, basename, final_inode)
            if temporary_inode is not None:
                _unlink_if_same_inode(
                    parent_fd,
                    temporary_name,
                    temporary_inode,
                )
            try:
                os.fsync(parent_fd)
            except OSError:
                pass
            os.close(parent_fd)


def read_private_plan(path: Path) -> HistoricalBatchPlan:
    try:
        snapshot = _open_stable_regular_file(
            path,
            keep_content=True,
            max_bytes=MAX_SUBTITLE_BYTES,
        )
        mode = Path(path).lstat().st_mode & 0o777
        if mode & 0o077:
            raise OSError
        assert snapshot.content is not None
        return HistoricalBatchPlan.from_json_bytes(snapshot.content)
    except OSError:
        raise ValueError("historical_plan_invalid") from None


def _row_to_repair(row: sqlite3.Row) -> HistoricalRepairRecord:
    return HistoricalRepairRecord(
        id=row["id"],
        batch_id=row["batch_id"],
        job_id=row["job_id"],
        movie_code=row["movie_code"],
        allowlist_sha256=row["allowlist_sha256"],
        state=HistoricalRepairState(row["state"]),
        attempt_count=row["attempt_count"],
        next_attempt_at=row["next_attempt_at"],
        reason_code=row["reason_code"],
        japanese_sha256=row["japanese_sha256"],
        audio_sha256=row["audio_sha256"],
        source_english_sha256=row["source_english_sha256"],
        english_sha256=row["english_sha256"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _validate_plan_confirmation(
    plan: HistoricalBatchPlan,
    allowlist_path: Path,
    *,
    confirm_plan_sha256: str,
) -> None:
    if (
        not isinstance(confirm_plan_sha256, str)
        or _LOWER_HEX_RE.fullmatch(confirm_plan_sha256) is None
        or confirm_plan_sha256 != plan.plan_sha256
        or plan.recalculate_sha256() != plan.plan_sha256
        or HistoricalBatchPlan.from_json_bytes(plan.to_json_bytes()) != plan
        or str(Path(allowlist_path).absolute()) != plan.allowlist_path
    ):
        raise ValueError("historical_plan_changed")


def _exact_existing_records(
    conn: sqlite3.Connection,
    plan: HistoricalBatchPlan,
) -> list[HistoricalRepairRecord] | None:
    rows = conn.execute(
        "SELECT * FROM historical_translation_repairs WHERE batch_id = ? "
        "ORDER BY movie_code ASC, job_id ASC",
        (plan.batch_id,),
    ).fetchall()
    if not rows:
        return None
    if len(rows) != len(plan.items):
        raise ValueError("historical_plan_changed")
    expected_by_job = {item.job_id: item for item in plan.items}
    if {row["job_id"] for row in rows} != set(expected_by_job):
        raise ValueError("historical_plan_changed")
    for row in rows:
        item = expected_by_job[row["job_id"]]
        expected_id = "repair_" + hashlib.sha256(
            f"{plan.plan_sha256}:{item.job_id}".encode()
        ).hexdigest()[:32]
        if (
            row["id"] != expected_id
            or row["batch_id"] != plan.batch_id
            or row["movie_code"] != item.movie_code
            or row["allowlist_sha256"] != plan.allowlist_sha256
            or row["japanese_sha256"] != item.japanese_sha256
            or row["audio_sha256"] != item.audio_sha256
            or row["source_english_sha256"] != item.english_sha256
        ):
            raise ValueError("historical_plan_changed")
    return [_row_to_repair(row) for row in rows]


def find_idempotent_historical_enqueue(
    store: JobStore,
    plan: HistoricalBatchPlan,
    allowlist_path: Path,
    *,
    confirm_plan_sha256: str,
) -> list[HistoricalRepairRecord] | None:
    try:
        _validate_plan_confirmation(
            plan,
            allowlist_path,
            confirm_plan_sha256=confirm_plan_sha256,
        )
        conn = _read_only_connection(store.db_path)
        try:
            return _exact_existing_records(conn, plan)
        finally:
            conn.close()
    except (sqlite3.Error, TypeError, ValueError):
        raise ValueError("historical_plan_changed") from None


def _enqueue_historical_repairs_transaction(
    store: JobStore,
    conn: sqlite3.Connection,
    plan: HistoricalBatchPlan,
    allowlist_path: Path,
    *,
    confirm_plan_sha256: str,
    root_lock: JobsRootLock,
    preheld_jobs: dict[str, _PreheldJobFiles],
) -> list[HistoricalRepairRecord]:
    try:
        _validate_plan_confirmation(
            plan,
            allowlist_path,
            confirm_plan_sha256=confirm_plan_sha256,
        )
        existing_records = _exact_existing_records(conn, plan)
        if existing_records is not None:
            return existing_records
        held_snapshots: list[_HeldJobSnapshot] = []
        recalculated = _build_plan(
            store,
            Path(allowlist_path),
            limit=plan.limit,
            conn=conn,
            root_lock=root_lock,
            ignore_batch_id=plan.batch_id,
            held_snapshots=held_snapshots,
            preheld_jobs=preheld_jobs,
        )
        if recalculated != plan:
            raise ValueError
        for snapshot in held_snapshots:
            snapshot.require_unchanged()
        now = utc_now_iso()
        for item in plan.items:
            repair_id = "repair_" + hashlib.sha256(
                f"{plan.plan_sha256}:{item.job_id}".encode()
            ).hexdigest()[:32]
            conn.execute(
                """
                INSERT INTO historical_translation_repairs (
                  id, batch_id, job_id, movie_code, allowlist_sha256, state,
                  attempt_count, next_attempt_at, reason_code, japanese_sha256,
                  audio_sha256, source_english_sha256, english_sha256,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    repair_id,
                    plan.batch_id,
                    item.job_id,
                    item.movie_code,
                    plan.allowlist_sha256,
                    HistoricalRepairState.PENDING.value,
                    item.japanese_sha256,
                    item.audio_sha256,
                    item.english_sha256,
                    now,
                    now,
                ),
            )
        rows = conn.execute(
            "SELECT * FROM historical_translation_repairs WHERE batch_id = ? "
            "ORDER BY movie_code ASC, job_id ASC",
            (plan.batch_id,),
        ).fetchall()
        return [_row_to_repair(row) for row in rows]
    except (OSError, sqlite3.Error, TypeError, ValueError):
        raise ValueError("historical_plan_changed") from None


def enqueue_historical_batch(
    store: JobStore,
    plan: HistoricalBatchPlan,
    allowlist_path: Path,
    *,
    confirm_plan_sha256: str,
) -> list[HistoricalRepairRecord]:
    return store.enqueue_historical_repairs(
        plan,
        Path(allowlist_path),
        confirm_plan_sha256=confirm_plan_sha256,
    )
