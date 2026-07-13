from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import struct
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from orchestrator.historical_repair import ELIGIBLE_STATUSES
from orchestrator.job_files_lock import (
    JobFilesLockError,
    JobsRootLock,
    exclusive_jobs_root_lock,
    open_job_directory_from_root,
    shared_job_files_lock_from_root,
    shared_jobs_root_lock,
)
from orchestrator.models import JobStatus
from orchestrator.movie_code import canonical_movie_code
from orchestrator.paths import normalize_movie_number
from orchestrator.store import (
    HistoricalControllerStateUnavailable,
    HistoricalRepairRecord,
    HistoricalRepairState,
    normal_translation_backlog_exists,
    pin_or_validate_historical_controller_identity_conn,
    utc_now_iso,
)
from orchestrator.subtitle_quality import validate_translation_quality_snapshots

if TYPE_CHECKING:
    from orchestrator.store import JobStore


PLAN_VERSION = 3
MAX_BATCH_LIMIT = 20
MAX_ALLOWLIST_BYTES = 1024 * 1024
MAX_ALLOWLIST_VERIFY_BYTES = MAX_ALLOWLIST_BYTES
MAX_ALLOWLIST_ENTRIES = 10_000
MAX_SUBTITLE_BYTES = 32 * 1024 * 1024
MAX_AUDIO_PROBE_BYTES = 4 * 1024
MAX_WAV_CHUNKS = 128
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
    audio_probe_snapshot_sha256: str
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
    scan_entries: int
    audio_probe_max_bytes: int
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
            "scan_entries": self.scan_entries,
            "audio_probe_max_bytes": self.audio_probe_max_bytes,
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
        scan_entries: int,
        audio_probe_max_bytes: int,
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
            scan_entries=scan_entries,
            audio_probe_max_bytes=audio_probe_max_bytes,
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
class HistoricalAllowlistIdentity:
    path_sha256: str
    content_sha256: str
    canonical_codes_sha256: str

    def as_store_tuple(self) -> tuple[str, str, str]:
        return (
            self.path_sha256,
            self.content_sha256,
            self.canonical_codes_sha256,
        )


@dataclass(frozen=True, slots=True)
class HistoricalControllerResult:
    action: str
    reason_code: str
    hard_pause: bool
    complete: bool
    enqueued: int
    batch_id: str | None
    plan_sha: str | None
    allowlist_sha: str | None
    counts: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class _FileSnapshot:
    sha256: str
    size: int
    mtime_ns: int
    content: bytes | None
    basename: str | None = None
    device: int = 0
    inode: int = 0
    ctime_ns: int = 0

    def require_unchanged_at(self, directory_fd: int) -> None:
        if self.basename is None:
            raise OSError("snapshot basename unavailable")
        current = os.stat(
            self.basename,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(current.st_mode)
            or (
                current.st_dev,
                current.st_ino,
                current.st_size,
                current.st_mtime_ns,
                current.st_ctime_ns,
            )
            != (
                self.device,
                self.inode,
                self.size,
                self.mtime_ns,
                self.ctime_ns,
            )
        ):
            raise OSError("snapshot path changed")


@dataclass(frozen=True, slots=True)
class _ScannedJobFiles:
    path_movie_number: str
    japanese: _FileSnapshot
    english: _FileSnapshot
    audio: _FileSnapshot
    quality_passed: bool
    quality_reason_codes: tuple[str, ...]

    def require_unchanged(self, root_lock: JobsRootLock) -> None:
        with open_job_directory_from_root(
            root_lock,
            self.path_movie_number,
        ) as job_fd:
            self.japanese.require_unchanged_at(job_fd)
            self.english.require_unchanged_at(job_fd)
            self.audio.require_unchanged_at(job_fd)


@dataclass(frozen=True, slots=True)
class _FilesystemScan:
    movies: tuple[str, ...]
    allowlist_sha256: str
    absolute_allowlist_path: str
    files_by_path: dict[str, _ScannedJobFiles]

    def require_unchanged(self, root_lock: JobsRootLock) -> None:
        root_lock.require_bound()
        for path_movie_number in sorted(self.files_by_path):
            self.files_by_path[path_movie_number].require_unchanged(root_lock)
        root_lock.require_bound()


@dataclass(frozen=True, slots=True)
class _AllowlistSnapshot:
    absolute_path: str
    parent: Path
    basename: str
    parent_fd: int
    file_fd: int
    parent_stat: os.stat_result
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int
    content: bytes
    sha256: str
    movies: tuple[str, ...]

    def require_metadata_unchanged(self) -> None:
        _require_parent_path_bound(self.parent, self.parent_stat)
        path_stat = os.stat(
            self.basename,
            dir_fd=self.parent_fd,
            follow_symlinks=False,
        )
        opened = os.fstat(self.file_fd)
        expected = (
            self.device,
            self.inode,
            self.size,
            self.mtime_ns,
            self.ctime_ns,
        )
        if (
            not stat.S_ISREG(path_stat.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or (
                path_stat.st_dev,
                path_stat.st_ino,
                path_stat.st_size,
                path_stat.st_mtime_ns,
                path_stat.st_ctime_ns,
            )
            != expected
            or (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            )
            != expected
        ):
            raise OSError("allowlist binding changed")

    def require_exact_unchanged(self) -> None:
        self.require_metadata_unchanged()
        content = _pread_exact_regular_file(
            self.file_fd,
            self.size,
            max_bytes=MAX_ALLOWLIST_VERIFY_BYTES,
        )
        if content != self.content or hashlib.sha256(content).hexdigest() != self.sha256:
            raise OSError("allowlist content changed")
        self.require_metadata_unchanged()


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
        "scan_entries",
        "audio_probe_max_bytes",
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
        "audio_probe_snapshot_sha256",
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
        audio_probe_snapshot_sha256=_require_digest(
            payload["audio_probe_snapshot_sha256"]
        ),
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
    scan_entries = _require_int(payload["scan_entries"], minimum=1)
    audio_probe_max_bytes = _require_int(
        payload["audio_probe_max_bytes"], minimum=1
    )
    entry_count = _require_int(payload["allowlist_entry_count"], minimum=1)
    if (
        len(items) > limit
        or len(items) > eligible_total
        or entry_count != eligible_total + already_repaired + ineligible + blocked
        or scan_entries != entry_count
        or audio_probe_max_bytes != MAX_AUDIO_PROBE_BYTES
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
        scan_entries=scan_entries,
        audio_probe_max_bytes=audio_probe_max_bytes,
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
            device=before.st_dev,
            inode=before.st_ino,
            ctime_ns=before.st_ctime_ns,
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
            device=before.st_dev,
            inode=before.st_ino,
            ctime_ns=before.st_ctime_ns,
        )
    finally:
        os.close(fd)


def _bounded_pread(
    fd: int,
    size: int,
    offset: int,
    *,
    consumed: list[int],
) -> bytes:
    if size < 0 or offset < 0 or consumed[0] + size > MAX_AUDIO_PROBE_BYTES:
        raise OSError("wav probe budget exceeded")
    result = bytearray()
    while len(result) < size:
        chunk = os.pread(fd, size - len(result), offset + len(result))
        if not chunk:
            raise OSError("truncated wav header")
        result.extend(chunk)
    consumed[0] += len(result)
    return bytes(result)


def _open_bounded_wav_snapshot_at(
    directory_fd: int,
    basename: str,
) -> _FileSnapshot:
    before_path = os.stat(basename, dir_fd=directory_fd, follow_symlinks=False)
    if not stat.S_ISREG(before_path.st_mode) or before_path.st_size < 44:
        raise OSError("invalid wav file")
    fd = os.open(
        basename,
        os.O_RDONLY | os.O_NOFOLLOW,
        dir_fd=directory_fd,
    )
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size < 44
            or (before.st_dev, before.st_ino)
            != (before_path.st_dev, before_path.st_ino)
        ):
            raise OSError("wav file changed")
        consumed = [0]
        header = _bounded_pread(fd, 12, 0, consumed=consumed)
        if header[:4] != b"RIFF" or header[8:] != b"WAVE":
            raise OSError("unsupported wav container")
        riff_size = struct.unpack_from("<I", header, 4)[0]
        riff_end = riff_size + 8
        if riff_end < 44 or riff_end > before.st_size:
            raise OSError("invalid wav RIFF size")
        fmt_metadata: tuple[int, int, int, int, int, int, int, int] | None = None
        data_metadata: tuple[int, int] | None = None
        offset = 12
        chunk_count = 0
        while offset + 8 <= riff_end:
            chunk_count += 1
            if chunk_count > MAX_WAV_CHUNKS:
                raise OSError("wav chunk limit exceeded")
            chunk_header = _bounded_pread(fd, 8, offset, consumed=consumed)
            chunk_id = chunk_header[:4]
            chunk_size = struct.unpack_from("<I", chunk_header, 4)[0]
            data_offset = offset + 8
            next_offset = data_offset + chunk_size + (chunk_size & 1)
            if next_offset > riff_end:
                raise OSError("wav chunk exceeds RIFF bounds")
            if chunk_id == b"fmt " and fmt_metadata is None:
                if chunk_size < 16:
                    raise OSError("invalid wav fmt chunk")
                raw_fmt = _bounded_pread(fd, 16, data_offset, consumed=consumed)
                (
                    audio_format,
                    channels,
                    sample_rate,
                    byte_rate,
                    block_align,
                    bits_per_sample,
                ) = struct.unpack("<HHIIHH", raw_fmt)
                if (
                    audio_format not in {1, 3, 0xFFFE}
                    or not 1 <= channels <= 32
                    or sample_rate <= 0
                    or byte_rate <= 0
                    or block_align <= 0
                    or not 1 <= bits_per_sample <= 64
                    or byte_rate != sample_rate * block_align
                ):
                    raise OSError("invalid wav format metadata")
                fmt_metadata = (
                    data_offset,
                    chunk_size,
                    audio_format,
                    channels,
                    sample_rate,
                    byte_rate,
                    block_align,
                    bits_per_sample,
                )
            elif chunk_id == b"data" and data_metadata is None:
                data_metadata = (data_offset, chunk_size)
            if fmt_metadata is not None and data_metadata is not None:
                break
            offset = next_offset
        if fmt_metadata is None or data_metadata is None:
            raise OSError("wav fmt/data chunks missing")
        (
            fmt_offset,
            fmt_size,
            audio_format,
            channels,
            sample_rate,
            byte_rate,
            block_align,
            bits_per_sample,
        ) = fmt_metadata
        data_offset, data_size = data_metadata
        sample_size = min(64, data_size)
        sample_offsets = tuple(
            sorted(
                {
                    data_offset,
                    data_offset + max(0, (data_size - sample_size) // 2),
                    data_offset + max(0, data_size - sample_size),
                }
            )
        )
        samples = [
            {
                "offset": offset,
                "sha256": hashlib.sha256(
                    _bounded_pread(fd, sample_size, offset, consumed=consumed)
                ).hexdigest(),
            }
            for offset in sample_offsets
        ]
        fingerprint = hashlib.sha256(
            canonical_plan_bytes(
                {
                    "file_size": before.st_size,
                    "mtime_ns": before.st_mtime_ns,
                    "riff_size": riff_size,
                    "fmt_offset": fmt_offset,
                    "fmt_size": fmt_size,
                    "audio_format": audio_format,
                    "channels": channels,
                    "sample_rate": sample_rate,
                    "byte_rate": byte_rate,
                    "block_align": block_align,
                    "bits_per_sample": bits_per_sample,
                    "data_offset": data_offset,
                    "data_size": data_size,
                    "duration_frames": data_size // block_align,
                    "duration_ms": data_size * 1000 // byte_rate,
                    "samples": samples,
                }
            )
        ).hexdigest()
        after = os.fstat(fd)
        after_path = os.stat(basename, dir_fd=directory_fd, follow_symlinks=False)
        snapshot_fields = (
            "st_dev",
            "st_ino",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if (
            any(getattr(before, key) != getattr(after, key) for key in snapshot_fields)
            or (
                after_path.st_dev,
                after_path.st_ino,
                after_path.st_size,
                after_path.st_mtime_ns,
            )
            != (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            )
        ):
            raise OSError("wav file changed")
        return _FileSnapshot(
            sha256=fingerprint,
            size=before.st_size,
            mtime_ns=before.st_mtime_ns,
            content=None,
            basename=basename,
            device=before.st_dev,
            inode=before.st_ino,
            ctime_ns=before.st_ctime_ns,
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
        _open_bounded_wav_snapshot_at(
            directory_fd,
            "audio.wav",
        ),
    )


def _pread_exact_regular_file(
    fd: int,
    size: int,
    *,
    max_bytes: int,
) -> bytes:
    if size <= 0 or size > max_bytes:
        raise OSError("bounded file size invalid")
    result = bytearray()
    while len(result) < size:
        chunk = os.pread(
            fd,
            min(64 * 1024, size - len(result)),
            len(result),
        )
        if not chunk:
            raise OSError("bounded file truncated")
        result.extend(chunk)
    if os.pread(fd, 1, size):
        raise OSError("bounded file grew")
    return bytes(result)


def _parse_allowlist_content(content: bytes) -> tuple[str, ...]:
    text = content.decode("utf-8")
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
    return tuple(sorted(movies))


@contextmanager
def _open_allowlist_snapshot(path: Path) -> Iterator[_AllowlistSnapshot]:
    absolute = Path(path).absolute()
    parent = absolute.parent
    basename = absolute.name
    parent_fd: int | None = None
    file_fd: int | None = None
    try:
        if not basename or basename in {".", ".."}:
            raise OSError("unsafe allowlist basename")
        parent_fd = _open_directory_chain(parent, create=False)
        parent_stat = os.fstat(parent_fd)
        path_stat = os.stat(
            basename,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(path_stat.st_mode)
            or path_stat.st_size <= 0
            or path_stat.st_size > MAX_ALLOWLIST_BYTES
        ):
            raise OSError("allowlist is not a bounded regular file")
        file_fd = os.open(
            basename,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=parent_fd,
        )
        opened = os.fstat(file_fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            )
            != (
                path_stat.st_dev,
                path_stat.st_ino,
                path_stat.st_size,
                path_stat.st_mtime_ns,
                path_stat.st_ctime_ns,
            )
        ):
            raise OSError("allowlist changed while opening")
        content = _pread_exact_regular_file(
            file_fd,
            opened.st_size,
            max_bytes=MAX_ALLOWLIST_BYTES,
        )
        snapshot = _AllowlistSnapshot(
            absolute_path=str(absolute),
            parent=parent,
            basename=basename,
            parent_fd=parent_fd,
            file_fd=file_fd,
            parent_stat=parent_stat,
            device=opened.st_dev,
            inode=opened.st_ino,
            size=opened.st_size,
            mtime_ns=opened.st_mtime_ns,
            ctime_ns=opened.st_ctime_ns,
            content=content,
            sha256=hashlib.sha256(content).hexdigest(),
            movies=_parse_allowlist_content(content),
        )
        snapshot.require_exact_unchanged()
    except (OSError, UnicodeError, ValueError):
        if file_fd is not None:
            os.close(file_fd)
        if parent_fd is not None:
            os.close(parent_fd)
        raise ValueError("allowlist_invalid") from None
    try:
        yield snapshot
    finally:
        os.close(file_fd)
        os.close(parent_fd)


def _read_allowlist(path: Path) -> tuple[tuple[str, ...], str, str]:
    with _open_allowlist_snapshot(path) as snapshot:
        return snapshot.movies, snapshot.sha256, snapshot.absolute_path


def load_repair_allowlist(path: Path) -> frozenset[str]:
    movies, _, _ = _read_allowlist(path)
    return frozenset(movies)


def load_historical_allowlist_identity(path: Path) -> HistoricalAllowlistIdentity:
    with _open_allowlist_snapshot(path) as snapshot:
        codes_sha256 = hashlib.sha256(
            canonical_plan_bytes({"movie_codes": list(snapshot.movies)})
        ).hexdigest()
        path_sha256 = hashlib.sha256(snapshot.absolute_path.encode("utf-8")).hexdigest()
        snapshot.require_exact_unchanged()
        return HistoricalAllowlistIdentity(
            path_sha256=path_sha256,
            content_sha256=snapshot.sha256,
            canonical_codes_sha256=codes_sha256,
        )


def _scan_filesystem(
    root_lock: JobsRootLock,
    allowlist: _AllowlistSnapshot,
) -> _FilesystemScan:
    allowed = frozenset(allowlist.movies)
    root_lock.require_bound()
    candidate_paths: list[str] = []
    for name in os.listdir(root_lock.root_fd):
        if Path(name).name != name or normalize_movie_number(name) not in allowed:
            continue
        try:
            entry = os.stat(
                name,
                dir_fd=root_lock.root_fd,
                follow_symlinks=False,
            )
        except OSError:
            continue
        if stat.S_ISDIR(entry.st_mode):
            candidate_paths.append(name)
    if len(candidate_paths) > MAX_ALLOWLIST_ENTRIES * 4:
        raise ValueError("historical_plan_changed")
    files_by_path: dict[str, _ScannedJobFiles] = {}
    for path_movie_number in sorted(candidate_paths):
        try:
            with open_job_directory_from_root(
                root_lock,
                path_movie_number,
            ) as job_fd:
                japanese, english, audio = _snapshot_three_job_files(
                    job_fd,
                    path_movie_number,
                )
            assert japanese.content is not None and english.content is not None
            quality = validate_translation_quality_snapshots(
                japanese.content,
                english.content,
            )
            files_by_path[path_movie_number] = _ScannedJobFiles(
                path_movie_number=path_movie_number,
                japanese=replace(japanese, content=None),
                english=replace(english, content=None),
                audio=audio,
                quality_passed=quality.passed,
                quality_reason_codes=tuple(quality.reason_codes),
            )
        except (JobFilesLockError, OSError):
            continue
    snapshot = _FilesystemScan(
        movies=allowlist.movies,
        allowlist_sha256=allowlist.sha256,
        absolute_allowlist_path=allowlist.absolute_path,
        files_by_path=files_by_path,
    )
    try:
        snapshot.require_unchanged(root_lock)
    except (JobFilesLockError, OSError):
        raise ValueError("historical_plan_changed") from None
    return snapshot


def _require_allowlist_unchanged(
    allowlist: _AllowlistSnapshot,
    *,
    exact: bool,
) -> None:
    try:
        if exact:
            allowlist.require_exact_unchanged()
        else:
            allowlist.require_metadata_unchanged()
    except OSError:
        raise ValueError("historical_plan_changed") from None


def _read_only_connection(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(Path(db_path).absolute()), safe='/')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _read_database_snapshot(
    conn: sqlite3.Connection,
) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
    if not conn.in_transaction:
        raise sqlite3.OperationalError("historical database snapshot requires transaction")
    jobs = conn.execute(
        "SELECT * FROM jobs ORDER BY created_at ASC, id ASC"
    ).fetchall()
    repairs = conn.execute(
        "SELECT * FROM historical_translation_repairs ORDER BY created_at ASC, id ASC"
    ).fetchall()
    return jobs, repairs


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
            "probe_snapshot_sha256": audio.sha256,
            "size": audio.size,
            "mtime_ns": audio.mtime_ns,
        },
    }


def _build_plan_from_snapshots(
    filesystem: _FilesystemScan,
    jobs: list[sqlite3.Row],
    repairs: list[sqlite3.Row],
    *,
    limit: int,
    ignore_batch_id: str | None = None,
    audio_content_by_path: dict[str, _FileSnapshot] | None = None,
) -> HistoricalBatchPlan:
    jobs_by_movie: dict[str, list[Any]] = {}
    for row in jobs:
        try:
            canonical = canonical_movie_code(row["normalized_movie_number"])
        except (TypeError, ValueError):
            continue
        jobs_by_movie.setdefault(canonical, []).append(row)
    repair_by_job = {
        row["job_id"]: row["batch_id"]
        for row in repairs
        if ignore_batch_id is None or row["batch_id"] != ignore_batch_id
    }
    repair_jobs = set(repair_by_job)
    eligible: list[HistoricalBatchItem] = []
    scan_entries: list[dict[str, object]] = []
    already_repaired = 0
    ineligible = 0
    blocked = 0
    for movie in filesystem.movies:
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
        scanned = filesystem.files_by_path.get(path_movie_number)
        if scanned is None:
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
        japanese, english, audio = (
            scanned.japanese,
            scanned.english,
            scanned.audio,
        )
        if scanned.quality_passed:
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
                reason_codes=scanned.quality_reason_codes,
            )
        )
        eligible.append(
            HistoricalBatchItem(
                job_id=row["id"],
                movie_code=movie,
                path_movie_number=path_movie_number,
                expected_status=status_value.value,
                expected_updated_at=row["updated_at"],
                reason_codes=scanned.quality_reason_codes,
                japanese_sha256=japanese.sha256,
                japanese_size=japanese.size,
                japanese_mtime_ns=japanese.mtime_ns,
                audio_probe_snapshot_sha256=audio.sha256,
                audio_sha256=(
                    audio_content_by_path[path_movie_number].sha256
                    if audio_content_by_path is not None
                    and path_movie_number in audio_content_by_path
                    else UNAVAILABLE_SELECTED_AUDIO_SHA256
                ),
                audio_size=audio.size,
                audio_mtime_ns=audio.mtime_ns,
                english_sha256=english.sha256,
                english_size=english.size,
                english_mtime_ns=english.mtime_ns,
            )
        )
    eligible.sort(key=lambda item: (item.movie_code, item.path_movie_number, item.job_id))
    selected = tuple(eligible[:limit])
    if audio_content_by_path is not None and any(
        item.path_movie_number not in audio_content_by_path for item in selected
    ):
        raise ValueError("historical_plan_changed")
    return HistoricalBatchPlan.build(
        allowlist_path=filesystem.absolute_allowlist_path,
        allowlist_sha256=filesystem.allowlist_sha256,
        allowlist_entry_count=len(filesystem.movies),
        limit=limit,
        eligible_total=len(eligible),
        already_repaired=already_repaired,
        ineligible=ineligible,
        blocked=blocked,
        scan_entries=len(scan_entries),
        audio_probe_max_bytes=MAX_AUDIO_PROBE_BYTES,
        scan_sha256=hashlib.sha256(
            canonical_plan_bytes({"entries": scan_entries})
        ).hexdigest(),
        items=selected,
    )


UNAVAILABLE_SELECTED_AUDIO_SHA256 = "0" * 64


def _hash_selected_audio(
    jobs_root: Path,
    items: tuple[HistoricalBatchItem, ...],
) -> dict[str, _FileSnapshot]:
    snapshots: dict[str, _FileSnapshot] = {}
    try:
        with shared_jobs_root_lock(jobs_root, blocking=True) as root_lock:
            for item in items:
                with shared_job_files_lock_from_root(
                    root_lock,
                    item.path_movie_number,
                    blocking=True,
                ) as job_lock:
                    snapshot = _open_stable_regular_file_at(
                        job_lock.job_fd,
                        "audio.wav",
                        keep_content=False,
                        max_bytes=None,
                    )
                    if (
                        snapshot.size != item.audio_size
                        or snapshot.mtime_ns != item.audio_mtime_ns
                        or item.path_movie_number in snapshots
                    ):
                        raise OSError("selected audio changed")
                    snapshots[item.path_movie_number] = snapshot
    except (JobFilesLockError, OSError):
        raise ValueError("historical_plan_changed") from None
    return snapshots


def _require_selected_audio_identity(
    filesystem: _FilesystemScan,
    selected_audio: dict[str, _FileSnapshot],
) -> None:
    for path_movie_number, content_snapshot in selected_audio.items():
        scanned = filesystem.files_by_path.get(path_movie_number)
        if scanned is None:
            raise ValueError("historical_plan_changed")
        probe = scanned.audio
        if (
            probe.device,
            probe.inode,
            probe.ctime_ns,
            probe.size,
            probe.mtime_ns,
        ) != (
            content_snapshot.device,
            content_snapshot.inode,
            content_snapshot.ctime_ns,
            content_snapshot.size,
            content_snapshot.mtime_ns,
        ):
            raise ValueError("historical_plan_changed")


def _require_same_provisional_selection(
    provisional: HistoricalBatchPlan,
    final: HistoricalBatchPlan,
) -> None:
    normalized = HistoricalBatchPlan.build(
        allowlist_path=final.allowlist_path,
        allowlist_sha256=final.allowlist_sha256,
        allowlist_entry_count=final.allowlist_entry_count,
        limit=final.limit,
        eligible_total=final.eligible_total,
        already_repaired=final.already_repaired,
        ineligible=final.ineligible,
        blocked=final.blocked,
        scan_entries=final.scan_entries,
        audio_probe_max_bytes=final.audio_probe_max_bytes,
        scan_sha256=final.scan_sha256,
        items=tuple(
            replace(item, audio_sha256=UNAVAILABLE_SELECTED_AUDIO_SHA256)
            for item in final.items
        ),
    )
    if normalized != provisional:
        raise ValueError("historical_plan_changed")


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
    with _open_allowlist_snapshot(Path(allowlist_path)) as allowlist:
        with exclusive_jobs_root_lock(
            store.jobs_root_mac,
            blocking=True,
        ) as root_lock:
            filesystem = _scan_filesystem(root_lock, allowlist)
            _require_allowlist_unchanged(allowlist, exact=False)
            conn = _read_only_connection(store.db_path)
            try:
                conn.execute("BEGIN")
                jobs, repairs = _read_database_snapshot(conn)
                provisional = _build_plan_from_snapshots(
                    filesystem,
                    jobs,
                    repairs,
                    limit=limit,
                )
                conn.commit()
            finally:
                if conn.in_transaction:
                    conn.rollback()
                conn.close()
        selected_audio = _hash_selected_audio(store.jobs_root_mac, provisional.items)
        with exclusive_jobs_root_lock(
            store.jobs_root_mac,
            blocking=True,
        ) as root_lock:
            filesystem = _scan_filesystem(root_lock, allowlist)
            _require_selected_audio_identity(filesystem, selected_audio)
            _require_allowlist_unchanged(allowlist, exact=False)
            conn = _read_only_connection(store.db_path)
            try:
                conn.execute("BEGIN")
                jobs, repairs = _read_database_snapshot(conn)
                plan = _build_plan_from_snapshots(
                    filesystem,
                    jobs,
                    repairs,
                    limit=limit,
                    audio_content_by_path=selected_audio,
                )
                _require_same_provisional_selection(provisional, plan)
                _require_allowlist_unchanged(allowlist, exact=True)
                conn.commit()
                return plan
            finally:
                if conn.in_transaction:
                    conn.rollback()
                conn.close()


def render_historical_batch_report(plan: HistoricalBatchPlan) -> str:
    lines = [
        f"planned=true batch_id={plan.batch_id} plan_sha256={plan.plan_sha256} "
        f"allowlist_sha256={plan.allowlist_sha256} "
        f"scan_sha256={plan.scan_sha256} "
        f"allowlist_entries={plan.allowlist_entry_count} "
        f"scan_entries={plan.scan_entries} "
        f"audio_probe_max_bytes={plan.audio_probe_max_bytes} "
        f"allowlist_verify_max_bytes={MAX_ALLOWLIST_VERIFY_BYTES} "
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


def _require_final_plan_bound(
    parent: Path,
    parent_stat: os.stat_result,
    parent_fd: int,
    basename: str,
    expected_inode: tuple[int, int],
) -> None:
    target = os.stat(basename, dir_fd=parent_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(target.st_mode)
        or (target.st_dev, target.st_ino) != expected_inode
        or stat.S_IMODE(target.st_mode) != 0o600
    ):
        raise OSError("final plan binding changed")
    _require_parent_path_bound(parent, parent_stat)


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
        assert final_inode is not None
        _require_final_plan_bound(
            parent,
            parent_stat,
            parent_fd,
            basename,
            final_inode,
        )
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
        audio_probe_snapshot_sha256=row["audio_probe_snapshot_sha256"],
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
    return _exact_existing_records_from_rows(rows, plan)


def _exact_existing_records_from_rows(
    rows: list[sqlite3.Row],
    plan: HistoricalBatchPlan,
) -> list[HistoricalRepairRecord] | None:
    rows = [row for row in rows if row["batch_id"] == plan.batch_id]
    rows.sort(key=lambda row: (row["movie_code"], row["job_id"]))
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
            or row["audio_probe_snapshot_sha256"]
            != item.audio_probe_snapshot_sha256
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
    conn: sqlite3.Connection,
    plan: HistoricalBatchPlan,
    allowlist_path: Path,
    *,
    confirm_plan_sha256: str,
    filesystem: _FilesystemScan,
    allowlist: _AllowlistSnapshot,
    selected_audio: dict[str, _FileSnapshot],
    controller_identity: tuple[str, str, str] | None = None,
) -> (
    list[HistoricalRepairRecord]
    | tuple[list[HistoricalRepairRecord], bool]
):
    try:
        _validate_plan_confirmation(
            plan,
            allowlist_path,
            confirm_plan_sha256=confirm_plan_sha256,
        )
        jobs, repairs = _read_database_snapshot(conn)
        if controller_identity is None:
            existing_records = _exact_existing_records_from_rows(repairs, plan)
            if existing_records is not None:
                return existing_records
        derived_identity = (
            hashlib.sha256(allowlist.absolute_path.encode("utf-8")).hexdigest(),
            allowlist.sha256,
            hashlib.sha256(
                canonical_plan_bytes({"movie_codes": list(allowlist.movies)})
            ).hexdigest(),
        )
        effective_identity = controller_identity or derived_identity
        if controller_identity is not None and controller_identity != derived_identity:
            raise ValueError("allowlist_changed")
        if any(_LOWER_HEX_RE.fullmatch(value) is None for value in effective_identity):
            raise ValueError("allowlist_changed")
        pin_or_validate_historical_controller_identity_conn(
            conn,
            effective_identity,
            now=utc_now_iso(),
        )
        if controller_identity is not None:
            control = conn.execute(
                "SELECT paused, reason_code FROM historical_repair_control "
                "WHERE singleton = 1"
            ).fetchone()
            if control is None:
                raise HistoricalControllerStateUnavailable(
                    "historical_controller_state_missing"
                )
            if control["paused"]:
                reason = control["reason_code"] or "historical_lane_paused"
                raise ValueError(f"historical_controller_paused:{reason}")
            if normal_translation_backlog_exists(conn):
                raise ValueError("normal_backlog")
            if any(
                row["state"]
                not in {
                    HistoricalRepairState.SUCCEEDED.value,
                    HistoricalRepairState.PERMANENT_FAILED.value,
                }
                for row in repairs
            ):
                raise ValueError("waiting_previous_batch")
            existing_records = _exact_existing_records_from_rows(repairs, plan)
            if existing_records is not None:
                return existing_records, False
        recalculated = _build_plan_from_snapshots(
            filesystem,
            jobs,
            repairs,
            limit=plan.limit,
            ignore_batch_id=plan.batch_id,
            audio_content_by_path=selected_audio,
        )
        if recalculated != plan:
            raise ValueError
        allowlist.require_exact_unchanged()
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
                  audio_probe_snapshot_sha256, audio_sha256,
                  source_english_sha256, english_sha256,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    repair_id,
                    plan.batch_id,
                    item.job_id,
                    item.movie_code,
                    plan.allowlist_sha256,
                    HistoricalRepairState.PENDING.value,
                    item.japanese_sha256,
                    item.audio_probe_snapshot_sha256,
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
        records = [_row_to_repair(row) for row in rows]
        conn.execute(
            """
            UPDATE historical_repair_control
            SET controller_allowlist_path_sha256 = ?,
                controller_allowlist_sha256 = ?,
                controller_allowlist_codes_sha256 = ?,
                controller_last_plan_sha256 = ?, updated_at = ?
            WHERE singleton = 1
            """,
            (*effective_identity, plan.plan_sha256, now),
        )
        if controller_identity is not None:
            return records, True
        return records
    except ValueError as exc:
        if str(exc) in {
            "allowlist_changed",
            "waiting_previous_batch",
            "normal_backlog",
        } or str(exc).startswith("historical_controller_paused:"):
            raise
        raise ValueError("historical_plan_changed") from None
    except HistoricalControllerStateUnavailable:
        if controller_identity is not None:
            raise
        raise ValueError("historical_plan_changed") from None
    except sqlite3.Error:
        if controller_identity is not None:
            raise HistoricalControllerStateUnavailable from None
        raise ValueError("historical_plan_changed") from None
    except (OSError, TypeError):
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


def enqueue_historical_controller_batch(
    store: JobStore,
    plan: HistoricalBatchPlan,
    allowlist_path: Path,
    identity: HistoricalAllowlistIdentity,
) -> tuple[list[HistoricalRepairRecord], bool]:
    result = store.enqueue_historical_repairs(
        plan,
        Path(allowlist_path),
        confirm_plan_sha256=plan.plan_sha256,
        controller_identity=identity.as_store_tuple(),
    )
    if not isinstance(result, tuple):
        raise RuntimeError("historical_controller_enqueue_contract")
    return result


_CONTROLLER_COUNT_KEYS = (
    "eligible_total",
    "already_repaired",
    "ineligible",
    "blocked",
    "pending",
    "running",
    "retry_wait",
    "succeeded",
    "permanent_failed",
    "paused",
    "planned",
    "total_records",
)
_NONTERMINAL_REPAIR_STATES = frozenset(
    {
        HistoricalRepairState.PLANNED.value,
        HistoricalRepairState.PENDING.value,
        HistoricalRepairState.RUNNING.value,
        HistoricalRepairState.RETRY_WAIT.value,
        HistoricalRepairState.PAUSED.value,
    }
)


def _controller_database_snapshot(
    store: JobStore,
) -> tuple[tuple[str | None, str | None, str | None], dict[str, int], set[str]]:
    conn = _read_only_connection(store.db_path)
    try:
        control = conn.execute(
            "SELECT controller_allowlist_path_sha256, "
            "controller_allowlist_sha256, controller_allowlist_codes_sha256 "
            "FROM historical_repair_control WHERE singleton = 1"
        ).fetchone()
        if control is None:
            raise HistoricalControllerStateUnavailable(
                "historical_controller_state_missing"
            )
        counts = {key: 0 for key in _CONTROLLER_COUNT_KEYS}
        allowlist_shas: set[str] = set()
        for row in conn.execute(
            "SELECT state, allowlist_sha256, COUNT(*) AS count "
            "FROM historical_translation_repairs "
            "GROUP BY state, allowlist_sha256"
        ).fetchall():
            state = row["state"]
            if state in counts:
                counts[state] += row["count"]
            counts["total_records"] += row["count"]
            allowlist_shas.add(row["allowlist_sha256"])
        baseline = (
            control["controller_allowlist_path_sha256"],
            control["controller_allowlist_sha256"],
            control["controller_allowlist_codes_sha256"],
        )
        return baseline, counts, allowlist_shas
    finally:
        conn.close()


def _with_plan_counts(
    repair_counts: Mapping[str, int],
    plan: HistoricalBatchPlan | None = None,
) -> dict[str, int]:
    counts = {key: int(repair_counts.get(key, 0)) for key in _CONTROLLER_COUNT_KEYS}
    if plan is not None:
        counts.update(
            {
                "eligible_total": plan.eligible_total,
                "already_repaired": plan.already_repaired,
                "ineligible": plan.ineligible,
                "blocked": plan.blocked,
            }
        )
    return counts


def _controller_result(
    *,
    action: str,
    reason_code: str,
    hard_pause: bool,
    complete: bool = False,
    enqueued: int = 0,
    batch_id: str | None = None,
    plan_sha: str | None = None,
    allowlist_sha: str | None = None,
    counts: Mapping[str, int] | None = None,
) -> HistoricalControllerResult:
    if _SAFE_REASON_RE.fullmatch(reason_code) is None:
        reason_code = "unsafe_controller_failure"
        hard_pause = True
        action = "paused"
    return HistoricalControllerResult(
        action=action,
        reason_code=reason_code,
        hard_pause=hard_pause,
        complete=complete,
        enqueued=enqueued,
        batch_id=batch_id,
        plan_sha=plan_sha,
        allowlist_sha=allowlist_sha,
        counts=_with_plan_counts(counts or {}),
    )


class HistoricalRepairController:
    """Make one safe bounded historical-queue decision per ``run_once`` call."""

    def __init__(
        self,
        store: JobStore,
        allowlist_path: Path,
        *,
        initial_batch_size: int = 5,
        batch_size: int = 20,
        process_health_probe: Callable[[], str | None] | None = None,
        worker_health_probe: Callable[[], str | None] | None = None,
        before_enqueue: Callable[[], None] | None = None,
    ) -> None:
        if (
            not isinstance(initial_batch_size, int)
            or isinstance(initial_batch_size, bool)
            or not 1 <= initial_batch_size <= 5
        ):
            raise ValueError("initial historical batch size must be between 1 and 5")
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or not 1 <= batch_size <= MAX_BATCH_LIMIT
        ):
            raise ValueError("historical batch size must be between 1 and 20")
        self.store = store
        self.allowlist_path = Path(allowlist_path)
        self.initial_batch_size = initial_batch_size
        self.batch_size = batch_size
        self.process_health_probe = process_health_probe or (lambda: None)
        self.worker_health_probe = worker_health_probe or (lambda: None)
        self.before_enqueue = before_enqueue or (lambda: None)

    def run_once(self) -> HistoricalControllerResult:
        try:
            identity = load_historical_allowlist_identity(self.allowlist_path)
        except ValueError:
            return _controller_result(
                action="paused",
                reason_code="allowlist_changed",
                hard_pause=True,
            )
        try:
            self.store.pin_or_validate_historical_controller_identity(
                identity.as_store_tuple()
            )
        except ValueError:
            return _controller_result(
                action="paused",
                reason_code="allowlist_changed",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
            )
        except (HistoricalControllerStateUnavailable, sqlite3.Error):
            return _controller_result(
                action="paused",
                reason_code="historical_controller_state_unavailable",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
            )
        try:
            process_reason = self.process_health_probe()
        except Exception:
            process_reason = "translation_worker_count_mismatch"
        if process_reason is not None:
            return _controller_result(
                action="paused",
                reason_code=process_reason,
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
            )
        try:
            lane = self.store.historical_lane_state()
        except (HistoricalControllerStateUnavailable, sqlite3.Error):
            return _controller_result(
                action="paused",
                reason_code="historical_controller_state_unavailable",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
            )
        if lane.paused:
            return _controller_result(
                action="paused",
                reason_code=lane.reason_code or "historical_lane_paused",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
            )
        try:
            baseline, repair_counts, repair_allowlist_shas = (
                _controller_database_snapshot(self.store)
            )
        except (HistoricalControllerStateUnavailable, sqlite3.Error):
            return _controller_result(
                action="paused",
                reason_code="historical_controller_state_unavailable",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
            )
        expected = identity.as_store_tuple()
        if (
            (any(value is not None for value in baseline) and baseline != expected)
            or (
                repair_allowlist_shas
                and repair_allowlist_shas != {identity.content_sha256}
            )
        ):
            return _controller_result(
                action="paused",
                reason_code="allowlist_changed",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        try:
            normal_backlog = self.store.has_normal_translation_backlog()
        except sqlite3.Error:
            return _controller_result(
                action="paused",
                reason_code="historical_controller_state_unavailable",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        if normal_backlog:
            return _controller_result(
                action="waiting",
                reason_code="normal_backlog",
                hard_pause=False,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        nonterminal = sum(
            repair_counts[state] for state in _NONTERMINAL_REPAIR_STATES
        )
        if repair_counts[HistoricalRepairState.PAUSED.value]:
            return _controller_result(
                action="paused",
                reason_code="historical_record_paused",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        if nonterminal:
            return _controller_result(
                action="waiting",
                reason_code="waiting_previous_batch",
                hard_pause=False,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        try:
            health_reason = self.worker_health_probe()
        except sqlite3.Error:
            return _controller_result(
                action="paused",
                reason_code="historical_controller_state_unavailable",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        if health_reason is not None:
            return _controller_result(
                action="paused",
                reason_code=health_reason,
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        limit = (
            self.initial_batch_size
            if repair_counts["total_records"] == 0
            else self.batch_size
        )
        try:
            plan = plan_historical_batch(self.store, self.allowlist_path, limit=limit)
        except ValueError:
            return _controller_result(
                action="paused",
                reason_code="plan_digest_changed",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        except (HistoricalControllerStateUnavailable, sqlite3.Error):
            return _controller_result(
                action="paused",
                reason_code="historical_controller_state_unavailable",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        except (JobFilesLockError, OSError):
            return _controller_result(
                action="paused",
                reason_code="historical_scan_failed",
                hard_pause=True,
                allowlist_sha=identity.content_sha256,
                counts=repair_counts,
            )
        counts = _with_plan_counts(repair_counts, plan)
        if plan.eligible_total == 0:
            if plan.blocked:
                return _controller_result(
                    action="paused",
                    reason_code="allowlist_blocked_entries",
                    hard_pause=True,
                    plan_sha=plan.plan_sha256,
                    allowlist_sha=identity.content_sha256,
                    counts=counts,
                )
            terminal = counts["succeeded"] + counts["permanent_failed"]
            if (
                terminal != plan.already_repaired
                or terminal + plan.ineligible != plan.allowlist_entry_count
            ):
                return _controller_result(
                    action="paused",
                    reason_code="allowlist_terminal_count_mismatch",
                    hard_pause=True,
                    plan_sha=plan.plan_sha256,
                    allowlist_sha=identity.content_sha256,
                    counts=counts,
                )
            return _controller_result(
                action="complete",
                reason_code="allowlist_complete",
                hard_pause=False,
                complete=True,
                plan_sha=plan.plan_sha256,
                allowlist_sha=identity.content_sha256,
                counts=counts,
            )
        if not plan.items:
            return _controller_result(
                action="paused",
                reason_code="historical_plan_empty",
                hard_pause=True,
                plan_sha=plan.plan_sha256,
                allowlist_sha=identity.content_sha256,
                counts=counts,
            )
        self.before_enqueue()
        try:
            records, created = enqueue_historical_controller_batch(
                self.store,
                plan,
                self.allowlist_path,
                identity,
            )
        except HistoricalControllerStateUnavailable:
            return _controller_result(
                action="paused",
                reason_code="historical_controller_state_unavailable",
                hard_pause=True,
                plan_sha=plan.plan_sha256,
                allowlist_sha=identity.content_sha256,
                counts=counts,
            )
        except ValueError as exc:
            reason = str(exc)
            if reason == "normal_backlog":
                return _controller_result(
                    action="waiting",
                    reason_code=reason,
                    hard_pause=False,
                    plan_sha=plan.plan_sha256,
                    allowlist_sha=identity.content_sha256,
                    counts=counts,
                )
            if reason == "waiting_previous_batch":
                return _controller_result(
                    action="waiting",
                    reason_code=reason,
                    hard_pause=False,
                    plan_sha=plan.plan_sha256,
                    allowlist_sha=identity.content_sha256,
                    counts=counts,
                )
            if reason.startswith("historical_controller_paused:"):
                reason = reason.split(":", 1)[1]
            elif reason == "historical_plan_changed":
                reason = "plan_digest_changed"
            return _controller_result(
                action="paused",
                reason_code=reason,
                hard_pause=True,
                plan_sha=plan.plan_sha256,
                allowlist_sha=identity.content_sha256,
                counts=counts,
            )
        except sqlite3.Error:
            return _controller_result(
                action="paused",
                reason_code="historical_controller_state_unavailable",
                hard_pause=True,
                plan_sha=plan.plan_sha256,
                allowlist_sha=identity.content_sha256,
                counts=counts,
            )
        except (JobFilesLockError, OSError):
            return _controller_result(
                action="paused",
                reason_code="historical_enqueue_failed",
                hard_pause=True,
                plan_sha=plan.plan_sha256,
                allowlist_sha=identity.content_sha256,
                counts=counts,
            )
        if not created:
            return _controller_result(
                action="waiting",
                reason_code="waiting_previous_batch",
                hard_pause=False,
                plan_sha=plan.plan_sha256,
                allowlist_sha=identity.content_sha256,
                counts=counts,
            )
        counts["pending"] += len(records)
        counts["total_records"] += len(records)
        return _controller_result(
            action="enqueued",
            reason_code="batch_enqueued",
            hard_pause=False,
            enqueued=len(records),
            batch_id=plan.batch_id,
            plan_sha=plan.plan_sha256,
            allowlist_sha=identity.content_sha256,
            counts=counts,
        )


def render_historical_controller_report(result: HistoricalControllerResult) -> str:
    payload: dict[str, object] = {
        "action": result.action,
        "reason_code": result.reason_code,
        "hard_pause": result.hard_pause,
        "complete": result.complete,
        "enqueued": result.enqueued,
        "batch_id": result.batch_id,
        "plan_sha": result.plan_sha,
        "allowlist_sha": result.allowlist_sha,
        "counts": {key: result.counts[key] for key in _CONTROLLER_COUNT_KEYS},
    }
    return canonical_plan_bytes(payload).decode("ascii")
