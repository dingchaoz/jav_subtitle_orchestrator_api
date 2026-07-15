from __future__ import annotations

import errno
import fcntl
import hashlib
import hmac
import json
import os
import re
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
from uuid import UUID, uuid4

from orchestrator.catalog_visibility import (
    MAX_PERSISTABLE_OBSERVED_SUBTITLE_IDS,
    AuditCandidateSnapshot,
    VisibilityStatus,
    normalize_catalog_api_origin,
)
from orchestrator.movie_code import canonical_movie_code


AUDIT_REPORT_SCHEMA_VERSION = 1
REPAIR_ELIGIBLE = frozenset({"missing", "not_found"})
MAX_AUDIT_CANDIDATES = 10_000
MAX_AUDIT_ALLOWLIST = 10_000
MAX_AUDIT_FINDINGS = 10_000
MAX_OBSERVED_SUBTITLE_IDS = MAX_PERSISTABLE_OBSERVED_SUBTITLE_IDS
MAX_AUDIT_DOCUMENT_BYTES = 64 * 1024 * 1024
MAX_AUDIT_REPORT_OVERHEAD_BYTES = 8 * 1024 * 1024
MAX_CHECKPOINT_LINE_BYTES = 64 * 1024
MAX_JSON_DEPTH = 100
_SECURE_DIR_FD_SUPPORTED = all(
    operation in os.supports_dir_fd
    for operation in (os.open, os.mkdir, os.unlink, os.link, os.stat)
) and os.stat in os.supports_follow_symlinks and os.listdir in os.supports_fd
_CHECKPOINT_PROCESS_LOCK = threading.RLock()

_MANIFEST_FILE_NAME = "audit-manifest.json"
_CHECKPOINT_FILE_NAME = "audit-findings.jsonl"
_REPORT_FILE_NAME = "audit-report.json"
_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "audit_id",
        "created_at",
        "api_origin",
        "database_path_sha256",
        "selection",
        "candidates",
    }
)
_CANDIDATE_FIELDS = frozenset(
    {
        "job_id",
        "movie_code",
        "movie_uuid",
        "metadata_status",
        "metadata_source",
        "subtitle_id",
        "storage_path",
        "content_sha256",
        "file_size",
        "job_updated_at",
    }
)
_FINDING_FIELDS = frozenset(
    {
        "schema_version",
        "candidate",
        "status",
        "reason_code",
        "observed_subtitle_ids",
    }
)
_REPORT_FIELDS = frozenset(
    {"manifest", "findings", "counts", "complete", "report_sha256"}
)
_SELECTION_FIELDS = frozenset({"allowlist", "limit"})
_RECEIPT_FIELD_NAMES = (
    "movie_uuid",
    "metadata_status",
    "metadata_source",
    "subtitle_id",
    "storage_path",
    "content_sha256",
    "file_size",
)
_STATUS_VALUES = frozenset(status.value for status in VisibilityStatus)
_LOWERCASE_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REASON_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SAFE_JOB_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_SAFE_MOVIE_CODE_INPUT_RE = re.compile(r"^[A-Za-z]+-?[0-9]+$")
_STRICT_UTC_TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|\+00:00)$"
)


class _FrozenList(tuple[object, ...]):
    def __eq__(self, other: object) -> bool:
        if isinstance(other, (list, tuple)):
            return tuple(self) == tuple(other)
        return False

    __hash__ = tuple.__hash__


def _freeze_selection(value: object) -> object:
    if not isinstance(value, Mapping):
        return value
    snapshot = dict(value)
    allowlist = snapshot.get("allowlist")
    if isinstance(allowlist, (list, tuple)):
        snapshot["allowlist"] = _FrozenList(allowlist)
    return MappingProxyType(snapshot)


def _freeze_counts(value: object) -> object:
    if not isinstance(value, Mapping):
        return value
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class AuditFinding:
    candidate: AuditCandidateSnapshot
    status: str
    reason_code: str | None
    observed_subtitle_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AuditManifest:
    schema_version: int
    audit_id: str
    created_at: str
    api_origin: str
    database_path_sha256: str
    selection: dict[str, object]
    candidates: tuple[AuditCandidateSnapshot, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "selection", _freeze_selection(self.selection))


@dataclass(frozen=True, slots=True)
class AuditReport:
    manifest: AuditManifest
    findings: tuple[AuditFinding, ...]
    counts: dict[str, int]
    complete: bool
    report_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "counts", _freeze_counts(self.counts))


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _exact_dict(value: object, fields: frozenset[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f"{label} has invalid fields")
    return value


def _validate_safe_job_id(value: object) -> None:
    if not isinstance(value, str) or not _SAFE_JOB_ID_RE.fullmatch(value):
        raise ValueError("candidate job_id is unsafe")


def _canonical_safe_movie_code(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 64
        or not _SAFE_MOVIE_CODE_INPUT_RE.fullmatch(value)
    ):
        raise ValueError(f"{label} is invalid")
    try:
        canonical = canonical_movie_code(value)
    except (AttributeError, TypeError, ValueError):
        raise ValueError(f"{label} is invalid") from None
    if len(canonical) > 64:
        raise ValueError(f"{label} is invalid")
    return canonical


def _validate_strict_utc_timestamp(value: object, label: str) -> str:
    if not isinstance(value, str) or not _STRICT_UTC_TIMESTAMP_RE.fullmatch(value):
        raise ValueError(f"{label} is invalid")
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        raise ValueError(f"{label} is invalid") from None
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError(f"{label} is invalid")
    return value


def _validate_canonical_uuid(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    try:
        if str(UUID(value)) != value:
            raise ValueError
    except (AttributeError, TypeError, ValueError):
        raise ValueError(f"{label} is invalid") from None
    return value


def _validate_candidate_identity(candidate: object) -> AuditCandidateSnapshot:
    if not isinstance(candidate, AuditCandidateSnapshot):
        raise TypeError("candidate must be an AuditCandidateSnapshot")
    _validate_safe_job_id(candidate.job_id)
    if not isinstance(candidate.movie_code, str):
        raise TypeError("candidate movie_code must be a string")
    canonical = _canonical_safe_movie_code(
        candidate.movie_code,
        "candidate movie_code",
    )
    if canonical != candidate.movie_code:
        raise ValueError("candidate movie_code must be canonical")
    _validate_strict_utc_timestamp(
        candidate.job_updated_at,
        "candidate job_updated_at",
    )
    return candidate


def _receipt_is_fully_redacted(candidate: AuditCandidateSnapshot) -> bool:
    return all(getattr(candidate, field_name) is None for field_name in _RECEIPT_FIELD_NAMES)


def _redacted_candidate(candidate: AuditCandidateSnapshot) -> AuditCandidateSnapshot:
    return replace(
        candidate,
        movie_uuid=None,
        metadata_status=None,
        metadata_source=None,
        subtitle_id=None,
        storage_path=None,
        content_sha256=None,
        file_size=None,
    )


def _canonicalize_candidate_for_create(candidate: object) -> AuditCandidateSnapshot:
    if not isinstance(candidate, AuditCandidateSnapshot):
        raise TypeError("candidate must be an AuditCandidateSnapshot")
    try:
        candidate.validated_receipt()
    except ValueError:
        receipt_is_valid = False
    else:
        receipt_is_valid = True
    _validate_candidate_identity(candidate)
    return candidate if receipt_is_valid else _redacted_candidate(candidate)


def _validate_candidate(candidate: object) -> AuditCandidateSnapshot:
    candidate = _validate_candidate_identity(candidate)
    try:
        candidate.validated_receipt()
    except ValueError:
        if not _receipt_is_fully_redacted(candidate):
            raise ValueError("candidate receipt is not canonical") from None
    return candidate


def _candidate_to_obj(candidate: AuditCandidateSnapshot) -> dict[str, object]:
    return {
        "job_id": candidate.job_id,
        "movie_code": candidate.movie_code,
        "movie_uuid": candidate.movie_uuid,
        "metadata_status": candidate.metadata_status,
        "metadata_source": candidate.metadata_source,
        "subtitle_id": candidate.subtitle_id,
        "storage_path": candidate.storage_path,
        "content_sha256": candidate.content_sha256,
        "file_size": candidate.file_size,
        "job_updated_at": candidate.job_updated_at,
    }


def _candidate_from_obj(value: object) -> AuditCandidateSnapshot:
    payload = _exact_dict(value, _CANDIDATE_FIELDS, "candidate")
    candidate = AuditCandidateSnapshot(
        job_id=payload["job_id"],  # type: ignore[arg-type]
        movie_code=payload["movie_code"],  # type: ignore[arg-type]
        movie_uuid=payload["movie_uuid"],  # type: ignore[arg-type]
        metadata_status=payload["metadata_status"],  # type: ignore[arg-type]
        metadata_source=payload["metadata_source"],  # type: ignore[arg-type]
        subtitle_id=payload["subtitle_id"],  # type: ignore[arg-type]
        storage_path=payload["storage_path"],  # type: ignore[arg-type]
        content_sha256=payload["content_sha256"],  # type: ignore[arg-type]
        file_size=payload["file_size"],  # type: ignore[arg-type]
        job_updated_at=payload["job_updated_at"],  # type: ignore[arg-type]
    )
    return _validate_candidate(candidate)


def _normalize_selection(selection: object) -> dict[str, object]:
    if not isinstance(selection, Mapping) or set(selection) != _SELECTION_FIELDS:
        raise ValueError("selection has invalid fields")
    payload = selection
    raw_allowlist = payload["allowlist"]
    if raw_allowlist is None:
        allowlist: list[str] | None = None
    else:
        if not isinstance(raw_allowlist, (list, tuple)):
            raise TypeError("selection allowlist must be a JSON array or null")
        if len(raw_allowlist) > MAX_AUDIT_ALLOWLIST:
            raise ValueError("selection allowlist is too large")
        normalized_codes: list[str] = []
        for code in raw_allowlist:
            if not isinstance(code, str):
                raise TypeError("selection allowlist entries must be strings")
            normalized_codes.append(
                _canonical_safe_movie_code(
                    code,
                    "selection allowlist movie code",
                )
            )
        allowlist = sorted(set(normalized_codes))

    limit = payload["limit"]
    if limit is not None and (
        not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0
    ):
        raise ValueError("selection limit must be a positive integer or null")
    return {"allowlist": allowlist, "limit": limit}


def _validate_manifest(manifest: object) -> AuditManifest:
    if not isinstance(manifest, AuditManifest):
        raise TypeError("manifest must be an AuditManifest")
    if manifest.schema_version != AUDIT_REPORT_SCHEMA_VERSION or isinstance(
        manifest.schema_version, bool
    ):
        raise ValueError("manifest schema version is unsupported")
    if not isinstance(manifest.audit_id, str):
        raise TypeError("manifest audit_id must be a string")
    try:
        if str(UUID(manifest.audit_id)) != manifest.audit_id:
            raise ValueError
    except (AttributeError, TypeError, ValueError):
        raise ValueError("manifest audit_id is not a canonical UUID") from None
    _validate_strict_utc_timestamp(manifest.created_at, "manifest created_at")
    if not isinstance(manifest.api_origin, str):
        raise TypeError("manifest api_origin must be a string")
    try:
        normalized_origin = normalize_catalog_api_origin(manifest.api_origin)
    except ValueError:
        raise ValueError("manifest api_origin is invalid") from None
    if normalized_origin != manifest.api_origin:
        raise ValueError("manifest api_origin is not normalized")
    if not isinstance(manifest.database_path_sha256, str) or not _LOWERCASE_SHA256_RE.fullmatch(
        manifest.database_path_sha256
    ):
        raise ValueError("manifest database path digest is invalid")
    normalized_selection = _normalize_selection(manifest.selection)
    if normalized_selection != manifest.selection:
        raise ValueError("manifest selection is not canonical")
    if not isinstance(manifest.candidates, tuple):
        raise TypeError("manifest candidates must be a tuple")
    if len(manifest.candidates) > MAX_AUDIT_CANDIDATES:
        raise ValueError("manifest contains too many candidates")
    validated_candidates = tuple(_validate_candidate(item) for item in manifest.candidates)
    deterministic_candidates = tuple(
        sorted(validated_candidates, key=lambda item: (item.job_updated_at, item.job_id))
    )
    if validated_candidates != deterministic_candidates:
        raise ValueError("manifest candidates are not deterministically ordered")
    job_ids = [item.job_id for item in validated_candidates]
    movie_codes = [item.movie_code for item in validated_candidates]
    if len(job_ids) != len(set(job_ids)):
        raise ValueError("manifest contains duplicate job IDs")
    if len(movie_codes) != len(set(movie_codes)):
        raise ValueError("manifest contains duplicate movie codes")
    return manifest


def _manifest_to_obj(manifest: AuditManifest) -> dict[str, object]:
    allowlist = manifest.selection["allowlist"]
    return {
        "schema_version": manifest.schema_version,
        "audit_id": manifest.audit_id,
        "created_at": manifest.created_at,
        "api_origin": manifest.api_origin,
        "database_path_sha256": manifest.database_path_sha256,
        "selection": {
            "allowlist": None if allowlist is None else list(allowlist),
            "limit": manifest.selection["limit"],
        },
        "candidates": [_candidate_to_obj(item) for item in manifest.candidates],
    }


def _manifest_from_obj(value: object) -> AuditManifest:
    payload = _exact_dict(value, _MANIFEST_FIELDS, "manifest")
    candidates_payload = payload["candidates"]
    if not isinstance(candidates_payload, list):
        raise TypeError("manifest candidates must be a JSON array")
    if len(candidates_payload) > MAX_AUDIT_CANDIDATES:
        raise ValueError("manifest contains too many candidates")
    selection = _normalize_selection(payload["selection"])
    if selection != payload["selection"]:
        raise ValueError("manifest selection is not canonical")
    manifest = AuditManifest(
        schema_version=payload["schema_version"],  # type: ignore[arg-type]
        audit_id=payload["audit_id"],  # type: ignore[arg-type]
        created_at=payload["created_at"],  # type: ignore[arg-type]
        api_origin=payload["api_origin"],  # type: ignore[arg-type]
        database_path_sha256=payload["database_path_sha256"],  # type: ignore[arg-type]
        selection=selection,
        candidates=tuple(_candidate_from_obj(item) for item in candidates_payload),
    )
    return _validate_manifest(manifest)


def create_audit_manifest(
    *,
    api_origin: str,
    database_path: str | os.PathLike[str],
    candidates: tuple[AuditCandidateSnapshot, ...],
    selection: dict[str, object],
) -> AuditManifest:
    if not isinstance(candidates, tuple):
        raise TypeError("candidates must be a tuple")
    if len(candidates) > MAX_AUDIT_CANDIDATES:
        raise ValueError("too many audit candidates")
    validated_candidates = tuple(
        _canonicalize_candidate_for_create(item) for item in candidates
    )
    ordered_candidates = tuple(
        sorted(validated_candidates, key=lambda item: (item.job_updated_at, item.job_id))
    )
    try:
        resolved_database_path = str(Path(database_path).resolve())
    except (OSError, TypeError, ValueError):
        raise ValueError("database_path is invalid") from None
    manifest = AuditManifest(
        schema_version=AUDIT_REPORT_SCHEMA_VERSION,
        audit_id=str(uuid4()),
        created_at=datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        api_origin=normalize_catalog_api_origin(api_origin),
        database_path_sha256=hashlib.sha256(resolved_database_path.encode("utf-8")).hexdigest(),
        selection=_normalize_selection(selection),
        candidates=ordered_candidates,
    )
    return _validate_manifest(manifest)


def validate_audit_resume_context(
    manifest: AuditManifest,
    *,
    api_origin: str,
    database_path: str | os.PathLike[str],
    selection: dict[str, object],
) -> None:
    manifest = _validate_manifest(manifest)
    current_origin = normalize_catalog_api_origin(api_origin)
    try:
        resolved_database_path = str(Path(database_path).resolve())
    except (OSError, TypeError, ValueError):
        raise ValueError("database_path is invalid") from None
    current_database_digest = hashlib.sha256(
        resolved_database_path.encode("utf-8")
    ).hexdigest()
    current_selection = _normalize_selection(selection)
    if current_origin != manifest.api_origin:
        raise ValueError("audit API origin differs from manifest")
    if not hmac.compare_digest(
        current_database_digest,
        manifest.database_path_sha256,
    ):
        raise ValueError("audit database path differs from manifest")
    if current_selection != manifest.selection:
        raise ValueError("audit selection differs from manifest")


def _directory_open_flags() -> int:
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise RuntimeError("secure directory descriptors are unavailable")
    if not _SECURE_DIR_FD_SUPPORTED:
        raise RuntimeError("secure directory-relative operations are unavailable")
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _fsync_directory_fd(descriptor: int) -> None:
    try:
        os.fsync(descriptor)
    except OSError as exc:
        unsupported = {errno.EINVAL, getattr(errno, "ENOTSUP", errno.EINVAL)}
        if exc.errno not in unsupported:
            raise


def _close_ignoring_error(descriptor: int | None) -> None:
    if descriptor is None:
        return
    try:
        os.close(descriptor)
    except OSError:
        pass


def _path_components(path: str | os.PathLike[str]) -> tuple[str, list[str]]:
    raw = Path(path)
    start = "/" if raw.is_absolute() else "."
    components: list[str] = []
    for part in raw.parts:
        if part in {"", ".", "/"}:
            continue
        if part == "..":
            raise ValueError("audit paths must not contain parent traversal")
        components.append(part)
    return start, components


def _open_directory_path(path: str | os.PathLike[str], *, create: bool) -> int:
    start, components = _path_components(path)
    flags = _directory_open_flags()
    descriptor = os.open(start, flags)
    try:
        for component in components:
            child: int | None = None
            created_component = False
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(component, 0o700, dir_fd=descriptor)
                except FileExistsError:
                    created_component = False
                else:
                    created_component = True
                    _fsync_directory_fd(descriptor)
                child = os.open(component, flags, dir_fd=descriptor)
            except OSError as exc:
                raise ValueError("audit directory path is unsafe") from exc
            try:
                assert child is not None
                if created_component:
                    os.fchmod(child, 0o700)
                    os.fsync(child)
                os.close(descriptor)
            except BaseException:
                _close_ignoring_error(child)
                raise
            descriptor = child
        return descriptor
    except BaseException:
        _close_ignoring_error(descriptor)
        raise


@contextmanager
def _pinned_directory(path: str | os.PathLike[str], *, create: bool):
    descriptor = _open_directory_path(path, create=create)
    try:
        yield descriptor
    except BaseException:
        _close_ignoring_error(descriptor)
        raise
    else:
        os.close(descriptor)


def _leaf_parent(path: str | os.PathLike[str]) -> tuple[Path, str]:
    value = Path(path)
    name = value.name
    if not name or name in {".", ".."} or "/" in name:
        raise ValueError("audit file path is invalid")
    return value.parent, name


def _validate_regular_fd(descriptor: int, label: str) -> os.stat_result:
    file_stat = os.fstat(descriptor)
    if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_nlink != 1:
        raise ValueError(f"{label} is not a private regular file")
    return file_stat


def _validate_published_leaf_identity(
    directory_fd: int,
    name: str,
    descriptor: int,
    label: str,
    *,
    expected_mode: int | None,
) -> os.stat_result:
    descriptor_stat = os.fstat(descriptor)
    try:
        published_stat = os.stat(
            name,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise ValueError(f"{label} is not a private published leaf") from exc
    descriptor_mode = stat.S_IMODE(descriptor_stat.st_mode)
    published_mode = stat.S_IMODE(published_stat.st_mode)
    if (
        not stat.S_ISREG(descriptor_stat.st_mode)
        or not stat.S_ISREG(published_stat.st_mode)
        or descriptor_stat.st_dev != published_stat.st_dev
        or descriptor_stat.st_ino != published_stat.st_ino
        or descriptor_stat.st_nlink != 1
        or published_stat.st_nlink != 1
        or descriptor_stat.st_uid != os.geteuid()
        or published_stat.st_uid != os.geteuid()
        or descriptor_mode != published_mode
        or (expected_mode is not None and descriptor_mode != expected_mode)
    ):
        raise ValueError(f"{label} is not a private published leaf")
    return descriptor_stat


def _open_leaf_without_link_validation(
    directory_fd: int,
    name: str,
    flags: int,
    label: str,
) -> int:
    try:
        descriptor = os.open(name, flags | os.O_NOFOLLOW, dir_fd=directory_fd)
    except OSError as exc:
        if isinstance(exc, FileNotFoundError):
            raise
        raise ValueError(f"{label} cannot be opened safely") from exc
    file_stat = os.fstat(descriptor)
    if not stat.S_ISREG(file_stat.st_mode):
        _close_ignoring_error(descriptor)
        raise ValueError(f"{label} is not a regular file")
    return descriptor


def _open_existing_leaf(directory_fd: int, name: str, flags: int, label: str) -> int:
    descriptor = _open_leaf_without_link_validation(
        directory_fd,
        name,
        flags,
        label,
    )
    try:
        _validate_regular_fd(descriptor, label)
        return descriptor
    except BaseException:
        _close_ignoring_error(descriptor)
        raise


def _recover_owned_temp_hardlink(
    directory_fd: int,
    final_name: str,
    final_fd: int,
    label: str,
) -> None:
    final_stat = os.fstat(final_fd)
    expected_mode = 0o600
    if (
        not stat.S_ISREG(final_stat.st_mode)
        or final_stat.st_nlink != 2
        or final_stat.st_uid != os.geteuid()
        or stat.S_IMODE(final_stat.st_mode) != expected_mode
    ):
        raise ValueError(f"{label} has an unsafe hardlink")
    pattern = re.compile(
        rf"^\.{re.escape(final_name)}\.[0-9a-f]{{32}}\.tmp$"
    )
    temp_names = [name for name in os.listdir(directory_fd) if pattern.fullmatch(name)]
    if len(temp_names) != 1:
        raise ValueError(f"{label} has an unsafe hardlink")
    temp_name = temp_names[0]
    temp_stat = os.stat(temp_name, dir_fd=directory_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(temp_stat.st_mode)
        or temp_stat.st_dev != final_stat.st_dev
        or temp_stat.st_ino != final_stat.st_ino
        or temp_stat.st_nlink != 2
        or temp_stat.st_uid != os.geteuid()
        or stat.S_IMODE(temp_stat.st_mode) != expected_mode
    ):
        raise ValueError(f"{label} has an unsafe hardlink")
    os.unlink(temp_name, dir_fd=directory_fd)
    _fsync_directory_fd(directory_fd)
    recovered = os.fstat(final_fd)
    if (
        recovered.st_dev != final_stat.st_dev
        or recovered.st_ino != final_stat.st_ino
        or recovered.st_nlink != 1
    ):
        raise ValueError(f"{label} hardlink recovery failed")


def _open_artifact_leaf(
    directory_fd: int,
    name: str,
    flags: int,
    label: str,
) -> int:
    descriptor = _open_leaf_without_link_validation(
        directory_fd,
        name,
        flags,
        label,
    )
    try:
        file_stat = os.fstat(descriptor)
        if file_stat.st_nlink == 2:
            _recover_owned_temp_hardlink(
                directory_fd,
                name,
                descriptor,
                label,
            )
        _validate_regular_fd(descriptor, label)
        return descriptor
    except BaseException:
        _close_ignoring_error(descriptor)
        raise


def _read_fd_limited(descriptor: int, label: str, limit: int) -> bytes:
    file_stat = _validate_regular_fd(descriptor, label)
    if file_stat.st_size > limit:
        raise ValueError(f"{label} is too large")
    os.lseek(descriptor, 0, os.SEEK_SET)
    remaining = limit + 1
    chunks: list[bytes] = []
    while remaining:
        chunk = os.read(descriptor, min(1024 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    data = b"".join(chunks)
    if len(data) > limit:
        raise ValueError(f"{label} is too large")
    return data


def _safe_read_bytes(
    path: Path,
    label: str = "audit file",
    *,
    recover_owned_temp: bool = False,
    limit: int | None = None,
) -> bytes:
    if limit is None:
        limit = MAX_AUDIT_DOCUMENT_BYTES
    parent, name = _leaf_parent(path)
    with _pinned_directory(parent, create=False) as directory_fd:
        if recover_owned_temp:
            fcntl.flock(directory_fd, fcntl.LOCK_EX)
        opener = _open_artifact_leaf if recover_owned_temp else _open_existing_leaf
        descriptor = opener(directory_fd, name, os.O_RDONLY, label)
        try:
            _validate_published_leaf_identity(
                directory_fd,
                name,
                descriptor,
                label,
                expected_mode=0o600,
            )
            data = _read_fd_limited(descriptor, label, limit)
            _validate_published_leaf_identity(
                directory_fd,
                name,
                descriptor,
                label,
                expected_mode=0o600,
            )
        except BaseException:
            _close_ignoring_error(descriptor)
            raise
        else:
            os.close(descriptor)
            return data


def _decode_json(data: bytes, label: str) -> object:
    def reject_duplicate_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
        decoded: dict[str, object] = {}
        for key, value in pairs:
            if key in decoded:
                raise ValueError("duplicate JSON field")
            decoded[key] = value
        return decoded

    def reject_nonstandard_constant(value: str) -> object:
        raise ValueError(f"nonstandard JSON constant: {value}")

    try:
        text = data.decode("utf-8")
        decoded = json.loads(
            text,
            object_pairs_hook=reject_duplicate_fields,
            parse_constant=reject_nonstandard_constant,
        )
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise ValueError(f"{label} is not valid JSON") from None
    stack: list[tuple[object, int]] = [(decoded, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > MAX_JSON_DEPTH:
            raise ValueError(f"{label} is too deeply nested")
        if isinstance(current, dict):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)
    return decoded


def _write_all_fd(descriptor: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        written = os.write(descriptor, data[offset:])
        if written <= 0:
            raise OSError("audit file write did not progress")
        offset += written


def _unlink_at_ignoring_error(directory_fd: int, name: str) -> None:
    try:
        os.unlink(name, dir_fd=directory_fd)
    except OSError:
        pass


def _remove_just_installed_final(
    directory_fd: int,
    temp_name: str,
    final_name: str,
) -> None:
    try:
        temp_stat = os.stat(temp_name, dir_fd=directory_fd, follow_symlinks=False)
        final_stat = os.stat(final_name, dir_fd=directory_fd, follow_symlinks=False)
        if (
            stat.S_ISREG(temp_stat.st_mode)
            and stat.S_ISREG(final_stat.st_mode)
            and temp_stat.st_dev == final_stat.st_dev
            and temp_stat.st_ino == final_stat.st_ino
            and temp_stat.st_nlink == final_stat.st_nlink == 2
            and temp_stat.st_uid == final_stat.st_uid == os.geteuid()
            and stat.S_IMODE(temp_stat.st_mode) == 0o600
            and stat.S_IMODE(final_stat.st_mode) == 0o600
        ):
            os.unlink(final_name, dir_fd=directory_fd)
    except OSError:
        pass


def _atomic_install_no_clobber(
    directory_fd: int,
    final_name: str,
    data: bytes,
    label: str,
    limit: int,
) -> bool:
    if len(data) > limit:
        raise ValueError(f"{label} is too large")
    temp_name = f".{final_name}.{uuid4().hex}.tmp"
    descriptor: int | None = None
    installed = False
    try:
        descriptor = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory_fd,
        )
        _validate_regular_fd(descriptor, label)
        os.fchmod(descriptor, 0o600)
        _write_all_fd(descriptor, data)
        os.fsync(descriptor)
        try:
            os.link(
                temp_name,
                final_name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            installed = False
        else:
            installed = True
        os.unlink(temp_name, dir_fd=directory_fd)
        _fsync_directory_fd(directory_fd)
        if installed:
            _validate_published_leaf_identity(
                directory_fd,
                final_name,
                descriptor,
                label,
                expected_mode=0o600,
            )
        os.close(descriptor)
        descriptor = None
        return installed
    except BaseException:
        _close_ignoring_error(descriptor)
        if installed:
            _remove_just_installed_final(
                directory_fd,
                temp_name,
                final_name,
            )
        _unlink_at_ignoring_error(directory_fd, temp_name)
        try:
            _fsync_directory_fd(directory_fd)
        except OSError:
            pass
        raise


def _load_manifest_fd(descriptor: int) -> AuditManifest:
    data = _read_fd_limited(
        descriptor,
        "audit manifest",
        MAX_AUDIT_DOCUMENT_BYTES,
    )
    return _manifest_from_obj(_decode_json(data, "audit manifest"))


def write_audit_manifest(
    output_dir: str | os.PathLike[str], manifest: AuditManifest
) -> None:
    manifest = _validate_manifest(manifest)
    data = _canonical_bytes(_manifest_to_obj(manifest))
    with _pinned_directory(output_dir, create=True) as directory_fd:
        fcntl.flock(directory_fd, fcntl.LOCK_EX)
        installed = _atomic_install_no_clobber(
            directory_fd,
            _MANIFEST_FILE_NAME,
            data,
            "audit manifest",
            MAX_AUDIT_DOCUMENT_BYTES,
        )
        if installed:
            return
        descriptor = _open_artifact_leaf(
            directory_fd,
            _MANIFEST_FILE_NAME,
            os.O_RDWR,
            "audit manifest",
        )
        try:
            _validate_published_leaf_identity(
                directory_fd,
                _MANIFEST_FILE_NAME,
                descriptor,
                "audit manifest",
                expected_mode=None,
            )
            existing = _load_manifest_fd(descriptor)
            _validate_published_leaf_identity(
                directory_fd,
                _MANIFEST_FILE_NAME,
                descriptor,
                "audit manifest",
                expected_mode=None,
            )
            if existing != manifest:
                raise ValueError("existing audit manifest differs")
            os.fchmod(descriptor, 0o600)
            _validate_published_leaf_identity(
                directory_fd,
                _MANIFEST_FILE_NAME,
                descriptor,
                "audit manifest",
                expected_mode=0o600,
            )
            os.fsync(descriptor)
            _validate_published_leaf_identity(
                directory_fd,
                _MANIFEST_FILE_NAME,
                descriptor,
                "audit manifest",
                expected_mode=0o600,
            )
        except BaseException:
            _close_ignoring_error(descriptor)
            raise
        else:
            os.close(descriptor)


def load_audit_manifest(path: str | os.PathLike[str]) -> AuditManifest:
    payload = _decode_json(
        _safe_read_bytes(
            Path(path),
            "audit manifest",
            recover_owned_temp=True,
        ),
        "audit manifest",
    )
    return _manifest_from_obj(payload)


def _validate_finding(finding: object) -> AuditFinding:
    if not isinstance(finding, AuditFinding):
        raise TypeError("finding must be an AuditFinding")
    candidate = _validate_candidate(finding.candidate)
    if not isinstance(finding.status, str) or finding.status not in _STATUS_VALUES:
        raise ValueError("finding status is invalid")
    if finding.reason_code is not None and (
        not isinstance(finding.reason_code, str)
        or not _REASON_CODE_RE.fullmatch(finding.reason_code)
    ):
        raise ValueError("finding reason_code is invalid")
    if not isinstance(finding.observed_subtitle_ids, tuple):
        raise TypeError("finding observed_subtitle_ids must be a string tuple")
    if len(finding.observed_subtitle_ids) > MAX_OBSERVED_SUBTITLE_IDS:
        raise ValueError("finding contains too many observed subtitle IDs")
    for observed_subtitle_id in finding.observed_subtitle_ids:
        _validate_canonical_uuid(
            observed_subtitle_id,
            "finding observed_subtitle_id",
        )
    try:
        candidate.validated_receipt()
    except ValueError:
        receipt_is_valid = False
    else:
        receipt_is_valid = True
    if finding.status == VisibilityStatus.INVALID_RECEIPT.value:
        if receipt_is_valid:
            raise ValueError("invalid_receipt requires an invalid verified receipt")
    elif not receipt_is_valid:
        raise ValueError("finding requires a valid verified receipt")
    return finding


def _finding_to_obj(finding: AuditFinding) -> dict[str, object]:
    return {
        "schema_version": AUDIT_REPORT_SCHEMA_VERSION,
        "candidate": _candidate_to_obj(finding.candidate),
        "status": finding.status,
        "reason_code": finding.reason_code,
        "observed_subtitle_ids": list(finding.observed_subtitle_ids),
    }


def _finding_from_obj(value: object) -> AuditFinding:
    payload = _exact_dict(value, _FINDING_FIELDS, "finding")
    schema_version = payload["schema_version"]
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != AUDIT_REPORT_SCHEMA_VERSION
    ):
        raise ValueError("finding schema version is unsupported")
    observed = payload["observed_subtitle_ids"]
    if not isinstance(observed, list) or any(not isinstance(item, str) for item in observed):
        raise TypeError("finding observed_subtitle_ids must be a JSON string array")
    if len(observed) > MAX_OBSERVED_SUBTITLE_IDS:
        raise ValueError("finding contains too many observed subtitle IDs")
    finding = AuditFinding(
        candidate=_candidate_from_obj(payload["candidate"]),
        status=payload["status"],  # type: ignore[arg-type]
        reason_code=payload["reason_code"],  # type: ignore[arg-type]
        observed_subtitle_ids=tuple(observed),
    )
    return _validate_finding(finding)


def _match_finding_to_manifest(
    finding: AuditFinding,
    candidates_by_job_id: Mapping[str, AuditCandidateSnapshot],
) -> None:
    expected = candidates_by_job_id.get(finding.candidate.job_id)
    if expected is None:
        raise ValueError("finding candidate is not in the manifest")
    if finding.candidate != expected:
        raise ValueError("finding candidate differs from the manifest snapshot")


def _load_manifest_at(directory_fd: int) -> AuditManifest:
    fcntl.flock(directory_fd, fcntl.LOCK_EX)
    descriptor = _open_artifact_leaf(
        directory_fd,
        _MANIFEST_FILE_NAME,
        os.O_RDONLY,
        "audit manifest",
    )
    try:
        _validate_published_leaf_identity(
            directory_fd,
            _MANIFEST_FILE_NAME,
            descriptor,
            "audit manifest",
            expected_mode=0o600,
        )
        manifest = _load_manifest_fd(descriptor)
        _validate_published_leaf_identity(
            directory_fd,
            _MANIFEST_FILE_NAME,
            descriptor,
            "audit manifest",
            expected_mode=0o600,
        )
    except BaseException:
        _close_ignoring_error(descriptor)
        raise
    else:
        os.close(descriptor)
        return manifest


def _load_findings_from_fd(
    descriptor: int,
    manifest: AuditManifest,
) -> tuple[AuditFinding, ...]:
    file_stat = _validate_regular_fd(descriptor, "audit checkpoint")
    if file_stat.st_size > MAX_AUDIT_DOCUMENT_BYTES:
        raise ValueError("audit checkpoint is too large")
    os.lseek(descriptor, 0, os.SEEK_SET)
    candidates_by_job_id = {item.job_id: item for item in manifest.candidates}
    findings: list[AuditFinding] = []
    seen_job_ids: set[str] = set()
    stream = os.fdopen(os.dup(descriptor), "rb")
    try:
        while True:
            line = stream.readline(MAX_CHECKPOINT_LINE_BYTES + 1)
            if not line:
                break
            if len(line) > MAX_CHECKPOINT_LINE_BYTES:
                raise ValueError("audit checkpoint row is too large")
            if line == b"\n":
                raise ValueError("audit checkpoint contains a blank row")
            if not line.endswith(b"\n"):
                raise ValueError("audit checkpoint contains a truncated row")
            if len(findings) >= min(MAX_AUDIT_FINDINGS, len(manifest.candidates)):
                raise ValueError("audit checkpoint contains too many findings")
            finding = _finding_from_obj(
                _decode_json(line[:-1], "audit checkpoint row")
            )
            _match_finding_to_manifest(finding, candidates_by_job_id)
            if finding.candidate.job_id in seen_job_ids:
                raise ValueError("audit checkpoint contains a duplicate job finding")
            seen_job_ids.add(finding.candidate.job_id)
            findings.append(finding)
    except BaseException:
        try:
            stream.close()
        except OSError:
            pass
        raise
    else:
        stream.close()
    return tuple(findings)


def _load_findings_at(
    directory_fd: int,
    manifest: AuditManifest,
) -> tuple[AuditFinding, ...]:
    try:
        descriptor = _open_existing_leaf(
            directory_fd,
            _CHECKPOINT_FILE_NAME,
            os.O_RDONLY,
            "audit checkpoint",
        )
    except FileNotFoundError:
        return ()
    try:
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        _validate_published_leaf_identity(
            directory_fd,
            _CHECKPOINT_FILE_NAME,
            descriptor,
            "audit checkpoint",
            expected_mode=0o600,
        )
        findings = _load_findings_from_fd(descriptor, manifest)
        _validate_published_leaf_identity(
            directory_fd,
            _CHECKPOINT_FILE_NAME,
            descriptor,
            "audit checkpoint",
            expected_mode=0o600,
        )
    except BaseException:
        _close_ignoring_error(descriptor)
        raise
    else:
        os.close(descriptor)
        return findings


def load_audit_findings(
    output_dir: str | os.PathLike[str], manifest: AuditManifest
) -> tuple[AuditFinding, ...]:
    manifest = _validate_manifest(manifest)
    with _CHECKPOINT_PROCESS_LOCK:
        with _pinned_directory(output_dir, create=False) as directory_fd:
            return _load_findings_at(directory_fd, manifest)


def _append_audit_finding_locked(
    output_dir: str | os.PathLike[str], finding: AuditFinding
) -> None:
    finding = _validate_finding(finding)
    row = _canonical_bytes(_finding_to_obj(finding)) + b"\n"
    if len(row) > MAX_CHECKPOINT_LINE_BYTES:
        raise ValueError("audit checkpoint row is too large")
    with _pinned_directory(output_dir, create=False) as directory_fd:
        manifest = _load_manifest_at(directory_fd)
        candidates_by_job_id = {item.job_id: item for item in manifest.candidates}
        _match_finding_to_manifest(finding, candidates_by_job_id)
        created = False
        try:
            descriptor = os.open(
                _CHECKPOINT_FILE_NAME,
                os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory_fd,
            )
            created = True
        except FileExistsError:
            descriptor = _open_existing_leaf(
                directory_fd,
                _CHECKPOINT_FILE_NAME,
                os.O_RDWR | os.O_APPEND,
                "audit checkpoint",
            )
        original_size: int | None = None
        row_durable = False
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            _validate_published_leaf_identity(
                directory_fd,
                _CHECKPOINT_FILE_NAME,
                descriptor,
                "audit checkpoint",
                expected_mode=None,
            )
            existing = _load_findings_from_fd(descriptor, manifest)
            original_size = _validate_published_leaf_identity(
                directory_fd,
                _CHECKPOINT_FILE_NAME,
                descriptor,
                "audit checkpoint",
                expected_mode=None,
            ).st_size
            if finding.candidate.job_id in {
                item.candidate.job_id for item in existing
            }:
                raise ValueError("audit checkpoint already contains this job finding")
            if original_size + len(row) > MAX_AUDIT_DOCUMENT_BYTES:
                raise ValueError("audit checkpoint would exceed the document size limit")
            os.fchmod(descriptor, 0o600)
            _validate_published_leaf_identity(
                directory_fd,
                _CHECKPOINT_FILE_NAME,
                descriptor,
                "audit checkpoint",
                expected_mode=0o600,
            )
            written = os.write(descriptor, row)
            if written != len(row):
                raise OSError("audit checkpoint append was incomplete")
            _validate_published_leaf_identity(
                directory_fd,
                _CHECKPOINT_FILE_NAME,
                descriptor,
                "audit checkpoint",
                expected_mode=0o600,
            )
            os.fsync(descriptor)
            _validate_published_leaf_identity(
                directory_fd,
                _CHECKPOINT_FILE_NAME,
                descriptor,
                "audit checkpoint",
                expected_mode=0o600,
            )
            if created:
                _fsync_directory_fd(directory_fd)
            _validate_published_leaf_identity(
                directory_fd,
                _CHECKPOINT_FILE_NAME,
                descriptor,
                "audit checkpoint",
                expected_mode=0o600,
            )
            row_durable = True
        except BaseException:
            if original_size is not None and not row_durable:
                try:
                    os.ftruncate(descriptor, original_size)
                    os.fsync(descriptor)
                except OSError:
                    pass
            _close_ignoring_error(descriptor)
            raise
        else:
            os.close(descriptor)


def append_audit_finding(
    output_dir: str | os.PathLike[str], finding: AuditFinding
) -> None:
    with _CHECKPOINT_PROCESS_LOCK:
        _append_audit_finding_locked(output_dir, finding)


def _status_counts(findings: tuple[AuditFinding, ...]) -> dict[str, int]:
    counts = {status: 0 for status in sorted(_STATUS_VALUES)}
    for finding in findings:
        counts[finding.status] += 1
    return counts


def _report_payload_without_digest(report: AuditReport) -> dict[str, object]:
    return {
        "manifest": _manifest_to_obj(report.manifest),
        "findings": [_finding_to_obj(item) for item in report.findings],
        "counts": dict(report.counts),
        "complete": report.complete,
    }


def audit_report_sha256(report: AuditReport) -> str:
    return hashlib.sha256(_canonical_bytes(_report_payload_without_digest(report))).hexdigest()


def _report_to_obj(report: AuditReport) -> dict[str, object]:
    payload = _report_payload_without_digest(report)
    payload["report_sha256"] = report.report_sha256
    return payload


def _max_audit_report_bytes() -> int:
    return 2 * MAX_AUDIT_DOCUMENT_BYTES + MAX_AUDIT_REPORT_OVERHEAD_BYTES


def _validate_report(report: object) -> AuditReport:
    if not isinstance(report, AuditReport):
        raise TypeError("report must be an AuditReport")
    manifest = _validate_manifest(report.manifest)
    if not isinstance(report.findings, tuple):
        raise TypeError("report findings must be a tuple")
    if len(report.findings) > MAX_AUDIT_FINDINGS:
        raise ValueError("audit report contains too many findings")
    findings = tuple(_validate_finding(item) for item in report.findings)
    expected_candidates = manifest.candidates
    if tuple(item.candidate for item in findings) != expected_candidates:
        raise ValueError("report findings are incomplete, extra, or out of order")
    if report.complete is not True:
        raise ValueError("audit report must be complete")
    expected_counts = _status_counts(findings)
    if report.counts != expected_counts:
        raise ValueError("audit report counts do not match findings")
    if not isinstance(report.report_sha256, str) or not _LOWERCASE_SHA256_RE.fullmatch(
        report.report_sha256
    ):
        raise ValueError("audit report digest is invalid")
    if not hmac.compare_digest(report.report_sha256, audit_report_sha256(report)):
        raise ValueError("audit report digest does not match payload")
    return report


def _report_from_obj(value: object) -> AuditReport:
    payload = _exact_dict(value, _REPORT_FIELDS, "audit report")
    findings_payload = payload["findings"]
    if not isinstance(findings_payload, list):
        raise TypeError("audit report findings must be a JSON array")
    if len(findings_payload) > MAX_AUDIT_FINDINGS:
        raise ValueError("audit report contains too many findings")
    counts_payload = payload["counts"]
    if not isinstance(counts_payload, dict) or set(counts_payload) != _STATUS_VALUES:
        raise ValueError("audit report counts have invalid fields")
    if any(
        not isinstance(count, int) or isinstance(count, bool) or count < 0
        for count in counts_payload.values()
    ):
        raise TypeError("audit report counts must be non-negative integers")
    report = AuditReport(
        manifest=_manifest_from_obj(payload["manifest"]),
        findings=tuple(_finding_from_obj(item) for item in findings_payload),
        counts=dict(counts_payload),  # type: ignore[arg-type]
        complete=payload["complete"],  # type: ignore[arg-type]
        report_sha256=payload["report_sha256"],  # type: ignore[arg-type]
    )
    return _validate_report(report)


def _load_report_fd(descriptor: int) -> AuditReport:
    data = _read_fd_limited(
        descriptor,
        "audit report",
        _max_audit_report_bytes(),
    )
    return _report_from_obj(_decode_json(data, "audit report"))


def _finalize_audit_report_locked(
    output_dir: str | os.PathLike[str],
) -> AuditReport:
    with _pinned_directory(output_dir, create=False) as directory_fd:
        fcntl.flock(directory_fd, fcntl.LOCK_EX)
        manifest = _load_manifest_at(directory_fd)
        checkpoint_findings = _load_findings_at(directory_fd, manifest)
        findings_by_job_id = {
            item.candidate.job_id: item for item in checkpoint_findings
        }
        if len(findings_by_job_id) != len(manifest.candidates) or set(
            findings_by_job_id
        ) != {item.job_id for item in manifest.candidates}:
            raise ValueError("audit checkpoint is incomplete")
        ordered_findings = tuple(
            findings_by_job_id[candidate.job_id] for candidate in manifest.candidates
        )
        unsigned = AuditReport(
            manifest=manifest,
            findings=ordered_findings,
            counts=_status_counts(ordered_findings),
            complete=True,
            report_sha256="",
        )
        report = replace(unsigned, report_sha256=audit_report_sha256(unsigned))
        _validate_report(report)
        data = _canonical_bytes(_report_to_obj(report))
        report_limit = _max_audit_report_bytes()
        if len(data) > report_limit:
            raise ValueError("audit report is too large")
        installed = _atomic_install_no_clobber(
            directory_fd,
            _REPORT_FILE_NAME,
            data,
            "audit report",
            report_limit,
        )
        if installed:
            return report
        descriptor = _open_artifact_leaf(
            directory_fd,
            _REPORT_FILE_NAME,
            os.O_RDWR,
            "audit report",
        )
        try:
            _validate_published_leaf_identity(
                directory_fd,
                _REPORT_FILE_NAME,
                descriptor,
                "audit report",
                expected_mode=None,
            )
            existing = _load_report_fd(descriptor)
            _validate_published_leaf_identity(
                directory_fd,
                _REPORT_FILE_NAME,
                descriptor,
                "audit report",
                expected_mode=None,
            )
            if existing != report:
                raise ValueError("existing audit report differs")
            os.fchmod(descriptor, 0o600)
            _validate_published_leaf_identity(
                directory_fd,
                _REPORT_FILE_NAME,
                descriptor,
                "audit report",
                expected_mode=0o600,
            )
            os.fsync(descriptor)
            _validate_published_leaf_identity(
                directory_fd,
                _REPORT_FILE_NAME,
                descriptor,
                "audit report",
                expected_mode=0o600,
            )
        except BaseException:
            _close_ignoring_error(descriptor)
            raise
        else:
            os.close(descriptor)
            return existing


def finalize_audit_report(output_dir: str | os.PathLike[str]) -> AuditReport:
    with _CHECKPOINT_PROCESS_LOCK:
        return _finalize_audit_report_locked(output_dir)


def load_audit_report(path: str | os.PathLike[str]) -> AuditReport:
    return _report_from_obj(
        _decode_json(
            _safe_read_bytes(
                Path(path),
                "audit report",
                recover_owned_temp=True,
                limit=_max_audit_report_bytes(),
            ),
            "audit report",
        )
    )


def load_private_json_artifact(
    path: str | os.PathLike[str],
    *,
    label: str,
    limit: int,
) -> object:
    """Load one private JSON artifact through the descriptor-pinned storage layer."""
    if not isinstance(label, str) or not label:
        raise ValueError("artifact label is invalid")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise ValueError("artifact size limit is invalid")
    return _decode_json(
        _safe_read_bytes(
            Path(path),
            label,
            recover_owned_temp=True,
            limit=limit,
        ),
        label,
    )


def write_private_json_artifact(
    path: str | os.PathLike[str],
    payload: object,
    *,
    label: str,
    limit: int,
) -> None:
    """Canonically install a private JSON artifact, idempotently and without clobber."""
    if not isinstance(label, str) or not label:
        raise ValueError("artifact label is invalid")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise ValueError("artifact size limit is invalid")
    data = _canonical_bytes(payload)
    parent, name = _leaf_parent(path)
    with _pinned_directory(parent, create=True) as directory_fd:
        fcntl.flock(directory_fd, fcntl.LOCK_EX)
        installed = _atomic_install_no_clobber(
            directory_fd,
            name,
            data,
            label,
            limit,
        )
        if installed:
            return
        descriptor = _open_artifact_leaf(
            directory_fd,
            name,
            os.O_RDWR,
            label,
        )
        try:
            _validate_published_leaf_identity(
                directory_fd,
                name,
                descriptor,
                label,
                expected_mode=None,
            )
            existing = _read_fd_limited(descriptor, label, limit)
            _validate_published_leaf_identity(
                directory_fd,
                name,
                descriptor,
                label,
                expected_mode=None,
            )
            if not hmac.compare_digest(existing, data):
                raise ValueError(f"existing {label} differs")
            os.fchmod(descriptor, 0o600)
            _validate_published_leaf_identity(
                directory_fd,
                name,
                descriptor,
                label,
                expected_mode=0o600,
            )
            os.fsync(descriptor)
            _validate_published_leaf_identity(
                directory_fd,
                name,
                descriptor,
                label,
                expected_mode=0o600,
            )
        except BaseException:
            _close_ignoring_error(descriptor)
            raise
        else:
            os.close(descriptor)
