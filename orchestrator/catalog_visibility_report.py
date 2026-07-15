from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from uuid import UUID, uuid4

from orchestrator.catalog_visibility import (
    AuditCandidateSnapshot,
    VisibilityStatus,
    normalize_catalog_api_origin,
)
from orchestrator.movie_code import canonical_movie_code


AUDIT_REPORT_SCHEMA_VERSION = 1
REPAIR_ELIGIBLE = frozenset({"missing", "not_found"})

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


@dataclass(frozen=True, slots=True)
class AuditReport:
    manifest: AuditManifest
    findings: tuple[AuditFinding, ...]
    counts: dict[str, int]
    complete: bool
    report_sha256: str


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
    payload = _exact_dict(selection, _SELECTION_FIELDS, "selection")
    raw_allowlist = payload["allowlist"]
    if raw_allowlist is None:
        allowlist: list[str] | None = None
    else:
        if not isinstance(raw_allowlist, list):
            raise TypeError("selection allowlist must be a JSON array or null")
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
    return {
        "schema_version": manifest.schema_version,
        "audit_id": manifest.audit_id,
        "created_at": manifest.created_at,
        "api_origin": manifest.api_origin,
        "database_path_sha256": manifest.database_path_sha256,
        "selection": manifest.selection,
        "candidates": [_candidate_to_obj(item) for item in manifest.candidates],
    }


def _manifest_from_obj(value: object) -> AuditManifest:
    payload = _exact_dict(value, _MANIFEST_FIELDS, "manifest")
    candidates_payload = payload["candidates"]
    if not isinstance(candidates_payload, list):
        raise TypeError("manifest candidates must be a JSON array")
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


def _absolute_without_resolving(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _reject_symlink_components(path: Path) -> None:
    absolute = _absolute_without_resolving(path)
    for component in reversed((absolute, *absolute.parents)):
        try:
            component_stat = component.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ValueError("audit path cannot be inspected safely") from exc
        if stat.S_ISLNK(component_stat.st_mode):
            raise ValueError("audit paths must not contain symlinks")


def _prepare_output_dir(output_dir: str | os.PathLike[str]) -> Path:
    path = _absolute_without_resolving(Path(output_dir))
    _reject_symlink_components(path)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ValueError("audit output directory cannot be created") from exc
    _reject_symlink_components(path)
    try:
        if not path.is_dir():
            raise ValueError("audit output path is not a directory")
    except OSError as exc:
        raise ValueError("audit output directory cannot be inspected") from exc
    return path


def _nofollow_flag() -> int:
    return getattr(os, "O_NOFOLLOW", 0)


def _safe_read_bytes(path: Path) -> bytes:
    path = _absolute_without_resolving(path)
    _reject_symlink_components(path)
    flags = os.O_RDONLY | _nofollow_flag()
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if isinstance(exc, FileNotFoundError):
            raise
        raise ValueError("audit file cannot be opened safely") from exc
    try:
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError("audit file is not a regular file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    except OSError as exc:
        raise ValueError("audit file cannot be read safely") from exc
    finally:
        os.close(descriptor)


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
        return json.loads(
            text,
            object_pairs_hook=reject_duplicate_fields,
            parse_constant=reject_nonstandard_constant,
        )
    except (UnicodeDecodeError, ValueError):
        raise ValueError(f"{label} is not valid JSON") from None


def _write_exclusive_private(path: Path, data: bytes) -> None:
    _reject_symlink_components(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _nofollow_flag()
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_audit_manifest(
    output_dir: str | os.PathLike[str], manifest: AuditManifest
) -> None:
    manifest = _validate_manifest(manifest)
    directory = _prepare_output_dir(output_dir)
    path = directory / _MANIFEST_FILE_NAME
    _reject_symlink_components(path)
    if path.exists():
        if load_audit_manifest(path) != manifest:
            raise ValueError("existing audit manifest differs")
        return
    data = _canonical_bytes(_manifest_to_obj(manifest))
    try:
        _write_exclusive_private(path, data)
    except FileExistsError:
        if load_audit_manifest(path) != manifest:
            raise ValueError("existing audit manifest differs") from None


def load_audit_manifest(path: str | os.PathLike[str]) -> AuditManifest:
    payload = _decode_json(_safe_read_bytes(Path(path)), "audit manifest")
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


def load_audit_findings(
    output_dir: str | os.PathLike[str], manifest: AuditManifest
) -> tuple[AuditFinding, ...]:
    manifest = _validate_manifest(manifest)
    directory = _prepare_output_dir(output_dir)
    path = directory / _CHECKPOINT_FILE_NAME
    _reject_symlink_components(path)
    if not path.exists():
        return ()
    data = _safe_read_bytes(path)
    if not data:
        return ()
    if not data.endswith(b"\n"):
        raise ValueError("audit checkpoint contains a truncated row")
    raw_lines = data.split(b"\n")[:-1]
    if any(not line for line in raw_lines):
        raise ValueError("audit checkpoint contains a blank row")
    candidates_by_job_id = {item.job_id: item for item in manifest.candidates}
    findings: list[AuditFinding] = []
    seen_job_ids: set[str] = set()
    for line in raw_lines:
        finding = _finding_from_obj(_decode_json(line, "audit checkpoint row"))
        _match_finding_to_manifest(finding, candidates_by_job_id)
        if finding.candidate.job_id in seen_job_ids:
            raise ValueError("audit checkpoint contains a duplicate job finding")
        seen_job_ids.add(finding.candidate.job_id)
        findings.append(finding)
    return tuple(findings)


def append_audit_finding(
    output_dir: str | os.PathLike[str], finding: AuditFinding
) -> None:
    finding = _validate_finding(finding)
    directory = _prepare_output_dir(output_dir)
    manifest = load_audit_manifest(directory / _MANIFEST_FILE_NAME)
    candidates_by_job_id = {item.job_id: item for item in manifest.candidates}
    _match_finding_to_manifest(finding, candidates_by_job_id)
    existing = load_audit_findings(directory, manifest)
    if finding.candidate.job_id in {item.candidate.job_id for item in existing}:
        raise ValueError("audit checkpoint already contains this job finding")
    path = directory / _CHECKPOINT_FILE_NAME
    _reject_symlink_components(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | _nofollow_flag()
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise ValueError("audit checkpoint cannot be opened safely") from exc
    try:
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError("audit checkpoint is not a regular file")
        os.fchmod(descriptor, 0o600)
        row = _canonical_bytes(_finding_to_obj(finding)) + b"\n"
        with os.fdopen(descriptor, "ab", closefd=False) as stream:
            stream.write(row)
            stream.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _status_counts(findings: tuple[AuditFinding, ...]) -> dict[str, int]:
    counts = {status: 0 for status in sorted(_STATUS_VALUES)}
    for finding in findings:
        counts[finding.status] += 1
    return counts


def _report_payload_without_digest(report: AuditReport) -> dict[str, object]:
    return {
        "manifest": _manifest_to_obj(report.manifest),
        "findings": [_finding_to_obj(item) for item in report.findings],
        "counts": report.counts,
        "complete": report.complete,
    }


def audit_report_sha256(report: AuditReport) -> str:
    return hashlib.sha256(_canonical_bytes(_report_payload_without_digest(report))).hexdigest()


def _report_to_obj(report: AuditReport) -> dict[str, object]:
    payload = _report_payload_without_digest(report)
    payload["report_sha256"] = report.report_sha256
    return payload


def _validate_report(report: object) -> AuditReport:
    if not isinstance(report, AuditReport):
        raise TypeError("report must be an AuditReport")
    manifest = _validate_manifest(report.manifest)
    if not isinstance(report.findings, tuple):
        raise TypeError("report findings must be a tuple")
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


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | _nofollow_flag()
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        return
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            pass
    finally:
        os.close(descriptor)


def _atomic_write_private(path: Path, data: bytes) -> None:
    _reject_symlink_components(path)
    if path.exists():
        try:
            if not stat.S_ISREG(path.lstat().st_mode):
                raise ValueError("audit report target is not a regular file")
        except OSError as exc:
            raise ValueError("audit report target cannot be inspected") from exc
    temp_path = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    descriptor: int | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _nofollow_flag()
        descriptor = os.open(temp_path, flags, 0o600)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        _reject_symlink_components(path)
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise ValueError("audit report cannot be written safely") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def finalize_audit_report(output_dir: str | os.PathLike[str]) -> AuditReport:
    directory = _prepare_output_dir(output_dir)
    report_path = directory / _REPORT_FILE_NAME
    _reject_symlink_components(report_path)
    manifest = load_audit_manifest(directory / _MANIFEST_FILE_NAME)
    checkpoint_findings = load_audit_findings(directory, manifest)
    findings_by_job_id = {
        item.candidate.job_id: item for item in checkpoint_findings
    }
    if len(findings_by_job_id) != len(manifest.candidates) or set(findings_by_job_id) != {
        item.job_id for item in manifest.candidates
    }:
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
    _atomic_write_private(report_path, _canonical_bytes(_report_to_obj(report)))
    return report


def load_audit_report(path: str | os.PathLike[str]) -> AuditReport:
    return _report_from_obj(_decode_json(_safe_read_bytes(Path(path)), "audit report"))
