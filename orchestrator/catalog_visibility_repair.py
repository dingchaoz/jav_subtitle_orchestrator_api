from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sqlite3
import stat
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import fcntl

import orchestrator.catalog_visibility_report as _report_storage
from orchestrator.catalog_visibility import (
    AuditCandidateSnapshot,
    PublicCatalogVisibilityClient,
    PublicVisibilityResult,
    PublicationReceiptSnapshot,
    VisibilityStatus,
    normalize_catalog_api_origin,
)
from orchestrator.catalog_sync import CatalogSyncClient, CatalogSyncError
from orchestrator.catalog_visibility_report import (
    REPAIR_ELIGIBLE,
    audit_report_sha256,
    load_audit_report,
    load_private_json_artifact,
    validate_audit_resume_context,
    write_private_json_artifact,
)
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


REPAIR_PLAN_SCHEMA_VERSION = 1
MAX_REPAIR_PLAN_BYTES = 64 * 1024 * 1024
_PLAN_FILE_NAME = "repair-plan.json"
_PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "api_origin",
        "report_sha256",
        "items",
        "skipped",
        "plan_sha256",
    }
)
_PLAN_ITEM_FIELDS = frozenset({"receipt", "starting_status"})
_RECEIPT_FIELDS = frozenset(
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
_SKIPPED_KEYS = frozenset(
    {
        *(status.value for status in VisibilityStatus if status.value not in REPAIR_ELIGIBLE),
        "receipt_changed",
    }
)
_LOWERCASE_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00$"
)
_EXECUTION_RECEIPT_FILE_NAME = "repair-execution.jsonl"
_EXECUTION_CLAIM_FILE_NAME = "repair-execution-claims.jsonl"
_EXECUTION_RECEIPT_FIELDS = frozenset(
    {
        "report_sha256",
        "job_id",
        "movie_code",
        "expected_subtitle_id",
        "starting_status",
        "outcome",
        "reason_code",
        "finished_at",
    }
)
_MAX_EXECUTION_RECEIPT_BYTES = 64 * 1024 * 1024
_MAX_EXECUTION_RECEIPT_ROW_BYTES = 8 * 1024
_EXECUTION_PROCESS_LOCK = threading.Lock()
_CLAIM_SCHEMA_VERSION = 1
_CLAIM_FIELDS = frozenset(
    {
        "schema_version",
        "report_sha256",
        "plan_sha256",
        "job_id",
        "movie_code",
        "expected_subtitle_id",
        "starting_status",
        "claimed_at",
    }
)
_SAFE_EXECUTION_FAILURE_REASONS = frozenset(
    {
        "catalog_fetch_failed",
        "catalog_redirect_rejected",
        "catalog_auth_failed",
        "catalog_sync_failed",
        "catalog_response_invalid",
        "catalog_response_mismatch",
        "public_visibility_fetch_failed",
        "public_visibility_redirect_rejected",
        "public_visibility_not_found",
        "public_visibility_response_invalid",
        "public_visibility_mismatch",
    }
)


@dataclass(frozen=True, slots=True)
class RepairPlanItem:
    receipt: PublicationReceiptSnapshot
    starting_status: str


@dataclass(frozen=True, slots=True)
class CatalogVisibilityRepairPlan:
    report_path: Path
    plan_path: Path
    report_sha256: str
    plan_sha256: str
    api_origin: str
    items: tuple[RepairPlanItem, ...]
    skipped: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "report_path", Path(self.report_path))
        object.__setattr__(self, "plan_path", Path(self.plan_path))
        object.__setattr__(self, "items", tuple(self.items))
        object.__setattr__(self, "skipped", MappingProxyType(dict(self.skipped)))


@dataclass(frozen=True, slots=True)
class RepairExecutionResult:
    action: str
    repaired: tuple[str, ...]
    failed: tuple[str, ...]
    skipped_receipt_changed: tuple[str, ...]
    stopped_reason: str | None
    receipt_path: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "repaired", tuple(self.repaired))
        object.__setattr__(self, "failed", tuple(self.failed))
        object.__setattr__(
            self,
            "skipped_receipt_changed",
            tuple(self.skipped_receipt_changed),
        )
        object.__setattr__(self, "receipt_path", Path(self.receipt_path))


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


def _receipt_to_obj(receipt: PublicationReceiptSnapshot) -> dict[str, object]:
    return {
        "job_id": receipt.job_id,
        "movie_code": receipt.movie_code,
        "movie_uuid": receipt.movie_uuid,
        "metadata_status": receipt.metadata_status,
        "metadata_source": receipt.metadata_source,
        "subtitle_id": receipt.subtitle_id,
        "storage_path": receipt.storage_path,
        "content_sha256": receipt.content_sha256,
        "file_size": receipt.file_size,
        "job_updated_at": receipt.job_updated_at,
    }


def _receipt_from_obj(value: object) -> PublicationReceiptSnapshot:
    payload = _exact_dict(value, _RECEIPT_FIELDS, "repair receipt")
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
    return candidate.validated_receipt()


def _item_to_obj(item: RepairPlanItem) -> dict[str, object]:
    return {
        "receipt": _receipt_to_obj(item.receipt),
        "starting_status": item.starting_status,
    }


def _payload_without_digest(
    *,
    api_origin: str,
    report_sha256: str,
    items: tuple[RepairPlanItem, ...],
    skipped: Mapping[str, int],
) -> dict[str, object]:
    return {
        "schema_version": REPAIR_PLAN_SCHEMA_VERSION,
        "api_origin": api_origin,
        "report_sha256": report_sha256,
        "items": [_item_to_obj(item) for item in items],
        "skipped": dict(skipped),
    }


def catalog_visibility_repair_plan_sha256(payload_without_digest: object) -> str:
    return hashlib.sha256(_canonical_bytes(payload_without_digest)).hexdigest()


def _validate_report_authorization(
    store: JobStore,
    report_path: Path,
    expected_api_origin: str,
):
    report = load_audit_report(report_path)
    normalized_origin = normalize_catalog_api_origin(expected_api_origin)
    if normalized_origin != report.manifest.api_origin:
        raise ValueError("repair API origin differs from audit report")
    validate_audit_resume_context(
        report.manifest,
        api_origin=normalized_origin,
        database_path=store.db_path,
        selection=dict(report.manifest.selection),
    )
    recomputed_report_digest = audit_report_sha256(report)
    if not hmac.compare_digest(report.report_sha256, recomputed_report_digest):
        raise ValueError("audit report digest does not match payload")
    job_ids = [finding.candidate.job_id for finding in report.findings]
    movie_codes = [finding.candidate.movie_code for finding in report.findings]
    if len(job_ids) != len(set(job_ids)):
        raise ValueError("audit report contains duplicate job IDs")
    if len(movie_codes) != len(set(movie_codes)):
        raise ValueError("audit report contains duplicate movie codes")
    return report


def plan_catalog_visibility_repair(
    store: JobStore,
    report_path: Path,
    *,
    expected_api_origin: str,
    output_dir: Path,
) -> CatalogVisibilityRepairPlan:
    report_path = Path(report_path)
    report = _validate_report_authorization(
        store,
        report_path,
        expected_api_origin,
    )
    items: list[RepairPlanItem] = []
    skipped: dict[str, int] = {}
    with store.connection() as connection:
        connection.execute("BEGIN")
        for finding in report.findings:
            if finding.status not in REPAIR_ELIGIBLE:
                skipped[finding.status] = skipped.get(finding.status, 0) + 1
                continue
            report_receipt = finding.candidate.validated_receipt()
            current = store.get_job(report_receipt.job_id, conn=connection)
            if current is None or current.status != JobStatus.ENGLISH_SRT_READY:
                skipped["receipt_changed"] = skipped.get("receipt_changed", 0) + 1
                continue
            try:
                current_receipt = AuditCandidateSnapshot.from_job(current).validated_receipt()
            except (TypeError, ValueError):
                skipped["receipt_changed"] = skipped.get("receipt_changed", 0) + 1
                continue
            if current_receipt != report_receipt:
                skipped["receipt_changed"] = skipped.get("receipt_changed", 0) + 1
                continue
            items.append(
                RepairPlanItem(
                    receipt=current_receipt,
                    starting_status=finding.status,
                )
            )

    frozen_items = tuple(items)
    frozen_skipped = {key: count for key, count in skipped.items() if count}
    unsigned = _payload_without_digest(
        api_origin=report.manifest.api_origin,
        report_sha256=report.report_sha256,
        items=frozen_items,
        skipped=frozen_skipped,
    )
    plan_digest = catalog_visibility_repair_plan_sha256(unsigned)
    payload = dict(unsigned)
    payload["plan_sha256"] = plan_digest
    plan_path = Path(output_dir) / _PLAN_FILE_NAME
    write_private_json_artifact(
        plan_path,
        payload,
        label="repair plan",
        limit=MAX_REPAIR_PLAN_BYTES,
    )
    return CatalogVisibilityRepairPlan(
        report_path=report_path,
        plan_path=plan_path,
        report_sha256=report.report_sha256,
        plan_sha256=plan_digest,
        api_origin=report.manifest.api_origin,
        items=frozen_items,
        skipped=frozen_skipped,
    )


def load_catalog_visibility_repair_plan(
    plan_path: Path,
    *,
    report_path: Path,
) -> CatalogVisibilityRepairPlan:
    plan_path = Path(plan_path)
    report_path = Path(report_path)
    report = load_audit_report(report_path)
    recomputed_report_digest = audit_report_sha256(report)
    if not hmac.compare_digest(report.report_sha256, recomputed_report_digest):
        raise ValueError("audit report digest does not match payload")
    payload = _exact_dict(
        load_private_json_artifact(
            plan_path,
            label="repair plan",
            limit=MAX_REPAIR_PLAN_BYTES,
        ),
        _PLAN_FIELDS,
        "repair plan",
    )
    schema_version = payload["schema_version"]
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != REPAIR_PLAN_SCHEMA_VERSION
    ):
        raise ValueError("repair plan schema version is unsupported")
    api_origin = payload["api_origin"]
    if not isinstance(api_origin, str):
        raise TypeError("repair plan API origin must be a string")
    try:
        normalized_origin = normalize_catalog_api_origin(api_origin)
    except ValueError:
        raise ValueError("repair plan API origin is invalid") from None
    if normalized_origin != api_origin or api_origin != report.manifest.api_origin:
        raise ValueError("repair plan API origin differs from audit report")
    report_digest = payload["report_sha256"]
    if (
        not isinstance(report_digest, str)
        or not _LOWERCASE_SHA256_RE.fullmatch(report_digest)
        or not hmac.compare_digest(report_digest, report.report_sha256)
    ):
        raise ValueError("repair plan report digest is invalid")
    plan_digest = payload["plan_sha256"]
    if not isinstance(plan_digest, str) or not _LOWERCASE_SHA256_RE.fullmatch(
        plan_digest
    ):
        raise ValueError("repair plan digest is invalid")
    unsigned = dict(payload)
    del unsigned["plan_sha256"]
    if not hmac.compare_digest(
        plan_digest,
        catalog_visibility_repair_plan_sha256(unsigned),
    ):
        raise ValueError("repair plan digest does not match payload")

    raw_items = payload["items"]
    if not isinstance(raw_items, list) or len(raw_items) > len(report.findings):
        raise ValueError("repair plan items are invalid")
    items: list[RepairPlanItem] = []
    for raw_item in raw_items:
        item_payload = _exact_dict(raw_item, _PLAN_ITEM_FIELDS, "repair plan item")
        starting_status = item_payload["starting_status"]
        if not isinstance(starting_status, str) or starting_status not in REPAIR_ELIGIBLE:
            raise ValueError("repair plan starting status is invalid")
        items.append(
            RepairPlanItem(
                receipt=_receipt_from_obj(item_payload["receipt"]),
                starting_status=starting_status,
            )
        )
    job_ids = [item.receipt.job_id for item in items]
    movie_codes = [item.receipt.movie_code for item in items]
    if len(job_ids) != len(set(job_ids)):
        raise ValueError("repair plan contains duplicate job IDs")
    if len(movie_codes) != len(set(movie_codes)):
        raise ValueError("repair plan contains duplicate movie codes")

    eligible_findings = [
        finding for finding in report.findings if finding.status in REPAIR_ELIGIBLE
    ]
    eligible_positions = {
        finding.candidate.job_id: index
        for index, finding in enumerate(eligible_findings)
    }
    positions: list[int] = []
    for item in items:
        position = eligible_positions.get(item.receipt.job_id)
        if position is None:
            raise ValueError("repair plan item is not eligible in the audit report")
        finding = eligible_findings[position]
        if (
            item.starting_status != finding.status
            or item.receipt != finding.candidate.validated_receipt()
        ):
            raise ValueError("repair plan item differs from the audit report")
        positions.append(position)
    if positions != sorted(positions) or len(positions) != len(set(positions)):
        raise ValueError("repair plan items are not in audit report order")

    raw_skipped = payload["skipped"]
    if not isinstance(raw_skipped, dict) or not set(raw_skipped).issubset(_SKIPPED_KEYS):
        raise ValueError("repair plan skipped counts have invalid fields")
    if any(
        not isinstance(count, int) or isinstance(count, bool) or count <= 0
        for count in raw_skipped.values()
    ):
        raise ValueError("repair plan skipped counts must be positive integers")
    expected_skipped: dict[str, int] = {}
    for finding in report.findings:
        if finding.status not in REPAIR_ELIGIBLE:
            expected_skipped[finding.status] = expected_skipped.get(finding.status, 0) + 1
    changed_count = len(eligible_findings) - len(items)
    if changed_count:
        expected_skipped["receipt_changed"] = changed_count
    if raw_skipped != expected_skipped:
        raise ValueError("repair plan skipped counts do not match items")

    return CatalogVisibilityRepairPlan(
        report_path=report_path,
        plan_path=plan_path,
        report_sha256=report_digest,
        plan_sha256=plan_digest,
        api_origin=api_origin,
        items=tuple(items),
        skipped=dict(raw_skipped),  # type: ignore[arg-type]
    )


def _validated_execution_plan(
    store: JobStore,
    plan: CatalogVisibilityRepairPlan,
) -> CatalogVisibilityRepairPlan:
    if not isinstance(plan, CatalogVisibilityRepairPlan):
        raise TypeError("plan must be a CatalogVisibilityRepairPlan")
    _validate_report_authorization(store, plan.report_path, plan.api_origin)
    loaded = load_catalog_visibility_repair_plan(
        plan.plan_path,
        report_path=plan.report_path,
    )
    if loaded != plan:
        raise ValueError("repair plan differs from persisted artifact")
    return loaded


def _execution_receipt_path(output_dir: Path) -> Path:
    _report_storage._path_components(output_dir)
    return output_dir / _EXECUTION_RECEIPT_FILE_NAME


def _validate_sync_client_binding(
    sync_client: object,
    api_origin: str,
) -> PublicCatalogVisibilityClient:
    try:
        client_origin = normalize_catalog_api_origin(getattr(sync_client, "base_url"))
    except (AttributeError, TypeError, ValueError):
        raise ValueError("sync client origin is invalid") from None
    if client_origin != api_origin:
        raise ValueError("sync client origin differs from repair plan")
    if getattr(sync_client, "public_visibility_verification_enabled", None) is not True:
        raise ValueError("sync client exact public visibility verification is required")
    visibility_client = getattr(sync_client, "public_visibility_client", None)
    if visibility_client is None or not callable(getattr(visibility_client, "check", None)):
        raise ValueError("sync client public visibility probe is required")
    try:
        visibility_origin = normalize_catalog_api_origin(
            getattr(visibility_client, "base_url")
        )
    except (AttributeError, TypeError, ValueError):
        raise ValueError("sync client public visibility origin is invalid") from None
    if visibility_origin != api_origin:
        raise ValueError("sync client public visibility origin differs from repair plan")
    if not callable(getattr(sync_client, "sync", None)):
        raise ValueError("sync client sync capability is required")
    return visibility_client  # type: ignore[return-value]


def _open_pinned_canonical_execution_directory(
    output_dir: Path,
    plan: CatalogVisibilityRepairPlan,
) -> tuple[Path, int]:
    requested = Path(output_dir)
    canonical = plan.plan_path.parent
    _report_storage._path_components(requested)
    _report_storage._path_components(canonical)
    requested_fd: int | None = None
    canonical_fd: int | None = None
    try:
        requested_fd = _report_storage._open_directory_path(requested, create=False)
        requested_stat = os.fstat(requested_fd)
        canonical_fd = _report_storage._open_directory_path(canonical, create=False)
        canonical_stat = os.fstat(canonical_fd)
        if (
            requested_stat.st_dev != canonical_stat.st_dev
            or requested_stat.st_ino != canonical_stat.st_ino
            or requested_stat.st_uid != os.geteuid()
            or canonical_stat.st_uid != os.geteuid()
            or (requested_stat.st_mode & 0o077) != 0
            or (canonical_stat.st_mode & 0o077) != 0
        ):
            raise ValueError(
                "execution output directory must be the private repair plan directory"
            )
        os.close(requested_fd)
        requested_fd = None
        result = canonical_fd
        canonical_fd = None
        return canonical, result
    except (FileNotFoundError, OSError, ValueError):
        raise ValueError(
            "execution output directory must be the repair plan directory"
        ) from None
    finally:
        _report_storage._close_ignoring_error(requested_fd)
        _report_storage._close_ignoring_error(canonical_fd)


def _validate_pinned_directory_path(
    canonical: Path,
    directory_fd: int,
) -> None:
    pinned = os.fstat(directory_fd)
    try:
        published = os.stat(canonical, follow_symlinks=False)
    except OSError:
        raise ValueError("execution plan directory changed after validation") from None
    if (
        not stat.S_ISDIR(pinned.st_mode)
        or not stat.S_ISDIR(published.st_mode)
        or pinned.st_dev != published.st_dev
        or pinned.st_ino != published.st_ino
        or pinned.st_uid != os.geteuid()
        or published.st_uid != os.geteuid()
        or (published.st_mode & 0o077) != 0
    ):
        raise ValueError("execution plan directory changed after validation")


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _validate_utc_timestamp(value: object, label: str) -> None:
    if not isinstance(value, str) or not _UTC_TIMESTAMP_RE.fullmatch(value):
        raise ValueError(f"{label} timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{label} timestamp is invalid") from None
    if (
        parsed.utcoffset() != timezone.utc.utcoffset(parsed)
        or parsed.isoformat(timespec="microseconds") != value
    ):
        raise ValueError(f"{label} timestamp is invalid")


def _execution_row(
    item: RepairPlanItem,
    *,
    report_sha256: str,
    outcome: str,
    reason_code: str | None,
) -> dict[str, object]:
    return {
        "report_sha256": report_sha256,
        "job_id": item.receipt.job_id,
        "movie_code": item.receipt.movie_code,
        "expected_subtitle_id": item.receipt.subtitle_id,
        "starting_status": item.starting_status,
        "outcome": outcome,
        "reason_code": reason_code,
        "finished_at": _utc_timestamp(),
    }


def _validate_execution_row(
    value: object,
    plan: CatalogVisibilityRepairPlan,
    items_by_job_id: Mapping[str, RepairPlanItem],
) -> dict[str, object]:
    row = _exact_dict(value, _EXECUTION_RECEIPT_FIELDS, "repair execution receipt")
    report_sha256 = row["report_sha256"]
    if (
        not isinstance(report_sha256, str)
        or not hmac.compare_digest(report_sha256, plan.report_sha256)
    ):
        raise ValueError("repair execution receipt belongs to a different report")
    job_id = row["job_id"]
    item = items_by_job_id.get(job_id) if isinstance(job_id, str) else None
    if item is None:
        raise ValueError("repair execution receipt item is not in the plan")
    outcome = row["outcome"]
    reason_code = row["reason_code"]
    valid_outcome = (
        (outcome == "repaired" and reason_code is None)
        or (
            outcome == "failed"
            and isinstance(reason_code, str)
            and reason_code in _SAFE_EXECUTION_FAILURE_REASONS
        )
        or (
            outcome == "skipped_receipt_changed"
            and reason_code == "receipt_changed"
        )
    )
    if (
        row["movie_code"] != item.receipt.movie_code
        or row["expected_subtitle_id"] != item.receipt.subtitle_id
        or row["starting_status"] != item.starting_status
        or not valid_outcome
        or not isinstance(row["finished_at"], str)
    ):
        raise ValueError("repair execution receipt row is invalid")
    _validate_utc_timestamp(row["finished_at"], "repair execution receipt")
    return row


def _load_execution_rows_from_fd(
    descriptor: int,
    plan: CatalogVisibilityRepairPlan,
    items_by_job_id: Mapping[str, RepairPlanItem],
) -> tuple[dict[str, object], ...]:
    data = _report_storage._read_fd_limited(
        descriptor,
        "repair execution receipt",
        _MAX_EXECUTION_RECEIPT_BYTES,
    )
    if not data:
        return ()
    if not data.endswith(b"\n"):
        complete_size = data.rfind(b"\n") + 1
        os.ftruncate(descriptor, complete_size)
        os.fsync(descriptor)
        data = data[:complete_size]
        if not data:
            return ()
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw_row in data.splitlines():
        if not raw_row or len(raw_row) > _MAX_EXECUTION_RECEIPT_ROW_BYTES:
            raise ValueError("repair execution receipt row is invalid")
        row = _validate_execution_row(
            _report_storage._decode_json(raw_row, "repair execution receipt row"),
            plan,
            items_by_job_id,
        )
        job_id = row["job_id"]
        assert isinstance(job_id, str)
        if job_id in seen:
            raise ValueError("repair execution receipt contains a duplicate job")
        seen.add(job_id)
        rows.append(row)
    if len(rows) > len(plan.items):
        raise ValueError("repair execution receipt contains too many rows")
    observed_job_ids = [row["job_id"] for row in rows]
    expected_job_ids = [
        item.receipt.job_id for item in plan.items[: len(rows)]
    ]
    if observed_job_ids != expected_job_ids:
        raise ValueError("repair execution receipt rows are not a plan prefix")
    return tuple(rows)


def _claim_row(
    item: RepairPlanItem,
    plan: CatalogVisibilityRepairPlan,
) -> dict[str, object]:
    return {
        "schema_version": _CLAIM_SCHEMA_VERSION,
        "report_sha256": plan.report_sha256,
        "plan_sha256": plan.plan_sha256,
        "job_id": item.receipt.job_id,
        "movie_code": item.receipt.movie_code,
        "expected_subtitle_id": item.receipt.subtitle_id,
        "starting_status": item.starting_status,
        "claimed_at": _utc_timestamp(),
    }


def _validate_claim_row(
    value: object,
    plan: CatalogVisibilityRepairPlan,
    items_by_job_id: Mapping[str, RepairPlanItem],
) -> dict[str, object]:
    row = _exact_dict(value, _CLAIM_FIELDS, "repair execution claim")
    if row["schema_version"] != _CLAIM_SCHEMA_VERSION:
        raise ValueError("repair execution claim schema is invalid")
    if (
        not isinstance(row["report_sha256"], str)
        or not hmac.compare_digest(row["report_sha256"], plan.report_sha256)
        or not isinstance(row["plan_sha256"], str)
        or not hmac.compare_digest(row["plan_sha256"], plan.plan_sha256)
    ):
        raise ValueError("repair execution claim belongs to a different plan")
    job_id = row["job_id"]
    item = items_by_job_id.get(job_id) if isinstance(job_id, str) else None
    if item is None:
        raise ValueError("repair execution claim item is not in the plan")
    if (
        row["movie_code"] != item.receipt.movie_code
        or row["expected_subtitle_id"] != item.receipt.subtitle_id
        or row["starting_status"] != item.starting_status
        or not isinstance(row["claimed_at"], str)
    ):
        raise ValueError("repair execution claim row is invalid")
    _validate_utc_timestamp(row["claimed_at"], "repair execution claim")
    return row


def _load_claim_rows_from_fd(
    descriptor: int,
    plan: CatalogVisibilityRepairPlan,
    items_by_job_id: Mapping[str, RepairPlanItem],
) -> tuple[dict[str, object], ...]:
    data = _report_storage._read_fd_limited(
        descriptor,
        "repair execution claims",
        _MAX_EXECUTION_RECEIPT_BYTES,
    )
    if not data:
        return ()
    if not data.endswith(b"\n"):
        complete_size = data.rfind(b"\n") + 1
        os.ftruncate(descriptor, complete_size)
        os.fsync(descriptor)
        data = data[:complete_size]
        if not data:
            return ()
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw_row in data.splitlines():
        if not raw_row or len(raw_row) > _MAX_EXECUTION_RECEIPT_ROW_BYTES:
            raise ValueError("repair execution claim row is invalid")
        row = _validate_claim_row(
            _report_storage._decode_json(raw_row, "repair execution claim row"),
            plan,
            items_by_job_id,
        )
        job_id = row["job_id"]
        assert isinstance(job_id, str)
        if job_id in seen:
            raise ValueError("repair execution claims contain a duplicate job")
        seen.add(job_id)
        rows.append(row)
    if len(rows) > len(plan.items):
        raise ValueError("repair execution claims contain too many rows")
    positions = {
        item.receipt.job_id: index for index, item in enumerate(plan.items)
    }
    observed_positions = [positions[row["job_id"]] for row in rows]
    if observed_positions != sorted(observed_positions):
        raise ValueError("repair execution claim rows are out of plan order")
    return tuple(rows)


def _open_execution_receipt(
    directory_fd: int,
) -> tuple[int, bool]:
    try:
        descriptor = os.open(
            _EXECUTION_RECEIPT_FILE_NAME,
            os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory_fd,
        )
        os.fchmod(descriptor, 0o600)
        return descriptor, True
    except FileExistsError:
        return (
            _report_storage._open_existing_leaf(
                directory_fd,
                _EXECUTION_RECEIPT_FILE_NAME,
                os.O_RDWR | os.O_APPEND,
                "repair execution receipt",
            ),
            False,
        )


def _open_execution_claims(directory_fd: int) -> tuple[int, bool]:
    try:
        descriptor = os.open(
            _EXECUTION_CLAIM_FILE_NAME,
            os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory_fd,
        )
        os.fchmod(descriptor, 0o600)
        return descriptor, True
    except FileExistsError:
        return (
            _report_storage._open_existing_leaf(
                directory_fd,
                _EXECUTION_CLAIM_FILE_NAME,
                os.O_RDWR | os.O_APPEND,
                "repair execution claims",
            ),
            False,
        )


def _append_execution_row_at(
    directory_fd: int,
    descriptor: int,
    row: dict[str, object],
) -> None:
    data = _canonical_bytes(row) + b"\n"
    if len(data) > _MAX_EXECUTION_RECEIPT_ROW_BYTES:
        raise ValueError("repair execution receipt row is too large")
    original_size = _report_storage._validate_published_leaf_identity(
        directory_fd,
        _EXECUTION_RECEIPT_FILE_NAME,
        descriptor,
        "repair execution receipt",
        expected_mode=0o600,
    ).st_size
    if original_size + len(data) > _MAX_EXECUTION_RECEIPT_BYTES:
        raise ValueError("repair execution receipt would exceed the size limit")
    durable = False
    try:
        written = os.write(descriptor, data)
        if written != len(data):
            raise OSError("repair execution receipt append was incomplete")
        os.fsync(descriptor)
        _report_storage._validate_published_leaf_identity(
            directory_fd,
            _EXECUTION_RECEIPT_FILE_NAME,
            descriptor,
            "repair execution receipt",
            expected_mode=0o600,
        )
        durable = True
    finally:
        if not durable:
            try:
                os.ftruncate(descriptor, original_size)
                os.fsync(descriptor)
            except OSError:
                pass


def _append_claim_row_at(
    directory_fd: int,
    descriptor: int,
    row: dict[str, object],
) -> None:
    data = _canonical_bytes(row) + b"\n"
    if len(data) > _MAX_EXECUTION_RECEIPT_ROW_BYTES:
        raise ValueError("repair execution claim row is too large")
    original_size = _report_storage._validate_published_leaf_identity(
        directory_fd,
        _EXECUTION_CLAIM_FILE_NAME,
        descriptor,
        "repair execution claims",
        expected_mode=0o600,
    ).st_size
    if original_size + len(data) > _MAX_EXECUTION_RECEIPT_BYTES:
        raise ValueError("repair execution claims would exceed the size limit")
    durable = False
    try:
        written = os.write(descriptor, data)
        if written != len(data):
            raise OSError("repair execution claim append was incomplete")
        os.fsync(descriptor)
        _report_storage._validate_published_leaf_identity(
            directory_fd,
            _EXECUTION_CLAIM_FILE_NAME,
            descriptor,
            "repair execution claims",
            expected_mode=0o600,
        )
        durable = True
    finally:
        if not durable:
            try:
                os.ftruncate(descriptor, original_size)
                os.fsync(descriptor)
            except OSError:
                pass


def _trailing_consecutive_failures(
    plan: CatalogVisibilityRepairPlan,
    terminal_by_job_id: Mapping[str, dict[str, object]],
) -> int:
    """Receipt-changed skips are neutral; repaired success resets the breaker."""
    consecutive = 0
    for item in plan.items:
        row = terminal_by_job_id.get(item.receipt.job_id)
        if row is None:
            break
        if row["outcome"] == "repaired":
            consecutive = 0
        elif row["outcome"] == "failed":
            consecutive += 1
    return consecutive


def _classify_unresolved_visibility(
    result: object,
    item: RepairPlanItem,
) -> tuple[bool, bool]:
    if (
        not isinstance(result, PublicVisibilityResult)
        or result.canonical_code != item.receipt.movie_code
        or result.expected_subtitle_id != item.receipt.subtitle_id
    ):
        return False, False
    if (
        result.status is VisibilityStatus.VISIBLE
        and result.reason_code is None
        and result.observed_subtitle_ids.count(item.receipt.subtitle_id) == 1
    ):
        return True, False
    if result.status in {VisibilityStatus.MISSING, VisibilityStatus.NOT_FOUND}:
        return False, True
    return False, False


def _current_receipt_in_write_transaction(
    store: JobStore,
    connection: sqlite3.Connection,
    item: RepairPlanItem,
) -> PublicationReceiptSnapshot | None:
    current = store.get_job(item.receipt.job_id, conn=connection)
    try:
        return (
            AuditCandidateSnapshot.from_job(current).validated_receipt()
            if current is not None and current.status == JobStatus.ENGLISH_SRT_READY
            else None
        )
    except (TypeError, ValueError):
        return None


def execute_catalog_visibility_repair(
    store: JobStore,
    plan: CatalogVisibilityRepairPlan,
    *,
    sync_client: CatalogSyncClient,
    output_dir: Path,
    execute: bool = False,
    confirm_report_sha256: str | None = None,
    consecutive_failure_limit: int = 3,
    recover_unresolved_claims: bool = False,
) -> RepairExecutionResult:
    if not isinstance(execute, bool):
        raise ValueError("execute must be a boolean")
    if not isinstance(recover_unresolved_claims, bool):
        raise ValueError("recover_unresolved_claims must be a boolean")
    if recover_unresolved_claims and not execute:
        raise ValueError("recover_unresolved_claims requires execute=True")
    if (
        not isinstance(consecutive_failure_limit, int)
        or isinstance(consecutive_failure_limit, bool)
        or consecutive_failure_limit <= 0
    ):
        raise ValueError("consecutive_failure_limit must be a positive integer")
    validated = _validated_execution_plan(store, plan)
    visibility_client = _validate_sync_client_binding(sync_client, validated.api_origin)
    canonical_output_dir, directory_fd = _open_pinned_canonical_execution_directory(
        Path(output_dir),
        validated,
    )
    receipt_path = _execution_receipt_path(canonical_output_dir)
    if not execute:
        os.close(directory_fd)
        return RepairExecutionResult(
            action="dry_run",
            repaired=(),
            failed=(),
            skipped_receipt_changed=(),
            stopped_reason=None,
            receipt_path=receipt_path,
        )
    if (
        not isinstance(confirm_report_sha256, str)
        or not hmac.compare_digest(confirm_report_sha256, validated.report_sha256)
    ):
        os.close(directory_fd)
        raise ValueError("confirm_report_sha256 mismatch")

    with _EXECUTION_PROCESS_LOCK:
        # Lock order is process -> canonical ledger directory -> SQLite writer.
        # Store publication writers only take the SQLite lock, so there is no inverse.
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_EX)
            _validate_pinned_directory_path(canonical_output_dir, directory_fd)
            descriptor, created = _open_execution_receipt(directory_fd)
            claims_descriptor: int | None = None
            stopped_reason: str | None = None
            items_by_job_id = {
                item.receipt.job_id: item for item in validated.items
            }
            try:
                if created:
                    _report_storage._fsync_directory_fd(directory_fd)
                _report_storage._validate_published_leaf_identity(
                    directory_fd,
                    _EXECUTION_RECEIPT_FILE_NAME,
                    descriptor,
                    "repair execution receipt",
                    expected_mode=0o600,
                )
                existing = _load_execution_rows_from_fd(
                    descriptor,
                    validated,
                    items_by_job_id,
                )
                claims_descriptor, claims_created = _open_execution_claims(directory_fd)
                if claims_created:
                    _report_storage._fsync_directory_fd(directory_fd)
                _report_storage._validate_published_leaf_identity(
                    directory_fd,
                    _EXECUTION_CLAIM_FILE_NAME,
                    claims_descriptor,
                    "repair execution claims",
                    expected_mode=0o600,
                )
                claims = _load_claim_rows_from_fd(
                    claims_descriptor,
                    validated,
                    items_by_job_id,
                )
                terminal_by_job_id = {
                    row["job_id"]: row for row in existing
                }
                claim_by_job_id = {row["job_id"]: row for row in claims}
                unresolved_job_ids = set(claim_by_job_id) - set(terminal_by_job_id)
                for item in validated.items:
                    job_id = item.receipt.job_id
                    if job_id not in unresolved_job_ids:
                        continue
                    terminal_row: dict[str, object] | None = None
                    with store.connection() as connection:
                        connection.execute("BEGIN IMMEDIATE")
                        current_receipt = _current_receipt_in_write_transaction(
                            store,
                            connection,
                            item,
                        )
                        if current_receipt != item.receipt:
                            terminal_row = _execution_row(
                                item,
                                report_sha256=validated.report_sha256,
                                outcome="skipped_receipt_changed",
                                reason_code="receipt_changed",
                            )
                        else:
                            try:
                                probe = visibility_client.check(
                                    item.receipt.movie_code,
                                    item.receipt.subtitle_id,
                                    item.receipt.content_sha256,
                                )
                            except Exception:
                                exact_visible, definitely_not_exact = False, False
                            else:
                                exact_visible, definitely_not_exact = (
                                    _classify_unresolved_visibility(probe, item)
                                )
                            if exact_visible:
                                terminal_row = _execution_row(
                                    item,
                                    report_sha256=validated.report_sha256,
                                    outcome="repaired",
                                    reason_code=None,
                                )
                            elif recover_unresolved_claims and definitely_not_exact:
                                try:
                                    sync_client.sync(
                                        item.receipt.movie_code,
                                        expected_subtitle_id=item.receipt.subtitle_id,
                                        expected_content_sha256=(
                                            item.receipt.content_sha256
                                        ),
                                    )
                                except CatalogSyncError as exc:
                                    terminal_row = _execution_row(
                                        item,
                                        report_sha256=validated.report_sha256,
                                        outcome="failed",
                                        reason_code=exc.reason_code,
                                    )
                                else:
                                    terminal_row = _execution_row(
                                        item,
                                        report_sha256=validated.report_sha256,
                                        outcome="repaired",
                                        reason_code=None,
                                    )
                            else:
                                stopped_reason = "unresolved_claim"
                        if terminal_row is not None:
                            _append_execution_row_at(
                                directory_fd,
                                descriptor,
                                terminal_row,
                            )
                    if terminal_row is None:
                        break
                    existing += (terminal_row,)
                    terminal_by_job_id[job_id] = terminal_row
                    if terminal_row["reason_code"] == "catalog_auth_failed":
                        stopped_reason = "catalog_auth_failed"
                        break

                consecutive_failures = _trailing_consecutive_failures(
                    validated,
                    terminal_by_job_id,
                )
                if (
                    stopped_reason is None
                    and consecutive_failures >= consecutive_failure_limit
                ):
                    stopped_reason = "consecutive_remote_failures"

                for item in (validated.items if stopped_reason is None else ()):
                    job_id = item.receipt.job_id
                    if job_id in terminal_by_job_id:
                        continue
                    with store.connection() as connection:
                        connection.execute("BEGIN IMMEDIATE")
                        current_receipt = _current_receipt_in_write_transaction(
                            store,
                            connection,
                            item,
                        )
                        if current_receipt != item.receipt:
                            row = _execution_row(
                                item,
                                report_sha256=validated.report_sha256,
                                outcome="skipped_receipt_changed",
                                reason_code="receipt_changed",
                            )
                            _append_execution_row_at(directory_fd, descriptor, row)
                        else:
                            if job_id not in claim_by_job_id:
                                claim = _claim_row(item, validated)
                                _append_claim_row_at(
                                    directory_fd,
                                    claims_descriptor,
                                    claim,
                                )
                                claims += (claim,)
                                claim_by_job_id[job_id] = claim
                            try:
                                sync_client.sync(
                                    item.receipt.movie_code,
                                    expected_subtitle_id=item.receipt.subtitle_id,
                                    expected_content_sha256=item.receipt.content_sha256,
                                )
                            except CatalogSyncError as exc:
                                consecutive_failures += 1
                                row = _execution_row(
                                    item,
                                    report_sha256=validated.report_sha256,
                                    outcome="failed",
                                    reason_code=exc.reason_code,
                                )
                            else:
                                consecutive_failures = 0
                                row = _execution_row(
                                    item,
                                    report_sha256=validated.report_sha256,
                                    outcome="repaired",
                                    reason_code=None,
                                )
                            _append_execution_row_at(directory_fd, descriptor, row)
                        existing += (row,)
                        terminal_by_job_id[job_id] = row
                    if row["reason_code"] == "catalog_auth_failed":
                        stopped_reason = "catalog_auth_failed"
                        break
                    if consecutive_failures >= consecutive_failure_limit:
                        stopped_reason = "consecutive_remote_failures"
                        break
            finally:
                if claims_descriptor is not None:
                    os.close(claims_descriptor)
                os.close(descriptor)
        finally:
            os.close(directory_fd)

    terminal_by_job_id = {row["job_id"]: row for row in existing}
    return RepairExecutionResult(
        action="executed",
        repaired=tuple(
            item.receipt.movie_code
            for item in validated.items
            if terminal_by_job_id.get(item.receipt.job_id, {}).get("outcome")
            == "repaired"
        ),
        failed=tuple(
            item.receipt.movie_code
            for item in validated.items
            if terminal_by_job_id.get(item.receipt.job_id, {}).get("outcome")
            == "failed"
        ),
        skipped_receipt_changed=tuple(
            item.receipt.movie_code
            for item in validated.items
            if terminal_by_job_id.get(item.receipt.job_id, {}).get("outcome")
            == "skipped_receipt_changed"
        ),
        stopped_reason=stopped_reason,
        receipt_path=receipt_path,
    )
