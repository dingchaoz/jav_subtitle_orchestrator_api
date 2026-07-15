from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
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
) -> dict[str, object]:
    row = _exact_dict(value, _EXECUTION_RECEIPT_FIELDS, "repair execution receipt")
    report_sha256 = row["report_sha256"]
    if (
        not isinstance(report_sha256, str)
        or not hmac.compare_digest(report_sha256, plan.report_sha256)
    ):
        raise ValueError("repair execution receipt belongs to a different report")
    matching = [
        item
        for item in plan.items
        if item.receipt.job_id == row["job_id"]
    ]
    if len(matching) != 1:
        raise ValueError("repair execution receipt item is not in the plan")
    item = matching[0]
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
        )
        job_id = row["job_id"]
        assert isinstance(job_id, str)
        if job_id in seen:
            raise ValueError("repair execution receipt contains a duplicate job")
        seen.add(job_id)
        rows.append(row)
    if len(rows) > len(plan.items):
        raise ValueError("repair execution receipt contains too many rows")
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
    matching = [item for item in plan.items if item.receipt.job_id == row["job_id"]]
    if len(matching) != 1:
        raise ValueError("repair execution claim item is not in the plan")
    item = matching[0]
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
        )
        job_id = row["job_id"]
        assert isinstance(job_id, str)
        if job_id in seen:
            raise ValueError("repair execution claims contain a duplicate job")
        seen.add(job_id)
        rows.append(row)
    if len(rows) > len(plan.items):
        raise ValueError("repair execution claims contain too many rows")
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


def execute_catalog_visibility_repair(
    store: JobStore,
    plan: CatalogVisibilityRepairPlan,
    *,
    sync_client: CatalogSyncClient,
    output_dir: Path,
    execute: bool = False,
    confirm_report_sha256: str | None = None,
    consecutive_failure_limit: int = 3,
) -> RepairExecutionResult:
    if not isinstance(execute, bool):
        raise ValueError("execute must be a boolean")
    if (
        not isinstance(consecutive_failure_limit, int)
        or isinstance(consecutive_failure_limit, bool)
        or consecutive_failure_limit <= 0
    ):
        raise ValueError("consecutive_failure_limit must be a positive integer")
    validated = _validated_execution_plan(store, plan)
    receipt_path = _execution_receipt_path(Path(output_dir))
    if not execute:
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
        raise ValueError("confirm_report_sha256 mismatch")

    with _EXECUTION_PROCESS_LOCK:
        with _report_storage._pinned_directory(output_dir, create=True) as directory_fd:
            fcntl.flock(directory_fd, fcntl.LOCK_EX)
            descriptor, created = _open_execution_receipt(directory_fd)
            claims_descriptor: int | None = None
            stopped_reason: str | None = None
            consecutive_failures = 0
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
                existing = _load_execution_rows_from_fd(descriptor, validated)
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
                claims = _load_claim_rows_from_fd(claims_descriptor, validated)
                completed = {row["job_id"] for row in existing}
                claimed = {row["job_id"] for row in claims}
                unresolved = claimed - completed
                if unresolved:
                    stopped_reason = "unresolved_claim"
                for item in (validated.items if not unresolved else ()):
                    job_id = item.receipt.job_id
                    if job_id in completed:
                        continue
                    current = store.get_job(item.receipt.job_id)
                    try:
                        current_receipt = (
                            AuditCandidateSnapshot.from_job(current).validated_receipt()
                            if current is not None
                            and current.status == JobStatus.ENGLISH_SRT_READY
                            else None
                        )
                    except (TypeError, ValueError):
                        current_receipt = None
                    if current_receipt != item.receipt:
                        row = _execution_row(
                            item,
                            report_sha256=validated.report_sha256,
                            outcome="skipped_receipt_changed",
                            reason_code="receipt_changed",
                        )
                        _append_execution_row_at(directory_fd, descriptor, row)
                        existing += (row,)
                        continue
                    claim = _claim_row(item, validated)
                    _append_claim_row_at(
                        directory_fd,
                        claims_descriptor,
                        claim,
                    )
                    claims += (claim,)
                    claimed.add(job_id)
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

    return RepairExecutionResult(
        action="executed",
        repaired=tuple(
            item.receipt.movie_code
            for item in validated.items
            if any(
                row["job_id"] == item.receipt.job_id
                and row["outcome"] == "repaired"
                for row in existing
            )
        ),
        failed=tuple(
            item.receipt.movie_code
            for item in validated.items
            if any(
                row["job_id"] == item.receipt.job_id
                and row["outcome"] == "failed"
                for row in existing
            )
        ),
        skipped_receipt_changed=tuple(
            item.receipt.movie_code
            for item in validated.items
            if any(
                row["job_id"] == item.receipt.job_id
                and row["outcome"] == "skipped_receipt_changed"
                for row in existing
            )
        ),
        stopped_reason=stopped_reason,
        receipt_path=receipt_path,
    )
