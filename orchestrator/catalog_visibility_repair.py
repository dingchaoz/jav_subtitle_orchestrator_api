from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from orchestrator.catalog_visibility import (
    AuditCandidateSnapshot,
    PublicationReceiptSnapshot,
    VisibilityStatus,
    normalize_catalog_api_origin,
)
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
