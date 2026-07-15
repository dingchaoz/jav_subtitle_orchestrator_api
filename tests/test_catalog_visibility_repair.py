from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from dataclasses import asdict, FrozenInstanceError, replace
from pathlib import Path

import pytest

from orchestrator.catalog_visibility import AuditCandidateSnapshot
from orchestrator.catalog_visibility_report import (
    AuditReport,
    AuditFinding,
    append_audit_finding,
    audit_report_sha256,
    create_audit_manifest,
    finalize_audit_report,
    write_audit_manifest,
)
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


API_ORIGIN = "https://javsubtitle.example"


def _store(sqlite_path: Path, mac_jobs_root: Path) -> JobStore:
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    return store


def _ready_job(store: JobStore, code: str, index: int):
    job = store.submit_job(code, priority=100, force=False).job
    assert job is not None
    canonical = code.lower()
    subtitle_id = f"00000000-0000-4000-8000-{index:012d}"
    movie_uuid = f"10000000-0000-4000-8000-{index:012d}"
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, updated_at = ?, catalog_movie_uuid = ?, "
            "metadata_status = ?, metadata_source = ?, published_subtitle_id = ?, "
            "published_storage_path = ?, published_content_sha256 = ?, "
            "published_file_size = ? WHERE id = ?",
            (
                JobStatus.ENGLISH_SRT_READY.value,
                f"2026-07-15T10:00:{index:02d}+00:00",
                movie_uuid,
                "complete",
                "public",
                subtitle_id,
                f"{canonical.split('-', 1)[0]}/{canonical}/{canonical}-English_AI.srt",
                f"{index:x}" * 64,
                300 + index,
                job.id,
            ),
        )
    ready = store.get_job(job.id)
    assert ready is not None
    return ready


def _write_report(
    output_dir: Path,
    store: JobStore,
    candidates_and_statuses: list[tuple[AuditCandidateSnapshot, str]],
):
    manifest = create_audit_manifest(
        api_origin=API_ORIGIN,
        database_path=store.db_path,
        candidates=tuple(candidate for candidate, _ in candidates_and_statuses),
        selection={"allowlist": None, "limit": None},
    )
    statuses = {
        candidate.job_id: status for candidate, status in candidates_and_statuses
    }
    write_audit_manifest(output_dir, manifest)
    for candidate in manifest.candidates:
        status = statuses[candidate.job_id]
        append_audit_finding(
            output_dir,
            AuditFinding(
                candidate=candidate,
                status=status,
                reason_code=None if status == "visible" else "public_visibility_mismatch",
                observed_subtitle_ids=(),
            ),
        )
    return finalize_audit_report(output_dir)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()


def _one_missing_report(tmp_path: Path, store: JobStore, job):
    report_dir = tmp_path / "report"
    report = _write_report(
        report_dir,
        store,
        [(AuditCandidateSnapshot.from_job(job), "missing")],
    )
    return report_dir / "audit-report.json", report


def _rewrite_with_digest(path: Path, payload: dict[str, object], digest_field: str) -> None:
    unsigned = dict(payload)
    unsigned.pop(digest_field, None)
    payload[digest_field] = hashlib.sha256(_canonical_bytes(unsigned)).hexdigest()
    path.write_bytes(_canonical_bytes(payload))


def test_catalog_visibility_repair_module_exposes_public_api():
    from orchestrator.catalog_visibility_repair import (
        REPAIR_PLAN_SCHEMA_VERSION,
        CatalogVisibilityRepairPlan,
        RepairPlanItem,
        load_catalog_visibility_repair_plan,
        plan_catalog_visibility_repair,
    )

    assert REPAIR_PLAN_SCHEMA_VERSION == 1
    assert CatalogVisibilityRepairPlan is not None
    assert RepairPlanItem is not None
    assert callable(load_catalog_visibility_repair_plan)
    assert callable(plan_catalog_visibility_repair)


@pytest.mark.parametrize("non_finite", [float("nan"), float("inf"), float("-inf")])
def test_private_json_artifact_rejects_non_finite_values_before_publish(
    tmp_path: Path,
    non_finite: float,
):
    from orchestrator.catalog_visibility_report import (
        write_private_json_artifact,
    )

    artifact_path = tmp_path / "artifact.json"

    with pytest.raises(ValueError, match="test artifact") as raised:
        write_private_json_artifact(
            artifact_path,
            {"secret": "SENTINELVALUE", "value": non_finite},
            label="test artifact",
            limit=1024,
        )

    assert "SENTINELVALUE" not in str(raised.value)
    assert not artifact_path.exists()


def test_plan_includes_only_missing_and_not_found_in_report_order_and_writes_digest(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    statuses = [
        "missing",
        "visible",
        "not_found",
        "fetch_failed",
        "response_invalid",
        "invalid_receipt",
    ]
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in range(1, 7)]
    snapshots = [AuditCandidateSnapshot.from_job(job) for job in jobs]
    snapshots[-1] = replace(
        snapshots[-1],
        movie_uuid=None,
        metadata_status=None,
        metadata_source=None,
        subtitle_id=None,
        storage_path=None,
        content_sha256=None,
        file_size=None,
    )
    report_dir = tmp_path / "report"
    report = _write_report(
        report_dir,
        store,
        [
            (snapshot, status)
            for snapshot, status in zip(snapshots, statuses, strict=True)
        ],
    )

    plan = plan_catalog_visibility_repair(
        store,
        report_dir / "audit-report.json",
        expected_api_origin=f"{API_ORIGIN}/",
        output_dir=tmp_path / "plan",
    )

    assert [item.receipt.job_id for item in plan.items] == [jobs[0].id, jobs[2].id]
    assert [item.starting_status for item in plan.items] == ["missing", "not_found"]
    assert plan.items[0].receipt == AuditCandidateSnapshot.from_job(jobs[0]).validated_receipt()
    assert plan.report_path == report_dir / "audit-report.json"
    assert plan.plan_path == tmp_path / "plan" / "repair-plan.json"
    assert plan.api_origin == API_ORIGIN
    assert plan.report_sha256 == report.report_sha256
    assert plan.skipped == {
        "visible": 1,
        "fetch_failed": 1,
        "response_invalid": 1,
        "invalid_receipt": 1,
    }
    with pytest.raises(TypeError):
        plan.skipped["visible"] = 9  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        plan.items[0].starting_status = "not_found"  # type: ignore[misc]

    raw = (tmp_path / "plan" / "repair-plan.json").read_bytes()
    payload = json.loads(raw)
    assert stat.S_IMODE((tmp_path / "plan" / "repair-plan.json").stat().st_mode) == 0o600
    assert set(payload) == {
        "schema_version",
        "api_origin",
        "report_sha256",
        "items",
        "skipped",
        "plan_sha256",
    }
    assert payload["items"] == [
        {"receipt": asdict(item.receipt), "starting_status": item.starting_status}
        for item in plan.items
    ]
    unsigned = dict(payload)
    del unsigned["plan_sha256"]
    assert plan.plan_sha256 == hashlib.sha256(_canonical_bytes(unsigned)).hexdigest()
    assert payload["plan_sha256"] == plan.plan_sha256


@pytest.mark.parametrize(
    ("assignment", "value"),
    [
        ("status", JobStatus.FAILED.value),
        ("catalog_movie_uuid", None),
        ("catalog_movie_uuid", "20000000-0000-4000-8000-000000000099"),
        ("metadata_status", "partial"),
        ("metadata_source", "local"),
        ("published_subtitle_id", "30000000-0000-4000-8000-000000000099"),
        ("published_storage_path", "wrong/path.srt"),
        ("published_content_sha256", "f" * 64),
        ("published_file_size", 999),
        ("normalized_movie_number", "xyz-999"),
        ("updated_at", "2026-07-15T11:00:00+00:00"),
    ],
)
def test_plan_excludes_nonready_invalid_or_changed_current_receipt(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    assignment: str,
    value: object,
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)
    with store.connection() as conn:
        conn.execute(f"UPDATE jobs SET {assignment} = ? WHERE id = ?", (value, job.id))

    plan = plan_catalog_visibility_repair(
        store,
        report_path,
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / "plan",
    )

    assert plan.items == ()
    assert plan.skipped == {"receipt_changed": 1}


def test_plan_excludes_deleted_current_job_as_receipt_changed(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)
    with store.connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = ?", (job.id,))

    plan = plan_catalog_visibility_repair(
        store,
        report_path,
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / "plan",
    )

    assert plan.items == ()
    assert plan.skipped == {"receipt_changed": 1}


def test_plan_rejects_wrong_origin_and_different_database_before_writing(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)

    with pytest.raises(ValueError, match="origin"):
        plan_catalog_visibility_repair(
            store,
            report_path,
            expected_api_origin="https://other.example",
            output_dir=tmp_path / "wrong-origin",
        )
    other_store = _store(tmp_path / "other.sqlite3", tmp_path / "other-jobs")
    with pytest.raises(ValueError, match="database"):
        plan_catalog_visibility_repair(
            other_store,
            report_path,
            expected_api_origin=API_ORIGIN,
            output_dir=tmp_path / "wrong-database",
        )

    assert not (tmp_path / "wrong-origin").exists()
    assert not (tmp_path / "wrong-database").exists()


@pytest.mark.parametrize("tamper", ["digest", "receipt", "schema", "incomplete"])
def test_plan_rejects_tampered_or_incomplete_report(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    tamper: str,
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)
    payload = json.loads(report_path.read_bytes())
    if tamper == "digest":
        payload["report_sha256"] = "0" * 64
        report_path.write_bytes(_canonical_bytes(payload))
    elif tamper == "receipt":
        payload["findings"][0]["candidate"]["job_updated_at"] = (
            "2026-07-15T11:00:00+00:00"
        )
        _rewrite_with_digest(report_path, payload, "report_sha256")
    elif tamper == "schema":
        payload["manifest"]["schema_version"] = 2
        _rewrite_with_digest(report_path, payload, "report_sha256")
    else:
        payload["complete"] = False
        _rewrite_with_digest(report_path, payload, "report_sha256")

    with pytest.raises((TypeError, ValueError)):
        plan_catalog_visibility_repair(
            store,
            report_path,
            expected_api_origin=API_ORIGIN,
            output_dir=tmp_path / "plan",
        )

    assert not (tmp_path / "plan").exists()


def test_plan_rejects_checkpoint_instead_of_final_report(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)

    with pytest.raises(ValueError):
        plan_catalog_visibility_repair(
            store,
            report_path.parent / "audit-findings.jsonl",
            expected_api_origin=API_ORIGIN,
            output_dir=tmp_path / "plan",
        )

    assert not (tmp_path / "plan").exists()


def test_plan_round_trip_has_stable_digest_and_idempotent_same_content(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import (
        load_catalog_visibility_repair_plan,
        plan_catalog_visibility_repair,
    )

    store = _store(sqlite_path, mac_jobs_root)
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in (1, 2)]
    report_dir = tmp_path / "report"
    _write_report(
        report_dir,
        store,
        [
            (AuditCandidateSnapshot.from_job(jobs[0]), "missing"),
            (AuditCandidateSnapshot.from_job(jobs[1]), "not_found"),
        ],
    )
    arguments = {
        "expected_api_origin": API_ORIGIN,
        "output_dir": tmp_path / "plan",
    }

    first = plan_catalog_visibility_repair(
        store, report_dir / "audit-report.json", **arguments
    )
    original_bytes = first.plan_path.read_bytes()
    second = plan_catalog_visibility_repair(
        store, report_dir / "audit-report.json", **arguments
    )
    loaded = load_catalog_visibility_repair_plan(
        first.plan_path,
        report_path=report_dir / "audit-report.json",
    )

    assert second == first
    assert loaded == first
    assert first.plan_path.read_bytes() == original_bytes


@pytest.mark.parametrize(
    "tamper",
    [
        "digest",
        "field",
        "extra",
        "schema",
        "status",
        "receipt",
        "order",
        "duplicate_item",
        "skipped",
    ],
)
def test_plan_loader_rejects_tampered_payload(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    tamper: str,
):
    from orchestrator.catalog_visibility_repair import (
        load_catalog_visibility_repair_plan,
        plan_catalog_visibility_repair,
    )

    store = _store(sqlite_path, mac_jobs_root)
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in (1, 2)]
    report_dir = tmp_path / "report"
    _write_report(
        report_dir,
        store,
        [
            (AuditCandidateSnapshot.from_job(jobs[0]), "missing"),
            (AuditCandidateSnapshot.from_job(jobs[1]), "not_found"),
        ],
    )
    plan = plan_catalog_visibility_repair(
        store,
        report_dir / "audit-report.json",
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / "plan",
    )
    payload = json.loads(plan.plan_path.read_bytes())
    if tamper == "digest":
        payload["plan_sha256"] = "0" * 64
        plan.plan_path.write_bytes(_canonical_bytes(payload))
    else:
        if tamper == "field":
            payload["api_origin"] = "https://other.example"
        elif tamper == "extra":
            payload["credential"] = "SENTINELVALUE"
        elif tamper == "schema":
            payload["schema_version"] = 2
        elif tamper == "status":
            payload["items"][0]["starting_status"] = "visible"
        elif tamper == "receipt":
            payload["items"][0]["receipt"]["job_updated_at"] = (
                "2026-07-15T11:00:00+00:00"
            )
        elif tamper == "order":
            payload["items"].reverse()
        elif tamper == "duplicate_item":
            payload["items"][1] = payload["items"][0]
        else:
            payload["skipped"] = {"receipt_changed": 1}
        _rewrite_with_digest(plan.plan_path, payload, "plan_sha256")

    with pytest.raises((TypeError, ValueError)):
        load_catalog_visibility_repair_plan(
            plan.plan_path,
            report_path=report_dir / "audit-report.json",
        )


def test_plan_loader_rejects_duplicate_json_key(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import (
        load_catalog_visibility_repair_plan,
        plan_catalog_visibility_repair,
    )

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)
    plan = plan_catalog_visibility_repair(
        store,
        report_path,
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / "plan",
    )
    serialized = plan.plan_path.read_text().replace(
        '"schema_version":1',
        '"schema_version":1,"schema_version":1',
    )
    plan.plan_path.write_text(serialized)

    with pytest.raises(ValueError):
        load_catalog_visibility_repair_plan(plan.plan_path, report_path=report_path)


@pytest.mark.parametrize("duplicate_identity", ["job_id", "movie_code"])
def test_plan_rechecks_duplicate_report_identity_defensively(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    duplicate_identity: str,
):
    import orchestrator.catalog_visibility_repair as repair_module

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    _, report = _one_missing_report(tmp_path, store, job)
    first_candidate = report.findings[0].candidate
    duplicate_candidate = (
        first_candidate
        if duplicate_identity == "job_id"
        else replace(first_candidate, job_id="other-job")
    )
    duplicate_finding = replace(report.findings[0], candidate=duplicate_candidate)
    forged_unsigned = AuditReport(
        manifest=replace(
            report.manifest,
            candidates=(first_candidate, duplicate_candidate),
        ),
        findings=(report.findings[0], duplicate_finding),
        counts=dict(report.counts),
        complete=True,
        report_sha256="",
    )
    forged = replace(
        forged_unsigned,
        report_sha256=audit_report_sha256(forged_unsigned),
    )
    monkeypatch.setattr(repair_module, "load_audit_report", lambda path: forged)
    monkeypatch.setattr(
        repair_module,
        "validate_audit_resume_context",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(ValueError, match="duplicate"):
        repair_module.plan_catalog_visibility_repair(
            store,
            tmp_path / "unused-report-path",
            expected_api_origin=API_ORIGIN,
            output_dir=tmp_path / "plan",
        )

    assert not (tmp_path / "plan").exists()


def test_planning_is_dry_run_uses_only_get_job_and_leaves_database_unchanged(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import requests

    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in (1, 2)]
    report_dir = tmp_path / "report"
    _write_report(
        report_dir,
        store,
        [
            (AuditCandidateSnapshot.from_job(jobs[0]), "missing"),
            (AuditCandidateSnapshot.from_job(jobs[1]), "visible"),
        ],
    )
    with store.connection() as conn:
        before = [tuple(row) for row in conn.execute("SELECT * FROM jobs ORDER BY id")]
    get_job_calls: list[str] = []
    original_get_job = store.get_job

    def recording_get_job(job_id: str, *args: object, **kwargs: object):
        get_job_calls.append(job_id)
        return original_get_job(job_id, *args, **kwargs)

    def reject_network(*args: object, **kwargs: object):
        raise AssertionError("repair planning must not use the network")

    monkeypatch.setattr(store, "get_job", recording_get_job)
    monkeypatch.setattr(requests.Session, "request", reject_network)

    plan_catalog_visibility_repair(
        store,
        report_dir / "audit-report.json",
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / "plan",
    )

    with store.connection() as conn:
        after = [tuple(row) for row in conn.execute("SELECT * FROM jobs ORDER BY id")]
    assert get_job_calls == [jobs[0].id]
    assert after == before


def test_multi_item_planning_uses_one_cohesive_shared_read_snapshot(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in (1, 2)]
    report_dir = tmp_path / "report"
    _write_report(
        report_dir,
        store,
        [
            (AuditCandidateSnapshot.from_job(jobs[0]), "missing"),
            (AuditCandidateSnapshot.from_job(jobs[1]), "not_found"),
        ],
    )
    original_get_job = store.get_job
    observed_connections: list[sqlite3.Connection | None] = []
    changed_second_row = False

    def get_job_and_change_second_row(
        job_id: str,
        conn: sqlite3.Connection | None = None,
    ):
        nonlocal changed_second_row
        observed_connections.append(conn)
        current = original_get_job(job_id, conn=conn)
        if not changed_second_row:
            changed_second_row = True
            writer = sqlite3.connect(store.db_path)
            try:
                with writer:
                    writer.execute(
                        "UPDATE jobs SET updated_at = ? WHERE id = ?",
                        ("2026-07-15T11:00:00+00:00", jobs[1].id),
                    )
            finally:
                writer.close()
        return current

    monkeypatch.setattr(store, "get_job", get_job_and_change_second_row)

    plan = plan_catalog_visibility_repair(
        store,
        report_dir / "audit-report.json",
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / "plan",
    )

    assert [item.receipt.job_id for item in plan.items] == [job.id for job in jobs]
    assert len(observed_connections) == 2
    assert observed_connections[0] is not None
    assert observed_connections[1] is observed_connections[0]


def test_plan_serialization_omits_raw_paths_credentials_and_exception_text(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    sentinel = "SENTINELVALUE-never-serialize"
    monkeypatch.setenv("ADMIN_TOKEN", sentinel)
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", sentinel)
    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_dir = tmp_path / f"report-{sentinel}"
    _write_report(
        report_dir,
        store,
        [(AuditCandidateSnapshot.from_job(job), "missing")],
    )

    plan = plan_catalog_visibility_repair(
        store,
        report_dir / "audit-report.json",
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / f"plan-{sentinel}",
    )

    serialized = plan.plan_path.read_text()
    assert sentinel not in serialized
    assert str(store.db_path.resolve()) not in serialized
    assert str(report_dir.resolve()) not in serialized
    assert "signed_url" not in serialized
    assert "00:00:00,000 -->" not in serialized


def test_plan_constructor_defensively_copies_items_and_skipped():
    from orchestrator.catalog_visibility_repair import (
        CatalogVisibilityRepairPlan,
        RepairPlanItem,
    )

    receipt = AuditCandidateSnapshot(
        job_id="job-1",
        movie_code="abc-001",
        movie_uuid="10000000-0000-4000-8000-000000000001",
        metadata_status="complete",
        metadata_source="public",
        subtitle_id="00000000-0000-4000-8000-000000000001",
        storage_path="abc/abc-001/abc-001-English_AI.srt",
        content_sha256="1" * 64,
        file_size=301,
        job_updated_at="2026-07-15T10:00:01+00:00",
    ).validated_receipt()
    source_items = [RepairPlanItem(receipt=receipt, starting_status="missing")]
    source_skipped = {"visible": 1}

    plan = CatalogVisibilityRepairPlan(
        report_path=Path("report.json"),
        plan_path=Path("plan.json"),
        report_sha256="a" * 64,
        plan_sha256="b" * 64,
        api_origin=API_ORIGIN,
        items=source_items,  # type: ignore[arg-type]
        skipped=source_skipped,
    )
    source_items.clear()
    source_skipped["visible"] = 99

    assert len(plan.items) == 1
    assert plan.skipped == {"visible": 1}
    with pytest.raises(TypeError):
        plan.skipped["new"] = 1  # type: ignore[index]


def test_changed_plan_is_no_clobber_and_preserves_existing_bytes(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)
    output_dir = tmp_path / "plan"
    first = plan_catalog_visibility_repair(
        store,
        report_path,
        expected_api_origin=API_ORIGIN,
        output_dir=output_dir,
    )
    original = first.plan_path.read_bytes()
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET updated_at = ? WHERE id = ?",
            ("2026-07-15T11:00:00+00:00", job.id),
        )

    with pytest.raises(ValueError, match="differs"):
        plan_catalog_visibility_repair(
            store,
            report_path,
            expected_api_origin=API_ORIGIN,
            output_dir=output_dir,
        )

    assert first.plan_path.read_bytes() == original


def test_plan_rejects_symlinked_output_directory_without_writing_target(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)
    target = tmp_path / "target"
    target.mkdir()
    output_link = tmp_path / "output-link"
    output_link.symlink_to(target, target_is_directory=True)

    with pytest.raises(ValueError):
        plan_catalog_visibility_repair(
            store,
            report_path,
            expected_api_origin=API_ORIGIN,
            output_dir=output_link,
        )

    assert list(target.iterdir()) == []


@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
def test_plan_rejects_linked_plan_leaf_without_changing_target(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    link_kind: str,
):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)
    output_dir = tmp_path / "plan"
    output_dir.mkdir()
    victim = tmp_path / "victim.json"
    victim.write_text("victim-content")
    victim.chmod(0o600)
    plan_path = output_dir / "repair-plan.json"
    if link_kind == "symlink":
        plan_path.symlink_to(victim)
    else:
        os.link(victim, plan_path)

    with pytest.raises(ValueError):
        plan_catalog_visibility_repair(
            store,
            report_path,
            expected_api_origin=API_ORIGIN,
            output_dir=output_dir,
        )

    assert victim.read_text() == "victim-content"


def test_partial_plan_write_failure_leaves_no_published_or_temporary_file(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import orchestrator.catalog_visibility_report as report_module

    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)

    def fail_after_partial_write(descriptor: int, data: bytes) -> None:
        os.write(descriptor, data[: max(1, len(data) // 2)])
        raise OSError("simulated partial write")

    monkeypatch.setattr(report_module, "_write_all_fd", fail_after_partial_write)
    output_dir = tmp_path / "plan"

    with pytest.raises(OSError, match="partial write"):
        plan_catalog_visibility_repair(
            store,
            report_path,
            expected_api_origin=API_ORIGIN,
            output_dir=output_dir,
        )

    assert not (output_dir / "repair-plan.json").exists()
    assert list(output_dir.iterdir()) == []


@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
def test_plan_loader_rejects_linked_plan_leaf(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    link_kind: str,
):
    from orchestrator.catalog_visibility_repair import (
        load_catalog_visibility_repair_plan,
        plan_catalog_visibility_repair,
    )

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    report_path, _ = _one_missing_report(tmp_path, store, job)
    plan = plan_catalog_visibility_repair(
        store,
        report_path,
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / "plan",
    )
    detached = tmp_path / "detached-plan.json"
    detached.write_bytes(plan.plan_path.read_bytes())
    detached.chmod(0o600)
    plan.plan_path.unlink()
    if link_kind == "symlink":
        plan.plan_path.symlink_to(detached)
    else:
        os.link(detached, plan.plan_path)

    with pytest.raises(ValueError):
        load_catalog_visibility_repair_plan(
            plan.plan_path,
            report_path=report_path,
        )
