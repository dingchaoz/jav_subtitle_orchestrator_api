from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, asdict, fields, replace
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


def _one_missing_plan(tmp_path: Path, store: JobStore, job):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    report_path, _ = _one_missing_report(tmp_path, store, job)
    plan = plan_catalog_visibility_repair(
        store,
        report_path,
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / "plan",
    )
    return plan


def _missing_plan_for_jobs(tmp_path: Path, store: JobStore, jobs):
    from orchestrator.catalog_visibility_repair import plan_catalog_visibility_repair

    report_dir = tmp_path / "report"
    _write_report(
        report_dir,
        store,
        [
            (AuditCandidateSnapshot.from_job(job), "missing")
            for job in jobs
        ],
    )
    return plan_catalog_visibility_repair(
        store,
        report_dir / "audit-report.json",
        expected_api_origin=API_ORIGIN,
        output_dir=tmp_path / "plan",
    )


def _assert_no_execution_journals(plan) -> None:
    assert not (plan.plan_path.parent / "repair-execution.jsonl").exists()
    assert not (plan.plan_path.parent / "repair-execution-claims.jsonl").exists()


class _RecordingSyncClient:
    def __init__(self, errors: list[Exception | None] | None = None) -> None:
        self.base_url = API_ORIGIN
        self.public_visibility_verification_enabled = True
        self.public_visibility_client = _RecordingVisibilityClient()
        self.calls: list[tuple[str, str, str]] = []
        self.errors = list(errors or [])

    def sync(
        self,
        movie_code: str,
        *,
        expected_subtitle_id: str,
        expected_content_sha256: str,
    ) -> object:
        self.calls.append(
            (movie_code, expected_subtitle_id, expected_content_sha256)
        )
        if self.errors:
            error = self.errors.pop(0)
            if error is not None:
                raise error
        return object()


class _RecordingVisibilityClient:
    def __init__(self, results: list[object] | None = None) -> None:
        self.base_url = API_ORIGIN
        self.results = list(results or [])
        self.calls: list[tuple[str, str, str]] = []

    def check(
        self,
        movie_code: str,
        expected_subtitle_id: str,
        content_sha256: str,
    ) -> object:
        from orchestrator.catalog_visibility import PublicVisibilityResult, VisibilityStatus

        self.calls.append((movie_code, expected_subtitle_id, content_sha256))
        if self.results:
            result = self.results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        return PublicVisibilityResult(
            VisibilityStatus.MISSING,
            movie_code,
            expected_subtitle_id,
            reason_code="public_visibility_mismatch",
        )


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


def test_execution_result_has_exact_frozen_slotted_shape():
    from orchestrator.catalog_visibility_repair import RepairExecutionResult

    assert [field.name for field in fields(RepairExecutionResult)] == [
        "action",
        "repaired",
        "failed",
        "skipped_receipt_changed",
        "stopped_reason",
        "receipt_path",
    ]
    result = RepairExecutionResult(
        action="dry_run",
        repaired=(),
        failed=(),
        skipped_receipt_changed=(),
        stopped_reason=None,
        receipt_path=Path("receipt.jsonl"),
    )
    with pytest.raises(FrozenInstanceError):
        result.action = "executed"  # type: ignore[misc]
    assert not hasattr(result, "__dict__")


def test_execute_dry_run_is_inert_and_confirmation_mismatch_precedes_artifacts(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import (
        execute_catalog_visibility_repair,
    )

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    output_dir = plan.plan_path.parent

    dry_run = execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=output_dir,
    )

    assert dry_run.action == "dry_run"
    assert dry_run.repaired == ()
    assert client.calls == []
    _assert_no_execution_journals(plan)

    with pytest.raises(ValueError, match="^confirm_report_sha256 mismatch$"):
        execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=output_dir,
            execute=True,
            confirm_report_sha256="0" * 64,
        )
    assert client.calls == []
    _assert_no_execution_journals(plan)


def test_execute_calls_exact_sync_once_and_does_not_change_job_row(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import (
        execute_catalog_visibility_repair,
    )

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    item = plan.items[0]
    client = _RecordingSyncClient()
    with store.connection() as conn:
        before = tuple(conn.execute("SELECT * FROM jobs WHERE id = ?", (job.id,)).fetchone())

    result = execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )

    with store.connection() as conn:
        after = tuple(conn.execute("SELECT * FROM jobs WHERE id = ?", (job.id,)).fetchone())
    assert client.calls == [
        (
            item.receipt.movie_code,
            item.receipt.subtitle_id,
            item.receipt.content_sha256,
        )
    ]
    assert after == before
    assert result.action == "executed"
    assert result.repaired == ("abc-001",)
    assert result.failed == ()
    assert result.receipt_path.is_file()
    row = json.loads(result.receipt_path.read_text())
    assert set(row) == {
        "report_sha256",
        "job_id",
        "movie_code",
        "expected_subtitle_id",
        "starting_status",
        "outcome",
        "reason_code",
        "finished_at",
    }
    assert row["outcome"] == "repaired"
    assert row["reason_code"] is None


@pytest.mark.parametrize("change", ["changed", "deleted", "not_ready", "invalid"])
def test_execute_rechecks_receipt_and_classifies_changed_without_sync(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    change: str,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    with store.connection() as conn:
        if change == "deleted":
            conn.execute("DELETE FROM jobs WHERE id = ?", (job.id,))
        elif change == "not_ready":
            conn.execute(
                "UPDATE jobs SET status = ? WHERE id = ?",
                (JobStatus.FAILED.value, job.id),
            )
        elif change == "invalid":
            conn.execute(
                "UPDATE jobs SET published_subtitle_id = NULL WHERE id = ?",
                (job.id,),
            )
        else:
            conn.execute(
                "UPDATE jobs SET updated_at = ? WHERE id = ?",
                ("2026-07-15T11:00:00+00:00", job.id),
            )
    client = _RecordingSyncClient()

    result = execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )

    assert client.calls == []
    assert result.repaired == ()
    assert result.failed == ()
    assert result.skipped_receipt_changed == ("abc-001",)
    row = json.loads(result.receipt_path.read_text())
    assert row["outcome"] == "skipped_receipt_changed"
    assert row["reason_code"] == "receipt_changed"


def test_auth_failure_stops_immediately_and_records_only_safe_code(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_sync import CatalogSyncError
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in range(1, 4)]
    plan = _missing_plan_for_jobs(tmp_path, store, jobs)
    client = _RecordingSyncClient([CatalogSyncError("catalog_auth_failed")])

    result = execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )

    assert [call[0] for call in client.calls] == ["abc-001"]
    assert result.failed == ("abc-001",)
    assert result.stopped_reason == "catalog_auth_failed"
    serialized = result.receipt_path.read_text()
    assert "catalog_auth_failed" in serialized
    assert str(client.errors) not in serialized


def test_three_consecutive_remote_failures_stop_before_fourth(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_sync import CatalogSyncError
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in range(1, 5)]
    plan = _missing_plan_for_jobs(tmp_path, store, jobs)
    client = _RecordingSyncClient(
        [CatalogSyncError("catalog_fetch_failed") for _ in range(4)]
    )

    result = execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )

    assert [call[0] for call in client.calls] == ["abc-001", "abc-002", "abc-003"]
    assert result.failed == ("abc-001", "abc-002", "abc-003")
    assert result.stopped_reason == "consecutive_remote_failures"


def test_success_resets_consecutive_failure_breaker_and_visibility_failure_is_failure(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_sync import CatalogSyncError
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in range(1, 7)]
    plan = _missing_plan_for_jobs(tmp_path, store, jobs)
    client = _RecordingSyncClient(
        [
            CatalogSyncError("public_visibility_mismatch"),
            CatalogSyncError("catalog_fetch_failed"),
            None,
            CatalogSyncError("catalog_fetch_failed"),
            CatalogSyncError("catalog_fetch_failed"),
            None,
        ]
    )

    result = execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )

    assert len(client.calls) == 6
    assert result.repaired == ("abc-003", "abc-006")
    assert result.failed == ("abc-001", "abc-002", "abc-004", "abc-005")
    assert result.stopped_reason is None
    first = json.loads(result.receipt_path.read_text().splitlines()[0])
    assert first["outcome"] == "failed"
    assert first["reason_code"] == "public_visibility_mismatch"


def test_completed_terminal_receipt_is_skipped_on_resume(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }

    first = execute_catalog_visibility_repair(store, plan, **arguments)
    original = first.receipt_path.read_bytes()
    second = execute_catalog_visibility_repair(store, plan, **arguments)

    assert len(client.calls) == 1
    assert second == first
    assert second.receipt_path.read_bytes() == original


def test_truncated_trailing_receipt_fragment_is_recovered_without_repeat_sync(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }
    first = execute_catalog_visibility_repair(store, plan, **arguments)
    complete = first.receipt_path.read_bytes()
    with first.receipt_path.open("ab") as stream:
        stream.write(b'{"report_sha256":"partial')

    resumed = execute_catalog_visibility_repair(store, plan, **arguments)

    assert len(client.calls) == 1
    assert resumed.repaired == ("abc-001",)
    assert resumed.receipt_path.read_bytes() == complete


@pytest.mark.parametrize(
    "tamper",
    ["cross_plan", "extra", "duplicate_key", "bad_outcome", "duplicate_row"],
)
def test_resume_rejects_tampered_or_conflicting_terminal_receipts(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    tamper: str,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }
    result = execute_catalog_visibility_repair(store, plan, **arguments)
    original = result.receipt_path.read_text()
    row = json.loads(original)
    if tamper == "cross_plan":
        row["report_sha256"] = "0" * 64
        replacement = _canonical_bytes(row) + b"\n"
    elif tamper == "extra":
        row["admin_token"] = "SENTINELVALUE"
        replacement = _canonical_bytes(row) + b"\n"
    elif tamper == "duplicate_key":
        replacement = original.replace(
            '"outcome":"repaired"',
            '"outcome":"repaired","outcome":"repaired"',
        ).encode()
    elif tamper == "bad_outcome":
        row["outcome"] = "repaired"
        row["reason_code"] = "catalog_fetch_failed"
        replacement = _canonical_bytes(row) + b"\n"
    else:
        replacement = original.encode() * 2
    result.receipt_path.write_bytes(replacement)

    with pytest.raises(ValueError, match="repair execution receipt"):
        execute_catalog_visibility_repair(store, plan, **arguments)
    assert len(client.calls) == 1


def test_resume_rejects_terminal_ledger_with_deleted_prefix_row(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in (1, 2)]
    plan = _missing_plan_for_jobs(tmp_path, store, jobs)
    client = _RecordingSyncClient()
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }
    result = execute_catalog_visibility_repair(store, plan, **arguments)
    rows = result.receipt_path.read_text().splitlines(keepends=True)
    result.receipt_path.write_text(rows[1])

    with pytest.raises(ValueError, match="plan prefix"):
        execute_catalog_visibility_repair(store, plan, **arguments)
    assert len(client.calls) == 2


def test_resume_rejects_oversize_receipt_before_sync(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    import orchestrator.catalog_visibility_repair as repair_module

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    output_dir = plan.plan_path.parent
    output_dir.chmod(0o700)
    receipt = output_dir / "repair-execution.jsonl"
    with receipt.open("wb") as stream:
        stream.truncate(repair_module._MAX_EXECUTION_RECEIPT_BYTES + 1)
    receipt.chmod(0o600)
    client = _RecordingSyncClient()

    with pytest.raises(ValueError, match="too large"):
        repair_module.execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=output_dir,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    assert client.calls == []


@pytest.mark.parametrize("attack", ["symlink", "hardlink", "permissions"])
def test_execute_rejects_unsafe_receipt_filesystem_leaf(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    attack: str,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    output_dir = plan.plan_path.parent
    receipt = output_dir / "repair-execution.jsonl"
    victim = tmp_path / "victim"
    victim.write_text("do-not-touch")
    victim.chmod(0o600)
    if attack == "symlink":
        receipt.symlink_to(victim)
    elif attack == "hardlink":
        os.link(victim, receipt)
    else:
        receipt.write_bytes(b"")
        receipt.chmod(0o644)
    client = _RecordingSyncClient()

    with pytest.raises(ValueError):
        execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=output_dir,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    assert client.calls == []
    assert victim.read_text() == "do-not-touch"


def test_persisted_claim_prevents_repeat_sync_after_terminal_append_crash(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import orchestrator.catalog_visibility_repair as repair_module

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    original_append = repair_module._append_execution_row_at

    def crash_before_terminal(*args: object, **kwargs: object) -> None:
        raise OSError("simulated terminal append crash")

    monkeypatch.setattr(repair_module, "_append_execution_row_at", crash_before_terminal)
    with pytest.raises(OSError, match="terminal append crash"):
        repair_module.execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=plan.plan_path.parent,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    monkeypatch.setattr(repair_module, "_append_execution_row_at", original_append)

    resumed = repair_module.execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )

    assert len(client.calls) == 1
    assert resumed.repaired == ()
    assert resumed.failed == ()
    assert resumed.stopped_reason == "unresolved_claim"


def test_concurrent_executors_claim_once_and_return_same_deterministic_result(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    entered = threading.Event()
    release = threading.Event()
    original_sync = client.sync

    def blocking_sync(*args: object, **kwargs: object):
        entered.set()
        assert release.wait(timeout=5)
        return original_sync(*args, **kwargs)

    client.sync = blocking_sync  # type: ignore[method-assign]
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }
    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(execute_catalog_visibility_repair, store, plan, **arguments)
        assert entered.wait(timeout=5)
        second_future = pool.submit(execute_catalog_visibility_repair, store, plan, **arguments)
        release.set()
        first = first_future.result(timeout=5)
        second = second_future.result(timeout=5)

    assert len(client.calls) == 1
    assert first == second


@pytest.mark.parametrize("journal", ["terminal", "claim"])
def test_resume_rejects_noncanonical_journal_timestamp(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    journal: str,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }
    result = execute_catalog_visibility_repair(store, plan, **arguments)
    path = (
        result.receipt_path
        if journal == "terminal"
        else result.receipt_path.with_name("repair-execution-claims.jsonl")
    )
    row = json.loads(path.read_text())
    timestamp_field = "finished_at" if journal == "terminal" else "claimed_at"
    row[timestamp_field] = "2026-07-15T10:00:00Z"
    path.write_bytes(_canonical_bytes(row) + b"\n")

    with pytest.raises(ValueError, match="timestamp"):
        execute_catalog_visibility_repair(store, plan, **arguments)
    assert len(client.calls) == 1


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "3"])
def test_execute_rejects_invalid_failure_limit_before_artifact_mutation(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    limit: object,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    output_dir = plan.plan_path.parent

    with pytest.raises(ValueError, match="positive integer"):
        execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=output_dir,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
            consecutive_failure_limit=limit,  # type: ignore[arg-type]
        )
    assert client.calls == []
    _assert_no_execution_journals(plan)


@pytest.mark.parametrize("execute_value", [1, "true", None])
def test_non_boolean_execute_flag_cannot_authorize_sync(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    execute_value: object,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    output_dir = plan.plan_path.parent

    with pytest.raises(ValueError, match="execute must be a boolean"):
        execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=output_dir,
            execute=execute_value,  # type: ignore[arg-type]
            confirm_report_sha256=plan.report_sha256,
        )
    assert client.calls == []
    _assert_no_execution_journals(plan)


def test_execute_rejects_caller_forged_plan_before_artifact_mutation(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    forged = replace(plan, plan_sha256="0" * 64)
    client = _RecordingSyncClient()
    output_dir = plan.plan_path.parent

    with pytest.raises(ValueError, match="persisted artifact"):
        execute_catalog_visibility_repair(
            store,
            forged,
            sync_client=client,
            output_dir=output_dir,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    assert client.calls == []
    _assert_no_execution_journals(plan)


def test_partial_terminal_append_rolls_back_and_durable_claim_blocks_repeat(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import orchestrator.catalog_visibility_repair as repair_module

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    real_write = os.write
    journal_writes = 0

    def fail_second_journal_write(descriptor: int, data: bytes) -> int:
        nonlocal journal_writes
        if b'"report_sha256"' in data and data.endswith(b"\n"):
            journal_writes += 1
            if journal_writes == 2:
                real_write(descriptor, data[: len(data) // 2])
                raise OSError("simulated partial terminal append")
        return real_write(descriptor, data)

    monkeypatch.setattr(repair_module.os, "write", fail_second_journal_write)
    with pytest.raises(OSError, match="partial terminal append"):
        repair_module.execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=plan.plan_path.parent,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    monkeypatch.setattr(repair_module.os, "write", real_write)
    receipt = plan.plan_path.parent / "repair-execution.jsonl"
    assert receipt.read_bytes() == b""

    resumed = repair_module.execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )
    assert len(client.calls) == 1
    assert resumed.stopped_reason == "unresolved_claim"


def test_execution_journals_redact_credentials_exception_text_and_content_hash(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from orchestrator.catalog_sync import CatalogSyncError
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    sentinel = "SENTINELVALUE-never-persist"
    monkeypatch.setenv("ADMIN_TOKEN", sentinel)
    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    content_hash = plan.items[0].receipt.content_sha256
    client = _RecordingSyncClient([CatalogSyncError(sentinel)])

    result = execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )

    serialized = result.receipt_path.read_text() + result.receipt_path.with_name(
        "repair-execution-claims.jsonl"
    ).read_text()
    assert sentinel not in serialized
    assert content_hash not in serialized
    assert "signed_url" not in serialized
    assert "00:00:00,000 -->" not in serialized
    assert result.failed == ("abc-001",)


def test_execution_forces_private_journal_modes_under_restrictive_umask(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    output_dir = plan.plan_path.parent
    previous_umask = os.umask(0o777)
    try:
        result = execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=output_dir,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    finally:
        os.umask(previous_umask)

    claims_path = result.receipt_path.with_name("repair-execution-claims.jsonl")
    assert stat.S_IMODE(result.receipt_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(claims_path.stat().st_mode) == 0o600


@pytest.mark.parametrize(
    "client_defect",
    [
        "wrong_origin",
        "invalid_origin",
        "missing_origin",
        "verification_disabled",
        "verification_not_bool",
        "missing_verification_capability",
        "wrong_probe_origin",
        "missing_probe",
        "missing_probe_origin",
    ],
)
def test_execute_rejects_unbound_or_unverified_sync_client_before_artifacts(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    client_defect: str,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    if client_defect == "wrong_origin":
        client.base_url = "https://other.example"
    elif client_defect == "invalid_origin":
        client.base_url = "https://user:secret@javsubtitle.example"
    elif client_defect == "missing_origin":
        del client.base_url
    elif client_defect == "verification_disabled":
        client.public_visibility_verification_enabled = False
    elif client_defect == "verification_not_bool":
        client.public_visibility_verification_enabled = 1
    elif client_defect == "missing_verification_capability":
        del client.public_visibility_verification_enabled
    elif client_defect == "wrong_probe_origin":
        client.public_visibility_client.base_url = "https://other.example"
    elif client_defect == "missing_probe":
        del client.public_visibility_client
    else:
        del client.public_visibility_client.base_url

    with pytest.raises(ValueError, match="sync client"):
        execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=plan.plan_path.parent,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    assert client.calls == []
    _assert_no_execution_journals(plan)


def test_execute_rejects_symlink_alias_to_canonical_plan_directory(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    alias = tmp_path / "plan-alias"
    alias.symlink_to(plan.plan_path.parent, target_is_directory=True)
    client = _RecordingSyncClient()

    with pytest.raises(ValueError, match="plan directory"):
        execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=alias,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    assert client.calls == []
    _assert_no_execution_journals(plan)


def test_same_plan_rejects_alternate_execution_directory_without_post_or_artifacts(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    alternate = tmp_path / "alternate-execution"
    first = execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )

    with pytest.raises(ValueError, match="plan directory"):
        execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=alternate,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )

    assert client.calls == [
        (
            plan.items[0].receipt.movie_code,
            plan.items[0].receipt.subtitle_id,
            plan.items[0].receipt.content_sha256,
        )
    ]
    assert not alternate.exists()
    assert first.receipt_path.is_file()


def test_directory_replacement_after_pin_refuses_before_post_or_ledgers(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import orchestrator.catalog_visibility_repair as repair_module

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    canonical_dir = plan.plan_path.parent
    moved_dir = tmp_path / "moved-plan"
    plan_bytes = plan.plan_path.read_bytes()
    client = _RecordingSyncClient()
    real_validate = repair_module._validate_pinned_directory_path
    swapped = False

    def swap_directory_after_pin(canonical: Path, descriptor: int) -> object:
        nonlocal swapped
        if not swapped:
            swapped = True
            canonical_dir.rename(moved_dir)
            canonical_dir.mkdir(mode=0o700)
            canonical_dir.chmod(0o700)
            replacement_plan = canonical_dir / "repair-plan.json"
            replacement_plan.write_bytes(plan_bytes)
            replacement_plan.chmod(0o600)
        return real_validate(canonical, descriptor)

    monkeypatch.setattr(
        repair_module,
        "_validate_pinned_directory_path",
        swap_directory_after_pin,
    )

    with pytest.raises(ValueError, match="changed after validation"):
        repair_module.execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=canonical_dir,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )

    assert swapped is True
    assert client.calls == []
    assert not (canonical_dir / "repair-execution.jsonl").exists()
    assert not (canonical_dir / "repair-execution-claims.jsonl").exists()
    assert not (moved_dir / "repair-execution.jsonl").exists()
    assert not (moved_dir / "repair-execution-claims.jsonl").exists()


def test_unresolved_remote_success_reconciles_exact_visibility_without_second_post(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import orchestrator.catalog_visibility_repair as repair_module
    from orchestrator.catalog_visibility import PublicVisibilityResult, VisibilityStatus

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    item = plan.items[0]
    client = _RecordingSyncClient()
    original_append = repair_module._append_execution_row_at
    monkeypatch.setattr(
        repair_module,
        "_append_execution_row_at",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("terminal crash")),
    )
    with pytest.raises(OSError, match="terminal crash"):
        repair_module.execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=plan.plan_path.parent,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    monkeypatch.setattr(repair_module, "_append_execution_row_at", original_append)
    client.public_visibility_client.results.append(
        PublicVisibilityResult(
            VisibilityStatus.VISIBLE,
            item.receipt.movie_code,
            item.receipt.subtitle_id,
            observed_subtitle_ids=(item.receipt.subtitle_id,),
        )
    )

    resumed = repair_module.execute_catalog_visibility_repair(
        store,
        plan,
        sync_client=client,
        output_dir=plan.plan_path.parent,
        execute=True,
        confirm_report_sha256=plan.report_sha256,
    )

    assert len(client.calls) == 1
    assert client.public_visibility_client.calls == [
        (
            item.receipt.movie_code,
            item.receipt.subtitle_id,
            item.receipt.content_sha256,
        )
    ]
    assert resumed.repaired == (item.receipt.movie_code,)
    assert resumed.stopped_reason is None


def test_unresolved_claim_requires_explicit_recovery_before_revalidated_retry(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient([RuntimeError("crash before remote call")])
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }
    with pytest.raises(RuntimeError, match="crash before remote call"):
        execute_catalog_visibility_repair(store, plan, **arguments)

    stopped = execute_catalog_visibility_repair(store, plan, **arguments)
    assert len(client.calls) == 1
    assert stopped.stopped_reason == "unresolved_claim"

    recovered = execute_catalog_visibility_repair(
        store,
        plan,
        **arguments,
        recover_unresolved_claims=True,
    )
    assert len(client.calls) == 2
    assert recovered.repaired == ("abc-001",)
    assert recovered.stopped_reason is None


def test_unresolved_probe_failure_stops_without_post_or_false_repair(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient([RuntimeError("crash before remote call")])
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }
    with pytest.raises(RuntimeError):
        execute_catalog_visibility_repair(store, plan, **arguments)
    client.public_visibility_client.results.append(RuntimeError("probe secret"))

    stopped = execute_catalog_visibility_repair(
        store,
        plan,
        **arguments,
        recover_unresolved_claims=True,
    )

    assert len(client.calls) == 1
    assert stopped.repaired == ()
    assert stopped.stopped_reason == "unresolved_claim"
    assert "probe secret" not in stopped.receipt_path.read_text()


@pytest.mark.parametrize("recovery_value", [1, "true", None])
def test_non_boolean_unresolved_recovery_flag_cannot_authorize_post(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    recovery_value: object,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()

    with pytest.raises(ValueError, match="recover_unresolved_claims must be a boolean"):
        execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=plan.plan_path.parent,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
            recover_unresolved_claims=recovery_value,  # type: ignore[arg-type]
        )
    assert client.calls == []
    _assert_no_execution_journals(plan)


def test_execution_holds_sqlite_write_lock_through_sync_and_terminal_receipt(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    sync_entered = threading.Event()
    release_sync = threading.Event()
    writer_started = threading.Event()
    writer_acquired = threading.Event()
    original_sync = client.sync

    def blocking_sync(*args: object, **kwargs: object):
        sync_entered.set()
        assert release_sync.wait(timeout=5)
        return original_sync(*args, **kwargs)

    def writer() -> None:
        writer_started.set()
        with store.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            writer_acquired.set()
            conn.execute(
                "UPDATE jobs SET updated_at = ? WHERE id = ?",
                ("2026-07-15T12:00:00+00:00", job.id),
            )

    client.sync = blocking_sync  # type: ignore[method-assign]
    with ThreadPoolExecutor(max_workers=2) as pool:
        execution = pool.submit(
            execute_catalog_visibility_repair,
            store,
            plan,
            sync_client=client,
            output_dir=plan.plan_path.parent,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
        assert sync_entered.wait(timeout=5)
        writer_future = pool.submit(writer)
        assert writer_started.wait(timeout=5)
        assert not writer_acquired.wait(timeout=0.2)
        release_sync.set()
        result = execution.result(timeout=5)
        writer_future.result(timeout=5)

    assert result.repaired == ("abc-001",)
    assert writer_acquired.is_set()
    changed = store.get_job(job.id)
    assert changed is not None
    assert changed.updated_at == "2026-07-15T12:00:00+00:00"


def test_breaker_state_persists_across_resume_and_stops_before_next_post(
    tmp_path: Path, sqlite_path: Path, mac_jobs_root: Path
):
    from orchestrator.catalog_sync import CatalogSyncError
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    jobs = [_ready_job(store, f"abc-{index:03d}", index) for index in range(1, 5)]
    plan = _missing_plan_for_jobs(tmp_path, store, jobs)
    client = _RecordingSyncClient(
        [
            CatalogSyncError("catalog_fetch_failed"),
            CatalogSyncError("catalog_fetch_failed"),
            CatalogSyncError("catalog_fetch_failed"),
            None,
        ]
    )
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }
    first = execute_catalog_visibility_repair(
        store, plan, **arguments, consecutive_failure_limit=2
    )
    assert first.stopped_reason == "consecutive_remote_failures"
    assert len(client.calls) == 2

    still_stopped = execute_catalog_visibility_repair(
        store, plan, **arguments, consecutive_failure_limit=2
    )
    assert still_stopped.stopped_reason == "consecutive_remote_failures"
    assert len(client.calls) == 2

    third_failure = execute_catalog_visibility_repair(
        store, plan, **arguments, consecutive_failure_limit=3
    )
    assert third_failure.stopped_reason == "consecutive_remote_failures"
    assert len(client.calls) == 3


@pytest.mark.parametrize("tamper", ["cross_plan", "extra", "duplicate_key", "duplicate_row"])
def test_resume_rejects_tampered_or_conflicting_claim_journal(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    tamper: str,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    client = _RecordingSyncClient()
    arguments = {
        "sync_client": client,
        "output_dir": plan.plan_path.parent,
        "execute": True,
        "confirm_report_sha256": plan.report_sha256,
    }
    result = execute_catalog_visibility_repair(store, plan, **arguments)
    claims_path = result.receipt_path.with_name("repair-execution-claims.jsonl")
    original = claims_path.read_text()
    row = json.loads(original)
    if tamper == "cross_plan":
        row["plan_sha256"] = "0" * 64
        replacement = _canonical_bytes(row) + b"\n"
    elif tamper == "extra":
        row["admin_token"] = "SENTINELVALUE"
        replacement = _canonical_bytes(row) + b"\n"
    elif tamper == "duplicate_key":
        replacement = original.replace(
            '"schema_version":1',
            '"schema_version":1,"schema_version":1',
        ).encode()
    else:
        replacement = original.encode() * 2
    claims_path.write_bytes(replacement)

    with pytest.raises(ValueError, match="repair execution claim"):
        execute_catalog_visibility_repair(store, plan, **arguments)
    assert len(client.calls) == 1


@pytest.mark.parametrize("attack", ["symlink", "hardlink", "permissions"])
def test_execute_rejects_unsafe_claim_filesystem_leaf(
    tmp_path: Path,
    sqlite_path: Path,
    mac_jobs_root: Path,
    attack: str,
):
    from orchestrator.catalog_visibility_repair import execute_catalog_visibility_repair

    store = _store(sqlite_path, mac_jobs_root)
    job = _ready_job(store, "abc-001", 1)
    plan = _one_missing_plan(tmp_path, store, job)
    output_dir = plan.plan_path.parent
    receipt = output_dir / "repair-execution.jsonl"
    receipt.write_bytes(b"")
    receipt.chmod(0o600)
    claims = output_dir / "repair-execution-claims.jsonl"
    victim = tmp_path / "claim-victim"
    victim.write_text("do-not-touch")
    victim.chmod(0o600)
    if attack == "symlink":
        claims.symlink_to(victim)
    elif attack == "hardlink":
        os.link(victim, claims)
    else:
        claims.write_bytes(b"")
        claims.chmod(0o644)
    client = _RecordingSyncClient()

    with pytest.raises(ValueError):
        execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=client,
            output_dir=output_dir,
            execute=True,
            confirm_report_sha256=plan.report_sha256,
        )
    assert client.calls == []
    assert victim.read_text() == "do-not-touch"


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
