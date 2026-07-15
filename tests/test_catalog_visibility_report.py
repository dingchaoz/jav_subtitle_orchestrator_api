from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from orchestrator.catalog_visibility import AuditCandidateSnapshot
from orchestrator.catalog_visibility_report import (
    AUDIT_REPORT_SCHEMA_VERSION,
    REPAIR_ELIGIBLE,
    AuditFinding,
    append_audit_finding,
    audit_report_sha256,
    create_audit_manifest,
    finalize_audit_report,
    load_audit_findings,
    load_audit_manifest,
    load_audit_report,
    write_audit_manifest,
)


MOVIE_UUID = "12345678-1234-4234-8234-123456789abc"
SUBTITLE_ID = "87654321-4321-4321-8321-cba987654321"
CONTENT_SHA256 = "a" * 64


@pytest.fixture
def candidate() -> AuditCandidateSnapshot:
    return AuditCandidateSnapshot(
        job_id="job-1",
        movie_code="ktb-111",
        movie_uuid=MOVIE_UUID,
        metadata_status="complete",
        metadata_source="public",
        subtitle_id=SUBTITLE_ID,
        storage_path="ktb/ktb-111/ktb-111-English_AI.srt",
        content_sha256=CONTENT_SHA256,
        file_size=321,
        job_updated_at="2026-07-15T10:00:00+00:00",
    )


def make_manifest(tmp_path: Path, *candidates: AuditCandidateSnapshot):
    database_path = tmp_path / "jobs.sqlite3"
    return create_audit_manifest(
        api_origin="https://javsubtitle.example/",
        database_path=database_path,
        candidates=candidates,
        selection={"allowlist": [item.movie_code for item in candidates], "limit": 10},
    )


def write_manifest(tmp_path: Path, *candidates: AuditCandidateSnapshot):
    manifest = make_manifest(tmp_path, *candidates)
    write_audit_manifest(tmp_path, manifest)
    return manifest


def finding(candidate: AuditCandidateSnapshot, status: str = "visible") -> AuditFinding:
    return AuditFinding(
        candidate=candidate,
        status=status,
        reason_code=None if status == "visible" else "public_visibility_mismatch",
        observed_subtitle_ids=(SUBTITLE_ID,) if status == "visible" else (),
    )


def canonical_line(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def test_catalog_visibility_report_module_exposes_public_constants():
    assert AUDIT_REPORT_SCHEMA_VERSION == 1
    assert REPAIR_ELIGIBLE == frozenset({"missing", "not_found"})


def test_manifest_create_write_load_normalizes_values_and_uses_private_mode(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = create_audit_manifest(
        api_origin="https://javsubtitle.example/",
        database_path=tmp_path / "jobs.sqlite3",
        candidates=(candidate,),
        selection={"allowlist": ["KTB111", "ktb-111", "KTB-111"], "limit": 1},
    )

    assert manifest.schema_version == 1
    assert str(UUID(manifest.audit_id)) == manifest.audit_id
    assert datetime.fromisoformat(manifest.created_at).utcoffset().total_seconds() == 0
    assert manifest.api_origin == "https://javsubtitle.example"
    assert manifest.database_path_sha256 == hashlib.sha256(
        str((tmp_path / "jobs.sqlite3").resolve()).encode()
    ).hexdigest()
    assert manifest.selection == {"allowlist": ["ktb-111"], "limit": 1}
    assert manifest.candidates == (candidate,)

    write_audit_manifest(tmp_path, manifest)

    path = tmp_path / "audit-manifest.json"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert load_audit_manifest(path) == manifest


def test_manifest_write_is_idempotent_but_rejects_changed_manifest(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = write_manifest(tmp_path, candidate)

    write_audit_manifest(tmp_path, manifest)

    with pytest.raises(ValueError):
        write_audit_manifest(tmp_path, replace(manifest, audit_id=str(uuid4())))


def test_manifest_serializes_database_hash_not_raw_path(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = write_manifest(tmp_path, candidate)
    raw_path = str((tmp_path / "jobs.sqlite3").resolve())
    serialized = (tmp_path / "audit-manifest.json").read_text()

    assert raw_path not in serialized
    assert manifest.database_path_sha256 in serialized


def test_load_manifest_rejects_duplicate_json_fields(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    write_manifest(tmp_path, candidate)
    path = tmp_path / "audit-manifest.json"
    serialized = path.read_text().replace(
        '"schema_version":1',
        '"schema_version":1,"schema_version":1',
    )
    path.write_text(serialized)

    with pytest.raises(ValueError):
        load_audit_manifest(path)


@pytest.mark.parametrize(
    "selection",
    [
        {"allowlist": [], "limit": 0},
        {"allowlist": [], "limit": True},
        {"allowlist": "ktb-111", "limit": 1},
        {"allowlist": [object()], "limit": 1},
        {"allowlist": [], "limit": 1, "token": "secret"},
        {"limit": 1},
    ],
)
def test_manifest_rejects_invalid_or_non_json_safe_selection(
    tmp_path: Path, candidate: AuditCandidateSnapshot, selection: dict[str, object]
):
    with pytest.raises((TypeError, ValueError)):
        create_audit_manifest(
            api_origin="https://javsubtitle.example",
            database_path=tmp_path / "jobs.sqlite3",
            candidates=(candidate,),
            selection=selection,
        )


def test_manifest_accepts_null_selection_values_and_sorts_allowlist(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = create_audit_manifest(
        api_origin="https://javsubtitle.example",
        database_path=tmp_path / "jobs.sqlite3",
        candidates=(candidate,),
        selection={"allowlist": ["xyz-9", "ABC001"], "limit": None},
    )

    assert manifest.selection == {"allowlist": ["abc-001", "xyz-009"], "limit": None}

    empty = create_audit_manifest(
        api_origin="https://javsubtitle.example",
        database_path=tmp_path / "jobs.sqlite3",
        candidates=(),
        selection={"allowlist": None, "limit": None},
    )
    assert empty.candidates == ()


def test_manifest_rejects_duplicate_job_or_movie_identity(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    duplicate_job = replace(candidate, movie_code="abc-001")
    duplicate_code = replace(candidate, job_id="job-2")

    for candidates in ((candidate, duplicate_job), (candidate, duplicate_code)):
        with pytest.raises(ValueError):
            make_manifest(tmp_path, *candidates)


@pytest.mark.parametrize(
    "changed",
    [
        {"job_id": 1},
        {"movie_code": "KTB111"},
        {"movie_uuid": 1},
        {"file_size": True},
        {"job_updated_at": None},
    ],
)
def test_manifest_rejects_malformed_candidate_base_fields(
    tmp_path: Path, candidate: AuditCandidateSnapshot, changed: dict[str, object]
):
    with pytest.raises((TypeError, ValueError)):
        make_manifest(tmp_path, replace(candidate, **changed))


def test_jsonl_append_load_resume_and_duplicate_rejection(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = write_manifest(tmp_path, candidate)
    row = finding(candidate)

    append_audit_finding(tmp_path, row)

    checkpoint = tmp_path / "audit-findings.jsonl"
    assert stat.S_IMODE(checkpoint.stat().st_mode) == 0o600
    assert checkpoint.read_bytes().endswith(b"\n")
    assert load_audit_findings(tmp_path, manifest) == (row,)
    with pytest.raises(ValueError):
        append_audit_finding(tmp_path, row)


@pytest.mark.parametrize(
    "bad_content",
    [
        "\n",
        "{\n",
        "{}\n",
        '{"schema_version":1}\n',
        "[]\n",
    ],
)
def test_checkpoint_rejects_blank_malformed_truncated_or_wrong_schema_rows(
    tmp_path: Path, candidate: AuditCandidateSnapshot, bad_content: str
):
    manifest = write_manifest(tmp_path, candidate)
    (tmp_path / "audit-findings.jsonl").write_text(bad_content)

    with pytest.raises(ValueError):
        load_audit_findings(tmp_path, manifest)


def test_checkpoint_rejects_changed_candidate_and_candidate_not_in_manifest(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = write_manifest(tmp_path, candidate)
    checkpoint = tmp_path / "audit-findings.jsonl"

    for changed_candidate in (
        replace(candidate, file_size=322),
        replace(candidate, job_id="job-not-in-manifest"),
    ):
        payload = {
            "schema_version": 1,
            "candidate": asdict(changed_candidate),
            "status": "visible",
            "reason_code": None,
            "observed_subtitle_ids": [SUBTITLE_ID],
        }
        checkpoint.write_text(canonical_line(payload) + "\n")
        with pytest.raises(ValueError):
            load_audit_findings(tmp_path, manifest)


def test_finding_classification_requires_receipt_validity(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    valid_manifest = write_manifest(tmp_path / "valid", candidate)
    with pytest.raises(ValueError):
        append_audit_finding(tmp_path / "valid", finding(candidate, "invalid_receipt"))

    invalid = replace(candidate, job_id="job-bad", movie_code="abc-001", subtitle_id=None)
    invalid_manifest = write_manifest(tmp_path / "invalid", invalid)
    invalid_finding = finding(invalid, "invalid_receipt")
    append_audit_finding(tmp_path / "invalid", invalid_finding)
    assert load_audit_findings(tmp_path / "invalid", invalid_manifest) == (invalid_finding,)

    write_manifest(tmp_path / "invalid-regular", invalid)
    with pytest.raises(ValueError):
        append_audit_finding(tmp_path / "invalid-regular", finding(invalid, "missing"))

    assert valid_manifest.candidates == (candidate,)


@pytest.mark.parametrize(
    "change",
    [
        {"status": "unknown"},
        {"reason_code": "UPPERCASE"},
        {"reason_code": "a" * 65},
        {"observed_subtitle_ids": [SUBTITLE_ID]},
        {"observed_subtitle_ids": (1,)},
    ],
)
def test_append_rejects_invalid_finding_fields(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    change: dict[str, object],
):
    write_manifest(tmp_path, candidate)
    valid = finding(candidate)
    values = {
        "candidate": valid.candidate,
        "status": valid.status,
        "reason_code": valid.reason_code,
        "observed_subtitle_ids": valid.observed_subtitle_ids,
    }
    values.update(change)
    invalid = AuditFinding(**values)

    with pytest.raises((TypeError, ValueError)):
        append_audit_finding(tmp_path, invalid)


def test_finalize_rejects_incomplete_checkpoint(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    write_manifest(tmp_path, candidate)

    with pytest.raises(ValueError):
        finalize_audit_report(tmp_path)


def test_finalize_orders_findings_counts_statuses_and_writes_stable_private_report(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    second = replace(
        candidate,
        job_id="job-2",
        movie_code="abc-001",
        subtitle_id="11111111-2222-4333-8444-555555555555",
        storage_path="abc/abc-001/abc-001-English_AI.srt",
    )
    manifest = write_manifest(tmp_path, candidate, second)
    append_audit_finding(tmp_path, finding(second, "missing"))
    append_audit_finding(tmp_path, finding(candidate))

    report = finalize_audit_report(tmp_path)

    assert report.manifest == manifest
    assert [item.candidate.job_id for item in report.findings] == ["job-1", "job-2"]
    assert report.counts == {
        "fetch_failed": 0,
        "invalid_receipt": 0,
        "missing": 1,
        "not_found": 0,
        "response_invalid": 0,
        "visible": 1,
    }
    assert report.complete is True
    assert report.report_sha256 == audit_report_sha256(report)
    assert load_audit_report(tmp_path / "audit-report.json") == report
    assert stat.S_IMODE((tmp_path / "audit-report.json").stat().st_mode) == 0o600
    assert not list(tmp_path.glob(".audit-report.json.*.tmp"))


@pytest.mark.parametrize(
    "tamper",
    [
        lambda payload: payload.update(report_sha256="0" * 64),
        lambda payload: payload.update(complete=False),
        lambda payload: payload["counts"].update(visible=99),
        lambda payload: payload["manifest"].update(schema_version=2),
        lambda payload: payload.update(unknown="field"),
        lambda payload: payload["findings"][0].update(status="missing"),
    ],
)
def test_load_report_rejects_tampered_digest_fields_counts_schema_or_unknown_fields(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    tamper,
):
    write_manifest(tmp_path, candidate)
    append_audit_finding(tmp_path, finding(candidate))
    finalize_audit_report(tmp_path)
    path = tmp_path / "audit-report.json"
    payload = json.loads(path.read_text())
    tamper(payload)
    path.write_text(canonical_line(payload))

    with pytest.raises(ValueError):
        load_audit_report(path)


def test_symlink_output_directory_and_manifest_are_rejected(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    real = tmp_path / "real"
    real.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    manifest = make_manifest(tmp_path, candidate)

    with pytest.raises(ValueError):
        write_audit_manifest(linked, manifest)

    manifest_target = real / "target.json"
    manifest_target.write_text("{}")
    (real / "audit-manifest.json").symlink_to(manifest_target)
    with pytest.raises(ValueError):
        write_audit_manifest(real, manifest)
    with pytest.raises(ValueError):
        load_audit_manifest(real / "audit-manifest.json")


def test_checkpoint_and_report_symlinks_are_rejected(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = write_manifest(tmp_path, candidate)
    target = tmp_path / "target"
    target.write_text("")
    checkpoint = tmp_path / "audit-findings.jsonl"
    checkpoint.symlink_to(target)

    with pytest.raises(ValueError):
        append_audit_finding(tmp_path, finding(candidate))
    with pytest.raises(ValueError):
        load_audit_findings(tmp_path, manifest)

    checkpoint.unlink()
    append_audit_finding(tmp_path, finding(candidate))
    report_path = tmp_path / "audit-report.json"
    report_path.symlink_to(target)
    with pytest.raises(ValueError):
        finalize_audit_report(tmp_path)
    with pytest.raises(ValueError):
        load_audit_report(report_path)


def test_safe_serialization_never_contains_database_path_or_secret_fields(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    sentinel = "SENTINEL_ADMIN_TOKEN_SIGNED_URL_SRT_CONTENT"
    with pytest.raises(ValueError):
        create_audit_manifest(
            api_origin="https://user:password@javsubtitle.example",
            database_path=tmp_path / sentinel / "jobs.sqlite3",
            candidates=(candidate,),
            selection={"allowlist": [], "limit": 1, "admin_token": sentinel},
        )

    secret_path = tmp_path / sentinel / "jobs.sqlite3"
    manifest = create_audit_manifest(
        api_origin="https://javsubtitle.example",
        database_path=secret_path,
        candidates=(candidate,),
        selection={"allowlist": ["ktb-111"], "limit": 1},
    )
    write_audit_manifest(tmp_path, manifest)
    append_audit_finding(tmp_path, finding(candidate))
    finalize_audit_report(tmp_path)

    serialized = b"".join(path.read_bytes() for path in tmp_path.iterdir() if path.is_file())
    assert sentinel.encode() not in serialized

    payload = json.loads((tmp_path / "audit-findings.jsonl").read_text())
    payload["signed_url"] = sentinel
    (tmp_path / "audit-findings.jsonl").write_text(canonical_line(payload) + "\n")
    with pytest.raises(ValueError):
        load_audit_findings(tmp_path, manifest)
