from __future__ import annotations

import hashlib
import fcntl
import json
import os
import stat
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest

import orchestrator.catalog_visibility_report as report_module
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


def encoded_finding_row(item: AuditFinding) -> bytes:
    return (
        canonical_line(
            {
                "schema_version": 1,
                "candidate": asdict(item.candidate),
                "status": item.status,
                "reason_code": item.reason_code,
                "observed_subtitle_ids": list(item.observed_subtitle_ids),
            }
        )
        + "\n"
    ).encode()


def second_candidate(candidate: AuditCandidateSnapshot) -> AuditCandidateSnapshot:
    return replace(
        candidate,
        job_id="job-2",
        movie_code="abc-001",
        subtitle_id="11111111-2222-4333-8444-555555555555",
        storage_path="abc/abc-001/abc-001-English_AI.srt",
    )


def large_valid_finding(candidate: AuditCandidateSnapshot) -> AuditFinding:
    return replace(
        finding(candidate),
        observed_subtitle_ids=tuple(
            f"00000000-0000-4000-8000-{index:012d}" for index in range(20)
        ),
    )


def replace_published_leaf(path: Path, detached: Path, content: bytes) -> None:
    path.rename(detached)
    path.write_bytes(content)
    path.chmod(0o640)


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
        {"job_updated_at": None},
    ],
)
def test_manifest_rejects_malformed_candidate_base_fields(
    tmp_path: Path, candidate: AuditCandidateSnapshot, changed: dict[str, object]
):
    with pytest.raises((TypeError, ValueError)):
        make_manifest(tmp_path, replace(candidate, **changed))


@pytest.mark.parametrize(
    ("field_name", "unsafe_value"),
    [
        (
            "movie_uuid",
            "eyJhbGciOiJIUzI1NiJ9.SENTINELVALUE.c2lnbmF0dXJl",
        ),
        ("metadata_status", "bearer-SENTINELVALUE"),
        ("metadata_status", "secret-SENTINELVALUE"),
        ("metadata_source", "service-role-SENTINELVALUE"),
        ("metadata_source", "credential-SENTINELVALUE"),
        ("subtitle_id", "token-SENTINELVALUE"),
        ("content_sha256", "api-key-SENTINELVALUE"),
        (
            "storage_path",
            "https://storage.example/file.srt?X-Amz-Signature=SENTINELVALUE",
        ),
        ("storage_path", "../SENTINELVALUE/file.srt"),
        ("storage_path", "/SENTINELVALUE/file.srt"),
        (
            "storage_path",
            "safe/SENTINELVALUE.srt\n00:00:00,000 --> 00:00:01,000\nsubtitle",
        ),
    ],
)
def test_manifest_rejects_sensitive_or_content_bearing_candidate_strings_without_leakage(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    field_name: str,
    unsafe_value: str,
):
    manifest = make_manifest(tmp_path, candidate)
    unsafe_candidate = replace(candidate, **{field_name: unsafe_value})
    unsafe_manifest = replace(manifest, candidates=(unsafe_candidate,))
    output_dir = tmp_path / "unsafe-output"

    with pytest.raises((TypeError, ValueError)) as raised:
        write_audit_manifest(output_dir, unsafe_manifest)

    assert "SENTINELVALUE" not in str(raised.value)
    assert not output_dir.exists() or not any(output_dir.iterdir())


@pytest.mark.parametrize(
    ("field_name", "oversized_value"),
    [
        ("job_id", "a" * 129),
        ("movie_code", f"{'a' * 65}-111"),
    ],
)
def test_manifest_rejects_excessively_long_candidate_strings(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    field_name: str,
    oversized_value: str,
):
    with pytest.raises(ValueError):
        make_manifest(tmp_path, replace(candidate, **{field_name: oversized_value}))


def test_manifest_rejects_excessively_long_selection_movie_code(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    with pytest.raises(ValueError):
        create_audit_manifest(
            api_origin="https://javsubtitle.example",
            database_path=tmp_path / "jobs.sqlite3",
            candidates=(candidate,),
            selection={"allowlist": [f"{'a' * 65}-111"], "limit": 1},
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"metadata_status": "password-SENTINELVALUE"},
        {"metadata_source": "ghp_SENTINELVALUE123456789"},
        {"subtitle_id": "opaque-SENTINELVALUE-random-value"},
        {
            "movie_uuid": "",
            "metadata_status": "",
            "metadata_source": "",
            "subtitle_id": "",
            "storage_path": "",
            "content_sha256": "",
            "file_size": 0,
        },
        {"storage_path": "safe/wrong-path.srt"},
        {
            "storage_path": (
                "https://storage.example/file.srt?X-Amz-Signature=SENTINELVALUE"
            )
        },
        {"movie_uuid": "eyJhbGciOiJIUzI1NiJ9.SENTINELVALUE.c2lnbmF0dXJl"},
        {
            "storage_path": (
                "SENTINELVALUE\n00:00:00,000 --> 00:00:01,000\nsubtitle"
            )
        },
        {"content_sha256": "a" * 257},
    ],
)
def test_create_redacts_every_invalid_receipt_field_before_any_serialization(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    overrides: dict[str, object],
):
    raw_candidate = replace(candidate, **overrides)

    manifest = make_manifest(tmp_path, raw_candidate)
    canonical_candidate = manifest.candidates[0]

    assert canonical_candidate == AuditCandidateSnapshot(
        job_id=raw_candidate.job_id,
        movie_code=raw_candidate.movie_code,
        movie_uuid=None,
        metadata_status=None,
        metadata_source=None,
        subtitle_id=None,
        storage_path=None,
        content_sha256=None,
        file_size=None,
        job_updated_at=raw_candidate.job_updated_at,
    )
    write_audit_manifest(tmp_path, manifest)
    invalid_finding = finding(canonical_candidate, "invalid_receipt")
    append_audit_finding(tmp_path, invalid_finding)
    finalize_audit_report(tmp_path)

    serialized = b"".join(
        path.read_bytes() for path in tmp_path.iterdir() if path.is_file()
    )
    for raw_value in overrides.values():
        if isinstance(raw_value, str) and raw_value:
            assert raw_value.encode() not in serialized


def test_create_preserves_valid_receipt_candidate_byte_for_byte(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = make_manifest(tmp_path, candidate)

    assert manifest.candidates == (candidate,)


def test_direct_manifest_requires_valid_or_fully_redacted_receipt(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    base_manifest = make_manifest(tmp_path, candidate)
    partial = replace(candidate, movie_uuid=None)
    raw_invalid = replace(candidate, metadata_status="password-SENTINELVALUE")

    for direct_candidate in (partial, raw_invalid):
        with pytest.raises(ValueError):
            write_audit_manifest(
                tmp_path / direct_candidate.job_id / str(uuid4()),
                replace(base_manifest, candidates=(direct_candidate,)),
            )


def test_load_manifest_rejects_raw_invalid_receipt_instead_of_sanitizing(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    write_manifest(tmp_path, candidate)
    path = tmp_path / "audit-manifest.json"
    payload = json.loads(path.read_text())
    payload["candidates"][0]["metadata_source"] = "ghp_SENTINELVALUE123456789"
    path.write_text(canonical_line(payload))

    with pytest.raises(ValueError):
        load_audit_manifest(path)


def test_direct_fully_redacted_manifest_is_accepted_and_reportable(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    base_manifest = make_manifest(tmp_path, candidate)
    redacted = replace(
        candidate,
        movie_uuid=None,
        metadata_status=None,
        metadata_source=None,
        subtitle_id=None,
        storage_path=None,
        content_sha256=None,
        file_size=None,
    )
    manifest = replace(base_manifest, candidates=(redacted,))

    write_audit_manifest(tmp_path, manifest)
    loaded = load_audit_manifest(tmp_path / "audit-manifest.json")
    invalid_finding = finding(loaded.candidates[0], "invalid_receipt")
    append_audit_finding(tmp_path, invalid_finding)

    assert loaded == manifest
    assert load_audit_findings(tmp_path, loaded) == (invalid_finding,)


@pytest.mark.parametrize(
    "unsafe_observed_id",
    [
        "not-a-uuid-SENTINELVALUE",
        "https://catalog.example/subtitle?token=SENTINELVALUE",
        "bearer-SENTINELVALUE",
        "eyJhbGciOiJIUzI1NiJ9.SENTINELVALUE.c2lnbmF0dXJl",
        "SENTINELVALUE\n00:00:00,000 --> 00:00:01,000\nsubtitle",
    ],
)
def test_append_rejects_unsafe_observed_ids_without_writing_or_leaking(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    unsafe_observed_id: str,
):
    write_manifest(tmp_path, candidate)
    unsafe_finding = replace(
        finding(candidate),
        observed_subtitle_ids=(unsafe_observed_id,),
    )

    with pytest.raises((TypeError, ValueError)) as raised:
        append_audit_finding(tmp_path, unsafe_finding)

    assert "SENTINELVALUE" not in str(raised.value)
    assert not (tmp_path / "audit-findings.jsonl").exists()
    assert b"SENTINELVALUE" not in (tmp_path / "audit-manifest.json").read_bytes()


@pytest.mark.parametrize(
    "overrides",
    [
        {"subtitle_id": "bad-uuid"},
        {"storage_path": "safe/incorrect.srt"},
        {"movie_uuid": None},
    ],
)
def test_safe_malformed_or_missing_receipts_remain_reportable(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    overrides: dict[str, object],
):
    invalid = replace(candidate, **overrides)
    manifest = write_manifest(tmp_path, invalid)
    canonical_invalid = manifest.candidates[0]
    invalid_finding = finding(canonical_invalid, "invalid_receipt")

    append_audit_finding(tmp_path, invalid_finding)

    assert load_audit_findings(tmp_path, manifest) == (invalid_finding,)


def test_duplicate_canonical_observed_ids_remain_reportable(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = write_manifest(tmp_path, candidate)
    duplicate_finding = replace(
        finding(candidate, "missing"),
        observed_subtitle_ids=(SUBTITLE_ID, SUBTITLE_ID),
    )

    append_audit_finding(tmp_path, duplicate_finding)

    assert load_audit_findings(tmp_path, manifest) == (duplicate_finding,)


@pytest.mark.parametrize(
    "invalid_timestamp",
    [
        "2026-07-15Q10:00:00+00:00",
        "2026-07-15 10:00:00+00:00",
        "2026-07-15T10:00:00+01:00",
        "2026-07-15T10:00:00",
        "2026-07-15T10:00:00.1234567+00:00",
        "2026-02-30T10:00:00+00:00",
        "2026-07-15T24:00:00+00:00",
    ],
)
def test_manifest_and_candidate_timestamps_require_exact_utc_iso_contract(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    invalid_timestamp: str,
):
    manifest = make_manifest(tmp_path, candidate)

    with pytest.raises(ValueError) as created_at_error:
        write_audit_manifest(
            tmp_path / "bad-created-at",
            replace(manifest, created_at=invalid_timestamp),
        )
    with pytest.raises(ValueError) as job_updated_at_error:
        make_manifest(
            tmp_path / "bad-job-updated-at",
            replace(candidate, job_updated_at=invalid_timestamp),
        )

    assert invalid_timestamp not in str(created_at_error.value)
    assert invalid_timestamp not in str(job_updated_at_error.value)


@pytest.mark.parametrize(
    "valid_timestamp",
    [
        "2026-07-15T10:00:00Z",
        "2026-07-15T10:00:00.1Z",
        "2026-07-15T10:00:00.123456+00:00",
    ],
)
def test_manifest_and_candidate_timestamps_accept_exact_utc_iso_forms(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    valid_timestamp: str,
):
    timestamped_candidate = replace(candidate, job_updated_at=valid_timestamp)
    manifest = replace(
        make_manifest(tmp_path, timestamped_candidate),
        created_at=valid_timestamp,
    )

    write_audit_manifest(tmp_path, manifest)

    assert load_audit_manifest(tmp_path / "audit-manifest.json") == manifest


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
    canonical_invalid = invalid_manifest.candidates[0]
    invalid_finding = finding(canonical_invalid, "invalid_receipt")
    append_audit_finding(tmp_path / "invalid", invalid_finding)
    assert load_audit_findings(tmp_path / "invalid", invalid_manifest) == (invalid_finding,)

    regular_manifest = write_manifest(tmp_path / "invalid-regular", invalid)
    with pytest.raises(ValueError):
        append_audit_finding(
            tmp_path / "invalid-regular",
            finding(regular_manifest.candidates[0], "missing"),
        )

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


def test_manifest_and_report_mappings_are_defensive_and_immutable(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    allowlist = ["ktb-111"]
    selection = {"allowlist": allowlist, "limit": 1}
    manifest = create_audit_manifest(
        api_origin="https://javsubtitle.example",
        database_path=tmp_path / "jobs.sqlite3",
        candidates=(candidate,),
        selection=selection,
    )
    selection["limit"] = 99
    allowlist.append("abc-001")

    assert manifest.selection == {"allowlist": ["ktb-111"], "limit": 1}
    with pytest.raises(TypeError):
        manifest.selection["limit"] = 2
    with pytest.raises((AttributeError, TypeError)):
        manifest.selection["allowlist"].append("abc-001")

    write_audit_manifest(tmp_path, manifest)
    append_audit_finding(tmp_path, finding(candidate))
    report = finalize_audit_report(tmp_path)
    digest = report.report_sha256
    with pytest.raises(TypeError):
        report.counts["visible"] = 99
    assert audit_report_sha256(report) == digest


def test_hardlinked_checkpoint_is_rejected_without_modifying_external_inode(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    write_manifest(tmp_path, candidate)
    external = tmp_path / "external"
    external.write_bytes(b"external-content")
    external.chmod(0o644)
    os.link(external, tmp_path / "audit-findings.jsonl")
    before = external.stat()

    with pytest.raises(ValueError):
        append_audit_finding(tmp_path, finding(candidate))

    after = external.stat()
    assert external.read_bytes() == b"external-content"
    assert stat.S_IMODE(after.st_mode) == stat.S_IMODE(before.st_mode) == 0o644


def test_manifest_fsync_failure_leaves_no_final_or_temp_and_retry_succeeds(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    manifest = make_manifest(tmp_path, candidate)
    real_fsync = os.fsync
    failed = False

    def fail_first_file_fsync(descriptor: int):
        nonlocal failed
        if not failed and stat.S_ISREG(os.fstat(descriptor).st_mode):
            failed = True
            raise OSError("injected primary fsync failure")
        return real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_first_file_fsync)
    with pytest.raises((OSError, ValueError), match="fsync|written safely"):
        write_audit_manifest(tmp_path, manifest)
    assert not (tmp_path / "audit-manifest.json").exists()
    assert not list(tmp_path.glob(".audit-manifest.json.*.tmp"))

    monkeypatch.setattr(os, "fsync", real_fsync)
    write_audit_manifest(tmp_path, manifest)
    assert load_audit_manifest(tmp_path / "audit-manifest.json") == manifest


def test_identical_existing_manifest_is_hardened_to_private_mode(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = write_manifest(tmp_path, candidate)
    path = tmp_path / "audit-manifest.json"
    path.chmod(0o644)

    write_audit_manifest(tmp_path, manifest)

    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_concurrent_duplicate_append_has_one_success_and_remains_resumable(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = write_manifest(tmp_path, candidate)
    barrier = threading.Barrier(8)

    def append_once() -> str:
        barrier.wait()
        try:
            append_audit_finding(tmp_path, finding(candidate))
        except ValueError:
            return "duplicate"
        return "success"

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: append_once(), range(8)))

    assert results.count("success") == 1
    assert results.count("duplicate") == 7
    assert load_audit_findings(tmp_path, manifest) == (finding(candidate),)


def test_first_checkpoint_creation_fsyncs_directory_and_propagates_failure(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    write_manifest(tmp_path, candidate)
    calls = 0

    def fail_directory_fsync(descriptor: int):
        nonlocal calls
        calls += 1
        raise OSError("injected directory fsync failure")

    monkeypatch.setattr(
        report_module,
        "_fsync_directory_fd",
        fail_directory_fsync,
        raising=False,
    )
    with pytest.raises(OSError, match="directory fsync"):
        append_audit_finding(tmp_path, finding(candidate))
    assert calls == 1


def test_report_is_idempotent_and_cannot_be_replaced_by_changed_checkpoint(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    write_manifest(tmp_path, candidate)
    append_audit_finding(tmp_path, finding(candidate))
    first = finalize_audit_report(tmp_path)
    assert finalize_audit_report(tmp_path) == first

    checkpoint = tmp_path / "audit-findings.jsonl"
    payload = json.loads(checkpoint.read_text())
    payload["status"] = "missing"
    payload["reason_code"] = "public_visibility_mismatch"
    checkpoint.write_text(canonical_line(payload) + "\n")

    with pytest.raises(ValueError):
        finalize_audit_report(tmp_path)
    assert load_audit_report(tmp_path / "audit-report.json") == first


def test_pinned_directory_prevents_parent_swap_redirect(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    output = tmp_path / "output"
    output.mkdir()
    moved = tmp_path / "moved"
    redirect = tmp_path / "redirect"
    redirect.mkdir()
    manifest = make_manifest(tmp_path, candidate)
    real_open = os.open
    swapped = False

    def swapping_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal swapped
        if (
            not swapped
            and dir_fd is not None
            and isinstance(path, str)
            and path.startswith(".audit-manifest.json.")
        ):
            output.rename(moved)
            output.symlink_to(redirect, target_is_directory=True)
            swapped = True
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", swapping_open)
    write_audit_manifest(output, manifest)

    assert swapped is True
    assert not any(redirect.iterdir())
    assert load_audit_manifest(moved / "audit-manifest.json") == manifest


def test_resource_limits_and_deep_json_fail_safely(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    monkeypatch.setattr(report_module, "MAX_AUDIT_CANDIDATES", 0, raising=False)
    with pytest.raises(ValueError):
        make_manifest(tmp_path / "candidates", candidate)

    monkeypatch.setattr(report_module, "MAX_AUDIT_CANDIDATES", 10_000)
    monkeypatch.setattr(report_module, "MAX_AUDIT_ALLOWLIST", 0, raising=False)
    with pytest.raises(ValueError):
        make_manifest(tmp_path / "allowlist", candidate)

    monkeypatch.setattr(report_module, "MAX_AUDIT_ALLOWLIST", 10_000)
    manifest = write_manifest(tmp_path / "document", candidate)
    monkeypatch.setattr(report_module, "MAX_AUDIT_DOCUMENT_BYTES", 10, raising=False)
    with pytest.raises(ValueError):
        load_audit_manifest(tmp_path / "document" / "audit-manifest.json")

    deep = tmp_path / "deep.json"
    deep.write_text("[" * 200 + "0" + "]" * 200)
    monkeypatch.setattr(
        report_module,
        "MAX_AUDIT_DOCUMENT_BYTES",
        64 * 1024 * 1024,
    )
    with pytest.raises(ValueError):
        load_audit_manifest(deep)

    monkeypatch.setattr(report_module, "MAX_OBSERVED_SUBTITLE_IDS", 0, raising=False)
    with pytest.raises(ValueError):
        append_audit_finding(
            tmp_path / "document",
            replace(finding(candidate), observed_subtitle_ids=(SUBTITLE_ID,)),
        )


def test_checkpoint_line_and_document_and_report_limits_fail_safely(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    manifest = write_manifest(tmp_path, candidate)
    monkeypatch.setattr(report_module, "MAX_CHECKPOINT_LINE_BYTES", 10)
    with pytest.raises(ValueError):
        append_audit_finding(tmp_path, finding(candidate))

    monkeypatch.setattr(report_module, "MAX_CHECKPOINT_LINE_BYTES", 64 * 1024)
    append_audit_finding(tmp_path, finding(candidate))
    monkeypatch.setattr(report_module, "MAX_AUDIT_DOCUMENT_BYTES", 10)
    with pytest.raises(ValueError):
        load_audit_findings(tmp_path, manifest)

    monkeypatch.setattr(report_module, "MAX_AUDIT_DOCUMENT_BYTES", 64 * 1024 * 1024)
    report = finalize_audit_report(tmp_path)
    monkeypatch.setattr(report_module, "_max_audit_report_bytes", lambda: 10)
    with pytest.raises(ValueError):
        load_audit_report(tmp_path / "audit-report.json")
    assert report.complete is True


def test_checkpoint_append_crossing_document_limit_by_one_is_unchanged_and_resumable(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    second = second_candidate(candidate)
    manifest = write_manifest(tmp_path, candidate, second)
    first_finding = large_valid_finding(candidate)
    append_audit_finding(tmp_path, first_finding)
    checkpoint = tmp_path / "audit-findings.jsonl"
    checkpoint.chmod(0o640)
    before = checkpoint.read_bytes()
    before_mode = stat.S_IMODE(checkpoint.stat().st_mode)
    second_finding = finding(second, "missing")
    exact_final_size = len(before) + len(encoded_finding_row(second_finding))
    monkeypatch.setattr(
        report_module,
        "MAX_AUDIT_DOCUMENT_BYTES",
        exact_final_size - 1,
    )

    with pytest.raises(ValueError, match="checkpoint|large|limit"):
        append_audit_finding(tmp_path, second_finding)

    assert checkpoint.read_bytes() == before
    assert stat.S_IMODE(checkpoint.stat().st_mode) == before_mode == 0o640
    monkeypatch.setattr(report_module, "MAX_AUDIT_DOCUMENT_BYTES", exact_final_size)
    append_audit_finding(tmp_path, second_finding)
    assert load_audit_findings(tmp_path, manifest) == (
        first_finding,
        second_finding,
    )


def test_checkpoint_append_exactly_at_document_limit_succeeds(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    second = second_candidate(candidate)
    manifest = write_manifest(tmp_path, candidate, second)
    first_finding = large_valid_finding(candidate)
    append_audit_finding(tmp_path, first_finding)
    checkpoint = tmp_path / "audit-findings.jsonl"
    second_finding = finding(second, "missing")
    exact_final_size = checkpoint.stat().st_size + len(
        encoded_finding_row(second_finding)
    )
    monkeypatch.setattr(
        report_module,
        "MAX_AUDIT_DOCUMENT_BYTES",
        exact_final_size,
    )

    append_audit_finding(tmp_path, second_finding)

    assert checkpoint.stat().st_size == exact_final_size
    assert load_audit_findings(tmp_path, manifest) == (
        first_finding,
        second_finding,
    )


def test_checkpoint_append_rejects_an_existing_over_limit_file_unchanged(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    second = second_candidate(candidate)
    write_manifest(tmp_path, candidate, second)
    first_finding = large_valid_finding(candidate)
    append_audit_finding(tmp_path, first_finding)
    checkpoint = tmp_path / "audit-findings.jsonl"
    checkpoint.chmod(0o640)
    before = checkpoint.read_bytes()
    before_mode = stat.S_IMODE(checkpoint.stat().st_mode)
    assert (tmp_path / "audit-manifest.json").stat().st_size < len(before)
    monkeypatch.setattr(
        report_module,
        "MAX_AUDIT_DOCUMENT_BYTES",
        len(before) - 1,
    )

    with pytest.raises(ValueError, match="checkpoint|large|limit"):
        append_audit_finding(tmp_path, finding(second, "missing"))

    assert checkpoint.read_bytes() == before
    assert stat.S_IMODE(checkpoint.stat().st_mode) == before_mode == 0o640


def test_cleanup_fault_does_not_mask_primary_error_and_temp_is_removed(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    manifest = make_manifest(tmp_path, candidate)
    real_close = os.close
    close_failed = False

    def fail_write(_descriptor: int, _data: bytes):
        raise OSError("PRIMARY_WRITE_FAILURE")

    def close_then_fail_once(descriptor: int):
        nonlocal close_failed
        is_regular = stat.S_ISREG(os.fstat(descriptor).st_mode)
        real_close(descriptor)
        if is_regular and not close_failed:
            close_failed = True
            raise OSError("CLEANUP_CLOSE_FAILURE")

    monkeypatch.setattr(report_module, "_write_all_fd", fail_write)
    monkeypatch.setattr(os, "close", close_then_fail_once)
    with pytest.raises(OSError, match="PRIMARY_WRITE_FAILURE"):
        write_audit_manifest(tmp_path, manifest)

    assert close_failed is True
    assert not (tmp_path / "audit-manifest.json").exists()
    assert not list(tmp_path.glob(".audit-manifest.json.*.tmp"))


def test_concurrent_manifest_creators_converge_or_fail_closed(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = make_manifest(tmp_path, candidate)
    barrier = threading.Barrier(4)

    def write_same() -> None:
        barrier.wait()
        write_audit_manifest(tmp_path, manifest)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda _: write_same(), range(4)))
    assert load_audit_manifest(tmp_path / "audit-manifest.json") == manifest

    with pytest.raises(ValueError):
        write_audit_manifest(tmp_path, replace(manifest, audit_id=str(uuid4())))


def test_parent_traversal_is_rejected(
    tmp_path: Path, candidate: AuditCandidateSnapshot
):
    manifest = make_manifest(tmp_path, candidate)
    unsafe = os.path.join(str(tmp_path), "missing", "..", "redirect")

    with pytest.raises(ValueError):
        write_audit_manifest(unsafe, manifest)


def test_waiting_writer_rollback_preserves_prior_locked_append(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    second = replace(
        candidate,
        job_id="job-2",
        movie_code="abc-001",
        subtitle_id="11111111-2222-4333-8444-555555555555",
        storage_path="abc/abc-001/abc-001-English_AI.srt",
    )
    manifest = write_manifest(tmp_path, candidate, second)
    real_flock = fcntl.flock
    real_fsync = os.fsync
    waiting_fd = None
    interleaved = False
    first = finding(candidate)
    first_row = encoded_finding_row(first)

    def interleaving_flock(descriptor: int, operation: int):
        nonlocal waiting_fd, interleaved
        if (
            operation == fcntl.LOCK_EX
            and not interleaved
            and stat.S_ISREG(os.fstat(descriptor).st_mode)
        ):
            interleaved = True
            waiting_fd = descriptor
            prior_fd = os.open(
                tmp_path / "audit-findings.jsonl", os.O_WRONLY | os.O_APPEND
            )
            try:
                assert os.write(prior_fd, first_row) == len(first_row)
                real_fsync(prior_fd)
            finally:
                os.close(prior_fd)
        return real_flock(descriptor, operation)

    def fail_waiting_writer_fsync(descriptor: int):
        if interleaved and descriptor == waiting_fd:
            raise OSError("injected waiting writer fsync failure")
        return real_fsync(descriptor)

    monkeypatch.setattr(fcntl, "flock", interleaving_flock)
    monkeypatch.setattr(os, "fsync", fail_waiting_writer_fsync)
    with pytest.raises(OSError, match="waiting writer"):
        append_audit_finding(tmp_path, finding(second, "missing"))

    assert load_audit_findings(tmp_path, manifest) == (first,)


def test_owned_manifest_temp_hardlink_is_recovered_after_crash_window(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    manifest = make_manifest(tmp_path, candidate)
    real_unlink = os.unlink

    def fail_install_cleanup(path, *args, **kwargs):
        name = os.fspath(path)
        if name == "audit-manifest.json" or name.startswith(
            ".audit-manifest.json."
        ):
            raise OSError("simulated crash-window unlink failure")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", fail_install_cleanup)
    with pytest.raises(OSError, match="crash-window"):
        write_audit_manifest(tmp_path, manifest)
    final_path = tmp_path / "audit-manifest.json"
    assert final_path.stat().st_nlink == 2
    assert len(list(tmp_path.glob(".audit-manifest.json.*.tmp"))) == 1

    monkeypatch.setattr(os, "unlink", real_unlink)
    assert load_audit_manifest(final_path) == manifest
    assert final_path.stat().st_nlink == 1
    assert not list(tmp_path.glob(".audit-manifest.json.*.tmp"))


@pytest.mark.parametrize("link_stage", ["after_write", "after_fsync"])
def test_checkpoint_post_write_hardlink_race_rolls_back_without_data_loss(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    monkeypatch,
    link_stage: str,
):
    second = replace(
        candidate,
        job_id="job-2",
        movie_code="abc-001",
        subtitle_id="11111111-2222-4333-8444-555555555555",
        storage_path="abc/abc-001/abc-001-English_AI.srt",
    )
    manifest = write_manifest(tmp_path, candidate, second)
    append_audit_finding(tmp_path, finding(candidate))
    checkpoint = tmp_path / "audit-findings.jsonl"
    external = tmp_path / f"external-{link_stage}"
    before = checkpoint.read_bytes()
    checkpoint_inode = checkpoint.stat().st_ino
    real_write = os.write
    real_fsync = os.fsync
    linked = False

    def is_checkpoint(descriptor: int) -> bool:
        return os.fstat(descriptor).st_ino == checkpoint_inode

    def racing_write(descriptor: int, data: bytes):
        nonlocal linked
        written = real_write(descriptor, data)
        if link_stage == "after_write" and not linked and is_checkpoint(descriptor):
            os.link(checkpoint, external)
            linked = True
        return written

    def racing_fsync(descriptor: int):
        nonlocal linked
        result = real_fsync(descriptor)
        if link_stage == "after_fsync" and not linked and is_checkpoint(descriptor):
            os.link(checkpoint, external)
            linked = True
        return result

    monkeypatch.setattr(os, "write", racing_write)
    monkeypatch.setattr(os, "fsync", racing_fsync)
    with pytest.raises(ValueError, match="hardlink|private"):
        append_audit_finding(tmp_path, finding(second, "missing"))

    assert linked is True
    assert checkpoint.read_bytes() == before
    assert external.read_bytes() == before
    external.unlink()
    assert load_audit_findings(tmp_path, manifest) == (finding(candidate),)


@pytest.mark.parametrize("replacement_stage", ["before_write", "after_write", "after_fsync"])
def test_checkpoint_append_rejects_published_leaf_replacement_and_rolls_back(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    monkeypatch,
    replacement_stage: str,
):
    second = second_candidate(candidate)
    write_manifest(tmp_path, candidate, second)
    append_audit_finding(tmp_path, finding(candidate))
    checkpoint = tmp_path / "audit-findings.jsonl"
    detached = tmp_path / f"detached-{replacement_stage}.jsonl"
    before = checkpoint.read_bytes()
    checkpoint_inode = checkpoint.stat().st_ino
    real_fchmod = os.fchmod
    real_write = os.write
    real_fsync = os.fsync
    replaced = False

    def is_checkpoint(descriptor: int) -> bool:
        return os.fstat(descriptor).st_ino == checkpoint_inode

    def replace_once() -> None:
        nonlocal replaced
        replace_published_leaf(checkpoint, detached, before)
        replaced = True

    def replacing_fchmod(descriptor: int, mode: int):
        result = real_fchmod(descriptor, mode)
        if replacement_stage == "before_write" and not replaced and is_checkpoint(descriptor):
            replace_once()
        return result

    def replacing_write(descriptor: int, data: bytes):
        written = real_write(descriptor, data)
        if replacement_stage == "after_write" and not replaced and is_checkpoint(descriptor):
            replace_once()
        return written

    def replacing_fsync(descriptor: int):
        result = real_fsync(descriptor)
        if replacement_stage == "after_fsync" and not replaced and is_checkpoint(descriptor):
            replace_once()
        return result

    monkeypatch.setattr(os, "fchmod", replacing_fchmod)
    monkeypatch.setattr(os, "write", replacing_write)
    monkeypatch.setattr(os, "fsync", replacing_fsync)
    with pytest.raises(ValueError, match="identity|published|private|changed"):
        append_audit_finding(tmp_path, finding(second, "missing"))

    assert replaced is True
    assert checkpoint.read_bytes() == before
    assert stat.S_IMODE(checkpoint.stat().st_mode) == 0o640
    assert detached.read_bytes() == before


def test_checkpoint_load_rejects_published_leaf_replacement_after_read(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    manifest = write_manifest(tmp_path, candidate)
    expected = finding(candidate)
    append_audit_finding(tmp_path, expected)
    checkpoint = tmp_path / "audit-findings.jsonl"
    detached = tmp_path / "detached-checkpoint.jsonl"
    before = checkpoint.read_bytes()
    checkpoint_inode = checkpoint.stat().st_ino
    real_load = report_module._load_findings_from_fd
    replaced = False

    def replacing_load(descriptor: int, loaded_manifest):
        nonlocal replaced
        findings = real_load(descriptor, loaded_manifest)
        if not replaced and os.fstat(descriptor).st_ino == checkpoint_inode:
            replace_published_leaf(checkpoint, detached, before)
            replaced = True
        return findings

    monkeypatch.setattr(report_module, "_load_findings_from_fd", replacing_load)
    with pytest.raises(ValueError, match="identity|published|private|changed"):
        load_audit_findings(tmp_path, manifest)

    assert replaced is True
    assert checkpoint.read_bytes() == before
    assert stat.S_IMODE(checkpoint.stat().st_mode) == 0o640


@pytest.mark.parametrize("artifact", ["manifest", "report"])
def test_artifact_read_rejects_published_leaf_replacement_after_read(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    monkeypatch,
    artifact: str,
):
    manifest = write_manifest(tmp_path, candidate)
    if artifact == "manifest":
        path = tmp_path / "audit-manifest.json"
        label = "audit manifest"
        loader = lambda: load_audit_manifest(path)
    else:
        append_audit_finding(tmp_path, finding(candidate))
        finalize_audit_report(tmp_path)
        path = tmp_path / "audit-report.json"
        label = "audit report"
        loader = lambda: load_audit_report(path)
    detached = tmp_path / f"detached-{artifact}.json"
    before = path.read_bytes()
    artifact_inode = path.stat().st_ino
    real_read = report_module._read_fd_limited
    replaced = False

    def replacing_read(descriptor: int, read_label: str, limit: int):
        nonlocal replaced
        data = real_read(descriptor, read_label, limit)
        if (
            not replaced
            and read_label == label
            and os.fstat(descriptor).st_ino == artifact_inode
        ):
            replace_published_leaf(path, detached, before)
            replaced = True
        return data

    monkeypatch.setattr(report_module, "_read_fd_limited", replacing_read)
    with pytest.raises(ValueError, match="identity|published|private|changed"):
        loader()

    assert replaced is True
    assert path.read_bytes() == before
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


def test_new_manifest_install_rejects_replaced_published_leaf(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    manifest = make_manifest(tmp_path, candidate)
    path = tmp_path / "audit-manifest.json"
    detached = tmp_path / "detached-installed-manifest.json"
    replacement = b"replacement-manifest-content"
    real_fsync_directory = report_module._fsync_directory_fd
    replaced = False

    def replacing_directory_fsync(directory_fd: int):
        nonlocal replaced
        result = real_fsync_directory(directory_fd)
        if not replaced and path.exists():
            replace_published_leaf(path, detached, replacement)
            replaced = True
        return result

    monkeypatch.setattr(report_module, "_fsync_directory_fd", replacing_directory_fsync)
    with pytest.raises(ValueError, match="identity|published|private|changed"):
        write_audit_manifest(tmp_path, manifest)

    assert replaced is True
    assert path.read_bytes() == replacement
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


@pytest.mark.parametrize("artifact", ["manifest", "report"])
def test_idempotent_artifact_write_rejects_published_leaf_replacement(
    tmp_path: Path,
    candidate: AuditCandidateSnapshot,
    monkeypatch,
    artifact: str,
):
    manifest = write_manifest(tmp_path, candidate)
    if artifact == "manifest":
        path = tmp_path / "audit-manifest.json"
        operation = lambda: write_audit_manifest(tmp_path, manifest)
    else:
        append_audit_finding(tmp_path, finding(candidate))
        expected = finalize_audit_report(tmp_path)
        path = tmp_path / "audit-report.json"
        operation = lambda: finalize_audit_report(tmp_path)
        assert load_audit_report(path) == expected
    path.chmod(0o644)
    detached = tmp_path / f"detached-idempotent-{artifact}.json"
    replacement = path.read_bytes()
    artifact_inode = path.stat().st_ino
    real_fchmod = os.fchmod
    replaced = False

    def replacing_fchmod(descriptor: int, mode: int):
        nonlocal replaced
        result = real_fchmod(descriptor, mode)
        if not replaced and os.fstat(descriptor).st_ino == artifact_inode:
            replace_published_leaf(path, detached, replacement)
            replaced = True
        return result

    monkeypatch.setattr(os, "fchmod", replacing_fchmod)
    with pytest.raises(ValueError, match="identity|published|private|changed"):
        operation()

    assert replaced is True
    assert path.read_bytes() == replacement
    assert stat.S_IMODE(path.stat().st_mode) == 0o640


def test_report_uses_derived_budget_for_valid_base_limited_inputs(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    manifest = write_manifest(tmp_path, candidate)
    append_audit_finding(tmp_path, large_valid_finding(candidate))
    manifest_size = (tmp_path / "audit-manifest.json").stat().st_size
    checkpoint_size = (tmp_path / "audit-findings.jsonl").stat().st_size
    base_limit = max(manifest_size, checkpoint_size)
    monkeypatch.setattr(report_module, "MAX_AUDIT_DOCUMENT_BYTES", base_limit)

    report = finalize_audit_report(tmp_path)

    report_path = tmp_path / "audit-report.json"
    assert report_path.stat().st_size > base_limit
    assert load_audit_report(report_path) == report


def test_report_rejects_exact_bytes_one_over_report_specific_limit(
    tmp_path: Path, candidate: AuditCandidateSnapshot, monkeypatch
):
    write_manifest(tmp_path, candidate)
    append_audit_finding(tmp_path, finding(candidate))
    report = finalize_audit_report(tmp_path)
    report_path = tmp_path / "audit-report.json"
    report_size = report_path.stat().st_size
    report_path.unlink()
    monkeypatch.setattr(
        report_module,
        "_max_audit_report_bytes",
        lambda: report_size - 1,
        raising=False,
    )

    with pytest.raises(ValueError, match="report|large|limit"):
        finalize_audit_report(tmp_path)

    assert report.complete is True
    assert not report_path.exists()
    assert not list(tmp_path.glob(".audit-report.json.*.tmp"))
