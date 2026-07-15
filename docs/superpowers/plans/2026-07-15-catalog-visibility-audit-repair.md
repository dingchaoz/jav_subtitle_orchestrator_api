# Catalog Visibility Audit and Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an operator-controlled audit and catalog-only repair workflow that finds ready subtitles missing from javsubtitle.com and resyncs them without translation or Supabase publication.

**Architecture:** Freeze verified publication receipts into an immutable audit manifest, classify exact public subtitle visibility through a reusable GET-only probe, and write a digest-protected final report. A separate repair runner accepts only unchanged repair-eligible findings, calls the existing catalog sync client one movie at a time, verifies exact public identity, and records resumable execution receipts without changing job state.

**Tech Stack:** Python 3.11, SQLite, requests, argparse, dataclasses, JSON/JSONL, pytest

---

## File Structure

- Create `orchestrator/catalog_visibility.py`: receipt snapshots, public visibility probe, classifications, and audit runner.
- Create `orchestrator/catalog_visibility_report.py`: manifest/checkpoint/final-report validation, canonical digesting, and atomic writes.
- Create `orchestrator/catalog_visibility_repair.py`: repair planning, authorization checks, execution receipts, resume, and failure stop policy.
- Modify `orchestrator/store.py`: public verified-receipt wrapper and deterministic ready-job candidate selection.
- Modify `orchestrator/catalog_sync.py`: reuse the public visibility probe after sync.
- Modify `orchestrator/__main__.py`: add audit and repair CLI entrypoints.
- Create `tests/test_catalog_visibility.py`: probe and audit behavior.
- Create `tests/test_catalog_visibility_report.py`: report integrity and checkpoint behavior.
- Create `tests/test_catalog_visibility_repair.py`: dry-run, execution safety, and resume behavior.
- Modify `tests/test_catalog_sync.py`: prove catalog sync still fails closed through the shared probe.
- Modify `tests/test_store_worker_claims.py`: candidate ordering and receipt validation coverage.
- Modify `docs/setup/mac.md`: operator runbook and production approval boundary.

### Task 1: Reusable exact public visibility probe

**Files:**
- Create: `orchestrator/catalog_visibility.py`
- Modify: `orchestrator/catalog_sync.py:341-377`
- Create: `tests/test_catalog_visibility.py`
- Modify: `tests/test_catalog_sync.py:466-540`

- [ ] **Step 1: Write failing probe classification tests**

Add fakes and focused tests to `tests/test_catalog_visibility.py`:

```python
from __future__ import annotations

import requests

from orchestrator.catalog_visibility import (
    PublicCatalogVisibilityClient,
    VisibilityStatus,
)


SUBTITLE_ID = "fc9bed2a-f432-45a6-b7f9-bf141dd61810"
SHA256 = "8" * 64


class Response:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        if isinstance(self._body, BaseException):
            raise self._body
        return self._body


class Session:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        if self.error:
            raise self.error
        return self.response


def test_probe_requires_exact_subtitle_identity():
    session = Session(Response(body={
        "canonicalCode": "ktb-111",
        "subtitles": [{"id": SUBTITLE_ID, "lang": "English_AI"}],
    }))
    result = PublicCatalogVisibilityClient(
        "https://javsubtitle.example", session=session
    ).check("ktb-111", SUBTITLE_ID, SHA256)

    assert result.status is VisibilityStatus.VISIBLE
    assert session.requests == [(
        f"https://javsubtitle.example/api/movie/ktb-111?cacheNonce={SHA256}",
        {"timeout": 30, "allow_redirects": False},
    )]


def test_probe_classifies_valid_payload_without_expected_id_as_missing():
    result = PublicCatalogVisibilityClient(
        "https://javsubtitle.example",
        session=Session(Response(body={"canonicalCode": "ktb-111", "subtitles": []})),
    ).check("ktb-111", SUBTITLE_ID, SHA256)
    assert result.status is VisibilityStatus.MISSING


def test_probe_classifies_404_separately():
    result = PublicCatalogVisibilityClient(
        "https://javsubtitle.example", session=Session(Response(status_code=404))
    ).check("ktb-111", SUBTITLE_ID, SHA256)
    assert result.status is VisibilityStatus.NOT_FOUND


def test_probe_fails_closed_on_redirect_invalid_json_and_network_error():
    redirect = PublicCatalogVisibilityClient(
        "https://javsubtitle.example", session=Session(Response(status_code=302))
    ).check("ktb-111", SUBTITLE_ID, SHA256)
    invalid = PublicCatalogVisibilityClient(
        "https://javsubtitle.example", session=Session(Response(body=ValueError("bad")))
    ).check("ktb-111", SUBTITLE_ID, SHA256)
    failed = PublicCatalogVisibilityClient(
        "https://javsubtitle.example",
        session=Session(error=requests.ConnectionError("secret details")),
    ).check("ktb-111", SUBTITLE_ID, SHA256)
    assert redirect.status is VisibilityStatus.FETCH_FAILED
    assert invalid.status is VisibilityStatus.RESPONSE_INVALID
    assert failed.status is VisibilityStatus.FETCH_FAILED
    assert "secret details" not in repr(failed)


def test_probe_rejects_malformed_subtitle_rows_instead_of_calling_them_missing():
    result = PublicCatalogVisibilityClient(
        "https://javsubtitle.example",
        session=Session(Response(body={
            "canonicalCode": "ktb-111",
            "subtitles": [{"language": "English_AI"}],
        })),
    ).check("ktb-111", SUBTITLE_ID, SHA256)
    assert result.status is VisibilityStatus.RESPONSE_INVALID
```

- [ ] **Step 2: Run the new tests and confirm the module is missing**

Run: `pytest tests/test_catalog_visibility.py -q`

Expected: FAIL during collection with `ModuleNotFoundError: orchestrator.catalog_visibility`.

- [ ] **Step 3: Implement the probe and immutable result types**

Create `orchestrator/catalog_visibility.py` with these public contracts and validation rules:

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol
from urllib.parse import quote, urlencode, urlsplit

import requests

from orchestrator.movie_code import canonical_movie_code


class VisibilityStatus(str, Enum):
    VISIBLE = "visible"
    MISSING = "missing"
    NOT_FOUND = "not_found"
    FETCH_FAILED = "fetch_failed"
    RESPONSE_INVALID = "response_invalid"
    INVALID_RECEIPT = "invalid_receipt"


@dataclass(frozen=True, slots=True)
class PublicVisibilityResult:
    status: VisibilityStatus
    canonical_code: str
    expected_subtitle_id: str
    observed_subtitle_ids: tuple[str, ...] = ()
    reason_code: str | None = None


class VisibilitySession(Protocol):
    def get(self, url: str, **kwargs: Any) -> Any: ...


_LOCAL_HTTP_HOSTS = {"localhost", "127.0.0.1", "::1"}


def normalize_catalog_api_origin(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url or base_url != base_url.strip():
        raise ValueError("catalog API base URL is invalid")
    try:
        parsed = urlsplit(base_url)
        hostname = parsed.hostname
        has_credentials = parsed.username is not None or parsed.password is not None
    except ValueError:
        raise ValueError("catalog API base URL is invalid") from None
    valid_transport = parsed.scheme == "https" or (
        parsed.scheme == "http" and hostname in _LOCAL_HTTP_HOSTS
    )
    if (
        not valid_transport
        or not parsed.netloc
        or not hostname
        or has_credentials
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("catalog API base URL is invalid")
    return f"{parsed.scheme}://{parsed.netloc}"


class PublicCatalogVisibilityClient:
    def __init__(self, base_url: str, *, timeout_seconds: int = 30, session=None):
        self.base_url = normalize_catalog_api_origin(base_url)
        self.timeout_seconds = timeout_seconds
        self.session: VisibilitySession = session or requests.Session()

    def check(self, movie_code: str, expected_subtitle_id: str, content_sha256: str):
        canonical = canonical_movie_code(movie_code)
        if not expected_subtitle_id:
            raise ValueError("expected subtitle id is required")
        if len(content_sha256) != 64 or any(c not in "0123456789abcdef" for c in content_sha256):
            raise ValueError("content sha256 is invalid")
        url = (
            f"{self.base_url}/api/movie/{quote(canonical, safe='')}?"
            f"{urlencode({'cacheNonce': content_sha256})}"
        )
        try:
            response = self.session.get(
                url, timeout=self.timeout_seconds, allow_redirects=False
            )
        except requests.RequestException:
            return PublicVisibilityResult(
                VisibilityStatus.FETCH_FAILED, canonical, expected_subtitle_id,
                reason_code="public_visibility_fetch_failed",
            )
        if 300 <= response.status_code < 400:
            return PublicVisibilityResult(
                VisibilityStatus.FETCH_FAILED, canonical, expected_subtitle_id,
                reason_code="public_visibility_redirect_rejected",
            )
        if response.status_code == 404:
            return PublicVisibilityResult(
                VisibilityStatus.NOT_FOUND, canonical, expected_subtitle_id,
                reason_code="public_visibility_not_found",
            )
        if response.status_code != 200:
            return PublicVisibilityResult(
                VisibilityStatus.FETCH_FAILED, canonical, expected_subtitle_id,
                reason_code="public_visibility_fetch_failed",
            )
        try:
            payload = response.json()
        except (TypeError, ValueError):
            payload = None
        if (
            not isinstance(payload, dict)
            or payload.get("canonicalCode") != canonical
            or not isinstance(payload.get("subtitles"), list)
        ):
            return PublicVisibilityResult(
                VisibilityStatus.RESPONSE_INVALID, canonical, expected_subtitle_id,
                reason_code="public_visibility_response_invalid",
            )
        if any(
            not isinstance(row, dict) or not isinstance(row.get("id"), str)
            for row in payload["subtitles"]
        ):
            return PublicVisibilityResult(
                VisibilityStatus.RESPONSE_INVALID, canonical, expected_subtitle_id,
                reason_code="public_visibility_response_invalid",
            )
        ids = tuple(row["id"] for row in payload["subtitles"])
        matches = sum(value == expected_subtitle_id for value in ids)
        return PublicVisibilityResult(
            VisibilityStatus.VISIBLE if matches == 1 else VisibilityStatus.MISSING,
            canonical,
            expected_subtitle_id,
            observed_subtitle_ids=ids,
            reason_code=None if matches == 1 else "public_visibility_mismatch",
        )
```

Move the existing origin validation behavior out of `catalog_sync.py` into
`normalize_catalog_api_origin`; `CatalogSyncClient._endpoints` must call the same
helper. Preserve HTTPS-only production origins and explicit HTTP support for
`localhost`, `127.0.0.1`, and `::1`. Add tests for valid local HTTP, credentials,
paths, queries, fragments, and malformed ports.

- [ ] **Step 4: Make `CatalogSyncClient` delegate public verification to the probe**

Construct one `PublicCatalogVisibilityClient` with the same base URL, timeout, and session. Replace `_verify_public_visibility` parsing with status-to-error mapping:

```python
result = self.public_visibility_client.check(
    canonical,
    expected_subtitle_id,
    expected_content_sha256,
)
reason_by_status = {
    VisibilityStatus.NOT_FOUND: "public_visibility_not_found",
    VisibilityStatus.FETCH_FAILED: result.reason_code or "public_visibility_fetch_failed",
    VisibilityStatus.RESPONSE_INVALID: "public_visibility_response_invalid",
    VisibilityStatus.MISSING: "public_visibility_mismatch",
}
if result.status is not VisibilityStatus.VISIBLE:
    raise CatalogSyncError(reason_by_status[result.status])
```

- [ ] **Step 5: Accept the current and strengthened v4 sync response contracts**

Add strict `tests/test_catalog_sync.py` fixtures for both the currently deployed
response and this forward-compatible v4 response:

```python
{
    "success": True,
    "requested": 1,
    "synced": 1,
    "failed": [],
    "cacheSchemaVersion": "v4",
    "results": [{
        "canonicalCode": "ktb-111",
        "d1RowsUpdated": 1,
        "d1Verified": True,
        "subtitleCount": 1,
        "kvAction": "written",
        "kvKeysTouched": [
            "movie:full:v4:ktb-111",
            "movie:light:ktb-111",
        ],
        "kvKeysDeleted": [
            "movie:full:v4:ktb-111",
            "movie:light:ktb-111",
        ],
    }],
}
```

Recognize only `v<positive integer>` schema values; require `d1Verified is True`,
`kvAction` in the documented enum, identical touched/compatibility arrays, exactly
one matching versioned full key, and the canonical light key. Continue to accept
the two existing production schemas unchanged. Reject partial mixtures and unknown
extra fields. This orchestrator compatibility change must reach production before
the website v4 response is deployed.

- [ ] **Step 6: Run focused compatibility tests**

Run: `pytest tests/test_catalog_visibility.py tests/test_catalog_sync.py -q`

Expected: PASS.

- [ ] **Step 7: Commit the shared probe and sync compatibility**

```bash
git add orchestrator/catalog_visibility.py orchestrator/catalog_sync.py \
  tests/test_catalog_visibility.py tests/test_catalog_sync.py
git commit -m "refactor: share exact catalog visibility probe"
```

### Task 2: Deterministic verified-receipt candidates

**Files:**
- Modify: `orchestrator/store.py:86-151,947-959`
- Modify: `orchestrator/catalog_visibility.py`
- Modify: `tests/test_store_worker_claims.py`
- Modify: `tests/test_catalog_visibility.py`

- [ ] **Step 1: Write failing candidate and snapshot tests**

Add tests that create ready, failed, ready-without-receipt, and ready-with-valid-receipt jobs. Assert every ready row is selected, including the invalid one, while failed rows are excluded:

```python
candidates = store.list_catalog_visibility_candidates(
    allowlist={"ktb-111", "iene-963"}
)
assert [job.normalized_movie_number for job in candidates] == ["ktb-111", "iene-963"]
snapshot = AuditCandidateSnapshot.from_job(candidates[0]).validated_receipt()
assert snapshot.subtitle_id == candidates[0].published_subtitle_id
assert snapshot.content_sha256 == candidates[0].published_content_sha256
```

Also assert invalid `limit`, an empty allowlist member, a wrong storage path, and a non-UUID subtitle ID fail closed.

- [ ] **Step 2: Run focused tests and confirm missing APIs**

Run: `pytest tests/test_store_worker_claims.py -k catalog_visibility -q && pytest tests/test_catalog_visibility.py -k receipt -q`

Expected: FAIL because the store method and snapshot do not exist.

- [ ] **Step 3: Expose a public wrapper around the existing receipt validator**

Add this wrapper beside `_validate_verified_supabase_receipt` without changing existing call sites:

```python
def validate_verified_supabase_receipt(**kwargs: object) -> None:
    _validate_verified_supabase_receipt(**kwargs)
```

Do not duplicate validation logic.

- [ ] **Step 4: Implement deterministic candidate selection**

Add to `JobStore`:

```python
def list_catalog_visibility_candidates(
    self,
    *,
    allowlist: set[str] | None = None,
    limit: int | None = None,
) -> list[JobRecord]:
    if limit is not None and (isinstance(limit, bool) or limit < 1):
        raise ValueError("limit must be a positive integer")
    canonical_allowlist = (
        {canonical_movie_code(code) for code in allowlist}
        if allowlist is not None else None
    )
    rows = self.list_jobs(JobStatus.ENGLISH_SRT_READY)
    selected = [
        row for row in rows
        if (
            canonical_allowlist is None
            or canonical_movie_code(row.normalized_movie_number) in canonical_allowlist
        )
    ]
    selected.sort(key=lambda row: (row.updated_at, row.id))
    return selected[:limit] if limit is not None else selected
```

- [ ] **Step 5: Implement immutable candidate and receipt snapshots**

Add `AuditCandidateSnapshot` with exactly these fields. Its `from_job` constructor
canonicalizes only the movie code and does not validate receipt fields, allowing an
invalid ready row to be frozen and reported. Its `validated_receipt()` method calls
the existing validator and returns the non-null `PublicationReceiptSnapshot` below.

```python
@dataclass(frozen=True, slots=True)
class AuditCandidateSnapshot:
    job_id: str
    movie_code: str
    movie_uuid: str | None
    metadata_status: str | None
    metadata_source: str | None
    subtitle_id: str | None
    storage_path: str | None
    content_sha256: str | None
    file_size: int | None
    job_updated_at: str

    @classmethod
    def from_job(cls, job: JobRecord) -> "AuditCandidateSnapshot":
        return cls(
            job_id=job.id,
            movie_code=canonical_movie_code(job.normalized_movie_number),
            movie_uuid=job.catalog_movie_uuid,
            metadata_status=job.metadata_status,
            metadata_source=job.metadata_source,
            subtitle_id=job.published_subtitle_id,
            storage_path=job.published_storage_path,
            content_sha256=job.published_content_sha256,
            file_size=job.published_file_size,
            job_updated_at=job.updated_at,
        )

    def validated_receipt(self) -> "PublicationReceiptSnapshot":
        return PublicationReceiptSnapshot.from_candidate(self)
```

Add `PublicationReceiptSnapshot` with exactly these fields:

```python
@dataclass(frozen=True, slots=True)
class PublicationReceiptSnapshot:
    job_id: str
    movie_code: str
    movie_uuid: str
    metadata_status: str
    metadata_source: str
    subtitle_id: str
    storage_path: str
    content_sha256: str
    file_size: int
    job_updated_at: str

    @classmethod
    def from_candidate(
        cls, candidate: AuditCandidateSnapshot
    ) -> "PublicationReceiptSnapshot":
        validate_verified_supabase_receipt(
            movie_code=candidate.movie_code,
            movie_uuid=candidate.movie_uuid,
            metadata_status=candidate.metadata_status,
            metadata_source=candidate.metadata_source,
            subtitle_id=candidate.subtitle_id,
            storage_path=candidate.storage_path,
            content_sha256=candidate.content_sha256,
            file_size=candidate.file_size,
        )
        return cls(
            job_id=candidate.job_id,
            movie_code=candidate.movie_code,
            movie_uuid=candidate.movie_uuid,
            metadata_status=candidate.metadata_status,
            metadata_source=candidate.metadata_source,
            subtitle_id=candidate.subtitle_id,
            storage_path=candidate.storage_path,
            content_sha256=candidate.content_sha256,
            file_size=candidate.file_size,
            job_updated_at=candidate.job_updated_at,
        )
```

Use assertions or a private typed constructor after validation so static type checking does not require ignores.

- [ ] **Step 6: Run tests and commit**

Run: `pytest tests/test_store_worker_claims.py -k catalog_visibility -q && pytest tests/test_catalog_visibility.py -k receipt -q`

Expected: PASS.

```bash
git add orchestrator/store.py orchestrator/catalog_visibility.py \
  tests/test_store_worker_claims.py tests/test_catalog_visibility.py
git commit -m "feat: select verified catalog visibility candidates"
```

### Task 3: Immutable manifest, checkpoint, and final report

**Files:**
- Create: `orchestrator/catalog_visibility_report.py`
- Create: `tests/test_catalog_visibility_report.py`

- [ ] **Step 1: Write failing report integrity tests**

Cover manifest atomic write/read, duplicate job rejection, JSONL checkpoint resume, final report digest stability, changed-field rejection, symlink rejection, and incomplete-report rejection. Use this required public surface:

```python
manifest = create_audit_manifest(
    api_origin="https://javsubtitle.example",
    database_path=tmp_path / "jobs.sqlite3",
    candidates=(candidate,),
    selection={"allowlist": ["ktb-111"], "limit": 1},
)
write_audit_manifest(output_dir, manifest)
append_audit_finding(output_dir, finding)
report = finalize_audit_report(output_dir)
assert report.report_sha256 == audit_report_sha256(report)
assert load_audit_report(output_dir / "audit-report.json") == report
```

- [ ] **Step 2: Run tests and confirm the module is missing**

Run: `pytest tests/test_catalog_visibility_report.py -q`

Expected: FAIL during collection.

- [ ] **Step 3: Implement explicit versioned report dataclasses**

Define:

```python
AUDIT_REPORT_SCHEMA_VERSION = 1
REPAIR_ELIGIBLE = frozenset({"missing", "not_found"})

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
```

Hash the resolved database path, not its contents, and never write credentials.

- [ ] **Step 4: Implement canonical digest and secure atomic writes**

Use sorted compact JSON and exclude `report_sha256` from the digest input:

```python
def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")

def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), 0o600)
        json.dump(payload, handle, sort_keys=True, ensure_ascii=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
```

Validate exact field sets, canonical codes, classifications, one finding per
manifest candidate, and `complete=True` before accepting a final report. For
non-`invalid_receipt` findings, require `candidate.validated_receipt()` to pass.
For `invalid_receipt`, require it to fail; this prevents a valid receipt from being
misclassified. Validate UUIDs, hashes, and positive sizes through that one receipt
validator rather than a second copy.

- [ ] **Step 5: Implement append-safe JSONL checkpointing**

Open `audit-findings.jsonl` with mode `a`, permissions `0600`, one canonical JSON object per line, `flush`, and `os.fsync`. Loader rejects malformed lines, duplicate job IDs, candidate identities not in the manifest, and changed candidate fields.

- [ ] **Step 6: Run tests and commit**

Run: `pytest tests/test_catalog_visibility_report.py -q`

Expected: PASS.

```bash
git add orchestrator/catalog_visibility_report.py tests/test_catalog_visibility_report.py
git commit -m "feat: add immutable catalog visibility reports"
```

### Task 4: GET-only audit runner with safe resume

**Files:**
- Modify: `orchestrator/catalog_visibility.py`
- Modify: `orchestrator/catalog_visibility_report.py`
- Modify: `tests/test_catalog_visibility.py`

- [ ] **Step 1: Write failing audit runner tests**

Test that the runner freezes candidates before requests, resumes terminal findings, classifies invalid receipts without a request, never calls a mutating session method, writes a complete report, and refuses a changed API origin or selection on resume.

Required call shape:

```python
summary = CatalogVisibilityAuditor(store, visibility_client).scan(
    output_dir,
    allowlist={"ktb-111"},
    limit=1,
)
assert summary.counts == {"missing": 1}
assert summary.report_path == output_dir / "audit-report.json"
assert session.methods == ["GET"]
```

- [ ] **Step 2: Run tests and confirm the runner is absent**

Run: `pytest tests/test_catalog_visibility.py -k audit -q`

Expected: FAIL with missing `CatalogVisibilityAuditor`.

- [ ] **Step 3: Implement manifest-first audit flow**

Add:

```python
@dataclass(frozen=True, slots=True)
class AuditRunSummary:
    discovered: int
    checked: int
    skipped: int
    counts: dict[str, int]
    report_path: Path
    report_sha256: str

class CatalogVisibilityAuditor:
    def __init__(self, store: JobStore, client: PublicCatalogVisibilityClient):
        self.store = store
        self.client = client

    def scan(self, output_dir: Path, *, allowlist=None, limit=None) -> AuditRunSummary:
        # Create or validate the frozen manifest.
        # Load and validate existing checkpoint rows.
        # For each unfinished candidate with a valid receipt, call client.check once.
        # Append and fsync one terminal finding.
        # Finalize only when every manifest candidate has one finding.
        # Return aggregate counts and digest.
```

Freeze `AuditCandidateSnapshot` values before any request. For each unfinished
candidate, call `validated_receipt()`. Catch only its documented receipt-validation
exception and append `invalid_receipt` without a request. Do not convert
programming errors into findings.

- [ ] **Step 4: Prove resume and read-only behavior**

Run: `pytest tests/test_catalog_visibility.py -k 'audit or resume or read_only' -q`

Expected: PASS.

- [ ] **Step 5: Commit the audit runner**

```bash
git add orchestrator/catalog_visibility.py orchestrator/catalog_visibility_report.py \
  tests/test_catalog_visibility.py
git commit -m "feat: audit exact public subtitle visibility"
```

### Task 5: Digest-protected repair planning

**Files:**
- Create: `orchestrator/catalog_visibility_repair.py`
- Create: `tests/test_catalog_visibility_repair.py`

- [ ] **Step 1: Write failing repair-plan tests**

Cover visible/fetch-failed exclusion, missing/not-found inclusion, deterministic order, wrong digest, altered report, duplicate code, API-origin mismatch, receipt change, and dry-run no POST.

```python
plan = plan_catalog_visibility_repair(
    store,
    report_path,
    expected_api_origin="https://javsubtitle.example",
    output_dir=tmp_path / "repair-plan",
)
assert [item.movie_code for item in plan.items] == ["ktb-111"]
assert plan.report_sha256 == report.report_sha256
assert plan.skipped == {"visible": 1, "fetch_failed": 1}
```

- [ ] **Step 2: Run tests and confirm the module is missing**

Run: `pytest tests/test_catalog_visibility_repair.py -k plan -q`

Expected: FAIL during collection.

- [ ] **Step 3: Implement repair plan types and current-receipt comparison**

```python
@dataclass(frozen=True, slots=True)
class RepairPlanItem:
    receipt: PublicationReceiptSnapshot
    starting_status: str

@dataclass(frozen=True, slots=True)
class CatalogVisibilityRepairPlan:
    report_path: Path
    report_sha256: str
    api_origin: str
    items: tuple[RepairPlanItem, ...]
    skipped: dict[str, int]
```

For each repair-eligible finding, obtain its validated receipt, reload
`store.get_job(job_id)`, require status `english_srt_ready`, rebuild
`AuditCandidateSnapshot.from_job(...).validated_receipt()`, and compare the full
receipt dataclass. Changed or missing receipts are excluded as `receipt_changed`;
they are never repaired. Serialize the exact ordered dry-run plan atomically to
`repair-plan.json`, including the source report digest and every selected receipt;
the CLI prints that absolute path. Loading the plan must recompute and verify its
own digest before execution.

- [ ] **Step 4: Run plan tests and commit**

Run: `pytest tests/test_catalog_visibility_repair.py -k plan -q`

Expected: PASS.

```bash
git add orchestrator/catalog_visibility_repair.py tests/test_catalog_visibility_repair.py
git commit -m "feat: plan catalog-only visibility repairs"
```

### Task 6: Catalog-only execution, verification, and resume

**Files:**
- Modify: `orchestrator/catalog_visibility_repair.py`
- Modify: `tests/test_catalog_visibility_repair.py`

- [ ] **Step 1: Write failing execution safety tests**

Use a recording sync client and store snapshot. Assert:

- no call without `execute=True` and matching confirmation digest;
- one exact sync call per eligible movie;
- exact expected subtitle ID and content hash are passed;
- job row before and after execution is equal;
- authentication failure stops immediately;
- three consecutive remote failures stop before item four;
- a success requires post-sync exact public visibility;
- completed receipt rows are skipped on resume.

```python
result = execute_catalog_visibility_repair(
    store,
    plan,
    sync_client=recording_client,
    output_dir=tmp_path / "repair",
    execute=True,
    confirm_report_sha256=plan.report_sha256,
    consecutive_failure_limit=3,
)
assert result.repaired == ("ktb-111",)
assert store.get_job(job.id) == before
assert recording_client.calls == [(
    "ktb-111", job.published_subtitle_id, job.published_content_sha256
)]
```

- [ ] **Step 2: Run tests and confirm execution API is missing**

Run: `pytest tests/test_catalog_visibility_repair.py -k execute -q`

Expected: FAIL with missing executor.

- [ ] **Step 3: Implement execution result and receipt schema**

```python
@dataclass(frozen=True, slots=True)
class RepairExecutionResult:
    action: str
    repaired: tuple[str, ...]
    failed: tuple[str, ...]
    skipped_receipt_changed: tuple[str, ...]
    stopped_reason: str | None
    receipt_path: Path
```

Each JSONL receipt contains exact fields:

```python
{
    "report_sha256": plan.report_sha256,
    "job_id": item.receipt.job_id,
    "movie_code": item.receipt.movie_code,
    "expected_subtitle_id": item.receipt.subtitle_id,
    "starting_status": item.starting_status,
    "outcome": "repaired",  # or safe classified failure
    "reason_code": None,
    "finished_at": utc_now_iso(),
}
```

Never include the admin token, signed URLs, or subtitle bytes.

- [ ] **Step 4: Implement guarded execution**

Pseudocode must be implemented literally with one item at a time:

```python
if not execute:
    return dry_run_result(plan)
if confirm_report_sha256 != plan.report_sha256:
    raise ValueError("confirm_report_sha256 mismatch")
for item in unfinished_items:
    current = AuditCandidateSnapshot.from_job(
        require_ready_job(store, item.receipt.job_id)
    ).validated_receipt()
    if current != item.receipt:
        append_receipt("skipped_receipt_changed")
        continue
    try:
        sync_client.sync(
            item.receipt.movie_code,
            expected_subtitle_id=item.receipt.subtitle_id,
            expected_content_sha256=item.receipt.content_sha256,
        )
    except CatalogSyncError as exc:
        append_safe_failure(exc.reason_code)
        if exc.reason_code == "catalog_auth_failed":
            stop_immediately()
        increment_consecutive_failures()
    else:
        append_receipt("repaired")
        reset_consecutive_failures()
```

The existing `CatalogSyncClient.sync` performs the post-sync public identity check; do not add a second success path that skips it.

- [ ] **Step 5: Run execution tests and commit**

Run: `pytest tests/test_catalog_visibility_repair.py -q`

Expected: PASS.

```bash
git add orchestrator/catalog_visibility_repair.py tests/test_catalog_visibility_repair.py
git commit -m "feat: execute catalog-only visibility repairs"
```

### Task 7: CLI entrypoints and operator output

**Files:**
- Modify: `orchestrator/__main__.py:700-1125`
- Create: `tests/test_catalog_visibility_cli.py`

- [ ] **Step 1: Write failing parser and runner tests**

Require these interfaces:

```bash
python -m orchestrator catalog-visibility-audit \
  --output reports/catalog-visibility-20260715 \
  --allowlist ktb-111 iene-963 \
  --limit 2

python -m orchestrator catalog-visibility-repair \
  --report reports/catalog-visibility-20260715/audit-report.json \
  --output reports/catalog-visibility-repair-20260715

python -m orchestrator catalog-visibility-repair \
  --report reports/catalog-visibility-20260715/audit-report.json \
  --output reports/catalog-visibility-repair-20260715 \
  --execute \
  --confirm-report-sha256 <64-lowercase-hex>
```

Parser tests assert `--limit` is positive, `--execute` does not make confirmation optional, and the audit command has no mutation flag.

- [ ] **Step 2: Run tests and confirm commands are absent**

Run: `pytest tests/test_catalog_visibility_cli.py -q`

Expected: FAIL with invalid command choices.

- [ ] **Step 3: Add run functions and parser wiring**

`run_catalog_visibility_audit` builds settings, store, and `PublicCatalogVisibilityClient`, then prints:

```text
audit_complete=true discovered=<count> checked=<count> visible=<count> missing=<count> not_found=<count> fetch_failed=<count> response_invalid=<count> invalid_receipt=<count> report_sha256=<sha256> report=/absolute/path/audit-report.json
```

`run_catalog_visibility_repair` builds the plan first. Without `--execute`, print `action=dry_run`, exact count, digest, and resume command. With execution, require configured admin token and use `build_catalog_sync_client(settings)`.

- [ ] **Step 4: Run CLI tests and commit**

Run: `pytest tests/test_catalog_visibility_cli.py tests/test_process_lock.py -q`

Expected: PASS.

```bash
git add orchestrator/__main__.py tests/test_catalog_visibility_cli.py
git commit -m "feat: add catalog visibility audit and repair commands"
```

### Task 8: Runbook, complete regression suite, and production dry-run

**Files:**
- Modify: `docs/setup/mac.md`

- [ ] **Step 1: Document safety and exact operator sequence**

Add a section that states:

1. Audit is GET-only and resumable.
2. Review `audit-report.json`, counts, and digest.
3. Run repair without `--execute` and review the exact plan.
4. Begin with an allowlisted KTB-111 audit and repair canary.
5. Obtain explicit production approval before `--execute`.
6. Re-audit the repaired set and require `visible`.
7. Expand in bounded allowlisted batches before the full eligible set.

Include the exact CLI commands from Task 7 and state that neither command republishes or retranslates an SRT.

- [ ] **Step 2: Run formatting and focused tests**

Run:

```bash
python -m compileall -q orchestrator tests
pytest tests/test_catalog_visibility.py \
  tests/test_catalog_visibility_report.py \
  tests/test_catalog_visibility_repair.py \
  tests/test_catalog_visibility_cli.py \
  tests/test_catalog_sync.py \
  tests/test_store_worker_claims.py -q
```

Expected: PASS.

- [ ] **Step 3: Run the full orchestrator suite**

Run: `pytest -q`

Expected: PASS with no new failures.

- [ ] **Step 4: Commit the runbook**

```bash
git add docs/setup/mac.md
git commit -m "docs: add catalog visibility recovery runbook"
```

- [ ] **Step 5: Produce a GET-only KTB-111 audit artifact**

Run:

```bash
.venv/bin/python -m orchestrator catalog-visibility-audit \
  --output reports/catalog-visibility-ktb111-canary \
  --allowlist ktb-111 \
  --limit 1
```

Expected before repair: completed report, one `missing` finding, no POST request, and no database change.

- [ ] **Step 6: Stop at the production mutation gate**

Print the dry-run repair plan and its digest. Do not pass `--execute` until the user explicitly approves the KTB-111 production repair after reviewing the generated artifacts.
