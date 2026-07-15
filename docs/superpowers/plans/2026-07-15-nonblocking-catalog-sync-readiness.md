# Non-Blocking Catalog Sync Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make verified Supabase publication immediately and permanently ready while catalog synchronization runs as an observable, retryable, non-blocking follow-up.

**Architecture:** Add independent artifact/catalog fields to the existing SQLite job row, keep the public main status ready after Supabase verification, and make the catalog worker claim by its substate. Parse catalog responses semantically, persist bounded redacted diagnostics, restore the existing signed ready webhook, and add a dry-run-first reconciliation command that remotely verifies both Supabase row and Storage object before repairing legacy false failures.

**Tech Stack:** Python 3.11, FastAPI, Pydantic, SQLite, requests, pytest; no new third-party dependencies

---

## File Map

**Create:**

- `orchestrator/callbacks.py` — signed, per-client, idempotent ready webhook delivery.
- `orchestrator/catalog_sync_reconciliation.py` — candidate selection, remote verification orchestration, and dry-run/execute results.
- `tests/test_api_callbacks.py` — callback association, payload, timing, and failure isolation.
- `tests/test_catalog_sync_reconciliation.py` — legacy false-failure verification and repair.

**Modify:**

- `orchestrator/models.py` — artifact/catalog enums, warning model, additive public API fields.
- `orchestrator/store.py` — idempotent schema migration, independent catalog queue, warning diagnostics, callback persistence, reconciliation CAS.
- `orchestrator/catalog_sync.py` — semantic response parser, redaction, retry metadata, idempotency header.
- `orchestrator/supabase_publisher.py` — read-only verification of an existing publication receipt.
- `orchestrator/mac_worker.py` — ready transition/webhook before catalog, non-blocking catalog failure handling.
- `orchestrator/api.py` — additive response fields and callback client association.
- `orchestrator/config.py` — callback configuration and capped catalog backoff.
- `orchestrator/__main__.py` — construct notifier and add reconciliation CLI.
- `README.md`, `.env.example`, `docs/setup/mac.md` — new state contract and operator commands.
- Existing tests for catalog, store, worker, API, dashboard, historical repair, config, models, and publisher.

## Task 1: Public Models and Durable Columns

**Files:**

- Modify: `tests/test_models.py`
- Modify: `tests/test_api_jobs.py`
- Modify: `tests/test_store_submit.py`
- Modify: `orchestrator/models.py`
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/api.py`

- [ ] **Step 1: Write failing model/API tests for additive readiness fields**

Add assertions equivalent to:

```python
response = client.get(f"/jobs/{job.id}")
assert response.json() == {
    "id": job.id,
    "movie_number": "abc-001",
    "status": "queued",
    "job_dir_mac": str(mac_jobs_root / "abc-001"),
    "job_dir_windows": "M:\\abc-001",
    "error": None,
    "ready": False,
    "artifact_status": None,
    "catalog_sync_status": None,
    "warnings": [],
}
```

Add enum coverage:

```python
assert [value.value for value in ArtifactStatus] == ["ready"]
assert [value.value for value in CatalogSyncStatus] == [
    "pending", "succeeded", "failed"
]
```

- [ ] **Step 2: Run focused tests and confirm missing types/fields**

Run:

```bash
pytest tests/test_models.py tests/test_api_jobs.py -q
```

Expected: FAIL because `ArtifactStatus`, `CatalogSyncStatus`, and public response fields do not exist.

- [ ] **Step 3: Add models with backward-compatible defaults**

Implement in `orchestrator/models.py`:

```python
class ArtifactStatus(StrEnum):
    READY = "ready"


class CatalogSyncStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobWarning(BaseModel):
    code: str
    message: str


class JobResponse(BaseModel):
    id: str
    movie_number: str
    status: JobStatus
    job_dir_mac: str
    job_dir_windows: str
    error: str | None = None
    ready: bool = False
    artifact_status: ArtifactStatus | None = None
    catalog_sync_status: CatalogSyncStatus | None = None
    warnings: list[JobWarning] = Field(default_factory=list)
```

- [ ] **Step 4: Add nullable fields to `JobRecord` and idempotent schema initialization**

Add fields after the publication receipt:

```python
artifact_status: str | None
catalog_sync_status: str | None
catalog_sync_warning_code: str | None
catalog_sync_warning_message: str | None
catalog_sync_last_http_status: int | None
catalog_sync_last_response_json: str | None
catalog_sync_last_attempt_at: str | None
callback_client_key: str | None
```

Add missing columns through the existing `PRAGMA table_info(jobs)` loop. Do not
rewrite or drop the table. Add CHECK-free nullable text columns so older rows and
older binaries remain readable.

- [ ] **Step 5: Derive warnings and ready in one API helper**

Implement:

```python
def job_warnings(job: JobRecord) -> list[JobWarning]:
    if not job.catalog_sync_warning_code:
        return []
    return [JobWarning(
        code=job.catalog_sync_warning_code,
        message=job.catalog_sync_warning_message or "Catalog synchronization failed.",
    )]


def job_response(job: JobRecord) -> JobResponse:
    return JobResponse(
        id=job.id,
        movie_number=job.normalized_movie_number,
        status=job.status,
        job_dir_mac=job.job_dir_mac,
        job_dir_windows=job.job_dir_windows,
        error=job.error,
        ready=job.status is JobStatus.ENGLISH_SRT_READY,
        artifact_status=job.artifact_status,
        catalog_sync_status=job.catalog_sync_status,
        warnings=job_warnings(job),
    )
```

- [ ] **Step 6: Make row mapping tolerant of just-added columns**

Update `_row_to_job` to map every new field. Run initialization twice in a test
against both a new database and the legacy fixture schema.

- [ ] **Step 7: Run focused tests**

Run:

```bash
pytest tests/test_models.py tests/test_api_jobs.py tests/test_store_submit.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit Task 1**

```bash
git add orchestrator/models.py orchestrator/store.py orchestrator/api.py \
  tests/test_models.py tests/test_api_jobs.py tests/test_store_submit.py
git commit -m "feat: add independent artifact and catalog state"
```

## Task 2: Supabase Completion Becomes Immediately Ready

**Files:**

- Modify: `tests/test_store_worker_claims.py`
- Modify: `tests/test_mac_worker.py`
- Modify: `orchestrator/store.py`

- [ ] **Step 1: Replace blocking-publication expectations with failing ready expectations**

For a verified publication receipt, assert:

```python
completed = store.complete_supabase_publication(...)
assert completed.status is JobStatus.ENGLISH_SRT_READY
assert completed.artifact_status == ArtifactStatus.READY.value
assert completed.catalog_sync_status == CatalogSyncStatus.PENDING.value
assert completed.error is None
assert completed.claimed_by is None
assert completed.catalog_lease_token is None
```

Keep the existing invalid receipt and lost publication lease tests unchanged.

- [ ] **Step 2: Add an invariant test preventing catalog downgrade**

Prepare a verified publication, claim catalog work, exhaust it, and assert:

```python
failed_catalog = store.fail_catalog_sync(
    syncing.id,
    "catalog-worker",
    "catalog_sync_failed",
    retryable=True,
    http_status=500,
    response_json='{"success":false}',
    lease_token=syncing.catalog_lease_token,
    max_catalog_sync_attempts=1,
    retry_seconds=30,
    max_retry_seconds=300,
)
assert failed_catalog.status is JobStatus.ENGLISH_SRT_READY
assert failed_catalog.artifact_status == "ready"
assert failed_catalog.catalog_sync_status == "failed"
assert failed_catalog.error is None
assert failed_catalog.catalog_sync_warning_code == "catalog_sync_failed"
```

- [ ] **Step 3: Run the exact failing tests**

Run:

```bash
pytest tests/test_store_worker_claims.py -q -k 'supabase_publication or catalog_sync_claim_failure'
```

Expected: FAIL because completion still writes `catalog_sync_pending` and exhaustion still writes `failed`.

- [ ] **Step 4: Change `complete_supabase_publication` transaction**

Update only the post-verification assignment:

```sql
SET status = 'english_srt_ready',
    artifact_status = 'ready',
    catalog_sync_status = 'pending',
    catalog_sync_warning_code = NULL,
    catalog_sync_warning_message = NULL,
    catalog_sync_last_http_status = NULL,
    catalog_sync_last_response_json = NULL,
    catalog_sync_last_attempt_at = NULL,
    ...,
    error = NULL
```

Preserve the receipt, lease fencing, quality counter reset, and transaction
boundaries.

- [ ] **Step 5: Add a store invariant helper and use it in catalog mutations**

Implement a private validation that requires a valid verified receipt plus:

```python
if job.status is not JobStatus.ENGLISH_SRT_READY or job.artifact_status != "ready":
    raise ValueError("catalog work requires a ready published artifact")
```

Use it before catalog claim completion/failure updates.

- [ ] **Step 6: Run focused store tests**

Run:

```bash
pytest tests/test_store_worker_claims.py -q
```

Expected: some catalog queue tests still fail until Task 4, while all publication
receipt and readiness assertions pass. Record the remaining failing node IDs.

- [ ] **Step 7: Commit Task 2**

```bash
git add orchestrator/store.py tests/test_store_worker_claims.py tests/test_mac_worker.py
git commit -m "fix: mark verified Supabase artifacts ready"
```

## Task 3: Semantic Catalog Parser and Redacted Diagnostics

**Files:**

- Modify: `tests/test_catalog_sync.py`
- Modify: `orchestrator/catalog_sync.py`

- [ ] **Step 1: Add production-schema and optional-field fixtures**

Add a response fixture containing both compatibility key fields:

```python
def production_body() -> dict[str, object]:
    keys = [
        f"movie:full:{CANONICAL_CODE}",
        f"movie:light:{CANONICAL_CODE}",
    ]
    return {
        "success": True,
        "requested": 1,
        "synced": 1,
        "failed": [],
        "results": [{
            "canonicalCode": CANONICAL_CODE.upper(),
            "d1RowsUpdated": 1,
            "kvKeysTouched": keys,
            "kvKeysDeleted": keys,
            "subtitleCount": 1,
            "futureField": ADULT_TEXT,
        }],
        "fence": {"value": 42, "accepted": True},
        "action": "sync",
        "futureTopLevel": ADULT_TEXT,
    }
```

Assert it succeeds, unknown values do not appear in diagnostics, and unknown field
names do.

- [ ] **Step 2: Add HTTP 207, 500, invalid-body, and idempotency tests**

For 207 assert:

```python
with pytest.raises(CatalogSyncError) as raised:
    sync_with_response(207, body={
        "success": False,
        "requested": 1,
        "synced": 0,
        "failed": [{"canonicalCode": CANONICAL_CODE, "error": "movie_not_found"}],
        "results": [],
    })
assert raised.value.retryable is True
assert raised.value.http_status == 207
assert "movie_not_found" in raised.value.response_json
```

For arbitrary non-JSON text assert diagnostic JSON contains only byte length and
SHA-256, never `ADULT_TEXT` or token strings. Assert repeated calls for the same
receipt send the same `Idempotency-Key` header.

- [ ] **Step 3: Run parser tests and confirm failures**

Run:

```bash
pytest tests/test_catalog_sync.py -q
```

Expected: FAIL on both-fields schema, additive fields, 207 metadata, and missing
idempotency header.

- [ ] **Step 4: Add diagnostic and error types**

Implement:

```python
@dataclass(frozen=True)
class CatalogSyncDiagnostic:
    http_status: int | None
    response_json: str | None


@dataclass(frozen=True)
class CatalogSyncResult:
    canonical_code: str
    d1_rows_updated: int
    subtitle_count: int
    kv_keys_deleted: tuple[str, ...]
    diagnostic: CatalogSyncDiagnostic


class CatalogSyncError(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        *,
        retryable: bool = False,
        http_status: int | None = None,
        response_json: str | None = None,
    ) -> None:
        self.reason_code = safe_reason_code(reason_code)
        self.retryable = retryable
        self.http_status = http_status
        self.response_json = response_json
        super().__init__(self.reason_code)
```

- [ ] **Step 5: Implement a whitelist sanitizer**

Build a new dict rather than mutating or serializing the upstream body. Keep only
contract booleans/counts, bounded canonical/error codes, counts, key names, valid
fence/action, and `unknownFields`. Serialize with sorted compact JSON and cap the
encoded result. Hash/size invalid text without retaining it.

- [ ] **Step 6: Parse diagnostics before status classification**

Always attempt safe JSON parsing and construct a diagnostic before branching on
status. Classify network, 207, 5xx, auth, redirect, invalid JSON, and mismatches
with the retryability described in the design.

- [ ] **Step 7: Replace exact key-set equality with semantic validation**

Normalize returned canonical identity through `canonical_movie_code`. Accept
missing optional fields, valid `fence/action`, supported legacy fields, and
unknown additive fields. Reject contradictory touched/deleted sets, dry-run true,
claim-fence, rejected fence, zero subtitle count, and wrong identity.

- [ ] **Step 8: Add the stable header**

Compute:

```python
material = f"{canonical}\0{expected_subtitle_id}\0{expected_content_sha256}"
idempotency_key = "jso-catalog-" + hashlib.sha256(material.encode()).hexdigest()
```

Send it only as `Idempotency-Key`; do not change the exact JSON request body.

- [ ] **Step 9: Run parser tests**

Run:

```bash
pytest tests/test_catalog_sync.py -q
```

Expected: PASS.

- [ ] **Step 10: Commit Task 3**

```bash
git add orchestrator/catalog_sync.py tests/test_catalog_sync.py
git commit -m "fix: parse catalog responses semantically"
```

## Task 4: Independent Catalog Queue and Exponential Retry

**Files:**

- Modify: `tests/test_store_worker_claims.py`
- Modify: `tests/test_mac_worker.py`
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/config.py`
- Modify: `.env.example`

- [ ] **Step 1: Add failing claim/success/retry tests against ready main status**

Assert claim selects `english_srt_ready + artifact ready + catalog pending`, keeps
the main status ready, and sets only lease fields. Assert completion sets
`catalog_sync_status=succeeded` without touching `status/error`.

- [ ] **Step 2: Add exact exponential schedule tests**

For base 30 and cap 120, patch the clock or compare generated ISO timestamps and
assert delays for attempts 1–5 are `30, 60, 120, 120, 120`. Assert non-retryable
failure immediately sets catalog substate failed while main status remains ready.

- [ ] **Step 3: Add expired-lease invariant coverage**

Exhaust a catalog lease and assert:

```python
assert recovered.status is JobStatus.ENGLISH_SRT_READY
assert recovered.error is None
assert recovered.catalog_sync_status in {"pending", "failed"}
assert recovered.catalog_sync_warning_code == "catalog_sync_lease_expired"
```

- [ ] **Step 4: Run focused store tests**

Run:

```bash
pytest tests/test_store_worker_claims.py -q -k catalog
```

Expected: FAIL because claims still select and mutate main catalog states.

- [ ] **Step 5: Rewrite catalog claim predicates and updates**

Select by main ready/artifact ready/catalog pending. On claim, update
`claimed_by`, `lease_expires_at`, `catalog_lease_token`, and `updated_at` only.
Use CAS predicates for the same independent state and lease token.

- [ ] **Step 6: Rewrite success/failure/recovery mutations**

Success writes diagnostics, `catalog_sync_status=succeeded`, clears warnings and
leases. Failure writes diagnostics and warning, computes:

```python
delay = min(retry_seconds * (2 ** max(attempts - 1, 0)), max_retry_seconds)
```

and leaves the main status/error unchanged. Exhaustion changes only
`catalog_sync_status=failed`.

- [ ] **Step 7: Decouple historical catalog handling**

Mark the historical artifact repair successful after verified Supabase
publication. Route its later catalog work through the same non-blocking catalog
substate methods; remove catalog-response reasons from historical permanent-fail
and pause decisions. Preserve historical file checks before publication, where
they still protect the artifact.

- [ ] **Step 8: Add capped-backoff configuration**

Add a positive setting:

```python
catalog_sync_max_retry_seconds: int = Field(
    default=900,
    alias="CATALOG_SYNC_MAX_RETRY_SECONDS",
    ge=1,
)
```

Pass it to worker/store methods and document it in `.env.example`.

- [ ] **Step 9: Run store and worker tests**

Run:

```bash
pytest tests/test_store_worker_claims.py tests/test_mac_worker.py \
  tests/test_historical_scheduler.py tests/test_historical_batch.py -q
```

Expected: PASS after updating assertions that intentionally encoded catalog as a
blocking terminal stage.

- [ ] **Step 10: Commit Task 4**

```bash
git add orchestrator/store.py orchestrator/config.py .env.example \
  tests/test_store_worker_claims.py tests/test_mac_worker.py \
  tests/test_historical_scheduler.py tests/test_historical_batch.py
git commit -m "fix: make catalog sync a nonblocking substate"
```

## Task 5: Ready Webhook Immediately After Publication

**Files:**

- Create: `orchestrator/callbacks.py`
- Create: `tests/test_api_callbacks.py`
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/api.py`
- Modify: `orchestrator/config.py`
- Modify: `orchestrator/__main__.py`
- Modify: `orchestrator/mac_worker.py`
- Modify: `tests/test_mac_worker.py`

- [ ] **Step 1: Restore signature and payload unit tests**

Port the existing HMAC contract from commit `be450bb1` and assert the payload is:

```python
{
    "event": "subtitle.ready",
    "job_id": job.id,
    "movie_number": job.normalized_movie_number,
    "status": "english_srt_ready",
    "published_to_supabase": True,
    "ready_at": job.updated_at,
}
```

Assert secrets are used only in the signature header and never stored in payload
or callback errors.

- [ ] **Step 2: Add a failing worker event-order test**

Record events from fake publisher, callback sender, and catalog client. Execute
translation/publication/catalog cycles and assert:

```python
assert events == ["translate", "publish", "webhook", "catalog"]
assert sender.calls[0]["payload"]["status"] == "english_srt_ready"
```

Add the catalog-failure variant and assert no failed webhook and no second ready
webhook.

- [ ] **Step 3: Add callback tables and per-client methods idempotently**

Create/reuse `callback_events` and `job_requests`, add nullable
`callback_client_key`, and add the unique partial index on
`(job_id,event_type,client_key)`. Implement create/get/delivery/list-client
methods with safe bounded errors.

- [ ] **Step 4: Associate API submissions with configured clients**

Read `CF-Access-Client-Id`, accept it only if it exists in server callback config,
and pass it to `submit_job/submit_batch`. Never accept a callback URL or secret in
the request body.

- [ ] **Step 5: Implement notifier with per-client dedupe**

Port the later behavior from `codex/subtitle-quality-audit-remediation`: enumerate
job request client keys, skip already-delivered events, create a pending event,
send the signed request, and record delivered/failed without raising into the job
workflow. Redact arbitrary sender exception text before persistence.

- [ ] **Step 6: Inject notifier into the Mac publication worker**

After `complete_supabase_publication` returns the committed ready row:

```python
if self.callback_notifier is not None:
    self.callback_notifier.notify_subtitle_ready(updated)
```

Call this before returning from `_process_publication`; catalog is claimed only on
a later worker cycle.

- [ ] **Step 7: Add callback settings/builders**

Restore `CALLBACK_CLIENTS_JSON` and `CALLBACK_TIMEOUT_SECONDS` parsing without a
new dependency. Build the notifier in `run_mac_translation_worker` and one-shot
worker construction.

- [ ] **Step 8: Run callback/API/worker tests**

Run:

```bash
pytest tests/test_api_callbacks.py tests/test_api_jobs.py tests/test_mac_worker.py -q
```

Expected: PASS.

- [ ] **Step 9: Commit Task 5**

```bash
git add orchestrator/callbacks.py orchestrator/store.py orchestrator/api.py \
  orchestrator/config.py orchestrator/__main__.py orchestrator/mac_worker.py \
  tests/test_api_callbacks.py tests/test_api_jobs.py tests/test_mac_worker.py
git commit -m "feat: send ready webhook after Supabase publication"
```

## Task 6: Supabase Verification and Legacy Reconciliation

**Files:**

- Create: `orchestrator/catalog_sync_reconciliation.py`
- Create: `tests/test_catalog_sync_reconciliation.py`
- Modify: `orchestrator/supabase_publisher.py`
- Modify: `tests/test_supabase_publisher.py`
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/__main__.py`

- [ ] **Step 1: Add failing read-only receipt verification tests**

Use the existing fake Supabase sessions and assert verification performs only:

- GET exact `movie_languages` row;
- GET exact Storage object;
- no POST, PATCH, DELETE, catalog ensure, or upload.

Assert it rejects missing row, wrong path/language/movie ID/file size, missing
object, size mismatch, and SHA-256 mismatch.

- [ ] **Step 2: Add a public verifier method**

Implement:

```python
def verify_existing_publication(
    self,
    *,
    subtitle_id: str,
    movie_uuid: str,
    storage_path: str,
    file_size: int,
    content_sha256: str,
) -> None:
    self._verify_catalog(subtitle_id, movie_uuid, storage_path, file_size)
    self._verify_storage_receipt_once(storage_path, file_size, content_sha256)
```

The one-shot Storage verifier streams with a strict upper bound and hashes bytes;
it does not use publication polling or upload methods.

- [ ] **Step 3: Add reconciliation candidate/result tests including KTB codes**

Create false-failed fixtures for KTB-104, KTB-110, and KTB-111. Assert dry-run
performs remote reads but no database mutation. Assert execute restores all three
to ready, clears main error, and preserves catalog warning/substate failed.

- [ ] **Step 4: Add negative and idempotency tests**

Cover missing row, missing object, changed receipt during verification, non-catalog
failure, claimed job, already-ready row, repeated execution, optional catalog
requeue, and optional webhook resend.

- [ ] **Step 5: Implement transactional store methods**

Add a read-only candidate list and a compare-and-swap method whose WHERE clause
includes job ID, `status=failed`, exact original error, unclaimed state, and every
receipt field. The update writes ready/artifact/catalog/warning fields exactly as
specified and never changes publication receipt fields.

- [ ] **Step 6: Implement reconciliation orchestration**

Define immutable result items with outcomes such as `eligible`, `verified`,
`restored`, `already_ready`, `remote_missing`, `receipt_changed`, and `skipped`.
Normalize optional allowlist codes. Keep dry-run default and process one row at a
time.

- [ ] **Step 7: Add CLI parsing and execution boundary**

Add:

```text
reconcile-catalog-sync-failures
  --movie CODE          (repeatable)
  --limit N
  --execute
  --retry-catalog-sync
  --resend-ready-webhook
```

Reject retry/resend flags without `--execute`. Print a safe deterministic summary
without tokens, response bodies, or subtitle contents.

- [ ] **Step 8: Run reconciliation/publisher tests**

Run:

```bash
pytest tests/test_supabase_publisher.py tests/test_catalog_sync_reconciliation.py -q
```

Expected: PASS.

- [ ] **Step 9: Commit Task 6**

```bash
git add orchestrator/supabase_publisher.py orchestrator/store.py \
  orchestrator/catalog_sync_reconciliation.py orchestrator/__main__.py \
  tests/test_supabase_publisher.py tests/test_catalog_sync_reconciliation.py
git commit -m "feat: reconcile published catalog false failures"
```

## Task 7: Dashboard, Documentation, and Compatibility Sweep

**Files:**

- Modify: `tests/test_dashboard_state.py`
- Modify: `tests/test_api_dashboard.py`
- Modify: `orchestrator/dashboard.py`
- Modify: `README.md`
- Modify: `.env.example`
- Modify: `docs/setup/mac.md`

- [ ] **Step 1: Add dashboard tests for ready-with-warning**

Create a ready artifact with `catalog_sync_status=failed` and assert it appears in
the ready view, not failed view, while detail exposes the catalog warning and last
safe status.

- [ ] **Step 2: Update dashboard classification**

Classify only the main `JobStatus` for ready/failed tabs. Render catalog status and
warning as secondary operational metadata. Do not synthesize a failed main state.

- [ ] **Step 3: Update the state contract documentation**

Replace the blocking pipeline diagram with:

```text
... -> publishing -> english_srt_ready
                         |
                         +-> catalog_sync_status=pending -> succeeded|failed
```

Document additive API fields, immediate webhook timing, backoff variables,
sanitized diagnostics, dry-run reconciliation, and the explicit no-production-
execution boundary.

- [ ] **Step 4: Run dashboard/config/docs tests**

Run:

```bash
pytest tests/test_dashboard_state.py tests/test_api_dashboard.py \
  tests/test_config_paths.py tests/test_api_jobs.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 7**

```bash
git add orchestrator/dashboard.py README.md .env.example docs/setup/mac.md \
  tests/test_dashboard_state.py tests/test_api_dashboard.py \
  tests/test_config_paths.py tests/test_api_jobs.py
git commit -m "docs: describe nonblocking catalog readiness"
```

## Task 8: Full Verification and Handoff

**Files:**

- Modify only if verification exposes an in-scope defect.

- [ ] **Step 1: Run static repository checks**

Run:

```bash
python -m compileall -q orchestrator tests
git diff --check
```

Expected: both exit 0.

- [ ] **Step 2: Run the required focused matrix**

Run:

```bash
pytest tests/test_catalog_sync.py tests/test_store_worker_claims.py \
  tests/test_mac_worker.py tests/test_supabase_publisher.py \
  tests/test_catalog_sync_reconciliation.py tests/test_api_callbacks.py \
  tests/test_api_jobs.py tests/test_dashboard_state.py \
  tests/test_historical_scheduler.py tests/test_historical_batch.py -q
```

Expected: PASS.

- [ ] **Step 3: Run the complete suite**

Run:

```bash
pytest -q
```

Expected: PASS with no skipped incident requirement.

- [ ] **Step 4: Verify the KTB local database read-only**

Run a SELECT only and report current KTB-104/KTB-110/KTB-111 state. Do not run the
new command with `--execute`, do not call production catalog sync, and do not send
webhooks.

- [ ] **Step 5: Review the final diff and production boundary**

Confirm only planned source/tests/docs changed, user-owned dirty files remain
untouched, no secret or production response body entered git, and no deployment
command was run.

- [ ] **Step 6: Final implementation commit if verification required fixes**

```bash
git add orchestrator/catalog_sync.py orchestrator/store.py \
  orchestrator/mac_worker.py orchestrator/models.py orchestrator/api.py \
  orchestrator/config.py orchestrator/__main__.py \
  orchestrator/supabase_publisher.py orchestrator/callbacks.py \
  orchestrator/catalog_sync_reconciliation.py tests/test_catalog_sync.py \
  tests/test_store_worker_claims.py tests/test_mac_worker.py \
  tests/test_supabase_publisher.py tests/test_catalog_sync_reconciliation.py \
  tests/test_api_callbacks.py tests/test_api_jobs.py tests/test_models.py \
  tests/test_store_submit.py tests/test_dashboard_state.py \
  tests/test_api_dashboard.py tests/test_config_paths.py \
  tests/test_historical_scheduler.py tests/test_historical_batch.py \
  orchestrator/dashboard.py README.md .env.example docs/setup/mac.md
git commit -m "test: verify nonblocking catalog readiness"
```

- [ ] **Step 7: Report handoff**

Report root cause, exact modified files, focused/full test results, current KTB
read-only status, reconciliation dry-run/execute syntax, and separately approved
deployment steps. Explicitly state that production was not deployed or mutated.
