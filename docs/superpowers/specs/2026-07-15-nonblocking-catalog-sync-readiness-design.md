# Non-Blocking Catalog Sync Readiness Design

**Date:** 2026-07-15

**Status:** Approved direction, pending written-spec review

## Goal

Make verified Supabase subtitle publication the only remote prerequisite for the
public `english_srt_ready` state. Website D1/KV catalog synchronization remains a
durable, observable, retryable follow-up, but it can never downgrade a published
subtitle job to `failed` or delay the existing `subtitle.ready` webhook.

The design also provides a safe reconciliation command for historical jobs whose
main status was incorrectly set to `failed` by a `catalog_sync:` error.

## Incident Findings

The current production pipeline transitions through:

```text
publishing -> catalog_sync_pending -> catalog_syncing -> english_srt_ready
```

`JobStore.complete_supabase_publication` records the verified Supabase receipt but
sets the main job status to `catalog_sync_pending`. `JobStore.fail_catalog_sync`
later changes that same status to `failed` after the retry limit. The main status
therefore describes two different outcomes: subtitle artifact availability and
website catalog visibility. This coupling caused the reported false failures.

The current catalog response parser also validates exact object key sets rather
than the response's semantics. It rejects additive compatibility fields and maps
every non-200 response, including HTTP 207, to the same `catalog_sync_failed`
reason before reading the response body.

The local and website repository histories provide the following incident
timeline:

- KTB-110 and KTB-111 English SRT files were created before the website's
  subtitle-only catalog-row fix. A single-code sync could therefore return HTTP
  207 with a safe per-code error such as `movie_not_found`, but the orchestrator
  discarded the body and retained only `catalog_sync_failed`.
- KTB-104 was created immediately after the website added the legacy
  `kvKeysDeleted` compatibility field alongside `kvKeysTouched`. The orchestrator
  accepts either exact result schema, not both fields together, so an otherwise
  successful HTTP 200 response is classified `catalog_response_mismatch`.
- The original status and bodies cannot be reconstructed exactly because the
  current client does not log or persist catalog HTTP diagnostics.

KTB-104, KTB-110, and KTB-111 are already `english_srt_ready` in the local
database after catalog-only retries. This does not remove the need for a general
reconciliation command because other `catalog_sync:` terminal failures remain.

## Chosen Architecture

Keep one job row, but split artifact readiness from catalog follow-up state with
focused columns. This is preferred over a minimal status-only patch because the
catalog worker still needs a durable queue and retry state. It is preferred over
a new general-purpose workflow/outbox subsystem because the existing job leases
and attempt counters already provide the required bounded worker semantics.

The public main status remains backward compatible:

```text
Supabase publication verified
  -> status = english_srt_ready
  -> ready = true
  -> artifact_status = ready
  -> error = null
  -> catalog_sync_status = pending
```

The catalog follow-up then changes only its own fields:

```text
pending -> succeeded
pending -> pending   (retry scheduled with warning)
pending -> failed    (retry limit reached with warning)
```

`catalog_sync_pending` and `catalog_syncing` remain accepted `JobStatus` enum
values so existing databases and old snapshots can still be read and reconciled.
New verified publications do not enter those main states.

## Persistent State

Add the following nullable or defaulted columns through the existing idempotent
SQLite initialization migration pattern:

- `artifact_status TEXT`
  - `ready` after verified Supabase publication;
  - `NULL` for legacy/local-only jobs whose remote artifact semantics are unknown.
- `catalog_sync_status TEXT`
  - `pending`, `succeeded`, or `failed`;
  - `NULL` when catalog sync is not applicable, including publish-disabled local
    mode.
- `catalog_sync_warning_code TEXT`
- `catalog_sync_warning_message TEXT`
- `catalog_sync_last_http_status INTEGER`
- `catalog_sync_last_response_json TEXT`
- `catalog_sync_last_attempt_at TEXT`

Retain and reuse:

- `catalog_sync_attempt_count`;
- `next_catalog_sync_attempt_at`;
- `catalog_lease_token`;
- `claimed_by` and `lease_expires_at`;
- all verified Supabase receipt fields.

`ready` is a derived API property, not a separate database column. It is true
exactly when the public main status is `english_srt_ready`.

The warning columns persist the current catalog warning. The API renders them as
a `warnings` array, allowing future warning sources without changing the public
shape. A successful later catalog sync clears the current catalog warning but
retains the last sanitized response diagnostics.

### Initialization and legacy rows

Initialization must be safe against databases that already contain callback,
request, or observability tables from another branch. It only adds missing
columns and indexes.

Verified legacy rows are classified as follows:

- `english_srt_ready` plus a valid Supabase receipt: `artifact_status=ready`;
- `failed` with `error LIKE 'catalog_sync:%'` plus a valid local receipt remains
  unchanged until remote reconciliation verifies Supabase;
- legacy `catalog_sync_pending`/`catalog_syncing` rows are eligible for the same
  reconciliation and lease-recovery path;
- local-only ready rows without a publication receipt keep both new status fields
  `NULL`.

No initialization migration performs network I/O.

## Publication State Transition

`complete_supabase_publication` continues to require all existing lease and
verified-receipt checks. Its single transaction changes the main state to:

```text
status = english_srt_ready
artifact_status = ready
catalog_sync_status = pending
error = NULL
```

It resets the catalog attempt schedule and releases the publication lease. The
returned `JobRecord` is already publicly ready before any webhook or catalog call
occurs.

Supabase publication failures continue through the existing publication failure
path and may set the main status to `failed`. This is the boundary that separates
a genuine artifact failure from a website catalog warning.

For historical translation repairs, verified Supabase publication also completes
the artifact repair. A later catalog failure must not set the job or historical
artifact repair to permanent failure. Catalog health may still be surfaced to the
operator and historical controller as a warning, but it cannot pause normal
subtitle availability or change the ready result.

## Catalog Work Queue and Leases

`claim_catalog_sync_job` selects rows by:

- main `status=english_srt_ready`;
- `artifact_status=ready`;
- `catalog_sync_status=pending`;
- valid verified Supabase receipt;
- due retry time;
- no active claimant.

Claiming a catalog job sets the existing claimant and fenced lease fields but
does not alter the main status. `catalog_sync_status` remains `pending` while a
lease is active; `catalog_lease_token` identifies in-flight work. Worker health
may continue to report the operational stage `catalog_syncing`.

Successful completion atomically sets `catalog_sync_status=succeeded`, clears the
warning and retry schedule, and releases the lease. Failure atomically releases
the lease, records sanitized diagnostics, and either schedules another `pending`
attempt or sets only `catalog_sync_status=failed` after exhaustion.

Every catalog failure path, including expired leases, unexpected client
exceptions, public visibility verification, and historical work, must preserve:

```text
status = english_srt_ready
artifact_status = ready
error = NULL
```

## Catalog HTTP Contract

The client sends one canonical code per request. The request remains semantically
idempotent because D1 writes are upserts/replacements and KV work refreshes or
invalidates deterministic keys. It also sends a stable `Idempotency-Key` header
derived with SHA-256 from the canonical code, verified subtitle ID, and published
content hash. The key is identical across retries for the same publication
receipt and changes when the published artifact changes.

No unknown request-body fields are added because deployed website handlers may
reject them. No new dependency is required.

### HTTP handling

- HTTP 200: parse and semantically validate the success response.
- HTTP 207: parse and persist the partial-failure response, classify it as a
  retryable catalog warning, and schedule bounded retry.
- Network errors and HTTP 5xx: retryable catalog warning.
- Invalid JSON and recoverable response mismatch: retryable catalog warning.
- HTTP 401/403: non-retryable authentication warning for that catalog substate;
  it does not change artifact readiness.
- Redirects and other non-retryable 4xx responses: catalog substate failure with
  diagnostics, never main-job failure.

### Semantic success validation

A normal single-code success requires:

- `success is true`;
- `requested == 1` and `synced == 1` using exact integers, not booleans;
- an empty `failed` array;
- exactly one result;
- result `canonicalCode` normalizes to the requested canonical identity;
- positive `d1RowsUpdated` and `subtitleCount` for a verified subtitle
  publication;
- `kvKeysTouched`, or a supported legacy equivalent, contains the canonical full
  and light cache keys; variant keys may be present;
- when both `kvKeysTouched` and legacy `kvKeysDeleted` are present, both are
  well-formed and describe the same affected key set;
- optional `dryRun` is absent or false;
- optional `action` is absent or `sync` for this request;
- optional `fence`, when present, has a positive integer value and
  `accepted=true`.

Unknown additive top-level or result fields do not fail the request. Their names
are captured in diagnostics; their values are not retained. Wrong canonical
identity, a rejected fence, `claim-fence`, zero subtitle count, contradictory KV
receipts, or missing required semantic fields remain response mismatches.

The existing exact public visibility check remains part of catalog success. Its
failure is retryable catalog uncertainty and cannot affect artifact readiness.

## Sanitized Diagnostics

Every HTTP response, including 200, 207, 4xx, and 5xx, produces a bounded
diagnostic record and a single structured job-log entry. Network failures record
`http_status=NULL` and a safe reason code.

The sanitizer retains only:

- `success`, `requested`, and `synced`;
- bounded `failed[].canonicalCode` and safe machine-like error codes;
- result canonical code and integer counts;
- bounded `kvKeysTouched`/supported legacy key lists;
- valid `fence` and `action` fields;
- names of unknown fields.

It never retains:

- authorization headers or tokens;
- Supabase keys;
- signed URLs;
- subtitle text or arbitrary upstream response strings;
- unknown field values.

Invalid or non-JSON bodies are represented by byte length and SHA-256 only. The
serialized diagnostic is size-bounded before storing in
`catalog_sync_last_response_json` or writing to `mac-translation.log`.

`CatalogSyncError` carries a safe reason code, retryability flag, optional HTTP
status, and sanitized diagnostic. `str(error)` remains a safe reason code so
existing log and exception expectations remain compatible.

## Retry Policy

Reuse `MAX_CATALOG_SYNC_ATTEMPTS` as the hard attempt limit. Replace the fixed
delay with deterministic exponential backoff:

```text
delay = min(base_retry_seconds * 2 ** (attempt_number - 1), max_retry_seconds)
```

Add `CATALOG_SYNC_MAX_RETRY_SECONDS` with a conservative default cap. Tests use a
zero base delay where immediate retry is needed and verify exact computed due
times without sleeping.

Retryable classes are network failures, HTTP 5xx, HTTP 207, invalid response JSON,
recoverable response mismatch, and public visibility uncertainty. Authentication
and clearly invalid requests fail the catalog substate immediately. In all cases,
the artifact remains ready.

## Ready Webhook

Restore the existing signed `subtitle.ready` callback implementation from the
callback feature line, using its later per-client deduplication behavior. The
current pipeline branch does not contain that module even though some local
databases already contain `callback_events` and `job_requests` tables; schema
initialization must therefore tolerate and reuse those tables.

After `complete_supabase_publication` commits and returns a ready `JobRecord`, the
Mac publication worker immediately invokes the callback notifier before claiming
or attempting catalog work. The payload remains backward compatible:

```json
{
  "event": "subtitle.ready",
  "job_id": "...",
  "movie_number": "ktb-104",
  "status": "english_srt_ready",
  "published_to_supabase": true,
  "ready_at": "..."
}
```

Callback delivery failure is recorded in `callback_events` and does not change
the job. A unique job/event/client constraint prevents duplicate successful ready
events. Reconciliation may retry a missing or failed ready callback when
explicitly requested, but it never sends a `failed` event for catalog failure.

The API continues associating known callback clients through the configured
Cloudflare Access client identity and does not accept arbitrary callback URLs in
job submissions.

## Public API Compatibility

Keep all existing `JobResponse` fields and add:

```json
{
  "ready": true,
  "artifact_status": "ready",
  "catalog_sync_status": "pending",
  "warnings": [
    {
      "code": "catalog_sync_failed",
      "message": "Catalog synchronization is pending retry after HTTP 500."
    }
  ]
}
```

The added fields are additive. Existing clients that only inspect `status` and
`error` continue to work. Once Supabase publication is verified, every compact or
detailed job response must expose:

```text
status=english_srt_ready
ready=true
error=null
```

Warnings contain safe operator-facing messages only. Raw or sanitized response
diagnostics remain in the detailed operational view/logs and are not exposed by
the compact public response unless an existing authenticated detail endpoint is
used.

## Reconciliation Command

Add a management command, conceptually:

```text
python -m orchestrator reconcile-catalog-sync-failures
```

It is dry-run by default and selects jobs with:

```sql
status = 'failed' AND error LIKE 'catalog_sync:%'
```

An optional exact movie allowlist and positive limit support canary operation. An
actual local state mutation requires `--execute`. Optional flags control whether
catalog sync is requeued and whether a missing/failed ready webhook is retried.

For each candidate, the command performs read-only remote verification:

1. Validate the stored Supabase receipt and canonical storage path.
2. Fetch the exact `movie_languages` row and require matching subtitle ID, movie
   UUID, `English_AI`, storage path, and file size.
3. Fetch the Storage object without redirects and require its size and SHA-256 to
   match the stored publication receipt.
4. Re-read the job before mutation and compare the original status, error, and
   receipt to prevent time-of-check/time-of-use races.

If both remote checks pass, a transactional compare-and-swap sets:

```text
status = english_srt_ready
artifact_status = ready
ready = true (derived)
error = NULL
catalog_sync_status = failed
catalog_sync_warning_code = original catalog reason
catalog_sync_warning_message = safe reconciliation message
```

By default, the catalog substate remains `failed` because reconciliation proves
artifact readiness, not catalog success. `--retry-catalog-sync` changes it to
`pending`, resets the catalog attempt schedule, and preserves the warning until a
successful sync clears it.

`--resend-ready-webhook` delivers only when no ready callback was delivered for
the job/client. Repeated command execution is idempotent and reports already-ready
rows without rewriting them.

KTB-104, KTB-110, and KTB-111 are mandatory named test fixtures/canaries for the
command. The command is delivered but is not executed against production as part
of implementation.

## Test Strategy

Use the existing pytest stack and fake request sessions; add no third-party
dependency. Tests are written before implementation and cover:

1. Supabase publication succeeds and catalog sync succeeds: public state is ready
   immediately and catalog substate later succeeds.
2. Supabase succeeds and catalog returns HTTP 500: ready with warning, bounded
   exponential retry, then catalog-only failure after exhaustion.
3. Supabase succeeds and catalog returns HTTP 207: response is sanitized,
   retained, and retried while the job remains ready.
4. Supabase succeeds and catalog response mismatches: ready with warning and
   retry.
5. Supabase publication fails: main job may fail and no ready webhook is sent.
6. A ready job cannot be downgraded by any catalog failure or expired catalog
   lease.
7. Current `kvKeysTouched`, legacy `kvKeysDeleted`, both compatibility fields,
   optional `fence`/`action`, canonical case normalization, and subtitle-count
   semantics are covered independently.
8. Diagnostic logging redacts arbitrary response text, credentials, signed URLs,
   and unknown values while preserving status and safe contract fields.
9. Reconciliation verifies both `movie_languages` and Storage before restoring a
   job, preserves the original catalog failure as a warning, rejects partial or
   changed receipts, and is idempotent.
10. `subtitle.ready` is sent after the ready transaction and before any catalog
    attempt; catalog failure never sends or overwrites a failed state.
11. KTB-104, KTB-110, and KTB-111 reconciliation cases become ready.
12. Compact and detail APIs remain backward compatible and consistently derive
    `ready` and warnings.

Focused suites include catalog client, store claims, Mac worker, API jobs,
callbacks, Supabase publication verification, reconciliation, dashboard state,
historical repairs, and CLI parsing. The final verification runs the complete
pytest suite.

## Expected Source Changes

Existing files likely modified:

- `orchestrator/catalog_sync.py`
- `orchestrator/store.py`
- `orchestrator/mac_worker.py`
- `orchestrator/models.py`
- `orchestrator/api.py`
- `orchestrator/config.py`
- `orchestrator/__main__.py`
- `orchestrator/supabase_publisher.py`
- `README.md`
- `.env.example`

Focused files likely created or restored:

- `orchestrator/callbacks.py`
- `orchestrator/catalog_sync_reconciliation.py`
- callback and reconciliation test modules

Existing catalog, store, worker, API, publisher, dashboard, and historical tests
will be updated where they currently encode catalog-blocking readiness.

## Deployment and Operations Boundary

Implementation ends with code, tests, documentation, and a reconciliation command.
It does not deploy either repository, restart launchd services, mutate production
SQLite state, call the production admin catalog endpoint, resend production
webhooks, or run reconciliation with `--execute`.

After separate approval, deployment should proceed as:

1. Back up the production SQLite database and job logs.
2. Deploy/restart the orchestrator API and Mac translation worker from the reviewed
   commit.
3. Verify schema initialization and API compatibility without executing
   reconciliation.
4. Submit one publication canary and confirm ready/webhook occurs before catalog
   completion.
5. Run reconciliation dry-run for KTB-104, KTB-110, and KTB-111, followed by the
   remaining `catalog_sync:` failures.
6. Review remote verification and planned mutations.
7. Obtain explicit production approval before `--execute`, catalog requeue, or
   webhook resend.

Rollback may stop the new services and restore the database backup. Because the
new API fields are additive and old main status values remain readable, code
rollback does not require dropping the new columns.
