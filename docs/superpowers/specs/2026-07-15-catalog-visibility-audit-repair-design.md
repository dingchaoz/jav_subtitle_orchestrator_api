# Catalog Visibility Audit and Catalog-Only Repair Design

**Date:** 2026-07-15

**Status:** Approved for implementation planning

## Goal

Prevent a subtitle from being marked `english_srt_ready` while a later website
deployment serves a stale catalog cache, and provide an operator-controlled way to
detect and repair every existing occurrence without retranslating the subtitle,
uploading the SRT again, or changing the verified Supabase publication receipt.

## Incident Summary

The KTB-111 SRT, `movie_languages` row, Storage object, D1 row, and versioned KV
entry were correct. The orchestrator successfully called the admin catalog sync
endpoint and verified the published subtitle through the public movie API before
marking the job ready.

Later website Worker deployments changed the full-movie cache contract from
`movie:full:v3:<code>` to `movie:full:<code>`. The legacy KTB-111 key still held an
older zero-subtitle payload, so the public API returned no subtitles even though
the authoritative data and the dashboard receipt were valid. Jobs synced after
the deployment, such as IENE-963, used the active legacy key and remained visible.

This is a deployment compatibility failure across the website catalog reader and
writer, not a translation or publication failure.

## Scope

This design covers two repositories:

- `/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator`
  - receipt-driven public visibility audit;
  - operator-approved catalog-only repair;
  - reports, checkpoints, safety controls, and verification.
- the canonical `jav_subtitle_com` repository and production release workflow
  - one shared cache-key contract;
  - cache-schema downgrade protection;
  - read-only post-deploy subtitle canaries.

The implementation must not:

- run Japanese transcription or English translation;
- upload, overwrite, or delete a Supabase Storage object;
- insert, patch, or delete a Supabase subtitle row;
- clear or replace an orchestrator publication receipt;
- move a successful job back through translation or publication states;
- directly edit production KV as the normal recovery mechanism;
- deploy website changes without the website repository's explicit production
  approval and release guardrails.

## Chosen Approach

Use the orchestrator's verified publication receipts as the audit source of truth.
For each eligible job, compare the exact `published_subtitle_id` with the public
movie API. Save a durable dry-run report. A separate repair command consumes that
unchanged report, calls the existing admin catalog sync endpoint only for confirmed
mismatches, and verifies the exact subtitle ID again.

This approach is preferred over a website-wide rebuild because it proves identity
against the same receipt that allowed the job to become ready. It is preferred over
direct KV repair because it exercises the supported D1/KV catalog sync path and
remains valid when the cache implementation changes.

## Eligibility and Receipt Validation

An audit candidate must satisfy all of the following:

- job status is `english_srt_ready`;
- `published_subtitle_id` is a canonical UUID;
- `catalog_movie_uuid` is a canonical UUID;
- `published_storage_path` exactly matches the canonical English AI path for the
  normalized movie code;
- `published_content_sha256` is 64 lowercase hexadecimal characters;
- `published_file_size` is a positive integer;
- metadata status and source are values accepted by the existing verified receipt
  validator.

The audit reuses the store's existing verified Supabase receipt validation rather
than introducing a weaker parallel validator. A row that fails validation is
reported as `invalid_receipt` and is never eligible for automatic catalog repair.

The initial production population is 339 ready jobs with publication receipts.
The implementation must calculate the population at runtime and must not hard-code
that count.

## Audit Command

Add an orchestrator CLI command conceptually named
`catalog-visibility-audit`. Exact argparse spelling may follow existing CLI naming
conventions, but its behavior is fixed by this design.

### Inputs

- production database path from normal Mac settings;
- public website API base URL from `JAVSUBTITLE_API_BASE`;
- optional exact movie-code allowlist;
- optional positive `--limit`;
- optional report/checkpoint directory;
- bounded request timeout and retry settings.

The command is read-only. It must not accept an execution flag and must not call
the admin sync endpoint.

### Public check

For each eligible job, request:

`GET /api/movie/<canonical-code>?cacheNonce=<published-content-sha256>`

The check succeeds only when:

- the response is HTTP 200 without a redirect;
- the payload is a JSON object;
- `canonicalCode` equals the requested canonical code;
- `subtitles` is an array;
- exactly one subtitle has the expected `published_subtitle_id`.

The cache nonce remains useful for HTTP cache separation, but correctness must not
depend on the website honoring the nonce internally.

### Classifications

Each candidate receives exactly one terminal audit classification:

- `visible`: the expected subtitle ID appears exactly once;
- `missing`: the public response is valid but the expected ID does not appear;
- `not_found`: the public movie API returns HTTP 404;
- `fetch_failed`: timeout, network error, redirect, or non-200/non-404 response;
- `response_invalid`: invalid JSON, wrong canonical code, or invalid subtitle shape;
- `invalid_receipt`: the local verified publication receipt fails validation.

Only `missing` and `not_found` are repair-eligible. Fetch and response failures are
operational uncertainty, not proof that a catalog mutation is safe.

### Report and checkpoint

Write an append-safe checkpoint during the scan and an immutable final JSON report.
The final report contains:

- schema version;
- audit ID and creation timestamp;
- database identity without credentials;
- public API origin;
- normalized selection inputs;
- per-job receipt identity and classification;
- aggregate counts;
- SHA-256 digest over the canonical repair-relevant content.

Do not include service keys, admin tokens, SRT contents, signed URLs, or other
credentials. Resume must preserve prior terminal results and continue only missing
candidate IDs. The final report is written atomically after all selected candidates
reach a terminal classification.

## Repair Command

Add a separate CLI command conceptually named
`catalog-visibility-repair`. It consumes a completed audit report and is dry-run by
default.

### Authorization

A real repair requires all of:

- `--execute`;
- the path to a completed audit report;
- a `--confirm-report-sha256` value matching the report's computed digest;
- valid website admin API configuration;
- an unchanged current job receipt matching the report entry.

The command rejects checkpoints, incomplete reports, unknown report schema versions,
changed receipts, duplicate movie codes, and reports from a different public API
origin.

### Dry-run behavior

Without `--execute`, print and save the exact ordered repair plan. The plan includes
only report entries classified `missing` or `not_found` whose receipts still match
the database. No network mutation occurs.

### Execution behavior

Process one movie per admin sync request. Single-item requests provide clear error
attribution and let the existing catalog client verify the exact subtitle ID after
each sync.

For each item:

1. Reload the job and validate that the current receipt exactly matches the report.
2. Call `POST /api/admin/catalog/sync-subtitles` with the normal
   `subtitle_ingest` reason, orchestrator source, and `dryRun: false`.
3. Validate the endpoint response using the active catalog response contract.
4. Perform the public visibility check for the exact subtitle ID.
5. Record `repaired`, a safe classified failure, or `skipped_receipt_changed`.

The command does not call `publish_english_ai`, the translator, or any store method
that changes the job's workflow state. A repaired job remains
`english_srt_ready`, with publication fields byte-for-byte unchanged.

### Failure containment

- Default concurrency is one.
- Requests use bounded timeouts.
- A configurable consecutive-failure limit defaults to three and stops the batch.
- Authentication failure stops immediately.
- Receipt change skips that item and does not count as a remote failure.
- Public visibility uncertainty after a successful sync is reported as unverified;
  it is not silently counted as repaired.
- The execution report is append-safe and resumable. Resume never repeats an item
  already recorded as `repaired` unless the operator creates a new audit report.

## Website Cache Contract

### Shared key helper

The website code defines one exported full-movie cache-key helper and one exported
cache schema version. Movie reads, asynchronous cache writes, admin catalog sync,
tests, and invalidation must import these definitions. Literal full-movie cache-key
formats outside the helper are rejected by a focused source-contract test.

The new canonical full key is `movie:full:v4:<code>`, and the sync response reports
`cacheSchemaVersion: "v4"`. A new version is intentional: neither the stale legacy
key nor the existing v3 key can be mistaken for an entry created under the repaired
cross-component contract.

Light-movie keys may remain independently versioned or unversioned, but their
format must also come from one helper used by every reader and writer.

### Migration behavior

When introducing or changing a cache schema:

- reads try only the active full key;
- an active-key miss falls through to authoritative D1, not to an older full key;
- the D1 result repopulates the active key;
- admin sync writes or invalidates the active key through the same helper;
- legacy keys may be deleted best-effort but are never trusted as a fallback;
- a transition may write a legacy key for rollback compatibility only when this is
  explicitly declared and tested. Legacy writes do not change which key active
  readers trust.

This prevents a stale legacy entry from overriding a correct D1 row.

### Sync response

The admin sync response adds a stable `cacheSchemaVersion` field and reports the
exact active keys touched or invalidated. `d1RowsUpdated` must reflect confirmed D1
mutation results or be followed by an authoritative D1 readback; it must not be only
the number of rows the handler intended to update.

Before returning success, the handler verifies that D1 contains the subtitle IDs it
read from Supabase. KV mutation may remain separately classified, but a success
response must identify whether the active full key was written, deleted for D1
fallback, or left unchanged.

The orchestrator client validates the declared cache schema and exact canonical code
without embedding multiple competing key parsers.

## Deployment Protection

Production website deploys must run only through the canonical release script from
the canonical integration branch and a clean worktree. The release entrypoint must
fail before upload when these conditions are not met. A task-worktree override is
not part of the normal production path.

Add the active cache schema version to a safe health or build-info response. Before
deployment, compare the candidate schema version with production. A lower candidate
version is rejected. A schema change requires an explicit migration declaration and
the cache contract tests described above.

Production currently has no cache-schema field, so the first guarded deployment is
an explicit one-time initialization to v4. That initialization requires the normal
production approval, the migration declaration, and passing pre-deploy and
post-deploy subtitle canaries. After initialization, absence of a production schema
field or any downgrade is a preflight failure.

After an API deployment, run read-only subtitle canaries against at least:

- KTB-111, expected subtitle ID `fc9bed2a-f432-45a6-b7f9-bf141dd61810`;
- IENE-963, expected subtitle ID `abc955c5-52eb-4d33-a902-7f702523a0f2`.

Canary identities should ultimately be loaded from a small reviewed deployment
fixture rather than duplicated in shell commands. A deployment is not considered
healthy if either exact subtitle ID is absent, even when `/api/health` is HTTP 200.
The release workflow stops and follows its existing rollback procedure on failure.

## Observability

Audit and repair reports are the primary operator evidence. Dashboard integration
is limited to exposing the latest report timestamp and aggregate counts if an
existing report-summary pattern can be reused without expanding scope.

The repair log records movie code, job ID, expected subtitle ID, starting
classification, sync result classification, and final public visibility. It never
records credentials or signed subtitle URLs.

The website admin sync should log the canonical code, cache schema version, D1
verification outcome, and safe KV outcome. It must not log authorization headers,
service-role keys, or complete subtitle payloads.

## Testing Strategy

### Orchestrator tests

- candidate selection includes only ready jobs with publication receipts;
- existing receipt validation is reused and invalid receipts are classified safely;
- public results are classified deterministically;
- the audit command never performs POST, PATCH, PUT, or DELETE requests;
- report canonicalization and digest calculation are stable;
- resume preserves terminal results and does not duplicate candidates;
- dry-run repair performs no remote mutation;
- execution requires both explicit flags and a matching digest;
- changed receipts are skipped;
- only `missing` and `not_found` entries are synced;
- repair never calls translation, Storage publication, Supabase row publication, or
  workflow-state transition methods;
- a successful repair requires exact public subtitle identity;
- authentication and consecutive-failure stops work as designed.

### Website tests

- reader, writer, and admin sync resolve the same active full key;
- source-contract test rejects stray full-key literals;
- active-key miss falls through to D1 rather than stale legacy KV;
- D1 fallback repopulates only the active key unless a migration explicitly enables
  compatibility writes;
- admin sync reads Supabase, mutates D1, verifies D1 subtitle IDs, and handles KV;
- sync response exposes the active cache schema and exact outcome;
- deployment preflight rejects noncanonical worktrees, dirty release trees, branch
  mismatches, and cache-schema downgrades;
- post-deploy canary fails when either known subtitle ID is missing.

### End-to-end verification

1. Run an audit limited to KTB-111 and confirm it is classified `missing` before
   repair.
2. Run repair dry-run and verify no production state changes.
3. With explicit production approval, execute the single KTB-111 repair.
4. Confirm D1, active KV behavior, the public movie API, and the Chinese movie page
   expose the expected English AI subtitle.
5. Run a bounded multi-movie audit and approved repair batch.
6. Re-run the same audit and require all repaired entries to classify `visible`.
7. Deploy website prevention changes only through the website release guardrails,
   then require the KTB-111 and IENE-963 post-deploy canaries to pass.

## Rollout Sequence

1. Implement and test the orchestrator audit command.
2. Produce the full dry-run visibility report for current verified receipts.
3. Implement and test the catalog-only repair command.
4. Repair KTB-111 as the single canary after explicit production approval.
5. Run a bounded approved batch, inspect the execution report, then continue in
   bounded batches until the audit is clean or a stop condition occurs.
6. Implement the website shared cache-key contract and deployment guards in the
   canonical website repository.
7. Deploy the website API only after explicit approval and run post-deploy canaries.
8. Run a final full audit to prove that deployment did not reintroduce visibility
   mismatches.

## Success Criteria

- Every repair is derived from an immutable audit report and an unchanged verified
  publication receipt.
- No repair retranslates or republishes an SRT.
- KTB-111 and all approved mismatches expose their exact expected subtitle IDs after
  repair.
- A second full audit reports no repair-eligible mismatch for the repaired set.
- Website reads, writes, and sync use one active cache-key contract.
- Production deployment blocks cache-schema downgrade and noncanonical release
  sources.
- Post-deploy subtitle canaries prevent a healthy deployment result when catalog
  visibility regresses.
