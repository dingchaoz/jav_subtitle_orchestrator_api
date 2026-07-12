# Historical Subtitle Repair Canary Design

## Goal

Safely repair and publish exactly one historically bad `English_AI` subtitle through
the normal Mac translation worker, prove the corrected subtitle is visible through
Supabase and `https://javsubtitle.com`, then stop and request approval before any
five-to-ten-job batch.

## Scope and authorization boundary

The implementation covers integration of the completed GET-only audit, a controlled
historical-repair prepare command, translation-only state reset, Mac worker
publication, publication verification, and one canary execution. The audit found
628 exact `English_AI` rows and 340 hard-failure candidates.

The canary is authorized after implementation and verification. No second repair,
small batch, bulk requeue, or bulk overwrite is authorized. The system must stop
after reporting the canary result and ask the user whether to process a batch of
five to ten.

Old local English SRT files are moved to `rejected/` and retained. The canary has no
permanent-delete mode. Japanese SRT, `audio.wav`, and transcription results are
always preserved.

## Selected approach

Historical repair reuses the normal Mac translation worker instead of introducing a
second synchronous translation path. A controlled prepare command resets only the
translation stage. The worker then owns quarantine, translation, quality gating,
Supabase publication, post-publication verification, and the final ready transition.

Rejected alternatives are:

- a synchronous repair command that duplicates worker behavior;
- separate prepare, translate, and publish commands that increase operator-ordering
  risk.

## Components

### Read-only canary selector

The selector intersects `repair-allowlist.txt` with the local JobStore and returns
only idle candidates that meet all of these conditions:

- the movie and job exist locally;
- the job is unclaimed and its status is exactly `queued`, `failed`, or
  `english_srt_ready`; the transactional prepare step compares that expected status
  again to prevent a worker-claim race;
- Japanese SRT exists and is non-empty;
- `audio.wav` exists;
- the current English SRT exists and fails the production Japanese/English quality
  gate;
- the candidate appears in the explicit audit allowlist.

Selection is deterministic. `abf-279` is preferred only if eligible; otherwise the
first eligible allowlisted movie in stable order becomes the canary. The selector
is read-only and prints identifiers, paths, reason codes, and actions without
subtitle text.

### Controlled prepare command

The prepare command requires all of:

- an explicit `--allowlist-file`;
- exactly one `--movie`;
- `--limit 1`;
- `--confirm-job-id` matching the selected local job.

There is no `force`, wildcard, delete, bulk, or implicit-all mode. Before changing
state, the command repeats every selector precondition. A JobStore transaction then:

- changes the selected job to `transcription_done`;
- clears claim and lease fields;
- resets only a dedicated `translation_attempt_count`;
- clears the database English SRT paths;
- preserves audio paths, Japanese paths, metadata, priority, and transcription
  output.

The existing English file remains in place until the worker claims the job. At the
start of translation the existing worker quarantine behavior moves it to a unique
timestamped `rejected/` path. A restart or transient failure cannot overwrite that
quarantine artifact.

The jobs table gains `translation_attempt_count INTEGER NOT NULL DEFAULT 0` through
the existing idempotent SQLite initialization path. Windows continues to own
`worker_attempt_count`; Mac translation retry and historical prepare use only the
new translation counter. Existing rows migrate to zero without changing status or
paths.

### Exact-job one-shot worker

Canary execution never starts the unconstrained polling loop. The CLI gains an
explicit one-shot translation mode requiring `--job-id`; JobStore claims that exact
row only when it is `transcription_done` and unclaimed. The process runs startup
smoke, processes at most that one job, and exits. It cannot fall through to the next
queued translation.

Before prepare, the existing general Mac translation worker is stopped so it cannot
race the exact-job process. After a successful canary and verification, the normal
Mac translation worker is restarted with publishing enabled. Historical batch work
remains unprepared and therefore cannot be claimed as part of the canary.

### Mac worker publication

Supabase publication becomes an optional runtime capability that is explicitly
enabled in the production Mac `.env`. When enabled, missing Supabase URL, service
key, or bucket prevents the translation worker from starting; it cannot silently
degrade to ready-without-publication.

For a claimed job the worker executes:

1. quarantine the old English SRT;
2. translate Japanese to English;
3. run the production pair quality gate and write text-free `quality.log`;
4. on pass, call the server-side Supabase publisher with `x-upsert:true`;
5. verify the catalog row and download the stored object with a cache nonce;
6. compare downloaded SHA-256 and byte count to the local passing English file;
7. mark `english_srt_ready` only after verification passes.

The publisher re-runs the pair quality gate immediately before upload. Thus no bad
English file can reach the Supabase upload function even if a caller bypasses the
worker-level check.

### Supabase and CDN verification

The existing same-path Storage object is overwritten with `x-upsert:true`, and the
existing `movie_languages` row is patched or inserted idempotently. The server-side
service key stays outside browser code and logs.

After upload, the publisher polls an authenticated Storage GET with a unique cache
nonce for at most 90 seconds. Success requires the exact local SHA-256 and byte
count. It then verifies that the exact movie's `English_AI` catalog row points to
the expected Storage path and file size. HTTP success, object existence, or catalog
existence alone is insufficient.

Supabase documents that `x-upsert` overwrites an existing path and that Smart CDN
invalidation may take up to 60 seconds. The 90-second verification window covers
that propagation delay without treating stale bytes as success.

### Website verification

After the job reaches `english_srt_ready`, the canary is opened on
`https://javsubtitle.com`. Verification confirms that the movie's English subtitle
request resolves to the repaired catalog row/object and returns the repaired SHA-256
or byte-equivalent content. Browser or CDN cache is bypassed with a unique request
nonce where the application permits it. No subtitle text is copied into logs,
reports, screenshots, or the final response.

## Failure behavior

- Selector or prepare precondition failure causes no file, database, or Supabase
  mutation.
- Local deterministic quality failure quarantines the rejected output, marks the job
  failed/permanent, does not upload, does not delete audio, and is not retried.
- A transient translator, Supabase, catalog, or CDN verification failure returns the
  job to `transcription_done` for the existing bounded retry policy.
- After three consecutive quality failures the Mac translation worker exits before
  claiming another job.
- If Storage upload succeeds but catalog update or verification fails, the passing
  object remains; the job is not ready, and retry is idempotent.
- The old bad Supabase object is never restored. A failed repair leaves the existing
  published object unchanged unless a new passing object was already uploaded.
- Logs contain identifiers, reason codes, counts, sizes, and SHA-256 only. They never
  contain subtitle text, credentials, authorization headers, or adult content.

## Testing

Tests must prove:

- selection requires the explicit allowlist and every local eligibility condition;
- prepare requires `limit=1`, exact movie, and matching job ID;
- prepare resets only translation state and preserves Japanese SRT and `audio.wav`;
- the SQLite migration preserves the existing Windows worker attempt count and Mac
  retries use only `translation_attempt_count`;
- the worker quarantines rather than deletes the old English SRT;
- bad English never reaches the Supabase upload function;
- deterministic quality failures are permanent and are not retried;
- transient publication failures return to `transcription_done` within the bounded
  retry policy;
- a good translation uploads, verifies SHA-256/catalog, and only then becomes ready;
- repaired-subtitle same-path Storage upsert and catalog PATCH work;
- a stale CDN response is not accepted and a later matching response is accepted;
- logs and reports contain no full subtitle text or secrets;
- exact-job one-shot mode cannot claim a different `transcription_done` job and
  exits after one processing attempt;
- existing downloader, Windows transcription-only, disabled legacy complete, Mac
  smoke, dashboard, and audit tests continue passing.

The full suite runs before integration, after implementation, after merging, and
immediately before canary execution.

## Execution sequence

1. Merge `codex/local-english-ai-audit` into
   `codex/windows-transcription-mac-translation` and run the full test suite.
2. Push the updated orchestration branch and update the existing draft pull request.
3. Implement the controlled repair and worker publication path with test-first
   red-green cycles and frequent commits.
4. Update the production Mac `.env` with explicit publishing settings.
5. Stop the existing general Mac translation worker and run
   `mac-translation-smoke-test`; do not prepare a canary if smoke fails.
6. Run the read-only selector against the 340-candidate allowlist.
7. Execute prepare for exactly one selected canary using its exact job ID.
8. Run the exact-job one-shot translation worker for that job ID and wait for it to
   exit.
9. Monitor the complete state sequence, local quarantine, quality log, Supabase
   verification, and `https://javsubtitle.com` result.
10. After successful canary verification, restart the normal Mac translation worker
    with publishing enabled.
11. Report the canary outcome and stop. Ask the user whether to begin a five-to-ten
    candidate batch.

## Supabase references

- `https://supabase.com/docs/guides/storage/uploads/standard-uploads`
- `https://supabase.com/docs/guides/storage/security/access-control`
- `https://supabase.com/docs/guides/storage/cdn/smart-cdn`
