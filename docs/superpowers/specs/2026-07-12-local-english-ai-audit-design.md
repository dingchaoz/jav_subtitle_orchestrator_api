# Local English_AI Audit Fallback Design

## Goal

Audit every existing Supabase `English_AI` subtitle without requiring audit RPCs
and without writing to Supabase. Produce a local, resumable, text-free report that
can safely drive a later allowlisted repair canary.

## Scope

The fallback audits only catalog rows whose language is exactly `English_AI`.
It does not attempt the global catalog-to-Storage or Storage-to-catalog consistency
scan performed by the RPC-based auditor. A missing Storage object referenced by an
`English_AI` catalog row is still detected when its individual download returns
not found.

The command performs no subtitle upload, overwrite, catalog update, audit-table
upsert, requeue, translation reset, quarantine, deletion, or audio operation.

## Architecture

Add a focused `historical_english_ai_audit.py` module with three boundaries:

1. A GET-only Supabase reader paginates `movie_languages` joined to movie codes and
   downloads the corresponding object from the configured subtitle bucket.
2. A pure local inspector parses and classifies one English SRT using locked
   historical hard-failure thresholds without importing or changing the production
   Mac translation-pair gate.
3. A report writer appends resumable JSONL records and atomically derives CSV,
   summary JSON, and a hard-failure movie allowlist.

Expose this as `python -m orchestrator audit-english-ai-local` with explicit output,
rate, file-size, and record bounds. The command defaults to the configured Supabase
URL, server-side key, and `subtitles` bucket.

## Quality policy

Hard failures are:

- Storage object missing;
- empty file;
- no valid SRT cues;
- invalid timeline beyond the locked tolerance;
- severe mojibake;
- known refusal/placeholder phrase at least three times and at least two percent of
  text lines;
- dominant normalized line at least fifty percent for twenty or more text lines;
- unique ratio below fifteen percent together with dominant ratio at least
  twenty-five percent for one hundred or more text lines.

Warnings or review findings do not enter the automatic repair allowlist. Unknown
expected duration, language uncertainty, sparse content, or weak coverage evidence
remain non-hard findings.

## Read-only enforcement

- The network client exposes only GET operations.
- Tests fail if a POST, PATCH, PUT, or DELETE request occurs.
- The command never receives a `--persist`, `--apply`, `--force`, or upload flag.
- The Supabase service key remains server-side and is never serialized into reports,
  exceptions, logs, or browser output.
- Upstream response bodies are bounded before parsing.

## Resource and resume bounds

- Catalog pages contain at most 500 rows.
- Each subtitle object is capped at 32 MiB.
- Requests default to two per second and may use at most four workers.
- JSONL is the durable checkpoint. A rerun validates prior lines and skips completed
  subtitle IDs.
- Output paths are local and created beneath the explicitly supplied output root.
- Reports contain no raw or sampled subtitle text. Dominant text is represented only
  by SHA-256.

## Outputs

- `audit-results.jsonl`: one sanitized record per catalog subtitle.
- `audit-results.csv`: deterministic tabular projection.
- `audit-summary.json`: total, status, reason, error, and completeness counts.
- `repair-allowlist.txt`: sorted movie codes with hard failures only.

Completion is true only when catalog pagination is exhausted and every discovered
row has a terminal local result. A bounded or interrupted run remains partial and
prints a resume command.

## Verification and execution

Unit tests use fake HTTP sessions and safe synthetic SRT fixtures. A one-record
preflight must complete before the approved full run. The full run reads all
`English_AI` catalog rows, writes only the local report directory, and reports the
exact hard-failure count and recommended canary. Actual repair and publication
remain separately authorized operations.
