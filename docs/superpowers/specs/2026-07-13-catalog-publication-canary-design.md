# Catalog Publication-Only Canary Design

**Date:** 2026-07-13

## Purpose

Provide one auditable path for an existing Japanese/English SRT pair that already
passes the server-side quality gate but failed under the legacy catalog lookup to
resume at `publish_pending`. The first authorized use is exactly job
`job_5ca44399d21c40168821397f10c04538` (`mist-166`).

This path must prove metadata-resilient publication without translating again,
moving the accepted English SRT, deleting `audio.wav`, or claiming another job.

## Command contract

Add a `prepare-catalog-publication-canary` CLI command with all of these required
arguments:

```text
--allowlist-file PATH
--movie MOVIE_CODE
--limit 1
--confirm-job-id JOB_ID
```

The command refuses limits other than one. It canonicalizes the requested movie
only for comparison, requires the movie to appear in the explicit allowlist, and
requires the confirmed job ID to identify the same movie. It never selects a
different job automatically.

The command initializes additive SQLite columns before reading the job, but it does
not call Supabase, Storage, the translator, or any worker claim endpoint.

## Eligibility and validation

Preparation is allowed only when all conditions hold:

- the exact job exists, is unclaimed, and has status `failed` or legacy
  `english_srt_ready`;
- an `english_srt_ready` job is eligible only when its catalog UUID or validated
  metadata source/status is absent, so a modern verified publication cannot be
  reset accidentally;
- canonical Japanese and English SRT files exist, are non-empty regular files, and
  are under the job's canonical Mac directory;
- the current Japanese/English pair passes `validate_translation_quality` using the
  same thresholds as normal publication;
- no caller-supplied subtitle path is accepted;
- the allowlist, requested movie, exact job ID, current status, and validated paths
  still match inside the state-transition transaction.

A validation failure makes no database or filesystem change. Error output contains
only structured reason codes and paths/hashes where needed, never subtitle text.

## State transition

On success, one `BEGIN IMMEDIATE` transaction changes only the exact job:

```text
failed or legacy english_srt_ready -> publish_pending
```

It sets canonical `english_srt_path_mac` and `english_srt_path_windows`, clears any
claim/lease and legacy publication error, resets only publication retry fields and
catalog observability, and leaves `translation_attempt_count` unchanged. Download
attempts, translation attempts, Japanese path, audio path, and all input files are
preserved.

The command prints a sanitized receipt containing job ID, movie code, prior/new
status, unchanged translation-attempt count, English SHA-256, and quality summary.

## Publication and verification flow

After preparation, run the existing exact-job command:

```text
python -m orchestrator mac-translation-worker-once --job-id <exact job ID>
```

`process_job_id` may claim only that ID from `publish_pending`. The publisher reruns
the quality gate before any catalog or Storage call, ensures the catalog row (a
placeholder is acceptable), upserts the canonical English object, verifies Storage
SHA-256 and `movie_languages`, and only then marks `english_srt_ready`.

If publication is transiently unavailable, the job returns to `publish_pending`
under the independent bounded publication counter without retranslation. If the
publisher-side quality gate deterministically fails, the existing permanent-quality
failure behavior applies: no upload, English moved to `rejected/`, audio preserved,
and no automatic retry.

## Canary evidence and stop condition

Before preparation, record SHA-256 for Japanese, English, and `audio.wav` when
present. After the one-shot worker exits, require:

- `quality.log` has a final statistics-only `passed=true` record;
- Japanese and audio hashes are unchanged;
- Storage bytes equal the validated local English SHA-256;
- `public.movie_languages` points to the ensured movie UUID, canonical path, and
  size;
- job metadata records the catalog UUID and source/status;
- final job state is `english_srt_ready` only after both remote verifications.

Then stop. The command and the one-job authorization do not approve another canary,
historical batch, `force=True`, audio deletion, or bulk overwrite.

## Test coverage

Tests must prove:

- exact allowlist/movie/job binding and limit-one enforcement;
- bad quality and missing files leave the row and filesystem unchanged;
- preparation never invokes translator, publisher, or Supabase;
- only publication fields reset and translation attempts remain unchanged;
- accepted English is neither moved nor rewritten;
- exact-job worker publishes the prepared job without translating or claiming a
  second job;
- normal publisher quality failure and retry behavior remains unchanged.
