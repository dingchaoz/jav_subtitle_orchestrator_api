# Branch Consolidation Audit - 2026-07-19

Cutover baseline:

- Branch: `main`
- Current cutover commit: `50fef19d`
- Runtime fix commit included: `b4bbdae5`

## Summary

`main` is the clean cutover branch. It now includes the Dashboard/Mac-worker runtime fix and the standalone cross-site audio downloader that was still useful from an old branch.

The remaining old feature branches are not safe to merge directly. They all fork from `85e9cab0` and are about 198 commits behind current `main`. Tip-to-tip diffs would delete or downgrade current production modules such as `orchestrator/store.py`, `orchestrator/__main__.py`, `orchestrator/mac_worker.py`, launchd plists, historical repair modules, audio recovery modules, catalog visibility modules, and current Dashboard fixes.

## Consolidated

### `codex/dashboard-worker-cutover-clean-state`

Action: fast-forwarded into `main`.

Included:

- Default-off MissAV metadata refresh guardrail.
- Direct-page metadata fallback for catalog misses.
- Dashboard Mac downloader status fallback.
- Runtime `data/` ignore cleanup.

Verification:

- `pytest -q` passed with `1338 passed, 1 warning` before the next consolidation commit.

### `codex/cross-site-audio-download`

Action: partially consolidated into `main` as commit `50fef19d`.

Included files:

- `orchestrator/site_audio/*`
- `scripts/download_site_audio.py`
- `tests/test_site_audio_*.py`
- `docs/superpowers/plans/2026-07-11-cross-site-audio-download.md`
- `pyproject.toml` dependencies for `curl-cffi` and `playwright`

Reasoning:

- The site-audio package is self-contained and tested independently.
- The rest of the branch is an old fork containing superseded Dashboard/Supabase/documentation changes.

Verification:

- Site-audio focused tests: `67 passed`
- Full suite after consolidation: `1405 passed, 1 warning`

## Not Consolidated

### `codex/supabase-ai-publish`

Decision: do not merge.

Reason:

- This is an old Dashboard/Supabase publishing branch.
- Current `main` already has newer `orchestrator/supabase_publisher.py`, callback, catalog-sync, dashboard, and requested-subtitle importer code.
- Direct merge would downgrade large current modules and remove later production fixes.

Remaining branch-only files are mostly old docs/tests such as:

- `README-TranslateLocally-ja-en.md`
- `docs/api-client-guide.md`
- early dashboard/publishing plans/specs
- old API publish/doc tests

### `codex/reazon-asr-benchmark`

Decision: do not merge.

Reason:

- Despite the branch name, this branch does not contain the later Reazon/local-worker implementation files that appear in `codex/subtitle-quality-audit-remediation`.
- It is effectively the same old Dashboard/Supabase publishing line plus Mac post-publish cleanup docs.
- Direct merge would downgrade current production modules.

### `codex/subtitle-quality-audit-remediation`

Decision: retain as an archive or rebase as a separate future project; do not include in this cutover.

Reason:

- This branch contains real additional feature work, but it is tightly coupled across CLI, settings, store, worker, publisher, Supabase migrations, audit APIs, and remediation runners.
- It imports/defines contracts that diverged from current `main`; for example, current `main` keeps `AuditStatus` and `ReasonCode` in `orchestrator.models`, while this branch's new remediation/audit modules import them from `orchestrator.subtitle_quality`.
- Tip-to-tip diff against current `main` would heavily rewrite or delete current production code:
  - `orchestrator/__main__.py`
  - `orchestrator/config.py`
  - `orchestrator/mac_worker.py`
  - `orchestrator/store.py`
  - `orchestrator/subtitle_quality.py`
  - `orchestrator/supabase_publisher.py`

Branch-only feature areas that may be worth a dedicated rebase later:

- `orchestrator/srt_inspection.py`
- `orchestrator/subtitle_audit.py`
- `orchestrator/subtitle_audit_checkpoint.py`
- `orchestrator/subtitle_remediation.py`
- `orchestrator/subtitle_remediation_review.py`
- `orchestrator/subtitle_remediation_runner.py`
- `orchestrator/supabase_audit_client.py`
- `orchestrator/reazon_transcription.py`
- `orchestrator/reazon_benchmark.py`
- `orchestrator/local_worker.py`
- Supabase migrations under `supabase/migrations/20260712*`

Recommended future path:

1. Create a fresh branch from current `main`.
2. Port the audit/remediation domain model first.
3. Adapt imports to current `orchestrator.models`.
4. Port CLI/settings/store integrations in small tested slices.
5. Apply Supabase migrations only after schema review.

## Current Recommendation

Use `main` at `50fef19d` as the clean source cutover point.

Do not use any of the old unmerged branches as a source tree for migration. Treat them as archives, except `codex/subtitle-quality-audit-remediation`, which should become a separate planned rebase if that feature is still wanted.
