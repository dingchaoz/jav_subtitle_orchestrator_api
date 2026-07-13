# Mac Setup

1. Create the SMB job root:

```bash
mkdir -p /Users/ytt/MissAVJobs
```

2. Create `.env` from `.env.example` and keep these values for version 1:

```text
ORCHESTRATOR_HOST=0.0.0.0
ORCHESTRATOR_PORT=8000
ORCHESTRATOR_DB_PATH=/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3
MISSAV_PIPELINE_ROOT=/Users/ytt/Documents/startup/MissAV-Pipeline
JOBS_ROOT_MAC=/Users/ytt/MissAVJobs
JOBS_ROOT_WINDOWS=M:\
MAC_DOWNLOAD_CONCURRENCY=1
WORKER_LEASE_SECONDS=1800
MAX_DOWNLOAD_ATTEMPTS=3
MAX_WORKER_ATTEMPTS=3
MAC_TRANSLATE_SCRIPT_PATH=/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/scripts/translatelocally_translate_single.py
TRANSLATELOCALLY_PATH=/Applications/translateLocally.app/Contents/MacOS/translateLocally
TRANSLATELOCALLY_MODEL=ja-en-tiny
MAC_TRANSLATION_WORKER_ID=mac-translation-1
MAC_TRANSLATION_LEASE_SECONDS=1800
MAX_TRANSLATION_ATTEMPTS=3
MAX_PUBLISH_ATTEMPTS=10
MAC_PUBLISH_RETRY_SECONDS=30
MAC_TRANSLATION_POLL_INTERVAL_SECONDS=10
TRANSLATION_QUALITY_FAILURE_LIMIT=3
MAC_TRANSLATION_PUBLISH_ENABLED=false
SUBTITLE_AUDIT_VISIBILITY_ENABLED=true
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=replace-with-server-side-key
SUPABASE_SUBTITLE_BUCKET=subtitles
SUPABASE_PUBLISH_VERIFY_TIMEOUT_SECONDS=90
LOCAL_AUDIT_TIMEOUT_SECONDS=30
SUBTITLE_AUDIT_TIMEOUT_SECONDS=30
```

The Supabase key is server-side only. Never place it in dashboard HTML, browser
JavaScript, logs, screenshots, or committed files. If audit visibility is disabled
or credentials are absent, the jobs dashboard continues working and the Subtitle
Quality panel shows unavailable.

3. Install and run the API:

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python -m orchestrator api
```

4. In a second terminal, run the Mac downloader worker:

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
source .venv/bin/activate
python -m orchestrator mac-worker
```

5. Verify the real Mac translation runtime before starting its queue worker:

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
source .venv/bin/activate
python -m orchestrator mac-translation-smoke-test
```

The command must exit 0. It does not claim jobs. If it fails, keep the translation worker stopped; the downloader and Windows transcription worker can continue filling `transcription_done`.

6. In a third terminal, start the Mac translation worker:

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
source .venv/bin/activate
python -m orchestrator mac-translation-worker
```

When `MAC_TRANSLATION_PUBLISH_ENABLED=true`, the publication state flow is:

```text
transcription_done
→ translating
→ quality gate
→ publish_pending
→ publishing
→ ensure public.movies (MissAV/local metadata or code-only placeholder)
→ upsert the English_AI SRT in Storage
→ upsert and verify movie_languages
→ english_srt_ready
```

When `MAC_TRANSLATION_PUBLISH_ENABLED=false` (the default), the worker uses the
local-only compatibility flow. After the quality gate it skips `publish_pending`,
`publishing`, catalog resolution, Storage, and `movie_languages`, and moves directly
to `english_srt_ready`. In this mode, ready means only that the local English SRT is
quality-approved; it does not mean Supabase publication or verification occurred.

With publication enabled, `english_srt_ready` means both publication and
verification completed. A
`metadata_status=placeholder` result is successful: when neither usable local
metadata nor a MissAV catalog match is available, the RPC creates a code-only
`public.movies` row, publication continues, and later metadata enrichment keeps
the same stable movie UUID.

Failure handling is separated by stage:

- A translation quality failure is permanent. The rejected English SRT is retained
  under `rejected/`, audio is kept, and neither catalog resolution nor Storage is
  called.
- Missing or unusable metadata does not reject a quality-approved translation. A
  code-only placeholder is used and publication continues.
- A transient Supabase failure returns the job to `publish_pending` using the
  independent publication retry counter. The validated English SRT and audio stay
  in place, and the worker does not translate again. After
  `MAX_PUBLISH_ATTEMPTS`, the job becomes `failed` while retaining those files.
- A Storage or `movie_languages` verification failure never marks the job
  `english_srt_ready`; it follows the same bounded publication retry behavior.

`MAX_PUBLISH_ATTEMPTS` bounds publication attempts independently of translation.
`MAC_PUBLISH_RETRY_SECONDS` controls the delay before a pending publication may be
claimed again.

## Historical repair planning (dry-run only)

The planner opens the job database read-only and prints intended translation-stage
actions. It does not move files, change job state, requeue work, or contact Supabase.
Use an explicit allowlist and a small batch limit:

```bash
python -m orchestrator plan-historical-subtitle-repair \
  --allowlist abc-001 abc-002 \
  --limit 2
```

Each line identifies the job and movie, preserves the Japanese SRT, names the
planned `rejected/` quarantine path for the old English SRT, and states whether a
later authorized repair would requeue or overwrite. There is no apply mode and no
`force=True` path in this command.

## Catalog publication repair planning (dry-run only)

Use the catalog planner to identify quality-approved local subtitle pairs that a
later, separately approved operation could publish. Always provide both an explicit
comma-separated allowlist and a small limit:

```bash
python -m orchestrator plan-catalog-repairs \
  --allowlist abc-001,abc-002 \
  --limit 2
```

The report is labeled `DRY RUN` and names the expected metadata source and exact
deterministic Storage path. The command performs no HTTP request, database write,
file move, deletion, requeue, catalog mutation, or Storage upsert/overwrite. It has
no apply mode and does not invoke `force=True`.

## Historical English_AI read-only audit

This fallback audits exact `English_AI` catalog rows through Supabase GET requests
and writes reports only to the local output directory. Start with one record:

```bash
python -m orchestrator audit-english-ai-local \
  --output reports/subtitle-audit/english-ai-local-20260712 \
  --limit 1 \
  --workers 1
```

The intentionally bounded preflight exits successfully with `complete=false` and
`bounded=true`. Rerun against the same output directory without `--limit` to resume
from the durable JSONL checkpoint and exhaust the catalog:

```bash
python -m orchestrator audit-english-ai-local \
  --output reports/subtitle-audit/english-ai-local-20260712 \
  --workers 4 \
  --requests-per-second 2
```

The command has no persist, apply, force, upload, overwrite, requeue, deletion, or
repair mode. It never writes Supabase and never reads or changes the local job
database or audio files. Reports contain identifiers, structured reason codes,
metrics, and SHA-256 values, but no subtitle text or service key.

`repair-allowlist.txt` contains hard-failure candidates only. It is evidence for a
later canary decision, not authorization to translate, quarantine, upload,
overwrite, or requeue anything.

## One-job catalog publication-only canary

Use this path only after the exact job and publication operation have received
separate approval. Create a private allowlist file containing exactly `mist-166`,
then prepare and run only the approved job:

```bash
python -m orchestrator prepare-catalog-publication-canary \
  --allowlist-file /absolute/path/catalog-canary-allowlist.txt \
  --movie mist-166 \
  --limit 1 \
  --confirm-job-id job_5ca44399d21c40168821397f10c04538
python -m orchestrator mac-translation-worker-once \
  --job-id job_5ca44399d21c40168821397f10c04538
```

The prepare command initializes the local job database, reruns the local subtitle
quality gate, and moves only that exact eligible row to `publish_pending`. It does
not invoke translation, claim a worker job, contact Supabase, upload, move subtitle
files, or delete audio. Its one-line receipt contains only job/status identifiers,
quality statistics, and the accepted English SHA-256; it never prints subtitle
text, credentials, or adult metadata.

The one-shot worker is the separately authorized publication step. Before any
catalog or Storage call, it reruns the Japanese/English pair quality gate and may
claim only the confirmed job ID. Verify the local Japanese and audio hashes remain
unchanged, the accepted English file remains in place, the remote English hash and
catalog record are verified, and the final state is `english_srt_ready`.

Stop after this job and report its evidence. Preparing or publishing a second job
requires new explicit approval and a new exact-job invocation. This command has no
automatic selector, `--force`, or batch mode; one-job approval never authorizes a
batch, bulk overwrite, requeue, or `audio.wav` deletion.

## One-job historical repair canary

### Deployment approval gate

The repository contains the reviewed migration for the catalog RPC, but this
runbook does **not** state or assume that it has been deployed. Production behavior
of the RPC is unverified until Task 10. Before changing production, stop and obtain
explicit approval for this ordered sequence:

1. Apply only the reviewed migration, then verify RPC privileges and behavior.
2. Run exactly one approved canary and verify its stable catalog UUID, Storage hash,
   `movie_languages` row, unchanged Japanese/audio inputs, and final job status.
3. Report the canary result and stop. Any additional canary or batch requires a
   separate approval; migration approval and one-canary approval never authorize a
   batch.

Do not enable publication, restart a production worker, apply the migration, or use
the catalog repair report as execution authorization before that gate is approved.

After migration deployment and one-canary execution are explicitly approved,
enable verified server-side publication only on the production Mac:

```text
MAC_TRANSLATION_PUBLISH_ENABLED=true
SUPABASE_SUBTITLE_BUCKET=subtitles
SUPABASE_PUBLISH_VERIFY_TIMEOUT_SECONDS=90
```

Keep the Supabase service key server-side. Stop the general translation worker and
run the startup smoke before selecting or preparing a canary. Select exactly one
eligible job from the audit allowlist:

```bash
python -m orchestrator select-historical-repair-canary \
  --allowlist-file reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --preferred-movie abf-279 \
  --output reports/subtitle-audit/english-ai-local-20260712/canary-selection.json
```

Read the sanitized JSON, then bind prepare to its exact movie and job ID:

```bash
SELECTION=reports/subtitle-audit/english-ai-local-20260712/canary-selection.json
JOB_ID=$(jq -r .job_id "$SELECTION")
MOVIE=$(jq -r .movie_number "$SELECTION")
python -m orchestrator prepare-historical-repair-canary \
  --allowlist-file reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --movie "$MOVIE" \
  --limit 1 \
  --confirm-job-id "$JOB_ID"
python -m orchestrator mac-translation-worker-once --job-id "$JOB_ID"
```

The one-shot worker runs startup smoke and can claim only that exact ID. It moves
the old English SRT into `rejected/`, retains it, preserves Japanese SRT and any
preexisting `audio.wav`, and preserves audio absence when historical cleanup already
removed it. It translates, runs the pair quality gate, upserts Supabase, and verifies
Storage SHA-256 plus the `movie_languages` catalog record before marking ready. A
quality failure never creates catalog data or uploads and is permanent. A transient
publishing or verification failure returns to `publish_pending` under the bounded,
independent publication retry counter; it never triggers retranslation. A
code-only placeholder catalog result is allowed and successful.

After local, Supabase, CDN, and `https://javsubtitle.com` verification succeeds,
restart the normal `mac-translation-worker`. This procedure authorizes one canary
only. Do not prepare another historical job or begin a five-to-ten-job batch without
new approval.

7. Submit a batch:

```bash
curl -X POST http://127.0.0.1:8000/jobs/batch \
  -H "Content-Type: application/json" \
  -d '{"movie_numbers":["ktb-096","ktb-095","ktb-093"],"priority":100,"force":false}'
```
