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
MAC_TRANSLATION_POLL_INTERVAL_SECONDS=10
TRANSLATION_QUALITY_FAILURE_LIMIT=3
SUBTITLE_AUDIT_VISIBILITY_ENABLED=true
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=replace-with-server-side-key
SUPABASE_SUBTITLE_BUCKET=subtitles
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

It claims only `transcription_done`, writes English on the Mac/SMB job path, runs the quality gate, and exposes only passing files as `english_srt_ready`.

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

## One-job historical repair canary

Enable verified server-side publication only on the production Mac:

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
Storage SHA-256 plus catalog metadata before marking ready. A quality failure never
uploads and is permanent. A transient publishing failure returns to
`transcription_done` under the bounded translation retry counter.

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
