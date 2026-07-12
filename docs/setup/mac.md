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
```

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

7. Submit a batch:

```bash
curl -X POST http://127.0.0.1:8000/jobs/batch \
  -H "Content-Type: application/json" \
  -d '{"movie_numbers":["ktb-096","ktb-095","ktb-093"],"priority":100,"force":false}'
```
