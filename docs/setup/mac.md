# Mac Setup

1. Create the SMB job root:

```bash
mkdir -p /Users/ytt/MissAVJobs
```

2. Create `.env` from `.env.example` and keep these values for version 1:

```text
ORCHESTRATOR_HOST=0.0.0.0
ORCHESTRATOR_PORT=8010
ORCHESTRATOR_DB_PATH=/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3
MISSAV_PIPELINE_ROOT=/Users/ytt/Documents/startup/MissAV-Pipeline
JOBS_ROOT_MAC=/Users/ytt/MissAVJobs
JOBS_ROOT_WINDOWS=M:\
MAC_DOWNLOAD_CONCURRENCY=1
WORKER_LEASE_SECONDS=1800
MAX_DOWNLOAD_ATTEMPTS=3
MAX_WORKER_ATTEMPTS=3
PUBLISH_TO_SUPABASE=false
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=replace-with-service-role-key
SUPABASE_STORAGE_BUCKET=subtitles
JAVSUBTITLE_API_BASE=https://javsubtitle.com
JAVSUBTITLE_ADMIN_API_TOKEN=replace-with-admin-token
JAVSUBTITLE_POST_SYNC_ENABLED=false
```

Keep `JAVSUBTITLE_ADMIN_API_TOKEN` only in the Mac API `.env`. Do not put it in browser,
dashboard, or Windows frontend code.

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

5. Submit a batch:

```bash
curl -X POST http://127.0.0.1:8010/jobs/batch \
  -H "Content-Type: application/json" \
  -d '{"movie_numbers":["ktb-096","ktb-095","ktb-093"],"priority":100,"force":false}'
```
