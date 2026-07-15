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
JOBS_ROOT_WINDOWS="M:\\"
MAC_DOWNLOAD_CONCURRENCY=1
MAC_DOWNLOAD_WORKER_ID=mac-downloader-1
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
SUBTITLE_AUDIT_VISIBILITY_ENABLED=false
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=replace-with-server-side-key
SUPABASE_SUBTITLE_BUCKET=subtitles
SUPABASE_PUBLISH_VERIFY_TIMEOUT_SECONDS=90
JAVSUBTITLE_API_BASE=https://your-javsubtitle-api.example
JAVSUBTITLE_ADMIN_API_TOKEN=replace-with-server-side-admin-api-token
CATALOG_SYNC_RETRY_SECONDS=30
MAX_CATALOG_SYNC_ATTEMPTS=10
LOCAL_AUDIT_TIMEOUT_SECONDS=30
SUBTITLE_AUDIT_TIMEOUT_SECONDS=30
```

The Supabase key is server-side only. Never place it in dashboard HTML, browser
JavaScript, logs, screenshots, or committed files. If audit visibility is disabled
or credentials are absent, the jobs dashboard continues working and the Subtitle
Quality panel shows unavailable. `SUBTITLE_AUDIT_VISIBILITY_ENABLED=false` is the
default; enable it only as a separate, reviewed decision after server-side
credentials and exposure boundaries have been verified.

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

The command must exit 0. It does not claim jobs. If it fails, keep the translation
worker stopped; the downloader and Windows transcription worker can continue
filling `transcription_done`.
The safe smoke uses exactly ten fixed non-production sentences and logs only
aggregate quality data. Require `cues=10`, `unique_ratio` at least `0.500`, and
`known_bad=0`; never log or report the translated cue text.

6. In a third terminal, start the Mac translation worker:

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
source .venv/bin/activate
python -m orchestrator mac-translation-worker
```

When `MAC_TRANSLATION_PUBLISH_ENABLED=true`, the post-download production state
flow is:

```text
audio_ready → transcription_claimed → transcribing → transcription_done
→ translating → publish_pending → publishing
→ catalog_sync_pending → catalog_syncing → english_srt_ready
```

Windows owns `transcription_claimed` through `transcription_done`. The one Mac
translation worker then runs translation and the quality gate, verifies the
Supabase Storage/catalog publication, enters `catalog_sync_pending`, synchronizes
the exact movie through the javsubtitle.com admin API, verifies the public movie
API, and only then marks `english_srt_ready`.

When `MAC_TRANSLATION_PUBLISH_ENABLED=false` (the default), the worker uses the
local-only compatibility flow. After the quality gate it skips `publish_pending`,
`publishing`, catalog resolution, Storage, `movie_languages`,
`catalog_sync_pending`, and `catalog_syncing`, and moves directly to
`english_srt_ready`. In this mode, ready means only that the local English SRT is
quality-approved; it does not mean Supabase or javsubtitle.com verification
occurred.

With publication enabled, `english_srt_ready` means Supabase publication and
javsubtitle.com synchronization/visibility verification all completed. A
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
- A transient Supabase publication, Storage upload/verification, or
  `movie_languages` upsert/verification failure returns the job to
  `publish_pending` using the independent publication retry counter. The validated
  English SRT and audio stay in place, and the worker does not translate again.
  After `MAX_PUBLISH_ATTEMPTS`, the job becomes `failed` while retaining those
  files.
- Successful Supabase publication stores its verified receipt and advances the job
  to `catalog_sync_pending`. From that point, a javsubtitle.com exact catalog-sync
  or public-API visibility failure remains in or returns to
  `catalog_sync_pending`. Its independent retry revalidates the durable Supabase
  receipt and repeats only the exact sync/visibility checks; it does not
  retranslate, republish Supabase, delete audio, or replace the verified English
  SRT. Authentication failures and exhausted catalog retries hard-pause the
  historical lane.

`MAX_PUBLISH_ATTEMPTS` bounds publication attempts independently of translation.
`MAC_PUBLISH_RETRY_SECONDS` controls the delay before a pending publication may be
claimed again.
`MAX_CATALOG_SYNC_ATTEMPTS` and `CATALOG_SYNC_RETRY_SECONDS` independently bound
catalog synchronization after Supabase publication.

## Recover one interrupted audio download

This is an exact-job recovery for a stale `downloading_audio` row, not a selector or
general retry. Before running it, obtain independent approval that names the exact
job ID, canonical movie code, and staged WAV SHA-256. If any of those three approved
values is absent or differs from current evidence, stop. This recovery approval
does not authorize a restart, requeue, worker change, or any other job.

After approval, stop only the downloader, prove that the exact job is unclaimed,
identify the adapter's staged WAV, and calculate its SHA-256 without printing audio
or metadata. Then bind the operation to all three approved values:

```bash
JOB_ID=job_exact_id
MOVIE=abc-001
STAGED=/Users/ytt/MissAVJobs/$MOVIE/audio/$MOVIE.wav
EXPECTED_SHA256=$(shasum -a 256 "$STAGED" | awk '{print $1}')
.venv/bin/python -m orchestrator recover-interrupted-audio \
  --job-id "$JOB_ID" \
  --movie "$MOVIE" \
  --expected-sha256 "$EXPECTED_SHA256"
```

The command requires the row to remain unclaimed in `downloading_audio`, rejects
symlinks and path mismatches, accepts only a nonempty 16 kHz mono 16-bit PCM WAV
with the exact digest, atomically moves it to `audio.wav`, and advances only that
job to `audio_ready`. Its receipt contains only job/movie/status, SHA-256, size,
duration, and `reused_final`. If a crash happened after the move but before the
database commit, rerunning the identical command validates and reuses that exact
final file; it never redownloads or overwrites it. Any failed precondition leaves
the row and staged evidence unchanged. There is no batch, delete, or force option.
After a successful command, report its safe receipt and stop for the next explicit
instruction. Do not restart a worker, requeue a job, or continue into launchd or
historical repair as a side effect of recovery approval.

## launchd worker installation and status

The installer is deliberately scoped to exactly the downloader and translation
worker labels. The plists use the production checkout, its `.venv`, stable worker
IDs from `.env`, ten-second restart throttling, and separate logs under `logs/`.

Before inspecting or stopping terminal worker processes, and before invoking the
installer, enter the production checkout. Let `MacSettings` load that checkout's
`.env`; never source the file into a shell. The preflight below prints only safe
status words, rejects empty or known placeholder/example values, requires
publication to be enabled, and runs the fixed smoke only after every configuration
check passes. It first unsets only the four remote credentials and the publish flag
so interactive-shell exports cannot override the production checkout's `.env`.
The preflight process and smoke process each construct their own `MacSettings` and
read that same production `.env`; the later launchd services do the same. Do not
unset `TRANSLATELOCALLY_PATH` or `TRANSLATELOCALLY_MODEL`. The smoke process's own
`MacSettings` loads those values and exports them for its translation runtime; the
preflight process does not export them.

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
(
  set -e
  unset \
    SUPABASE_URL \
    SUPABASE_SERVICE_ROLE_KEY \
    JAVSUBTITLE_API_BASE \
    JAVSUBTITLE_ADMIN_API_TOKEN \
    MAC_TRANSLATION_PUBLISH_ENABLED
  .venv/bin/python - <<'PY'
import re
import socket
from ipaddress import ip_address
from pathlib import Path
from urllib.parse import urlsplit

from orchestrator.config import PROJECT_ROOT, MacSettings

EXPECTED_ROOT = Path(
    "/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator"
).resolve()


def production_url(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    raw = value.strip()
    try:
        parsed = urlsplit(raw)
        host = (parsed.hostname or "").lower().rstrip(".")
    except ValueError:
        return False
    if not host:
        return False
    try:
        address = ip_address(host)
    except ValueError:
        address = None
    try:
        legacy_ipv4_host = address is None and bool(socket.inet_aton(host))
    except OSError:
        legacy_ipv4_host = False
    loopback = address is not None and address.is_loopback
    numeric_host = re.fullmatch(r"[0-9.]+", host) is not None
    noncanonical_numeric_host = numeric_host and address is None
    decimal_integer_host = host.isdecimal()
    host_tokens = {
        token
        for label in host.split(".")
        for token in re.split(r"[-_]", label)
        if token
    }
    placeholder_host = (
        not host
        or loopback
        or legacy_ipv4_host
        or noncanonical_numeric_host
        or decimal_integer_host
        or host in {"example", "invalid", "test", "localhost"}
        or host.endswith((".example", ".invalid", ".test", ".localhost"))
        or bool(
            host_tokens
            & {"example", "placeholder", "dummy", "fake", "test", "your"}
        )
    )
    return (
        parsed.scheme == "https"
        and not placeholder_host
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {"", "/"}
        and not parsed.query
        and not parsed.fragment
    )


def production_secret(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    raw = value.strip()
    lowered = raw.lower()
    tokens = {
        token for token in re.split(r"[^a-z0-9]+", lowered) if token
    }
    return not (
        len(raw) < 16
        or lowered.startswith(
            (
                "replace-with-",
                "your-",
                "example-",
                "placeholder-",
                "placeholder_",
                "dummy-",
                "dummy_",
                "fake-",
                "fake_",
                "test-",
                "test_",
                "change-me",
                "changeme",
            )
        )
        or lowered in {
            "token",
            "secret",
            "placeholder",
            "dummy",
            "fake",
            "test",
        }
        or bool(tokens & {"placeholder", "dummy", "fake", "test"})
        or "<" in raw
        or ">" in raw
    )


try:
    settings = MacSettings()
except Exception:
    settings = None

root_ready = (
    settings is not None
    and Path.cwd().resolve() == EXPECTED_ROOT
    and PROJECT_ROOT.resolve() == EXPECTED_ROOT
)
checks = (
    ("PRODUCTION_CHECKOUT", root_ready),
    (
        "SUPABASE_URL",
        settings is not None and production_url(settings.supabase_url),
    ),
    (
        "SUPABASE_SERVICE_ROLE_KEY",
        settings is not None
        and production_secret(settings.supabase_service_role_key),
    ),
    (
        "JAVSUBTITLE_API_BASE",
        settings is not None and production_url(settings.javsubtitle_api_base),
    ),
    (
        "JAVSUBTITLE_ADMIN_API_TOKEN",
        settings is not None
        and production_secret(settings.javsubtitle_admin_api_token),
    ),
)
publish_enabled = (
    settings is not None
    and settings.mac_translation_publish_enabled is True
)
for name, ready in checks:
    print(f"{name}={'present' if ready else 'missing_or_placeholder'}")
print(
    "MAC_TRANSLATION_PUBLISH_ENABLED="
    + ("enabled" if publish_enabled else "disabled")
)
raise SystemExit(0 if all(ready for _, ready in checks) and publish_enabled else 1)
PY
  .venv/bin/python -m orchestrator mac-translation-smoke-test
)
```

The subshell must exit 0, the checkout and all four configuration names must report
`present`, the publish flag must report `enabled`, and the smoke must report
`cues=10`, `unique_ratio>=0.500`, and `known_bad=0`. Its fixed safe sentences are
not job subtitles. Never print environment values or include translated cue text in
a report. If any check reports `missing_or_placeholder`, publication reports
`disabled`, or the smoke exits nonzero, stop: do not run the installer and do not
install, bootstrap, or start either worker.

Only after that complete preflight succeeds, obtain separate installation approval
that explicitly names both labels: `com.javsubtitle.mac-worker` and
`com.javsubtitle.mac-translation-worker`. Without approval for those exact two
labels, stop; preflight success alone does not authorize install, bootstrap, or
start. After approval, identify and stop any exact terminal instances of the two
worker commands that would duplicate the launchd services. Do not stop the API,
Windows worker, or tunnel. Then invoke the installer:

```bash
pgrep -fal '^(.*/)?python(3(\.[0-9]+)?)? -m orchestrator mac-worker$'
pgrep -fal \
  '^(.*/)?python(3(\.[0-9]+)?)? -m orchestrator mac-translation-worker$'
# Stop only exact terminal duplicates found above; then confirm they are absent.
./scripts/install_mac_worker_launchd.sh
```

The script lints and installs both plists, bootstraps the downloader first, and then
replaces only the two managed launchd services. After it exits 0, check each exact
label and confirm one PID per command:

```bash
DOMAIN=gui/$(id -u)
launchctl print "$DOMAIN/com.javsubtitle.mac-worker" |
  rg 'state =|pid =|last exit code'
launchctl print "$DOMAIN/com.javsubtitle.mac-translation-worker" |
  rg 'state =|pid =|last exit code'
pgrep -fal '^(.*/)?python(3(\.[0-9]+)?)? -m orchestrator mac-worker$'
pgrep -fal \
  '^(.*/)?python(3(\.[0-9]+)?)? -m orchestrator mac-translation-worker$'
```

The historical controller is not installed by this script and must never be
mistaken for a third worker. After installation, report the exact two labels and
their PIDs, then stop for the next explicit instruction. Do not automatically start
the controller or enqueue/execute historical repair work.

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

## Immutable historical batch plan and enqueue

Use the batch planner for the authoritative eligibility totals and immutable enqueue
artifact. Keep the output directory private. The planner reads a bounded allowlist,
the job database, and exact file snapshots; it writes only the requested local plan
file and does not change jobs, repair records, subtitles, audio, or remote systems.

```bash
ALLOWLIST=/absolute/path/repair-allowlist.txt
REPORT_DIR=/absolute/private/path/historical-repair
mkdir -p -m 700 "$REPORT_DIR"
.venv/bin/python -m orchestrator plan-historical-repair-batch \
  --allowlist-file "$ALLOWLIST" \
  --limit 5 \
  --output "$REPORT_DIR/batch-001-plan.json"
```

The current audit allowlist contains 340 lines. That number is input evidence only,
not an affected-job count and not execution authorization. The fresh dry-run's
`eligible_total` is the authoritative affected count. Always retain and report
`allowlist_entries`, `eligible_total`, `selected`, `already_repaired`, `ineligible`,
and `blocked`; `--limit` bounds only `selected` (1 through 20), while eligibility is
calculated across the full allowlist.

The JSON plan binds the allowlist path and content digest, scan digest, exact job
status/timestamp, and Japanese, English, and true byte-for-byte audio hashes. Review
the safe one-line report, keep the JSON private, and enqueue only after separate
approval by confirming its exact digest:

```bash
PLAN="$REPORT_DIR/batch-001-plan.json"
PLAN_SHA=$(jq -er .plan_sha256 "$PLAN")
.venv/bin/python -m orchestrator enqueue-historical-repair-batch \
  --allowlist-file "$ALLOWLIST" \
  --plan-file "$PLAN" \
  --confirm-plan-sha256 "$PLAN_SHA"
```

Enqueue atomically rejects a changed plan, allowlist, database snapshot, or source
file. A successful enqueue creates only immutable pending repair records; it does
not reset a job stage or move a file until the single translation worker later
claims that exact record. An exact replay is idempotent. Never edit the plan JSON,
substitute a different allowlist path, use `force=True`, or treat an old dry-run as
current authority.

## Normal-first historical controller

Do not start the controller until the new-production canary has completed the full
state flow through `english_srt_ready`, all local/Supabase/catalog/visibility checks
have succeeded, and its evidence report has been written. Historical repair then
requires a separate explicit batch approval; successful canary approval alone is
not authorization. Also require exactly one healthy launchd translation worker and
a passing startup smoke.

With no existing historical repair records, the controller itself creates the
immutable plan and enqueues the first approved batch of at most five records. The
single translation worker executes those records one at a time. After that batch is
terminal, the controller may automatically plan and enqueue later batches of at
most 20 within the explicitly approved controller/allowlist scope:

```bash
.venv/bin/python -m orchestrator historical-repair-controller \
  --allowlist-file "$ALLOWLIST" \
  --initial-batch-size 5 \
  --batch-size 20 \
  --poll-interval-seconds 30
```

This process only makes bounded queue decisions; the one translation worker still
executes one unit at a time. Every cycle first yields while any normal translation,
publication, or catalog-sync backlog exists, then waits for the prior historical
batch to become terminal before planning another. A normal job arriving during one
already-running historical unit is selected before the next historical record.
There is never a second TranslateLocally process.

The manual immutable enqueue command in the preceding section is an alternative
explicitly approved first-batch path, not an extra batch. If it was used, the
controller detects those pending records and waits for them; it does not recreate
or duplicate the first batch.

Each stdout line is a path-free JSON receipt with `action`, `reason_code`, plan and
allowlist hashes, and counts. `waiting` is healthy and keeps polling; `complete`
exits 0. A fixed-safe `hard_pause=true` exits 2 immediately. A changed allowlist,
worker-count or heartbeat mismatch, preservation failure, three consecutive
historical quality failures, authentication/policy failure, or inconsistent counts
must be investigated rather than bypassed.

## Pause, resume, and rollback

There is no pause or resume CLI. For a deliberate pause, first interrupt only the
exact `historical-repair-controller` process and retain its last JSON receipt. Then
use the real durable store operation and print only the safe before/after lane state:

```bash
.venv/bin/python - <<'PY'
from orchestrator.config import MacSettings
from orchestrator.store import JobStore

settings = MacSettings()
store = JobStore(
    settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows
)
store.initialize()
before = store.historical_lane_state()
after = store.pause_historical_lane("operator_pause")
print(
    f"before_paused={str(before.paused).lower()} "
    f"before_reason={before.reason_code or 'none'} "
    f"after_paused={str(after.paused).lower()} "
    f"after_reason={after.reason_code or 'none'} "
    f"updated_at={after.updated_at}"
)
PY
```

The durable pause blocks new historical claims and controller enqueues but leaves
normal work available. It does not cancel a historical unit already executing in
the worker. Let that unit reach a recorded terminal/retry boundary unless the
incident requires stopping the exact translation launchd service.

Resume only after recording the pause reason, fixing it, confirming the allowlist
is unchanged, checking exactly one translation process and fresh heartbeat, and
rerunning `mac-translation-smoke-test`. The store operation below clears the reason
and resets the historical consecutive-quality-failure counter; retain its receipt,
then restart the controller with the identical allowlist and batch arguments:

```bash
.venv/bin/python - <<'PY'
from orchestrator.config import MacSettings
from orchestrator.store import JobStore

settings = MacSettings()
store = JobStore(
    settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows
)
store.initialize()
before = store.historical_lane_state()
after = store.resume_historical_lane()
print(
    f"before_paused={str(before.paused).lower()} "
    f"before_reason={before.reason_code or 'none'} "
    f"after_paused={str(after.paused).lower()} "
    f"after_reason={after.reason_code or 'none'} "
    f"quality_failures={after.consecutive_quality_failures} "
    f"updated_at={after.updated_at}"
)
PY
```

Rollback means stopping further historical expansion, not destructively pretending
completed remote or filesystem changes never happened:

1. Interrupt the controller, durably pause the lane as above, and capture its last
   JSON receipt plus `/dashboard/state` historical counts.
2. Leave pending/retry/terminal repair records in SQLite. Do not manually change job
   statuses, clear translation origins, or reset download/transcription stages.
3. Preserve Japanese SRTs, `audio.wav`, accepted English SRTs, and every file under
   `rejected/`. Do not delete, overwrite, requeue, or use `force=True`.
4. With the lane paused, the translation worker may continue normal jobs. If code
   itself must be rolled back, stop only that launchd service, deploy a separately
   reviewed database-compatible rollback commit, rerun compile/tests and the smoke
   gate, then restore exactly one translation worker. Never roll back SQLite or
   Supabase from a filesystem backup while workers are live.
5. Resume history only through the explicit audited operation above. Otherwise keep
   the durable pause and repair forward from the retained evidence.

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

### Deployment approval and preflight gate

Do not run either command below until the migration deployment and this exact
one-job publication have received separate approval, and all of these conditions
have been confirmed:

- The reviewed `ensure_subtitle_movie` migration is deployed to the intended
  Supabase project. RPC execute access has been verified as denied for `anon` and
  `authenticated`, and allowed only for `service_role`.
- The production `.env` contains the real `SUPABASE_URL` and
  `SUPABASE_SERVICE_ROLE_KEY`, plus `JAVSUBTITLE_API_BASE` and
  `JAVSUBTITLE_ADMIN_API_TOKEN`. Check only that all four are present; never print
  either secret or place it in logs, screenshots, shell history, or reports.
- `MAC_TRANSLATION_PUBLISH_ENABLED=true`,
  `SUPABASE_SUBTITLE_BUCKET=subtitles`, and
  `SUPABASE_PUBLISH_VERIFY_TIMEOUT_SECONDS=90` are set for the one-shot process.
- The API and worker commands use the reviewed code from the approved production
  checkout, not an older running process or an unreviewed local change.
- `python -m orchestrator mac-translation-smoke-test` has passed in that checkout
  with the production runtime configuration.
- Before preparation, identify the single ordinary
  `python -m orchestrator mac-translation-worker` process, stop that process, and
  confirm that no ordinary translation worker remains running. If the unique
  process cannot be identified, or zero ordinary translation workers cannot be
  confirmed after stopping it, do not prepare the canary. Do not stop the API,
  downloader, Windows worker, Cloudflare tunnel, or unrelated processes.

If any condition is not satisfied, do not prepare the canary. In particular,
`MAC_TRANSLATION_PUBLISH_ENABLED=false` is the default: with publication disabled,
the exact one-shot worker has no publisher and rejects a prepared
`publish_pending` job. Do not move the row to `publish_pending` first and plan to
enable publication later.

After the gate passes, create a private allowlist file containing exactly
`mist-166`, then prepare and run only the approved job:

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

Only after the one-shot process exits and every local, Supabase, catalog, Storage,
and hash verification succeeds may the ordinary translation worker be restored.
Confirm again that no ordinary translation worker is running, then start exactly
one `python -m orchestrator mac-translation-worker` process from the reviewed
production checkout and confirm there is no duplicate. Leave the API, downloader,
Windows worker, and Cloudflare tunnel running throughout this isolation and
restore sequence.

Stop the canary procedure after this job and report its evidence. Preparing or
publishing a second job requires new explicit approval and a new exact-job
invocation. This command has no automatic selector, `--force`, or batch mode;
one-job approval never authorizes a batch, bulk overwrite, requeue, or `audio.wav`
deletion.

## One-job historical repair canary

### Deployment approval gate

The repository contains the reviewed migration for the catalog RPC, but this
runbook does **not** state or assume that it has been deployed. Before changing
production, stop and obtain explicit approval for this ordered sequence:

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
JAVSUBTITLE_API_BASE=https://your-production-api.example
JAVSUBTITLE_ADMIN_API_TOKEN=replace-with-server-side-admin-api-token
CATALOG_SYNC_RETRY_SECONDS=30
MAX_CATALOG_SYNC_ATTEMPTS=10
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
Storage SHA-256 plus the `movie_languages` catalog record before entering
`catalog_sync_pending`. A quality failure never creates catalog data or uploads and
is permanent. A transient Supabase publication, Storage, or `movie_languages`
verification failure returns to `publish_pending` under the bounded, independent
publication retry counter. Once Supabase publication has succeeded, a
javsubtitle.com exact catalog-sync or public-API visibility failure remains in or
returns to `catalog_sync_pending`; its retry only revalidates the stored verified
Supabase receipt and repeats the exact sync/visibility checks. Neither retry path
retranslates, and the catalog path never republishes Supabase. A code-only
placeholder catalog result is allowed and successful.

After local, Supabase, CDN, and `https://javsubtitle.com` verification succeeds,
restart the normal `mac-translation-worker`. This procedure authorizes one canary
only. Do not prepare another historical job or begin any approved batch without new
approval. The controller limit remains at most five records for its initial batch
and at most 20 for later batches within the explicitly approved scope.

7. Submit a batch:

```bash
curl -X POST http://127.0.0.1:8010/jobs/batch \
  -H "Content-Type: application/json" \
  -d '{"movie_numbers":["ktb-096","ktb-095","ktb-093"],"priority":100,"force":false}'
```
