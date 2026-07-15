# TranslateLocally Mac Setup

Translation now runs only on the Mac. The Windows worker produces Japanese SRT and hands jobs back as `transcription_done`; it does not load TranslateLocally or create English files.

## Mac configuration

Set these values in the Mac `.env`:

```text
MAC_TRANSLATE_SCRIPT_PATH=/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/scripts/translatelocally_translate_single.py
TRANSLATELOCALLY_PATH=/Applications/translateLocally.app/Contents/MacOS/translateLocally
TRANSLATELOCALLY_MODEL=ja-en-tiny
MAC_TRANSLATION_WORKER_ID=mac-translation-1
MAC_TRANSLATION_LEASE_SECONDS=1800
MAX_TRANSLATION_ATTEMPTS=3
MAC_TRANSLATION_POLL_INTERVAL_SECONDS=10
TRANSLATION_QUALITY_FAILURE_LIMIT=3
```

Adjust `TRANSLATELOCALLY_PATH` to the actual executable inside the installed app bundle.

## Mandatory startup check

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
source .venv/bin/activate
python -m orchestrator mac-translation-smoke-test
```

The check invokes the configured wrapper and model on ten fixed safe Japanese sentences. It requires ten output cues, normalized unique ratio of at least 50%, and no known collapse phrases. It never claims a job.

Before model invocation, the wrapper removes any preexisting Unicode U+FFFD
replacement characters from its in-memory translation input. It never rewrites the
Japanese SRT. The wrapper logs aggregate removal counts without subtitle text and
fails before model invocation if a source line becomes empty. The unchanged
Japanese/English pair quality gate still decides whether publication is allowed.

Only after it exits successfully, start:

```bash
python -m orchestrator mac-translation-worker
```

## Quality and publication boundary

The Mac translation worker:

1. Claims only `transcription_done` jobs.
2. Moves a stale formal English SRT to `rejected/` before translating.
3. Writes the new English SRT atomically.
4. Runs the strict subtitle quality gate.
5. Quarantines deterministic quality failures and marks them permanently failed.
6. Marks a job `english_srt_ready` only after the gate passes.

After three consecutive deterministic quality failures, the Mac translation worker exits before claiming another job.

Any existing upload/Supabase process must consume only `english_srt_ready`. File existence alone is not a publication signal.

`SupabaseSubtitlePublisher.publish_english_ai()` also validates the Japanese and
English files immediately before its first network request. Rejected translations
raise `quality_gate_failed:<reason-codes>` and cannot reach Storage upload. A
repaired subtitle uses Storage `x-upsert: true` and updates the existing catalog
row; invoking that publisher still requires explicit operator authorization.

## Windows warning

The previously installed Windows HPLT `ja-en-tiny` runtime is known to collapse diverse Japanese input into repeated generic English. Do not reconnect it to the Windows worker and do not use its old benchmark output as a quality reference.
