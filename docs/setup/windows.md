# Windows Setup

## Transcription quality invariant

The built-in faster-whisper path must use `WHISPER_CHUNK_SECONDS=90`. The old
900-second chunk size reproducibly omitted more than 15 minutes of audible
dialogue in DVRT-6701. Treat 90 seconds as a production quality requirement,
not a performance-tuning suggestion. Any future change must be validated on a
labeled multi-movie coverage/noise benchmark before deployment.

Keep `TRANSCRIBE_SCRIPT_PATH=` blank to use the adaptive built-in path. An
external script bypasses the 90-second primary pass and the dual-grid gap
repair unless it independently implements the same contract.

1. Map the Mac SMB share as `M:\`.

2. Create `.env.windows` from `.env.windows.example`:

```text
MAC_API_BASE_URL=http://192.168.1.205:8010
WORKER_ID=windows-gpu-1
WINDOWS_JOBS_ROOT=M:\
WHISPER_MODEL=large-v3-turbo
WHISPER_DEVICE=cuda
WHISPER_COMPUTE_TYPE=float16
WHISPER_CHUNK_SECONDS=90
WHISPER_GAP_REPAIR_ENABLED=true
WHISPER_REPAIR_GAP_SECONDS=60
WHISPER_REPAIR_CHUNK_SECONDS=30
WHISPER_REPAIR_OFFSET_SECONDS=15
WHISPER_REPAIR_PADDING_SECONDS=15
WHISPER_REPAIR_MIN_SIMILARITY=0.72
TRANSCRIBE_SCRIPT_PATH=
TRANSCRIBE_PYTHON_EXECUTABLE=
POLL_INTERVAL_SECONDS=10
HEARTBEAT_INTERVAL_SECONDS=60
```

3. Install and run:

```powershell
cd C:\Users\ytt\Documents\startup\JAV-Subtitle-Orchestrator
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev,windows]"
python -m orchestrator windows-worker
```

4. `TRANSCRIBE_SCRIPT_PATH` lets the worker call the existing `batch_transcribe_enhanced.py` script instead of the built-in `faster-whisper` adapter. The worker stages a temp copy of each `audio.wav` before invoking that script, so the shared job audio is not deleted by the external pipeline.

5. Leave `TRANSCRIBE_PYTHON_EXECUTABLE` blank to run the external transcription script with this repo's `.venv`. Set it to another `python.exe` only if that interpreter already has the external script's Whisper dependencies installed cleanly.

6. When `TRANSCRIBE_SCRIPT_PATH` is blank, the built-in transcriber uses the DVRT-6701 coverage preset. It runs a 90-second primary pass, finds suspicious gaps of at least 60 seconds, and repairs only those ranges with two 30-second grids offset by 15 seconds. A repair cue is published only when both grids agree. `WHISPER_REPAIR_PADDING_SECONDS` adds context around each gap and `WHISPER_REPAIR_MIN_SIMILARITY` controls consensus strictness. These settings do not affect external-script mode.

7. Windows no longer needs TranslateLocally, Codex translation settings, or a translation API key. After Japanese SRT succeeds, it calls the Mac `transcription-complete` endpoint and releases the job as `transcription_done`.

8. Start the Windows transcription worker after the Mac API is running:

```powershell
.\.venv\Scripts\python.exe -m orchestrator windows-worker
```

The expected Windows terminal state is `transcription_done`; `english_srt_ready` is produced later by the separate Mac translation worker. Windows must never create `.English.srt` or call the legacy complete endpoint. Each successful built-in transcription also writes a `transcription_stats` JSON line to the job's `logs/whisper.log`.
