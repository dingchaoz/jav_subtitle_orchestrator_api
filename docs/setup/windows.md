# Windows Setup

1. Map the Mac SMB share as `M:\`.

2. Create `.env.windows` from `.env.windows.example`:

```text
MAC_API_BASE_URL=http://192.168.1.205:8010
WORKER_ID=windows-gpu-1
WINDOWS_JOBS_ROOT=M:\
WHISPER_MODEL=large-v3-turbo
WHISPER_DEVICE=cuda
WHISPER_COMPUTE_TYPE=float16
TRANSCRIBE_SCRIPT_PATH=C:\Users\dingc\OneDrive\Documents\startup\MissAV_AI_SUB_Generator\CLI\scripts\batch_transcribe_enhanced.py
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

6. Windows no longer needs TranslateLocally, Codex translation settings, or a translation API key. After Japanese SRT succeeds, it calls the Mac `transcription-complete` endpoint and releases the job as `transcription_done`.

7. Start the Windows transcription worker after the Mac API is running:

```powershell
.\.venv\Scripts\python.exe -m orchestrator windows-worker
```

The expected Windows terminal state is `transcription_done`; `english_srt_ready` is produced later by the separate Mac translation worker. Windows must never create `.English.srt` or call the legacy complete endpoint.
