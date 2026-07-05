# Windows Setup

1. Map the Mac SMB share as `M:\`.

2. Create `.env` from `.env.windows.example`:

```text
MAC_API_BASE_URL=http://192.168.1.25:8000
WORKER_ID=windows-gpu-1
WINDOWS_JOBS_ROOT=M:\
WHISPER_MODEL=large-v3-turbo
WHISPER_DEVICE=cuda
WHISPER_COMPUTE_TYPE=float16
OPENAI_API_KEY=replace-with-key
TRANSLATE_SCRIPT_PATH=C:\Users\ytt\Documents\startup\E2E-download-subtitle-generation-translation-scripts\scripts\subtitle_translate.py
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

4. The worker polls the Mac API and processes one GPU job at a time.
