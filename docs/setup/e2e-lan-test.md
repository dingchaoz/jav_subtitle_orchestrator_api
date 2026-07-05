# End-to-End LAN Test

Use this runbook after all unit and integration tests pass. It verifies one movie across the Mac API, Mac downloader worker, SMB share, and Windows worker.

## Preconditions

- Mac API is running on `http://0.0.0.0:8000`.
- Mac downloader worker is running.
- Windows worker is running.
- Windows can read and write `M:\`.
- `M:\` points to `/Users/ytt/MissAVJobs`.
- `.env` exists on Mac.
- `.env.windows` exists on Windows.
- `OPENAI_API_KEY` is set on Windows.
- `TRANSLATE_SCRIPT_PATH` points to the existing `subtitle_translate.py` script.

## Submit One Movie

Submit `ktb-096` from the Mac:

```bash
curl -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"movie_number":"ktb-096","priority":100,"force":false}'
```

Expected queued response:

```json
{
  "id": "job_...",
  "movie_number": "ktb-096",
  "status": "queued",
  "job_dir_mac": "/Users/ytt/MissAVJobs/ktb-096",
  "job_dir_windows": "M:\\ktb-096",
  "error": null
}
```

## Confirm Mac Job Folder

After the Mac downloader worker finishes, confirm the job folder:

```bash
ls -la /Users/ytt/MissAVJobs/ktb-096
```

Expected files:

```text
job.json
metadata.json
audio.wav
```

## Confirm Windows SMB Visibility

From Windows, confirm the same job files are visible through the mapped SMB drive:

```powershell
Get-ChildItem M:\ktb-096
```

Expected files:

```text
job.json
metadata.json
audio.wav
```

## Wait For Windows Worker Completion

Wait for the Windows worker to finish transcription and translation.

Expected final files:

```text
M:\ktb-096\ktb-096.Japanese.srt
M:\ktb-096\ktb-096.English.srt
```

## Confirm API Status

From the Mac, check job status:

```bash
curl http://127.0.0.1:8000/jobs
```

Expected `ktb-096` status:

```text
english_srt_ready
```

## Failure Checks

If status is `failed`, inspect:

```text
/Users/ytt/MissAVJobs/ktb-096/job.json
/Users/ytt/MissAVJobs/ktb-096/logs/mac-download.log
/Users/ytt/MissAVJobs/ktb-096/logs/windows-worker.log
/Users/ytt/MissAVJobs/ktb-096/logs/whisper.log
/Users/ytt/MissAVJobs/ktb-096/logs/translate.log
```
