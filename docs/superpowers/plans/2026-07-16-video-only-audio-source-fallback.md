# Video-Only Audio Source Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover audio jobs whose primary MissAV source is video-only by stopping futile extraction and retrying an authoritative same-base catalog variant inside the same orchestrator attempt.

**Architecture:** The MissAV pipeline classifies ffmpeg's no-audio signature as `source_no_audio` and stops before downloading a temporary video. The orchestrator adapter catches that explicit classification, enumerates bounded same-base entries from the existing release catalog, and moves the first successful candidate WAV to the requested job's canonical path. Existing worker retry backoff, transcription, publication, catalog sync, and verified cleanup remain unchanged.

**Tech Stack:** Python 3.11, `unittest`, `pytest`, subprocess-based MissAV adapter, SQLite/FastAPI orchestration, launchd.

---

## File map

- MissAV pipeline `missav_stream_downloader.py`: classify a resolved source with no audio and skip the futile temp-video fallback.
- MissAV pipeline `test_missav_stream_downloader.py`: regression test for the direct no-audio path.
- Orchestrator `orchestrator/missav_adapter.py`: isolate one candidate download, find same-base catalog variants, retry video-only sources, and record safe fallback logs.
- Orchestrator `tests/test_missav_adapter.py`: candidate-selection, requested-path preservation, unrelated-entry rejection, and exhaustion tests.
- Orchestrator design/plan documents: durable rationale and execution record.

### Task 1: Stop futile temp-video fallback for video-only streams

**Files:**
- Modify: `/Users/ytt/Documents/startup/MissAV-Pipeline/missav_stream_downloader.py:202-260`
- Modify: `/Users/ytt/Documents/startup/MissAV-Pipeline/test_missav_stream_downloader.py`

- [ ] **Step 1: Create an isolated pipeline worktree**

Run:

```bash
git -C /Users/ytt/Documents/startup/MissAV-Pipeline worktree add \
  /Users/ytt/Documents/startup/MissAV-Pipeline/.worktrees/video-only-audio-fallback \
  -b codex/video-only-audio-fallback codex/7mmtv-daily-pipeline
```

Expected: a clean worktree on `codex/video-only-audio-fallback`; the dirty primary checkout remains untouched.

- [ ] **Step 2: Write the failing pipeline test**

Add a test that uses the real control flow and mocks only external stream work:

```python
from tempfile import TemporaryDirectory


def test_direct_no_audio_failure_skips_temp_video_fallback(self):
    downloader = missav_stream_downloader.MissAVStreamDownloader.__new__(
        missav_stream_downloader.MissAVStreamDownloader
    )
    stream = missav_stream_downloader.StreamInfo(
        movie_id="mfyd-123",
        page_url="https://missav.live/en/mfyd-123",
        page_domain="missav.live",
        title="mfyd-123",
        master_m3u8_url="https://cdn.example/playlist.m3u8",
        stream_m3u8_url="https://cdn.example/360p/video.m3u8",
        cookie_header="a=b",
        request_headers={},
    )
    downloader.resolve_stream = mock.Mock(return_value=stream)
    downloader._extract_stream_audio_direct = mock.Mock(
        side_effect=missav_stream_downloader.MissAVDownloadError(
            "Output file does not contain any stream"
        )
    )
    downloader._download_stream_to_temp_video = mock.Mock()

    with TemporaryDirectory() as tmp_dir, self.assertRaisesRegex(
        missav_stream_downloader.MissAVDownloadError,
        "source_no_audio: mfyd-123",
    ):
        downloader.extract_audio_to_wav(
            "mfyd-123",
            Path(tmp_dir) / "audio.wav",
            prefer_direct_audio=True,
        )

    downloader._download_stream_to_temp_video.assert_not_called()
```

- [ ] **Step 3: Run the test and verify RED**

Run:

```bash
.venv/bin/python -m unittest \
  test_missav_stream_downloader.MissAVStreamDownloaderTests.test_direct_no_audio_failure_skips_temp_video_fallback
```

Expected: FAIL because the current method continues to `_download_stream_to_temp_video`.

- [ ] **Step 4: Implement the minimal classifier**

Add a focused helper:

```python
def _is_no_audio_stream_error(self, error: Exception | str) -> bool:
    lowered = str(error).lower()
    return any(
        token in lowered
        for token in (
            "output file does not contain any stream",
            "matches no streams",
            "does not contain any audio stream",
        )
    )
```

In both extraction exception paths, stop immediately when the source is known to have no audio:

```python
except MissAVDownloadError as exc:
    if self._is_no_audio_stream_error(exc):
        raise MissAVDownloadError(f"source_no_audio: {movie_id}") from exc
    last_error = exc
```

This must occur before entering the next mode in `operation_plan`.

- [ ] **Step 5: Run pipeline tests and verify GREEN**

Run:

```bash
.venv/bin/python -m unittest test_missav_stream_downloader
```

Expected: all `MissAVStreamDownloaderTests` pass.

- [ ] **Step 6: Commit the pipeline fix**

```bash
git add missav_stream_downloader.py test_missav_stream_downloader.py
git commit -m "fix: stop video-only audio fallback"
```

### Task 2: Retry authoritative same-base catalog variants

**Files:**
- Modify: `orchestrator/missav_adapter.py:62-215`
- Modify: `tests/test_missav_adapter.py`

- [ ] **Step 1: Create an isolated orchestrator worktree**

Run:

```bash
git worktree add \
  /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.worktrees/video-only-audio-fallback \
  -b codex/video-only-audio-fallback main
```

Expected: a clean worktree containing the committed design and plan.

- [ ] **Step 2: Write failing adapter tests**

Add `SourceNoAudioError` to imports and use a fake subprocess that writes the existing pipeline log schema. The primary candidate writes:

```python
{
    "failed": {
        "mfyd-123": {
            "error": "source_no_audio: mfyd-123",
            "attempts": 1,
        }
    }
}
```

The `mfyd-123-uncensored-leak` candidate writes a WAV under
`audio/mfyd-123-uncensored-leak.wav`. Assert:

```python
adapter.download_audio("mfyd-123", output_path)
assert output_path.read_bytes() == b"RIFFalternateWAVE"
assert attempted_numbers == ["mfyd-123", "mfyd-123-uncensored-leak"]
assert "source_fallback" in (
    output_path.parent / "logs" / "mac-download.log"
).read_text(encoding="utf-8")
```

Add a second test whose catalog contains `other-999-uncensored-leak`; assert it is never attempted and the raised message matches:

```python
with pytest.raises(SourceNoAudioError, match="source_no_audio.*mfyd-123"):
    adapter.download_audio("mfyd-123", output_path)
assert attempted_numbers == ["mfyd-123"]
```

- [ ] **Step 3: Run adapter tests and verify RED**

Run:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  -q tests/test_missav_adapter.py
```

Expected: collection/import failure for missing `SourceNoAudioError` or behavioral failure because only the primary candidate is attempted.

- [ ] **Step 4: Isolate one candidate download**

Add the explicit exception:

```python
class SourceNoAudioError(RuntimeError):
    pass
```

Move the current temporary catalog, queue, subprocess, log parsing, and produced-file lookup into:

```python
def _download_audio_candidate(
    self,
    queue_movie: dict[str, Any],
    output_path: Path,
) -> Path:
```

Use `queue_movie["number"]` when calling `_find_produced_audio`. Convert an upstream detail containing `source_no_audio` or the ffmpeg no-stream signature into `SourceNoAudioError` before generic `FileNotFoundError` handling.

- [ ] **Step 5: Add bounded same-base selection**

Implement:

```python
def _same_base_audio_candidates(
    self,
    movie_number: str,
) -> list[dict[str, Any]]:
    requested = movie_number.strip().lower()
    base = self._base_movie_id(requested)
    catalog_path = (
        self.missav_pipeline_root
        / "new-release"
        / "release_movies_complete.json"
    )
    candidates = []
    for movie in self._catalog_movies(catalog_path):
        number = str(movie.get("number", "")).strip().lower()
        if number == requested or self._base_movie_id(number) != base:
            continue
        candidates.append(self._queue_movie(movie, number))
    return sorted(
        candidates,
        key=lambda item: self._variant_rank(str(item["number"])),
    )[: len(VARIANT_SUFFIXES)]
```

`_variant_rank` returns the index of the matching suffix and places an
unsuffixed alternate last.

- [ ] **Step 6: Add fallback orchestration and safe logging**

Keep the primary path unchanged. Only after `SourceNoAudioError`, attempt the
bounded candidates:

```python
try:
    produced_path = self._download_audio_candidate(primary, output_path)
except SourceNoAudioError as primary_error:
    attempted = [movie_number]
    for candidate in self._same_base_audio_candidates(movie_number):
        candidate_number = str(candidate["number"])
        attempted.append(candidate_number)
        self._append_fallback_log(
            output_path,
            f"source_fallback {movie_number} -> {candidate_number}",
        )
        try:
            produced_path = self._download_audio_candidate(candidate, output_path)
            break
        except SourceNoAudioError:
            continue
    else:
        raise SourceNoAudioError(
            f"source_no_audio: {movie_number}; attempted={','.join(attempted)}"
        ) from primary_error
produced_path.replace(output_path)
```

Use the existing job-log helper so the log contains movie codes and outcomes
but never subprocess headers or cookies.

- [ ] **Step 7: Run adapter tests and verify GREEN**

Run:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  -q tests/test_missav_adapter.py
```

Expected: all adapter tests pass.

- [ ] **Step 8: Commit the orchestrator fix**

```bash
git add orchestrator/missav_adapter.py tests/test_missav_adapter.py
git commit -m "fix: fall back from video-only audio sources"
```

### Task 3: Integrate and verify both repositories

**Files:**
- Verify: both feature worktrees
- Modify only if tests reveal a scoped defect.

- [ ] **Step 1: Run complete pipeline unit coverage**

```bash
cd /Users/ytt/Documents/startup/MissAV-Pipeline/.worktrees/video-only-audio-fallback
.venv/bin/python -m unittest test_missav_stream_downloader test_audio_download_queue
```

Expected: all selected pipeline tests pass.

- [ ] **Step 2: Run the complete orchestrator suite**

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.worktrees/video-only-audio-fallback
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest -q
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/python -m compileall -q orchestrator
git diff --check
```

Expected: all tests pass, compilation exits 0, and diff check is empty.

- [ ] **Step 3: Review the focused diffs**

Confirm no source candidates outside the same base ID, no cookie/header logging,
no changes to publication/sync/cleanup receipt rules, and no edits to unrelated
dirty files in the primary MissAV checkout.

### Task 4: Merge, push, deploy, and restore production

**Files:**
- Merge orchestrator feature into `main`.
- Push pipeline feature branch and integrate the focused fix into its default `master` branch when cleanly applicable.
- Apply the tested pipeline file change to the live dirty checkout without touching unrelated files.
- Update live SQLite job state through the API and official recovery paths.

- [ ] **Step 1: Back up the live database**

Use SQLite's online backup API to create a mode-0600 timestamped copy under
`data/backups`; verify `PRAGMA integrity_check` returns `ok`.

- [ ] **Step 2: Merge and push tested code**

Merge the orchestrator feature into `main`, push `origin/main`, and verify local
`HEAD == origin/main`. Push the pipeline feature and default branch only after
their focused tests pass.

- [ ] **Step 3: Deploy exact tested code**

Restart API and translation LaunchAgents from the orchestrator main worktree.
Apply the focused, reviewed pipeline patch to the live primary checkout because
its unrelated dirty runtime files must be preserved. Keep the downloader
stopped until job state is repaired.

- [ ] **Step 4: Restore interrupted jobs**

Force-reset `mfyd-123` through `POST /jobs` so download and worker counters are
zero. Force-reset `mism-281`, removing only a demonstrably partial WAV if one
exists. Do not touch completed SRT artifacts for unrelated jobs.

- [ ] **Step 5: Resume and monitor**

Bootstrap `com.javsubtitle.mac-worker`. Confirm `mfyd-123` logs a same-base
fallback to `mfyd-123-uncensored-leak`, its canonical WAV grows, and it reaches
`audio_ready` or a Windows transcription state with zero exhausted counters.
Confirm `mism-281` resumes normally.

- [ ] **Step 6: Verify end-to-end services**

Require fresh evidence for all of the following:

- `/dashboard` and `/dashboard/state` return HTTP 200;
- API, downloader, Windows worker, and translation worker are online;
- at least one restored job reaches `english_srt_ready`;
- its `catalog_sync_status` is `succeeded` with HTTP 200;
- its canonical WAV is deleted only after the verified publication receipt;
- no affected job is failed or has exhausted download counters.
