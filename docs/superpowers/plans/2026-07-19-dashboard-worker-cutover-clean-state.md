# Dashboard Worker Cutover Clean State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce one clean cutover branch where Dashboard/API and Mac workers run the same consolidated code, authentication-related metadata refresh failures stop recurring, and current auth-failed download jobs are repaired safely.

**Architecture:** Use `main` as the functional baseline and keep the existing advanced audio fallback code. Add a default-off metadata refresh setting so missing catalog metadata uses local direct-page fallback instead of invoking the authenticated MissAV catalog refresh path. Repair only failed download jobs whose error proves they failed before audio creation due to missing catalog metadata.

**Tech Stack:** Python, FastAPI dashboard state builders, SQLite `jobs.sqlite3`, launchd plists, pytest.

---

### File Structure

- Modify `orchestrator/config.py`: add `MISSAV_METADATA_REFRESH_ENABLED`, default `False`.
- Modify `orchestrator/missav_adapter.py`: cache-first metadata lookup, direct-page fallback by default, opt-in authenticated refresh.
- Modify `orchestrator/__main__.py`: pass the setting into `MissAVAdapter` for `mac-worker`.
- Modify `orchestrator/mac_worker.py`: update worker heartbeat stage after transitioning the job to `downloading_audio`.
- Modify `orchestrator/dashboard.py`: keep the current fix that uses the active job status for Mac download activity.
- Modify `tests/test_missav_adapter.py`: verify default no-refresh behavior and opt-in refresh behavior.
- Modify `tests/test_process_lock.py`: verify worker runtime passes `allow_metadata_refresh=False`.
- Modify `tests/test_mac_worker.py`: verify worker stage is updated before audio download starts.
- Modify `tests/test_dashboard_state.py`: keep regression test for Dashboard showing `downloading_audio`.
- Runtime action: update launchd services to point API, Mac downloader, and Mac translator at the same cutover worktree.
- Runtime action: repair only failed jobs with `Movie ... not found in MissAV catalog` and no `audio_path_mac`.

### Task 1: Establish Cutover Branch

**Files:**
- No source edits.

- [x] **Step 1: Verify worktree isolation**

Run:
```bash
git check-ignore -v .worktrees
git branch --show-current
git status --short
```

Expected: `.worktrees/` is ignored, starting branch is `main`, only known Dashboard hotfix files are dirty.

- [x] **Step 2: Create cutover branch**

Run:
```bash
git switch -c codex/dashboard-worker-cutover-clean-state
```

Expected: branch switches successfully and carries the current Dashboard hotfix diff.

### Task 2: Add Metadata Refresh Guardrail

**Files:**
- Modify: `orchestrator/config.py`
- Modify: `orchestrator/missav_adapter.py`
- Modify: `orchestrator/__main__.py`
- Modify: `.env.example`
- Test: `tests/test_missav_adapter.py`
- Test: `tests/test_process_lock.py`

- [ ] **Step 1: Write failing tests for metadata behavior**

Add tests to `tests/test_missav_adapter.py`:
```python
def test_download_metadata_creates_direct_page_metadata_without_refresh(monkeypatch, tmp_path):
    pipeline_root = _fake_pipeline_root(tmp_path)
    output_path = tmp_path / "jobs" / "ktb-096" / "metadata.json"

    def fail_if_run(command, **kwargs):
        raise AssertionError("cache miss should not refresh MissAV catalog by default")

    monkeypatch.setattr(subprocess, "run", fail_if_run)

    MissAVAdapter(pipeline_root).download_metadata("ktb-096", output_path)

    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "number": "ktb-096",
        "title": "ktb-096",
        "link": "https://missav.live/en/ktb-096",
        "cover": "",
        "preview": "",
        "duration": "",
        "release_date": "",
        "metadata_source": "direct_page_fallback",
    }
```

Update existing refresh tests to construct `MissAVAdapter(..., allow_metadata_refresh=True)`.

Add expectation to `tests/test_process_lock.py`:
```python
class FakeSettings:
    def __init__(self):
        self.missav_metadata_refresh_enabled = False

class FakeAdapter:
    def __init__(self, _root, *, allow_metadata_refresh=False):
        events.append(("adapter_refresh", allow_metadata_refresh))

assert ("adapter_refresh", False) in events
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
pytest tests/test_missav_adapter.py::test_download_metadata_creates_direct_page_metadata_without_refresh tests/test_process_lock.py::test_downloader_runtime_holds_its_lock_and_uses_configured_worker_id -q
```

Expected: fail because `MissAVAdapter` has no `allow_metadata_refresh` behavior and the runtime does not pass it.

- [ ] **Step 3: Implement metadata guardrail**

In `orchestrator/config.py`, add:
```python
missav_metadata_refresh_enabled: bool = Field(
    default=False,
    alias="MISSAV_METADATA_REFRESH_ENABLED",
)
```

In `orchestrator/missav_adapter.py`, update constructor and `download_metadata`:
```python
def __init__(self, missav_pipeline_root: Path, python_executable: Path | str | None = None, allow_metadata_refresh: bool = False) -> None:
    self.missav_pipeline_root = missav_pipeline_root
    self.python_executable = self._default_python_executable(python_executable)
    self.allow_metadata_refresh = allow_metadata_refresh

def download_metadata(self, movie_number: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_path = self.missav_pipeline_root / "new-release" / "release_movies_complete.json"
    try:
        movie = self._find_movie_in_catalog(movie_number, catalog_path)
    except FileNotFoundError:
        movie = None
    if movie is not None:
        self._write_json(movie, output_path)
        return
    if not self.allow_metadata_refresh:
        self._write_json(self._direct_page_metadata(movie_number), output_path)
        return
    # existing unified_download.py refresh code remains below
```

Add helper:
```python
def _direct_page_metadata(self, movie_number: str) -> dict[str, str]:
    base_number = self._base_movie_id(movie_number)
    return {
        "number": movie_number,
        "title": movie_number,
        "link": f"https://missav.live/en/{base_number}",
        "cover": "",
        "preview": "",
        "duration": "",
        "release_date": "",
        "metadata_source": "direct_page_fallback",
    }
```

In `orchestrator/__main__.py`, pass:
```python
MissAVAdapter(
    settings.missav_pipeline_root,
    allow_metadata_refresh=settings.missav_metadata_refresh_enabled,
)
```

In `.env.example`, add:
```dotenv
MISSAV_METADATA_REFRESH_ENABLED=false
```

- [ ] **Step 4: Run focused tests**

Run:
```bash
pytest tests/test_missav_adapter.py tests/test_process_lock.py -q
```

Expected: all selected tests pass.

### Task 3: Keep Mac Download Activity Accurate

**Files:**
- Modify: `orchestrator/mac_worker.py`
- Modify: `orchestrator/dashboard.py`
- Test: `tests/test_mac_worker.py`
- Test: `tests/test_dashboard_state.py`

- [ ] **Step 1: Write failing worker stage test**

Add to `tests/test_mac_worker.py`:
```python
class WorkerStageInspectingAudioAdapter:
    def __init__(self, store: JobStore) -> None:
        self.store = store

    def download_metadata(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("{}\n", encoding="utf-8")

    def download_audio(self, movie_number: str, output_path: Path) -> None:
        status = self.store.get_worker_status("mac-downloader-1")
        assert status is not None
        assert status.stage == JobStatus.DOWNLOADING_AUDIO.value
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.with_suffix(".wav.tmp").write_bytes(b"RIFFfakeWAVE")
        output_path.with_suffix(".wav.tmp").replace(output_path)

def test_mac_worker_updates_worker_stage_before_audio_download(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-096", priority=100, force=False)
    worker = MacDownloadWorker(store, WorkerStageInspectingAudioAdapter(store), max_download_attempts=3)

    assert worker.process_one() is True
```

Keep `tests/test_dashboard_state.py::test_mac_download_activity_prefers_current_job_status_over_claim_stage`.

- [ ] **Step 2: Run tests to verify failure**

Run:
```bash
pytest tests/test_mac_worker.py::test_mac_worker_updates_worker_stage_before_audio_download tests/test_dashboard_state.py::test_mac_download_activity_prefers_current_job_status_over_claim_stage -q
```

Expected: worker stage test fails until `mac_worker.py` records `downloading_audio`.

- [ ] **Step 3: Implement stage update**

After `store.update_download_status(... JobStatus.DOWNLOADING_AUDIO ...)` in `orchestrator/mac_worker.py`, add:
```python
try:
    self.store.record_worker_processing(
        self.worker_id,
        role="mac_downloader",
        job=updated,
        stage=updated.status.value,
    )
except Exception:
    pass
```

Keep `orchestrator/dashboard.py` fallback logic:
```python
if fallback_job is not None and fallback_job.id == worker.current_job_id:
    return {
        "status": fallback_job.status.value,
        "movie_number": fallback_job.normalized_movie_number,
        "job_id": fallback_job.id,
        "worker_id": worker.worker_id,
        "updated_at": fallback_job.updated_at,
    }
```

- [ ] **Step 4: Run focused tests**

Run:
```bash
pytest tests/test_mac_worker.py::test_mac_worker_updates_worker_stage_before_audio_download tests/test_dashboard_state.py::test_mac_download_activity_prefers_current_job_status_over_claim_stage -q
```

Expected: both tests pass.

### Task 4: Verify Full Code State

**Files:**
- No additional edits.

- [ ] **Step 1: Run regression subset**

Run:
```bash
pytest tests/test_missav_adapter.py tests/test_mac_worker.py tests/test_dashboard_state.py tests/test_process_lock.py -q
```

Expected: selected regression tests pass.

- [ ] **Step 2: Run full test suite**

Run:
```bash
pytest -q
```

Expected: full suite passes.

### Task 5: Repair Runtime Failed Jobs

**Files:**
- Runtime SQLite DB: `/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3`

- [ ] **Step 1: Snapshot affected rows**

Run:
```bash
sqlite3 /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3 "
select id, normalized_movie_number, attempt_count, error
from jobs
where status='failed'
  and audio_path_mac is null
  and error like 'Movie % not found in MissAV catalog:%'
order by updated_at;"
```

Expected: only metadata/catalog-miss download failures are selected.

- [ ] **Step 2: Reset selected jobs to queued without forcing full pipeline rerun**

Run one transaction:
```sql
update jobs
set status='queued',
    claimed_by=null,
    lease_expires_at=null,
    attempt_count=0,
    next_download_attempt_at=null,
    error=null,
    updated_at=datetime('now')
where status='failed'
  and audio_path_mac is null
  and error like 'Movie % not found in MissAV catalog:%';
```

Expected: only the metadata/auth-derived download failures become download-eligible. Jobs with existing audio, translation quality failures, Supabase movie misses, or short-audio failures remain failed.

- [ ] **Step 3: Verify counts and no accidental repair**

Run:
```bash
sqlite3 /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3 "
select status, count(*) from jobs group by status order by count(*) desc;
select normalized_movie_number, substr(error,1,160)
from jobs where status='failed' order by updated_at desc limit 20;"
```

Expected: failed count drops only by the selected metadata/catalog-miss download jobs.

### Task 6: Cut Runtime Services Over To One Worktree

**Files:**
- Runtime launchd service definitions.

- [ ] **Step 1: Confirm service commands**

Run:
```bash
launchctl print gui/$(id -u)/com.javsubtitle.orchestrator-api | sed -n '/program =/,+6p'
launchctl print gui/$(id -u)/com.javsubtitle.mac-worker | sed -n '/program =/,+12p'
launchctl print gui/$(id -u)/com.javsubtitle.mac-translation-worker | sed -n '/program =/,+12p'
```

Expected: record current commands before changing them.

- [ ] **Step 2: Restart services from the cutover worktree**

Use launchd plist edits or service reload commands so API, downloader, and translator all use:
```text
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.worktrees/main-local-merge
```

Expected: all three services execute code from `codex/dashboard-worker-cutover-clean-state`.

- [ ] **Step 3: Verify live Dashboard and workers**

Run:
```bash
curl -s http://127.0.0.1:8010/dashboard/state
sqlite3 /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3 "select worker_id, role, state, stage, current_movie_number, last_seen_at from worker_statuses order by last_seen_at desc;"
```

Expected: Dashboard activity and worker heartbeats agree; Mac downloader no longer shows stale `downloading_metadata` when the job is actually `downloading_audio`.

### Self-Review

- Spec coverage: identifies recurrence cause, implements code fix, prevents default authenticated refresh, repairs only relevant failed jobs, and moves runtime toward one clean branch.
- Placeholder scan: no `TBD`, broad unspecified test steps, or undefined functions remain.
- Type consistency: `allow_metadata_refresh` and `missav_metadata_refresh_enabled` names are consistent across config, adapter, runtime, and tests.
