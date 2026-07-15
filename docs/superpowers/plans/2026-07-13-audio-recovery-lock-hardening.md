# Audio Recovery Lock Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bind interrupted-audio validation, file movement, and database finalization into one orchestrator-coordinated critical section with directory-FD path safety and complete RIFF structural validation.

**Architecture:** Add one shared advisory job-directory lock used by recovery and `MacDownloadWorker`; recovery holds root/job/audio directory descriptors and performs all file access relative to those descriptors. SQLite finalization checks the validated snapshot immediately before and after its CAS while the advisory lock remains held through commit. RIFF parsing walks every declared chunk before the existing bounded `wave` payload validation.

**Tech Stack:** Python 3.12, `fcntl.flock`, `openat`-style `dir_fd` APIs, SQLite, `wave`, pytest.

**Threat model:** The advisory lock coordinates every orchestrator audio writer. Arbitrary local processes that deliberately ignore OS advisory locks are out of scope, but path substitution is still fail-safe through held directory descriptors and inode checks.

---

### Task 1: Shared exclusive audio lock

**Files:**
- Create: `orchestrator/audio_lock.py`
- Modify: `orchestrator/mac_worker.py`
- Modify: `orchestrator/audio_recovery.py`
- Test: `tests/test_audio_recovery.py`
- Test: `tests/test_mac_worker.py`

- [ ] **Step 1: Write failing lock tests**

Add a recovery-busy test and a concurrent `MacDownloadWorker` test. The worker test starts its write while recovery is inside finalization and asserts the adapter cannot enter `download_audio` until recovery commits and releases the lock.

- [ ] **Step 2: Verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_audio_recovery.py tests/test_mac_worker.py -k "audio_recovery_busy or audio_writer_waits"
```

Expected: failure because no shared advisory lock exists.

- [ ] **Step 3: Implement the shared lock**

Create a context manager that opens the configured root and exact direct-child job directory with `O_DIRECTORY | O_NOFOLLOW`, verifies their `dev`/`ino`, and holds `fcntl.LOCK_EX` on the job directory descriptor. Recovery uses `LOCK_NB` and maps contention to `AudioRecoveryError("audio_recovery_busy")`; the worker blocks around both calls below:

```python
with exclusive_audio_job_lock(root, movie, blocking=True):
    adapter.download_audio(movie, paths.audio_path_mac)
    store.update_download_status(job.id, JobStatus.AUDIO_READY, ...)
```

- [ ] **Step 4: Verify GREEN**

Run the two tests from Step 2 and expect both to pass.

### Task 2: Directory-FD-bound recovery

**Files:**
- Modify: `orchestrator/audio_lock.py`
- Modify: `orchestrator/audio_recovery.py`
- Modify: `orchestrator/store.py`
- Test: `tests/test_audio_recovery.py`

- [ ] **Step 1: Write the failing directory-swap test**

After staged validation, rename the exact job directory and replace its configured-root entry with a symlink to an external directory containing another staged file. Assert recovery raises a fixed safe reason, leaves the external staged file unchanged, and leaves the job `downloading_audio`.

- [ ] **Step 2: Verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_audio_recovery.py -k job_directory_swap
```

Expected: failure because absolute `Path` validation and `os.replace` re-resolve the substituted job directory.

- [ ] **Step 3: Implement held descriptors and relative operations**

Expose held `root_fd` and `job_fd` from the lock, open nested `audio` with `O_DIRECTORY | O_NOFOLLOW`, and validate files with:

```python
fd = os.open(basename, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
```

Move only exact basenames:

```python
os.replace(
    staged_basename,
    "audio.wav",
    src_dir_fd=audio_fd,
    dst_dir_fd=job_fd,
)
```

Revalidate root-to-job binding and final basename immediately before and after the SQLite CAS. Expect the directory-swap test to pass.

### Task 3: Complete RIFF chunk validation

**Files:**
- Modify: `orchestrator/audio_recovery.py`
- Test: `tests/test_audio_recovery.py`

- [ ] **Step 1: Write a failing trailing-chunk test**

Append a `JUNK` header whose declared payload exceeds the remaining RIFF bytes and update the RIFF size to the actual file length. Assert recovery rejects it without DB or file changes.

- [ ] **Step 2: Verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_audio_recovery.py -k truncated_trailing_riff_chunk
```

Expected: failure because `wave` stops after `data` and the private data-chunk check does not inspect trailing chunks.

- [ ] **Step 3: Parse every RIFF chunk**

Walk from byte 12 to the declared RIFF boundary. For each chunk require an 8-byte header, payload end within the boundary, and the required even-byte padding; read the padding byte when present. Record the first `data` size for comparison with frame bytes and reject missing or duplicate `fmt ` or `data` chunks. Remove all use of `wave._data_chunk` and retain bounded `readframes(4096)` validation.

- [ ] **Step 4: Final verification and commit**

Run:

```bash
.venv/bin/pytest -q tests/test_audio_recovery.py tests/test_mac_worker.py
.venv/bin/pytest -q
.venv/bin/python -m compileall -q orchestrator tests
git diff --check
```

Expected: focused and full suites pass. Commit with:

```bash
git commit -m "fix: lock recovered audio through commit"
```
