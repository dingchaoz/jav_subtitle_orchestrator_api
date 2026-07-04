# JAV Subtitle Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the version 1 Mac + Windows subtitle orchestration system described in `docs/superpowers/specs/2026-07-04-jav-subtitle-orchestrator-design.md`.

**Architecture:** The Mac owns FastAPI, SQLite queue state, MissAV metadata/audio download, and the SMB-backed job folder. The Windows worker polls the Mac API, claims one ready job at a time, reads audio from the mapped SMB drive, runs faster-whisper transcription, runs the existing SRT translation script, and reports completion back to the Mac API. SQLite is the source of truth; `job.json` files are human-readable snapshots in each job folder.

**Tech Stack:** Python 3.11+, FastAPI, Uvicorn, Pydantic Settings, SQLite, pytest, httpx/TestClient, requests, faster-whisper on Windows CUDA, subprocess wrappers for existing MissAV and translation scripts, SMB shared folder mounted as `/Users/ytt/MissAVJobs` on Mac and `M:\` on Windows.

---

## Source Spec

Primary requirements are in:

```text
docs/superpowers/specs/2026-07-04-jav-subtitle-orchestrator-design.md
```

Follow the spec exactly for version 1:

- Mac API accepts one or many movie numbers.
- Mac stores jobs in SQLite at `data/jobs.sqlite3`.
- Mac downloader worker downloads metadata and `audio.wav` into `/Users/ytt/MissAVJobs/<movie>/`.
- Windows worker polls the Mac API, reads `M:\<movie>\audio.wav`, writes Japanese and English SRT files, and reports status.
- First-version concurrency is one Mac download job and one Windows AI job at a time.
- Do not build a web UI.
- Do not expose SMB publicly.
- Do not create a GitHub remote automatically.

## Planned File Structure

Create these files:

```text
pyproject.toml
.env.example
.env.windows.example
orchestrator/__init__.py
orchestrator/__main__.py
orchestrator/config.py
orchestrator/models.py
orchestrator/paths.py
orchestrator/store.py
orchestrator/api.py
orchestrator/job_snapshot.py
orchestrator/job_logs.py
orchestrator/missav_adapter.py
orchestrator/mac_worker.py
orchestrator/transcription.py
orchestrator/translation.py
orchestrator/windows_worker.py
orchestrator/logging_config.py
tests/conftest.py
tests/test_config_paths.py
tests/test_models.py
tests/test_store_submit.py
tests/test_store_worker_claims.py
tests/test_api_jobs.py
tests/test_api_worker.py
tests/test_job_snapshot.py
tests/test_job_logs.py
tests/test_mac_worker.py
tests/test_transcription.py
tests/test_translation.py
tests/test_windows_worker.py
tests/fixtures/tiny.ja.srt
tests/fixtures/tiny.wav
docs/setup/mac.md
docs/setup/windows.md
docs/setup/smb.md
docs/setup/e2e-lan-test.md
```

Responsibilities:

- `config.py`: load Mac and Windows settings from environment variables.
- `models.py`: shared enums and Pydantic request/response models.
- `paths.py`: movie normalization, job ID generation, Mac/Windows path mapping, deterministic output paths.
- `store.py`: SQLite schema, job submission, duplicate/force behavior, job listing, atomic worker claim/lease, heartbeat, completion, failure, and lease expiry recovery.
- `api.py`: FastAPI endpoints from the spec.
- `job_snapshot.py`: write `job.json` snapshots after state changes.
- `job_logs.py`: append human-readable per-job logs under each job folder's `logs/` directory.
- `missav_adapter.py`: wrapper around existing MissAV repo scripts and a fake adapter for tests.
- `mac_worker.py`: one-at-a-time downloader loop that moves jobs from `queued` to `audio_ready`.
- `transcription.py`: faster-whisper adapter that writes `<movie>.Japanese.srt`.
- `translation.py`: subprocess adapter around existing `subtitle_translate.py` and rename normalization to `<movie>.English.srt`.
- `windows_worker.py`: one-at-a-time polling loop that claims jobs, sends heartbeat, transcribes, translates, and reports completion/failure.
- `docs/setup/*.md`: exact machine setup and run commands for Mac, Windows, and SMB.

## Task 1: Project Scaffold And Settings

**Files:**

- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `.env.windows.example`
- Create: `orchestrator/__init__.py`
- Create: `orchestrator/__main__.py`
- Create: `orchestrator/config.py`
- Create: `tests/conftest.py`
- Test: `tests/test_config_paths.py`

- [ ] **Step 1: Write the failing config tests**

Create `tests/test_config_paths.py`:

```python
from pathlib import Path

from orchestrator.config import MacSettings, WindowsSettings


def test_mac_settings_defaults_match_design_spec(monkeypatch, tmp_path):
    db_path = tmp_path / "jobs.sqlite3"
    monkeypatch.setenv("ORCHESTRATOR_DB_PATH", str(db_path))
    monkeypatch.setenv("MISSAV_PIPELINE_ROOT", "/Users/ytt/Documents/startup/MissAV-Pipeline")
    monkeypatch.setenv("JOBS_ROOT_MAC", "/Users/ytt/MissAVJobs")
    monkeypatch.setenv("JOBS_ROOT_WINDOWS", "M:\\")

    settings = MacSettings()

    assert settings.host == "0.0.0.0"
    assert settings.port == 8000
    assert settings.db_path == db_path
    assert settings.missav_pipeline_root == Path("/Users/ytt/Documents/startup/MissAV-Pipeline")
    assert settings.jobs_root_mac == Path("/Users/ytt/MissAVJobs")
    assert settings.jobs_root_windows == "M:\\"
    assert settings.mac_download_concurrency == 1
    assert settings.worker_lease_seconds == 1800
    assert settings.max_download_attempts == 3
    assert settings.max_worker_attempts == 3


def test_windows_settings_defaults_match_design_spec(monkeypatch):
    monkeypatch.setenv("MAC_API_BASE_URL", "http://192.168.1.25:8000")
    monkeypatch.setenv("WORKER_ID", "windows-gpu-1")
    monkeypatch.setenv("WINDOWS_JOBS_ROOT", "M:\\")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv(
        "TRANSLATE_SCRIPT_PATH",
        "C:\\Users\\ytt\\Documents\\startup\\E2E-download-subtitle-generation-translation-scripts\\scripts\\subtitle_translate.py",
    )

    settings = WindowsSettings()

    assert settings.mac_api_base_url == "http://192.168.1.25:8000"
    assert settings.worker_id == "windows-gpu-1"
    assert settings.windows_jobs_root == "M:\\"
    assert settings.whisper_model == "large-v3-turbo"
    assert settings.whisper_device == "cuda"
    assert settings.whisper_compute_type == "float16"
    assert settings.openai_api_key == "test-key"
    assert settings.poll_interval_seconds == 10
    assert settings.heartbeat_interval_seconds == 60
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
pytest tests/test_config_paths.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'orchestrator'`.

- [ ] **Step 3: Add project metadata and dependencies**

Create `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=69"]
build-backend = "setuptools.build_meta"

[project]
name = "jav-subtitle-orchestrator"
version = "0.1.0"
description = "Mac + Windows orchestration service for queued JAV subtitle generation"
requires-python = ">=3.11"
dependencies = [
  "fastapi>=0.116.0",
  "uvicorn[standard]>=0.30.0",
  "pydantic>=2.8.0",
  "pydantic-settings>=2.4.0",
  "requests>=2.32.0",
  "python-dotenv>=1.0.1",
]

[project.optional-dependencies]
dev = [
  "pytest>=8.3.0",
  "httpx>=0.27.0",
]
windows = [
  "faster-whisper>=1.0.3",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]

[tool.ruff]
line-length = 100
target-version = "py311"
```

Create `.env.example`:

```text
ORCHESTRATOR_HOST=0.0.0.0
ORCHESTRATOR_PORT=8000
ORCHESTRATOR_DB_PATH=/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3
MISSAV_PIPELINE_ROOT=/Users/ytt/Documents/startup/MissAV-Pipeline
JOBS_ROOT_MAC=/Users/ytt/MissAVJobs
JOBS_ROOT_WINDOWS=M:\
MAC_DOWNLOAD_CONCURRENCY=1
WORKER_LEASE_SECONDS=1800
MAX_DOWNLOAD_ATTEMPTS=3
MAX_WORKER_ATTEMPTS=3
```

Create `.env.windows.example`:

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

- [ ] **Step 4: Add settings classes**

Create `orchestrator/__init__.py`:

```python
__all__ = ["__version__"]

__version__ = "0.1.0"
```

Create `orchestrator/config.py`:

```python
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MacSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = Field(default="0.0.0.0", alias="ORCHESTRATOR_HOST")
    port: int = Field(default=8000, alias="ORCHESTRATOR_PORT")
    db_path: Path = Field(
        default=Path("/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3"),
        alias="ORCHESTRATOR_DB_PATH",
    )
    missav_pipeline_root: Path = Field(
        default=Path("/Users/ytt/Documents/startup/MissAV-Pipeline"),
        alias="MISSAV_PIPELINE_ROOT",
    )
    jobs_root_mac: Path = Field(default=Path("/Users/ytt/MissAVJobs"), alias="JOBS_ROOT_MAC")
    jobs_root_windows: str = Field(default="M:\\", alias="JOBS_ROOT_WINDOWS")
    mac_download_concurrency: int = Field(default=1, alias="MAC_DOWNLOAD_CONCURRENCY")
    worker_lease_seconds: int = Field(default=1800, alias="WORKER_LEASE_SECONDS")
    max_download_attempts: int = Field(default=3, alias="MAX_DOWNLOAD_ATTEMPTS")
    max_worker_attempts: int = Field(default=3, alias="MAX_WORKER_ATTEMPTS")


class WindowsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mac_api_base_url: str = Field(alias="MAC_API_BASE_URL")
    worker_id: str = Field(default="windows-gpu-1", alias="WORKER_ID")
    windows_jobs_root: str = Field(default="M:\\", alias="WINDOWS_JOBS_ROOT")
    whisper_model: str = Field(default="large-v3-turbo", alias="WHISPER_MODEL")
    whisper_device: str = Field(default="cuda", alias="WHISPER_DEVICE")
    whisper_compute_type: str = Field(default="float16", alias="WHISPER_COMPUTE_TYPE")
    openai_api_key: str = Field(alias="OPENAI_API_KEY")
    translate_script_path: str = Field(alias="TRANSLATE_SCRIPT_PATH")
    poll_interval_seconds: int = Field(default=10, alias="POLL_INTERVAL_SECONDS")
    heartbeat_interval_seconds: int = Field(default=60, alias="HEARTBEAT_INTERVAL_SECONDS")
```

Create `orchestrator/__main__.py`:

```python
from orchestrator.config import MacSettings


def main() -> None:
    settings = MacSettings()
    print(f"JAV Subtitle Orchestrator API: {settings.host}:{settings.port}")


if __name__ == "__main__":
    main()
```

Create `tests/conftest.py`:

```python
from pathlib import Path

import pytest


@pytest.fixture
def mac_jobs_root(tmp_path: Path) -> Path:
    return tmp_path / "MissAVJobs"


@pytest.fixture
def sqlite_path(tmp_path: Path) -> Path:
    return tmp_path / "jobs.sqlite3"
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/test_config_paths.py -v
```

Expected: PASS with 2 tests.

Commit:

```bash
git add pyproject.toml .env.example .env.windows.example orchestrator/__init__.py orchestrator/__main__.py orchestrator/config.py tests/conftest.py tests/test_config_paths.py
git commit -m "chore: scaffold orchestrator settings"
```

## Task 2: Shared Models, Statuses, And Path Mapping

**Files:**

- Create: `orchestrator/models.py`
- Create: `orchestrator/paths.py`
- Test: `tests/test_models.py`
- Test: `tests/test_config_paths.py`

- [ ] **Step 1: Write failing tests for normalization, status enum, and paths**

Append to `tests/test_config_paths.py`:

```python
from orchestrator.paths import build_job_paths, normalize_movie_number


def test_normalize_movie_number_lowercases_and_keeps_dash():
    assert normalize_movie_number(" KTB-096 ") == "ktb-096"


def test_normalize_movie_number_accepts_id_without_dash():
    assert normalize_movie_number("KTB096") == "ktb-096"


def test_normalize_movie_number_rejects_invalid_ids():
    assert normalize_movie_number("bad id") is None
    assert normalize_movie_number("") is None


def test_build_job_paths_maps_mac_root_to_windows_root(tmp_path):
    paths = build_job_paths("ktb-096", tmp_path, "M:\\")

    assert paths.job_dir_mac == tmp_path / "ktb-096"
    assert paths.job_dir_windows == "M:\\ktb-096"
    assert paths.metadata_path_mac == tmp_path / "ktb-096" / "metadata.json"
    assert paths.audio_path_mac == tmp_path / "ktb-096" / "audio.wav"
    assert paths.audio_path_windows == "M:\\ktb-096\\audio.wav"
    assert paths.japanese_srt_path_windows == "M:\\ktb-096\\ktb-096.Japanese.srt"
    assert paths.english_srt_path_windows == "M:\\ktb-096\\ktb-096.English.srt"
```

Create `tests/test_models.py`:

```python
from orchestrator.models import JobStatus


def test_job_statuses_match_design_spec_order():
    assert [status.value for status in JobStatus] == [
        "queued",
        "downloading_metadata",
        "downloading_audio",
        "audio_ready",
        "transcription_claimed",
        "transcribing",
        "transcription_done",
        "translating",
        "english_srt_ready",
        "failed",
        "cancelled",
    ]
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_config_paths.py tests/test_models.py -v
```

Expected: FAIL with `ModuleNotFoundError` or import errors for `orchestrator.models` and `orchestrator.paths`.

- [ ] **Step 3: Add shared models**

Create `orchestrator/models.py`:

```python
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class JobStatus(StrEnum):
    QUEUED = "queued"
    DOWNLOADING_METADATA = "downloading_metadata"
    DOWNLOADING_AUDIO = "downloading_audio"
    AUDIO_READY = "audio_ready"
    TRANSCRIPTION_CLAIMED = "transcription_claimed"
    TRANSCRIBING = "transcribing"
    TRANSCRIPTION_DONE = "transcription_done"
    TRANSLATING = "translating"
    ENGLISH_SRT_READY = "english_srt_ready"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobPaths(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    job_dir_mac: Path
    job_dir_windows: str
    metadata_path_mac: Path
    audio_path_mac: Path
    audio_path_windows: str
    japanese_srt_path_mac: Path
    japanese_srt_path_windows: str
    english_srt_path_mac: Path
    english_srt_path_windows: str


class SubmitJobRequest(BaseModel):
    movie_number: str
    priority: int = 100
    force: bool = False


class SubmitBatchRequest(BaseModel):
    movie_numbers: list[str]
    priority: int = 100
    force: bool = False


class WorkerHeartbeatRequest(BaseModel):
    worker_id: str
    stage: JobStatus


class WorkerCompleteRequest(BaseModel):
    worker_id: str
    japanese_srt_path_windows: str
    english_srt_path_windows: str


class WorkerFailedRequest(BaseModel):
    worker_id: str
    stage: JobStatus
    error: str = Field(min_length=1)
```

- [ ] **Step 4: Add path helpers**

Create `orchestrator/paths.py`:

```python
import re
import uuid
from pathlib import Path

from orchestrator.models import JobPaths

MOVIE_RE = re.compile(r"^([a-zA-Z]+)-?(\d+)$")


def normalize_movie_number(raw: str) -> str | None:
    cleaned = raw.strip()
    match = MOVIE_RE.match(cleaned)
    if not match:
        return None
    prefix, number = match.groups()
    return f"{prefix.lower()}-{number}"


def new_job_id() -> str:
    return f"job_{uuid.uuid4().hex}"


def windows_join(root: str, *parts: str) -> str:
    clean_root = root.rstrip("\\/")
    return clean_root + "\\" + "\\".join(part.strip("\\/") for part in parts)


def build_job_paths(movie_number: str, jobs_root_mac: Path, jobs_root_windows: str) -> JobPaths:
    job_dir_mac = jobs_root_mac / movie_number
    job_dir_windows = windows_join(jobs_root_windows, movie_number)
    return JobPaths(
        job_dir_mac=job_dir_mac,
        job_dir_windows=job_dir_windows,
        metadata_path_mac=job_dir_mac / "metadata.json",
        audio_path_mac=job_dir_mac / "audio.wav",
        audio_path_windows=windows_join(job_dir_windows, "audio.wav"),
        japanese_srt_path_mac=job_dir_mac / f"{movie_number}.Japanese.srt",
        japanese_srt_path_windows=windows_join(job_dir_windows, f"{movie_number}.Japanese.srt"),
        english_srt_path_mac=job_dir_mac / f"{movie_number}.English.srt",
        english_srt_path_windows=windows_join(job_dir_windows, f"{movie_number}.English.srt"),
    )
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/test_config_paths.py tests/test_models.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/models.py orchestrator/paths.py tests/test_config_paths.py tests/test_models.py
git commit -m "feat: add shared job models and paths"
```

## Task 3: SQLite Schema And Job Submission Store

**Files:**

- Create: `orchestrator/store.py`
- Test: `tests/test_store_submit.py`

- [ ] **Step 1: Write failing store submission tests**

Create `tests/test_store_submit.py`:

```python
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def test_submit_job_creates_sqlite_row(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    result = store.submit_job("KTB-096", priority=100, force=False)

    assert result.kind == "created"
    assert result.job.movie_number == "KTB-096"
    assert result.job.normalized_movie_number == "ktb-096"
    assert result.job.status == JobStatus.QUEUED
    assert result.job.priority == 100
    assert result.job.job_dir_mac == str(mac_jobs_root / "ktb-096")
    assert result.job.job_dir_windows == "M:\\ktb-096"


def test_duplicate_submit_returns_existing(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    created = store.submit_job("ktb-096", priority=100, force=False)

    existing = store.submit_job("KTB096", priority=10, force=False)

    assert existing.kind == "existing"
    assert existing.job.id == created.job.id
    assert existing.job.priority == 100


def test_submit_invalid_movie_number(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    result = store.submit_job("bad id", priority=100, force=False)

    assert result.kind == "invalid"
    assert result.job is None
    assert result.movie_number == "bad id"


def test_batch_submission_groups_created_existing_and_invalid(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-095", priority=100, force=False)

    result = store.submit_batch(["ktb-096", "ktb-095", "bad id"], priority=100, force=False)

    assert [item.job.normalized_movie_number for item in result.created] == ["ktb-096"]
    assert [item.job.normalized_movie_number for item in result.existing] == ["ktb-095"]
    assert [item.movie_number for item in result.invalid] == ["bad id"]
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_store_submit.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'orchestrator.store'`.

- [ ] **Step 3: Add SQLite store schema and submission methods**

Create `orchestrator/store.py`:

```python
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths, new_job_id, normalize_movie_number


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class JobRecord:
    id: str
    movie_number: str
    normalized_movie_number: str
    status: JobStatus
    priority: int
    attempt_count: int
    worker_attempt_count: int
    claimed_by: str | None
    lease_expires_at: str | None
    created_at: str
    updated_at: str
    error: str | None
    job_dir_mac: str
    job_dir_windows: str
    metadata_path_mac: str | None
    audio_path_mac: str | None
    audio_path_windows: str | None
    japanese_srt_path_mac: str | None
    japanese_srt_path_windows: str | None
    english_srt_path_mac: str | None
    english_srt_path_windows: str | None


@dataclass(frozen=True)
class SubmitResult:
    kind: Literal["created", "existing", "invalid", "conflict"]
    movie_number: str
    job: JobRecord | None = None


@dataclass(frozen=True)
class BatchSubmitResult:
    created: list[SubmitResult]
    existing: list[SubmitResult]
    invalid: list[SubmitResult]


class JobStore:
    def __init__(self, db_path: Path, jobs_root_mac: Path, jobs_root_windows: str) -> None:
        self.db_path = db_path
        self.jobs_root_mac = jobs_root_mac
        self.jobs_root_windows = jobs_root_windows

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY,
                  movie_number TEXT NOT NULL,
                  normalized_movie_number TEXT NOT NULL UNIQUE,
                  status TEXT NOT NULL,
                  priority INTEGER NOT NULL DEFAULT 100,
                  attempt_count INTEGER NOT NULL DEFAULT 0,
                  worker_attempt_count INTEGER NOT NULL DEFAULT 0,
                  claimed_by TEXT,
                  lease_expires_at TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  error TEXT,
                  job_dir_mac TEXT NOT NULL,
                  job_dir_windows TEXT NOT NULL,
                  metadata_path_mac TEXT,
                  audio_path_mac TEXT,
                  audio_path_windows TEXT,
                  japanese_srt_path_mac TEXT,
                  japanese_srt_path_windows TEXT,
                  english_srt_path_mac TEXT,
                  english_srt_path_windows TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_status_priority_created "
                "ON jobs(status, priority, created_at)"
            )

    def submit_job(self, movie_number: str, priority: int, force: bool) -> SubmitResult:
        normalized = normalize_movie_number(movie_number)
        if normalized is None:
            return SubmitResult(kind="invalid", movie_number=movie_number)

        with self.connect() as conn:
            existing = self._get_by_normalized(conn, normalized)
            if existing and not force:
                return SubmitResult(kind="existing", movie_number=movie_number, job=existing)
            if existing and force:
                return self._force_reset(conn, existing, movie_number)

            now = utc_now_iso()
            paths = build_job_paths(normalized, self.jobs_root_mac, self.jobs_root_windows)
            job_id = new_job_id()
            conn.execute(
                """
                INSERT INTO jobs (
                  id, movie_number, normalized_movie_number, status, priority,
                  attempt_count, worker_attempt_count, claimed_by, lease_expires_at,
                  created_at, updated_at, error, job_dir_mac, job_dir_windows,
                  metadata_path_mac, audio_path_mac, audio_path_windows,
                  japanese_srt_path_mac, japanese_srt_path_windows,
                  english_srt_path_mac, english_srt_path_windows
                )
                VALUES (?, ?, ?, ?, ?, 0, 0, NULL, NULL, ?, ?, NULL, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """,
                (
                    job_id,
                    movie_number,
                    normalized,
                    JobStatus.QUEUED.value,
                    priority,
                    now,
                    now,
                    str(paths.job_dir_mac),
                    paths.job_dir_windows,
                ),
            )
            job = self.get_job(job_id, conn=conn)
            return SubmitResult(kind="created", movie_number=movie_number, job=job)

    def submit_batch(self, movie_numbers: list[str], priority: int, force: bool) -> BatchSubmitResult:
        created: list[SubmitResult] = []
        existing: list[SubmitResult] = []
        invalid: list[SubmitResult] = []
        for movie_number in movie_numbers:
            result = self.submit_job(movie_number, priority=priority, force=force)
            if result.kind == "created":
                created.append(result)
            elif result.kind in {"existing", "conflict"}:
                existing.append(result)
            else:
                invalid.append(result)
        return BatchSubmitResult(created=created, existing=existing, invalid=invalid)

    def get_job(self, job_id: str, conn: sqlite3.Connection | None = None) -> JobRecord | None:
        own_conn = conn is None
        active_conn = conn or self.connect()
        try:
            row = active_conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return self._row_to_job(row) if row else None
        finally:
            if own_conn:
                active_conn.close()

    def _get_by_normalized(self, conn: sqlite3.Connection, normalized: str) -> JobRecord | None:
        row = conn.execute(
            "SELECT * FROM jobs WHERE normalized_movie_number = ?",
            (normalized,),
        ).fetchone()
        return self._row_to_job(row) if row else None

    def _force_reset(
        self,
        conn: sqlite3.Connection,
        existing: JobRecord,
        movie_number: str,
    ) -> SubmitResult:
        if existing.status in {
            JobStatus.TRANSCRIPTION_CLAIMED,
            JobStatus.TRANSCRIBING,
            JobStatus.TRANSLATING,
        }:
            return SubmitResult(kind="conflict", movie_number=movie_number, job=existing)
        now = utc_now_iso()
        conn.execute(
            """
            UPDATE jobs
            SET status = ?, claimed_by = NULL, lease_expires_at = NULL, updated_at = ?,
                error = NULL, metadata_path_mac = NULL, audio_path_mac = NULL,
                audio_path_windows = NULL, japanese_srt_path_mac = NULL,
                japanese_srt_path_windows = NULL, english_srt_path_mac = NULL,
                english_srt_path_windows = NULL
            WHERE id = ?
            """,
            (JobStatus.QUEUED.value, now, existing.id),
        )
        job = self.get_job(existing.id, conn=conn)
        return SubmitResult(kind="created", movie_number=movie_number, job=job)

    def _row_to_job(self, row: sqlite3.Row) -> JobRecord:
        return JobRecord(
            id=row["id"],
            movie_number=row["movie_number"],
            normalized_movie_number=row["normalized_movie_number"],
            status=JobStatus(row["status"]),
            priority=row["priority"],
            attempt_count=row["attempt_count"],
            worker_attempt_count=row["worker_attempt_count"],
            claimed_by=row["claimed_by"],
            lease_expires_at=row["lease_expires_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            error=row["error"],
            job_dir_mac=row["job_dir_mac"],
            job_dir_windows=row["job_dir_windows"],
            metadata_path_mac=row["metadata_path_mac"],
            audio_path_mac=row["audio_path_mac"],
            audio_path_windows=row["audio_path_windows"],
            japanese_srt_path_mac=row["japanese_srt_path_mac"],
            japanese_srt_path_windows=row["japanese_srt_path_windows"],
            english_srt_path_mac=row["english_srt_path_mac"],
            english_srt_path_windows=row["english_srt_path_windows"],
        )
```

- [ ] **Step 4: Run tests and commit**

Run:

```bash
pytest tests/test_store_submit.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/store.py tests/test_store_submit.py
git commit -m "feat: add sqlite job submission store"
```

## Task 4: SQLite Status Transitions, Claims, Leases, And Completion

**Files:**

- Modify: `orchestrator/store.py`
- Test: `tests/test_store_worker_claims.py`

- [ ] **Step 1: Write failing worker claim and lease tests**

Create `tests/test_store_worker_claims.py`:

```python
from datetime import UTC, datetime, timedelta

from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def test_claim_next_audio_ready_job_is_atomic_and_ordered(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    slow = store.submit_job("ktb-096", priority=100, force=False).job
    fast = store.submit_job("ktb-095", priority=10, force=False).job
    store.mark_audio_ready(slow.id)
    store.mark_audio_ready(fast.id)

    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    second_claim = store.claim_next_worker_job("windows-gpu-2", lease_seconds=1800)

    assert claimed.id == fast.id
    assert claimed.status == JobStatus.TRANSCRIPTION_CLAIMED
    assert claimed.claimed_by == "windows-gpu-1"
    assert second_claim.id == slow.id


def test_heartbeat_extends_lease_and_updates_stage(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=60)

    updated = store.heartbeat(claimed.id, "windows-gpu-1", JobStatus.TRANSCRIBING, lease_seconds=1800)

    assert updated.status == JobStatus.TRANSCRIBING
    assert updated.claimed_by == "windows-gpu-1"
    assert updated.lease_expires_at > claimed.lease_expires_at


def test_worker_complete_requires_final_files(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=60)

    completed = store.complete_worker_job(
        claimed.id,
        "windows-gpu-1",
        japanese_srt_path_windows="M:\\ktb-096\\ktb-096.Japanese.srt",
        english_srt_path_windows="M:\\ktb-096\\ktb-096.English.srt",
        final_file_exists=lambda path: path.endswith(".English.srt"),
    )

    assert completed.status == JobStatus.ENGLISH_SRT_READY
    assert completed.japanese_srt_path_windows == "M:\\ktb-096\\ktb-096.Japanese.srt"
    assert completed.english_srt_path_windows == "M:\\ktb-096\\ktb-096.English.srt"
    assert completed.claimed_by is None
    assert completed.lease_expires_at is None


def test_expired_worker_lease_returns_job_to_audio_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1)
    expired = (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
    store.force_lease_expiry_for_test(claimed.id, expired)

    recovered = store.recover_expired_worker_leases(max_worker_attempts=3)

    assert recovered == 1
    refreshed = store.get_job(claimed.id)
    assert refreshed.status == JobStatus.AUDIO_READY
    assert refreshed.worker_attempt_count == 1
    assert refreshed.claimed_by is None
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_store_worker_claims.py -v
```

Expected: FAIL with `AttributeError` for missing store methods.

- [ ] **Step 3: Add status transition methods**

Append these methods to `JobStore` in `orchestrator/store.py`:

```python
    def list_jobs(self, status: JobStatus | None = None) -> list[JobRecord]:
        with self.connect() as conn:
            if status is None:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY priority ASC, created_at ASC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE status = ? ORDER BY priority ASC, created_at ASC",
                    (status.value,),
                ).fetchall()
            return [self._row_to_job(row) for row in rows]

    def update_download_status(
        self,
        job_id: str,
        status: JobStatus,
        *,
        metadata_path_mac: str | None = None,
        audio_path_mac: str | None = None,
        audio_path_windows: str | None = None,
        error: str | None = None,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?, metadata_path_mac = COALESCE(?, metadata_path_mac),
                    audio_path_mac = COALESCE(?, audio_path_mac),
                    audio_path_windows = COALESCE(?, audio_path_windows),
                    error = ?
                WHERE id = ?
                """,
                (
                    status.value,
                    now,
                    metadata_path_mac,
                    audio_path_mac,
                    audio_path_windows,
                    error,
                    job_id,
                ),
            )
            return self.get_job(job_id, conn=conn)

    def mark_audio_ready(self, job_id: str) -> JobRecord:
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        paths = build_job_paths(
            job.normalized_movie_number,
            self.jobs_root_mac,
            self.jobs_root_windows,
        )
        return self.update_download_status(
            job_id,
            JobStatus.AUDIO_READY,
            metadata_path_mac=str(paths.metadata_path_mac),
            audio_path_mac=str(paths.audio_path_mac),
            audio_path_windows=paths.audio_path_windows,
        )

    def claim_next_worker_job(self, worker_id: str, lease_seconds: int) -> JobRecord | None:
        now = utc_now_iso()
        lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).replace(microsecond=0).isoformat()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status = ?
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                """,
                (JobStatus.AUDIO_READY.value,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = ?, lease_expires_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (JobStatus.TRANSCRIPTION_CLAIMED.value, worker_id, lease, now, row["id"]),
            )
            return self.get_job(row["id"], conn=conn)

    def heartbeat(
        self,
        job_id: str,
        worker_id: str,
        stage: JobStatus,
        lease_seconds: int,
    ) -> JobRecord:
        lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).replace(microsecond=0).isoformat()
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, lease_expires_at = ?, updated_at = ?
                WHERE id = ? AND claimed_by = ?
                """,
                (stage.value, lease, now, job_id, worker_id),
            )
            job = self.get_job(job_id, conn=conn)
            if job is None or job.claimed_by != worker_id:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            return job

    def complete_worker_job(
        self,
        job_id: str,
        worker_id: str,
        japanese_srt_path_windows: str,
        english_srt_path_windows: str,
        final_file_exists,
    ) -> JobRecord:
        now = utc_now_iso()
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        paths = build_job_paths(job.normalized_movie_number, self.jobs_root_mac, self.jobs_root_windows)
        if not final_file_exists(str(paths.english_srt_path_mac)):
            raise FileNotFoundError(paths.english_srt_path_mac)
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                    updated_at = ?, error = NULL,
                    japanese_srt_path_mac = ?, japanese_srt_path_windows = ?,
                    english_srt_path_mac = ?, english_srt_path_windows = ?
                WHERE id = ? AND claimed_by = ?
                """,
                (
                    JobStatus.ENGLISH_SRT_READY.value,
                    now,
                    str(paths.japanese_srt_path_mac),
                    japanese_srt_path_windows,
                    str(paths.english_srt_path_mac),
                    english_srt_path_windows,
                    job_id,
                    worker_id,
                ),
            )
            completed = self.get_job(job_id, conn=conn)
            if completed is None or completed.status != JobStatus.ENGLISH_SRT_READY:
                raise PermissionError(f"job {job_id} is not claimed by {worker_id}")
            return completed

    def fail_worker_job(
        self,
        job_id: str,
        worker_id: str,
        stage: JobStatus,
        error: str,
        max_worker_attempts: int,
    ) -> JobRecord:
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        next_attempts = job.worker_attempt_count + 1
        next_status = JobStatus.FAILED if next_attempts >= max_worker_attempts else JobStatus.AUDIO_READY
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, worker_attempt_count = ?, claimed_by = NULL,
                    lease_expires_at = NULL, updated_at = ?, error = ?
                WHERE id = ? AND claimed_by = ?
                """,
                (next_status.value, next_attempts, now, f"{stage.value}: {error}", job_id, worker_id),
            )
            return self.get_job(job_id, conn=conn)

    def recover_expired_worker_leases(self, max_worker_attempts: int) -> int:
        now = utc_now_iso()
        recovered = 0
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM jobs
                WHERE status IN (?, ?, ?) AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
                """,
                (
                    JobStatus.TRANSCRIPTION_CLAIMED.value,
                    JobStatus.TRANSCRIBING.value,
                    JobStatus.TRANSLATING.value,
                    now,
                ),
            ).fetchall()
            for row in rows:
                attempts = row["worker_attempt_count"] + 1
                next_status = JobStatus.FAILED if attempts >= max_worker_attempts else JobStatus.AUDIO_READY
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, worker_attempt_count = ?, claimed_by = NULL,
                        lease_expires_at = NULL, updated_at = ?, error = ?
                    WHERE id = ?
                    """,
                    (
                        next_status.value,
                        attempts,
                        now,
                        "worker lease expired",
                        row["id"],
                    ),
                )
                recovered += 1
        return recovered

    def force_lease_expiry_for_test(self, job_id: str, lease_expires_at: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE jobs SET lease_expires_at = ? WHERE id = ?",
                (lease_expires_at, job_id),
            )
```

Also add this import at the top of `orchestrator/store.py`:

```python
from datetime import UTC, datetime, timedelta
```

Replace the existing datetime import so `timedelta` is available.

- [ ] **Step 4: Run tests and commit**

Run:

```bash
pytest tests/test_store_submit.py tests/test_store_worker_claims.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/store.py tests/test_store_worker_claims.py
git commit -m "feat: add sqlite worker claims and leases"
```

## Task 5: FastAPI Job And Worker API

**Files:**

- Create: `orchestrator/api.py`
- Modify: `orchestrator/models.py`
- Test: `tests/test_api_jobs.py`
- Test: `tests/test_api_worker.py`

- [ ] **Step 1: Write failing API tests**

Create `tests/test_api_jobs.py`:

```python
from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.store import JobStore


def test_post_job_creates_job(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    response = client.post("/jobs", json={"movie_number": "ktb-096", "priority": 100, "force": False})

    assert response.status_code == 200
    body = response.json()
    assert body["movie_number"] == "ktb-096"
    assert body["status"] == "queued"
    assert body["job_dir_mac"].endswith("/ktb-096")
    assert body["job_dir_windows"] == "M:\\ktb-096"


def test_post_batch_groups_created_existing_invalid(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-095", priority=100, force=False)
    client = TestClient(create_app(store))

    response = client.post(
        "/jobs/batch",
        json={"movie_numbers": ["ktb-096", "ktb-095", "bad id"], "priority": 100, "force": False},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["created"][0]["movie_number"] == "ktb-096"
    assert body["existing"][0]["movie_number"] == "ktb-095"
    assert body["invalid"] == ["bad id"]


def test_get_jobs_filters_by_status(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = store.submit_job("ktb-096", priority=100, force=False).job
    store.submit_job("ktb-095", priority=100, force=False)
    store.mark_audio_ready(first.id)
    client = TestClient(create_app(store))

    response = client.get("/jobs?status=audio_ready")

    assert response.status_code == 200
    assert [job["movie_number"] for job in response.json()] == ["ktb-096"]
```

Create `tests/test_api_worker.py`:

```python
from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.store import JobStore


def test_worker_next_job_returns_null_when_no_work(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    response = client.get("/worker/next-job?worker_id=windows-gpu-1")

    assert response.status_code == 200
    assert response.json() == {"job": None}


def test_worker_next_job_claims_audio_ready_job(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    client = TestClient(create_app(store, worker_lease_seconds=1800))

    response = client.get("/worker/next-job?worker_id=windows-gpu-1")

    assert response.status_code == 200
    body = response.json()["job"]
    assert body["id"] == job.id
    assert body["audio_path_windows"] == "M:\\ktb-096\\audio.wav"
    assert body["japanese_srt_path_windows"] == "M:\\ktb-096\\ktb-096.Japanese.srt"
    assert body["english_srt_path_windows"] == "M:\\ktb-096\\ktb-096.English.srt"


def test_worker_heartbeat_and_failure(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    client = TestClient(create_app(store))

    heartbeat = client.post(
        f"/worker/jobs/{claimed.id}/heartbeat",
        json={"worker_id": "windows-gpu-1", "stage": "transcribing"},
    )
    failed = client.post(
        f"/worker/jobs/{claimed.id}/failed",
        json={"worker_id": "windows-gpu-1", "stage": "transcribing", "error": "CUDA out of memory"},
    )

    assert heartbeat.status_code == 200
    assert heartbeat.json()["status"] == "transcribing"
    assert failed.status_code == 200
    assert failed.json()["status"] == "audio_ready"
    assert failed.json()["error"] == "transcribing: CUDA out of memory"
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_api_jobs.py tests/test_api_worker.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'orchestrator.api'`.

- [ ] **Step 3: Add API response models and FastAPI app factory**

Append to `orchestrator/models.py`:

```python
class JobResponse(BaseModel):
    id: str
    movie_number: str
    status: JobStatus
    job_dir_mac: str
    job_dir_windows: str
    error: str | None = None


class BatchJobResponse(BaseModel):
    created: list[JobResponse]
    existing: list[JobResponse]
    invalid: list[str]


class WorkerJobResponse(BaseModel):
    id: str
    movie_number: str
    audio_path_windows: str
    japanese_srt_path_windows: str
    english_srt_path_windows: str


class WorkerNextJobResponse(BaseModel):
    job: WorkerJobResponse | None
```

Create `orchestrator/api.py`:

```python
from collections.abc import Callable
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from orchestrator.models import (
    BatchJobResponse,
    JobResponse,
    JobStatus,
    SubmitBatchRequest,
    SubmitJobRequest,
    WorkerCompleteRequest,
    WorkerFailedRequest,
    WorkerHeartbeatRequest,
    WorkerJobResponse,
    WorkerNextJobResponse,
)
from orchestrator.store import JobRecord, JobStore


def job_response(job: JobRecord) -> JobResponse:
    return JobResponse(
        id=job.id,
        movie_number=job.normalized_movie_number,
        status=job.status,
        job_dir_mac=job.job_dir_mac,
        job_dir_windows=job.job_dir_windows,
        error=job.error,
    )


def worker_job_response(job: JobRecord) -> WorkerJobResponse:
    return WorkerJobResponse(
        id=job.id,
        movie_number=job.normalized_movie_number,
        audio_path_windows=job.audio_path_windows,
        japanese_srt_path_windows=job.japanese_srt_path_windows,
        english_srt_path_windows=job.english_srt_path_windows,
    )


def create_app(
    store: JobStore,
    *,
    worker_lease_seconds: int = 1800,
    max_worker_attempts: int = 3,
    final_file_exists: Callable[[str], bool] | None = None,
) -> FastAPI:
    app = FastAPI(title="JAV Subtitle Orchestrator")
    final_file_exists = final_file_exists or (lambda path: Path(path).exists())

    @app.post("/jobs", response_model=JobResponse)
    def submit_job(request: SubmitJobRequest) -> JobResponse:
        result = store.submit_job(request.movie_number, request.priority, request.force)
        if result.kind == "invalid":
            raise HTTPException(status_code=422, detail="invalid movie_number")
        if result.kind == "conflict":
            raise HTTPException(status_code=409, detail=job_response(result.job).model_dump(mode="json"))
        return job_response(result.job)

    @app.post("/jobs/batch", response_model=BatchJobResponse)
    def submit_batch(request: SubmitBatchRequest) -> BatchJobResponse:
        result = store.submit_batch(request.movie_numbers, request.priority, request.force)
        return BatchJobResponse(
            created=[job_response(item.job) for item in result.created],
            existing=[job_response(item.job) for item in result.existing if item.job is not None],
            invalid=[item.movie_number for item in result.invalid],
        )

    @app.get("/jobs", response_model=list[JobResponse])
    def list_jobs(status: JobStatus | None = Query(default=None)) -> list[JobResponse]:
        return [job_response(job) for job in store.list_jobs(status)]

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    def get_job(job_id: str) -> JobResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job_response(job)

    @app.get("/worker/next-job", response_model=WorkerNextJobResponse)
    def next_job(worker_id: str) -> WorkerNextJobResponse:
        job = store.claim_next_worker_job(worker_id, worker_lease_seconds)
        if job is None:
            return WorkerNextJobResponse(job=None)
        return WorkerNextJobResponse(job=worker_job_response(job))

    @app.post("/worker/jobs/{job_id}/heartbeat", response_model=JobResponse)
    def heartbeat(job_id: str, request: WorkerHeartbeatRequest) -> JobResponse:
        job = store.heartbeat(job_id, request.worker_id, request.stage, worker_lease_seconds)
        return job_response(job)

    @app.post("/worker/jobs/{job_id}/complete", response_model=JobResponse)
    def complete(job_id: str, request: WorkerCompleteRequest) -> JobResponse:
        job = store.complete_worker_job(
            job_id,
            request.worker_id,
            request.japanese_srt_path_windows,
            request.english_srt_path_windows,
            final_file_exists,
        )
        return job_response(job)

    @app.post("/worker/jobs/{job_id}/failed", response_model=JobResponse)
    def failed(job_id: str, request: WorkerFailedRequest) -> JobResponse:
        job = store.fail_worker_job(
            job_id,
            request.worker_id,
            request.stage,
            request.error,
            max_worker_attempts,
        )
        return job_response(job)

    return app
```

- [ ] **Step 4: Run API tests and commit**

Run:

```bash
pytest tests/test_api_jobs.py tests/test_api_worker.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/api.py orchestrator/models.py tests/test_api_jobs.py tests/test_api_worker.py
git commit -m "feat: add mac api endpoints"
```

## Task 6: Job Folder Snapshots

**Files:**

- Create: `orchestrator/job_snapshot.py`
- Modify: `orchestrator/store.py`
- Test: `tests/test_job_snapshot.py`

- [ ] **Step 1: Write failing snapshot tests**

Create `tests/test_job_snapshot.py`:

```python
import json

from orchestrator.job_snapshot import write_job_snapshot
from orchestrator.store import JobStore


def test_write_job_snapshot_creates_human_readable_job_json(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job

    snapshot_path = write_job_snapshot(job)

    assert snapshot_path == mac_jobs_root / "ktb-096" / "job.json"
    data = json.loads(snapshot_path.read_text())
    assert data["id"] == job.id
    assert data["movie_number"] == "ktb-096"
    assert data["status"] == "queued"
    assert data["job_dir_windows"] == "M:\\ktb-096"
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
pytest tests/test_job_snapshot.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'orchestrator.job_snapshot'`.

- [ ] **Step 3: Add snapshot writer**

Create `orchestrator/job_snapshot.py`:

```python
import json
from dataclasses import asdict
from pathlib import Path

from orchestrator.store import JobRecord


def write_job_snapshot(job: JobRecord) -> Path:
    job_dir = Path(job.job_dir_mac)
    job_dir.mkdir(parents=True, exist_ok=True)
    snapshot = asdict(job)
    snapshot["status"] = job.status.value
    path = job_dir / "job.json"
    path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
```

- [ ] **Step 4: Run test and commit**

Run:

```bash
pytest tests/test_job_snapshot.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/job_snapshot.py tests/test_job_snapshot.py
git commit -m "feat: add job folder snapshots"
```

## Task 7: Mac Downloader Worker With MissAV Adapter Boundary

**Files:**

- Create: `orchestrator/missav_adapter.py`
- Create: `orchestrator/mac_worker.py`
- Test: `tests/test_mac_worker.py`

- [ ] **Step 1: Write failing Mac worker tests using fake MissAV adapter**

Create `tests/test_mac_worker.py`:

```python
from pathlib import Path

from orchestrator.mac_worker import MacDownloadWorker
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


class FakeMissAVAdapter:
    def download_metadata(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text('{"movie_number":"%s"}\n' % movie_number, encoding="utf-8")

    def download_audio(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.with_suffix(".wav.tmp").write_bytes(b"RIFFfakeWAVE")
        output_path.with_suffix(".wav.tmp").replace(output_path)


def test_mac_worker_processes_one_queued_job_to_audio_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    processed = worker.process_one()

    assert processed is True
    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.AUDIO_READY
    assert Path(refreshed.metadata_path_mac).exists()
    assert Path(refreshed.audio_path_mac).exists()
    assert refreshed.audio_path_windows == "M:\\ktb-096\\audio.wav"
    assert (mac_jobs_root / "ktb-096" / "job.json").exists()


def test_mac_worker_returns_false_when_no_queued_jobs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    assert worker.process_one() is False
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_mac_worker.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'orchestrator.mac_worker'`.

- [ ] **Step 3: Add MissAV adapter wrapper**

Create `orchestrator/missav_adapter.py`:

```python
import json
import subprocess
import sys
from pathlib import Path


class MissAVAdapter:
    def __init__(self, missav_pipeline_root: Path) -> None:
        self.missav_pipeline_root = missav_pipeline_root

    def download_metadata(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(self.missav_pipeline_root / "new-release" / "unified_download.py"),
            "--movie-number",
            movie_number,
            "--metadata-output",
            str(output_path),
        ]
        completed = subprocess.run(
            command,
            cwd=self.missav_pipeline_root,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr or completed.stdout)
        if not output_path.exists():
            output_path.write_text(json.dumps({"movie_number": movie_number}) + "\n", encoding="utf-8")

    def download_audio(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        command = [
            sys.executable,
            str(self.missav_pipeline_root / "new-release" / "batch_audio_downloader.py"),
            "--movie-number",
            movie_number,
            "--output",
            str(tmp_path),
        ]
        completed = subprocess.run(
            command,
            cwd=self.missav_pipeline_root,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr or completed.stdout)
        if not tmp_path.exists() and output_path.exists():
            return
        tmp_path.replace(output_path)
```

Before executing this task against the real MissAV repo, inspect the actual CLI flags of:

```bash
python /Users/ytt/Documents/startup/MissAV-Pipeline/new-release/unified_download.py --help
python /Users/ytt/Documents/startup/MissAV-Pipeline/new-release/batch_audio_downloader.py --help
```

If the flag names differ, change only the `command` lists in `MissAVAdapter`; keep the adapter method signatures unchanged:

```python
def download_metadata(self, movie_number: str, output_path: Path) -> None:
    ...

def download_audio(self, movie_number: str, output_path: Path) -> None:
    ...
```

- [ ] **Step 4: Add Mac worker**

Create `orchestrator/mac_worker.py`:

```python
import time
from pathlib import Path

from orchestrator.job_snapshot import write_job_snapshot
from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobRecord, JobStore


class MacDownloadWorker:
    def __init__(self, store: JobStore, adapter, max_download_attempts: int) -> None:
        self.store = store
        self.adapter = adapter
        self.max_download_attempts = max_download_attempts

    def process_one(self) -> bool:
        queued = self.store.list_jobs(JobStatus.QUEUED)
        if not queued:
            return False
        job = queued[0]
        try:
            self._process_job(job)
        except Exception as exc:
            self._record_failure(job, str(exc))
        return True

    def _process_job(self, job: JobRecord) -> None:
        paths = build_job_paths(
            job.normalized_movie_number,
            self.store.jobs_root_mac,
            self.store.jobs_root_windows,
        )
        Path(paths.job_dir_mac).mkdir(parents=True, exist_ok=True)
        updated = self.store.update_download_status(job.id, JobStatus.DOWNLOADING_METADATA)
        write_job_snapshot(updated)
        self.adapter.download_metadata(job.normalized_movie_number, paths.metadata_path_mac)
        updated = self.store.update_download_status(
            job.id,
            JobStatus.DOWNLOADING_AUDIO,
            metadata_path_mac=str(paths.metadata_path_mac),
        )
        write_job_snapshot(updated)
        self.adapter.download_audio(job.normalized_movie_number, paths.audio_path_mac)
        updated = self.store.update_download_status(
            job.id,
            JobStatus.AUDIO_READY,
            metadata_path_mac=str(paths.metadata_path_mac),
            audio_path_mac=str(paths.audio_path_mac),
            audio_path_windows=paths.audio_path_windows,
        )
        write_job_snapshot(updated)

    def _record_failure(self, job: JobRecord, error: str) -> None:
        next_attempts = job.attempt_count + 1
        next_status = JobStatus.FAILED if next_attempts >= self.max_download_attempts else JobStatus.QUEUED
        updated = self.store.record_download_failure(job.id, next_status, next_attempts, error)
        write_job_snapshot(updated)


def run_forever(worker: MacDownloadWorker, poll_interval_seconds: int = 10) -> None:
    while True:
        worker.process_one()
        time.sleep(poll_interval_seconds)
```

Append this method to `JobStore`:

```python
    def record_download_failure(
        self,
        job_id: str,
        status: JobStatus,
        attempt_count: int,
        error: str,
    ) -> JobRecord:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = ?, attempt_count = ?, updated_at = ?, error = ?
                WHERE id = ?
                """,
                (status.value, attempt_count, now, error, job_id),
            )
            return self.get_job(job_id, conn=conn)
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/test_mac_worker.py tests/test_store_submit.py tests/test_store_worker_claims.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/missav_adapter.py orchestrator/mac_worker.py orchestrator/store.py tests/test_mac_worker.py
git commit -m "feat: add mac downloader worker"
```

## Task 8: Windows faster-whisper Transcription Adapter

**Files:**

- Create: `orchestrator/transcription.py`
- Test: `tests/test_transcription.py`
- Create: `tests/fixtures/tiny.wav`

- [ ] **Step 1: Add fixture and failing transcription tests**

Create `tests/fixtures/tiny.wav` as a tiny WAV fixture. Use this command during implementation:

```bash
python - <<'PY'
import wave
from pathlib import Path

path = Path("tests/fixtures/tiny.wav")
path.parent.mkdir(parents=True, exist_ok=True)
with wave.open(str(path), "wb") as wav:
    wav.setnchannels(1)
    wav.setsampwidth(2)
    wav.setframerate(16000)
    wav.writeframes(b"\x00\x00" * 16000)
PY
```

Create `tests/test_transcription.py`:

```python
from pathlib import Path

from orchestrator.transcription import Segment, write_srt


def test_write_srt_formats_segments_with_japanese_text(tmp_path):
    output = tmp_path / "ktb-096.Japanese.srt"
    segments = [
        Segment(start=0.0, end=1.5, text="こんにちは"),
        Segment(start=61.25, end=62.5, text="テストです"),
    ]

    write_srt(segments, output)

    assert output.read_text(encoding="utf-8") == (
        "1\n"
        "00:00:00,000 --> 00:00:01,500\n"
        "こんにちは\n\n"
        "2\n"
        "00:01:01,250 --> 00:01:02,500\n"
        "テストです\n\n"
    )
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
pytest tests/test_transcription.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'orchestrator.transcription'`.

- [ ] **Step 3: Add transcription adapter**

Create `orchestrator/transcription.py`:

```python
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Segment:
    start: float
    end: float
    text: str


def srt_timestamp(seconds: float) -> str:
    millis = round(seconds * 1000)
    hours, remainder = divmod(millis, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    secs, millis = divmod(remainder, 1000)
    return f"{hours:02}:{minutes:02}:{secs:02},{millis:03}"


def write_srt(segments: list[Segment], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="\n") as handle:
        for index, segment in enumerate(segments, start=1):
            handle.write(f"{index}\n")
            handle.write(f"{srt_timestamp(segment.start)} --> {srt_timestamp(segment.end)}\n")
            handle.write(segment.text.strip() + "\n\n")
    tmp_path.replace(output_path)


class FasterWhisperTranscriber:
    def __init__(self, model_name: str, device: str, compute_type: str) -> None:
        self.model_name = model_name
        self.device = device
        self.compute_type = compute_type
        self._model = None

    def _load_model(self):
        if self._model is None:
            from faster_whisper import WhisperModel

            self._model = WhisperModel(
                self.model_name,
                device=self.device,
                compute_type=self.compute_type,
            )
        return self._model

    def transcribe_to_srt(self, audio_path: Path, output_path: Path) -> None:
        model = self._load_model()
        raw_segments, _info = model.transcribe(str(audio_path), language="ja")
        segments = [
            Segment(start=segment.start, end=segment.end, text=segment.text)
            for segment in raw_segments
        ]
        write_srt(segments, output_path)
```

- [ ] **Step 4: Run tests and commit**

Run:

```bash
pytest tests/test_transcription.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/transcription.py tests/test_transcription.py tests/fixtures/tiny.wav
git commit -m "feat: add faster-whisper transcription adapter"
```

## Task 9: Translation Adapter

**Files:**

- Create: `orchestrator/translation.py`
- Create: `tests/fixtures/tiny.ja.srt`
- Test: `tests/test_translation.py`

- [ ] **Step 1: Write failing translation adapter tests**

Create `tests/fixtures/tiny.ja.srt`:

```text
1
00:00:00,000 --> 00:00:01,500
こんにちは

```

Create `tests/test_translation.py`:

```python
from pathlib import Path

from orchestrator.translation import SubtitleTranslator


def test_translation_adapter_renames_script_output_to_english_srt(tmp_path):
    script = tmp_path / "fake_translate.py"
    script.write_text(
        "import argparse\n"
        "from pathlib import Path\n"
        "parser = argparse.ArgumentParser()\n"
        "parser.add_argument('--input')\n"
        "parser.add_argument('--langs')\n"
        "parser.add_argument('--output-dir')\n"
        "args = parser.parse_args()\n"
        "Path(args.output_dir, 'ktb-096.en.srt').write_text('1\\n00:00:00,000 --> 00:00:01,500\\nHello\\n\\n', encoding='utf-8')\n",
        encoding="utf-8",
    )
    input_srt = tmp_path / "ktb-096.Japanese.srt"
    input_srt.write_text("1\n00:00:00,000 --> 00:00:01,500\nこんにちは\n\n", encoding="utf-8")
    output_srt = tmp_path / "ktb-096.English.srt"
    translator = SubtitleTranslator(str(script))

    translator.translate_to_english(input_srt, output_srt)

    assert output_srt.read_text(encoding="utf-8").startswith("1\n00:00:00,000")
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
pytest tests/test_translation.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'orchestrator.translation'`.

- [ ] **Step 3: Add translation adapter**

Create `orchestrator/translation.py`:

```python
import subprocess
import sys
from pathlib import Path


class SubtitleTranslator:
    def __init__(self, translate_script_path: str) -> None:
        self.translate_script_path = translate_script_path

    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        output_srt.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            self.translate_script_path,
            "--input",
            str(input_srt),
            "--langs",
            "en",
            "--output-dir",
            str(output_srt.parent),
        ]
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr or completed.stdout)

        candidates = [
            output_srt,
            output_srt.parent / f"{input_srt.stem}.en.srt",
            output_srt.parent / input_srt.name.replace(".Japanese.srt", ".en.srt"),
            output_srt.parent / input_srt.name.replace(".Japanese.srt", ".English.srt"),
        ]
        for candidate in candidates:
            if candidate.exists():
                if candidate != output_srt:
                    tmp_path = output_srt.with_suffix(output_srt.suffix + ".tmp")
                    tmp_path.write_text(candidate.read_text(encoding="utf-8"), encoding="utf-8")
                    tmp_path.replace(output_srt)
                return
        raise FileNotFoundError(f"translation script did not produce {output_srt}")
```

- [ ] **Step 4: Run tests and commit**

Run:

```bash
pytest tests/test_translation.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/translation.py tests/test_translation.py tests/fixtures/tiny.ja.srt
git commit -m "feat: add subtitle translation adapter"
```

## Task 10: Windows Worker Loop

**Files:**

- Create: `orchestrator/windows_worker.py`
- Test: `tests/test_windows_worker.py`

- [ ] **Step 1: Write failing Windows worker tests**

Create `tests/test_windows_worker.py`:

```python
from pathlib import Path

from orchestrator.windows_worker import WindowsWorker


class FakeClient:
    def __init__(self, job):
        self.job = job
        self.heartbeats = []
        self.completed = []
        self.failed = []

    def next_job(self):
        return self.job

    def heartbeat(self, job_id, stage):
        self.heartbeats.append((job_id, stage))

    def complete(self, job_id, japanese_srt_path_windows, english_srt_path_windows):
        self.completed.append((job_id, japanese_srt_path_windows, english_srt_path_windows))

    def failed(self, job_id, stage, error):
        self.failed.append((job_id, stage, error))


class FakeTranscriber:
    def transcribe_to_srt(self, audio_path: Path, output_path: Path) -> None:
        assert audio_path.name == "audio.wav"
        output_path.write_text("1\n00:00:00,000 --> 00:00:01,000\nこんにちは\n\n", encoding="utf-8")


class FakeTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        assert input_srt.name == "ktb-096.Japanese.srt"
        output_srt.write_text("1\n00:00:00,000 --> 00:00:01,000\nHello\n\n", encoding="utf-8")


def test_windows_worker_processes_one_job(tmp_path):
    job_dir = tmp_path / "ktb-096"
    job_dir.mkdir()
    audio = job_dir / "audio.wav"
    audio.write_bytes(b"RIFFfakeWAVE")
    job = {
        "id": "job_1",
        "audio_path_windows": str(audio),
        "japanese_srt_path_windows": str(job_dir / "ktb-096.Japanese.srt"),
        "english_srt_path_windows": str(job_dir / "ktb-096.English.srt"),
    }
    client = FakeClient(job)
    worker = WindowsWorker(client, FakeTranscriber(), FakeTranslator())

    processed = worker.process_one()

    assert processed is True
    assert client.heartbeats == [
        ("job_1", "transcribing"),
        ("job_1", "transcription_done"),
        ("job_1", "translating"),
    ]
    assert client.completed == [
        (
            "job_1",
            str(job_dir / "ktb-096.Japanese.srt"),
            str(job_dir / "ktb-096.English.srt"),
        )
    ]


def test_windows_worker_returns_false_when_no_job():
    client = FakeClient(None)
    worker = WindowsWorker(client, FakeTranscriber(), FakeTranslator())

    assert worker.process_one() is False
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
pytest tests/test_windows_worker.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'orchestrator.windows_worker'`.

- [ ] **Step 3: Add Windows API client and worker**

Create `orchestrator/windows_worker.py`:

```python
import time
from pathlib import Path

import requests


class MacApiClient:
    def __init__(self, base_url: str, worker_id: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id

    def next_job(self):
        response = requests.get(
            f"{self.base_url}/worker/next-job",
            params={"worker_id": self.worker_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["job"]

    def heartbeat(self, job_id: str, stage: str) -> None:
        response = requests.post(
            f"{self.base_url}/worker/jobs/{job_id}/heartbeat",
            json={"worker_id": self.worker_id, "stage": stage},
            timeout=30,
        )
        response.raise_for_status()

    def complete(
        self,
        job_id: str,
        japanese_srt_path_windows: str,
        english_srt_path_windows: str,
    ) -> None:
        response = requests.post(
            f"{self.base_url}/worker/jobs/{job_id}/complete",
            json={
                "worker_id": self.worker_id,
                "japanese_srt_path_windows": japanese_srt_path_windows,
                "english_srt_path_windows": english_srt_path_windows,
            },
            timeout=30,
        )
        response.raise_for_status()

    def failed(self, job_id: str, stage: str, error: str) -> None:
        response = requests.post(
            f"{self.base_url}/worker/jobs/{job_id}/failed",
            json={"worker_id": self.worker_id, "stage": stage, "error": error},
            timeout=30,
        )
        response.raise_for_status()


class WindowsWorker:
    def __init__(self, client, transcriber, translator) -> None:
        self.client = client
        self.transcriber = transcriber
        self.translator = translator

    def process_one(self) -> bool:
        job = self.client.next_job()
        if job is None:
            return False
        job_id = job["id"]
        stage = "transcribing"
        try:
            audio_path = Path(job["audio_path_windows"])
            japanese_srt = Path(job["japanese_srt_path_windows"])
            english_srt = Path(job["english_srt_path_windows"])
            self.client.heartbeat(job_id, stage)
            self.transcriber.transcribe_to_srt(audio_path, japanese_srt)
            stage = "transcription_done"
            self.client.heartbeat(job_id, stage)
            stage = "translating"
            self.client.heartbeat(job_id, stage)
            self.translator.translate_to_english(japanese_srt, english_srt)
            self.client.complete(job_id, str(japanese_srt), str(english_srt))
            return True
        except Exception as exc:
            self.client.failed(job_id, stage, str(exc))
            return True


def run_forever(worker: WindowsWorker, poll_interval_seconds: int) -> None:
    while True:
        worker.process_one()
        time.sleep(poll_interval_seconds)
```

- [ ] **Step 4: Run tests and commit**

Run:

```bash
pytest tests/test_windows_worker.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/windows_worker.py tests/test_windows_worker.py
git commit -m "feat: add windows worker loop"
```

## Task 11: Per-Job Log Files

**Files:**

- Create: `orchestrator/job_logs.py`
- Modify: `orchestrator/mac_worker.py`
- Modify: `orchestrator/windows_worker.py`
- Test: `tests/test_job_logs.py`
- Test: `tests/test_mac_worker.py`
- Test: `tests/test_windows_worker.py`

- [ ] **Step 1: Write failing log helper tests**

Create `tests/test_job_logs.py`:

```python
from orchestrator.job_logs import append_job_log


def test_append_job_log_creates_logs_directory_and_appends_lines(tmp_path):
    job_dir = tmp_path / "ktb-096"

    first = append_job_log(job_dir, "mac-download.log", "downloading metadata")
    second = append_job_log(job_dir, "mac-download.log", "downloaded audio")

    assert first == job_dir / "logs" / "mac-download.log"
    assert second == first
    assert first.read_text(encoding="utf-8").splitlines() == [
        "downloading metadata",
        "downloaded audio",
    ]
```

Append to `tests/test_mac_worker.py`:

```python
def test_mac_worker_writes_download_log(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-096", priority=100, force=False)
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    worker.process_one()

    log_text = (mac_jobs_root / "ktb-096" / "logs" / "mac-download.log").read_text(encoding="utf-8")
    assert "downloading_metadata ktb-096" in log_text
    assert "downloading_audio ktb-096" in log_text
    assert "audio_ready ktb-096" in log_text
```

Append to `tests/test_windows_worker.py`:

```python
def test_windows_worker_writes_worker_whisper_and_translate_logs(tmp_path):
    job_dir = tmp_path / "ktb-096"
    job_dir.mkdir()
    audio = job_dir / "audio.wav"
    audio.write_bytes(b"RIFFfakeWAVE")
    job = {
        "id": "job_1",
        "audio_path_windows": str(audio),
        "japanese_srt_path_windows": str(job_dir / "ktb-096.Japanese.srt"),
        "english_srt_path_windows": str(job_dir / "ktb-096.English.srt"),
    }
    client = FakeClient(job)
    worker = WindowsWorker(client, FakeTranscriber(), FakeTranslator())

    worker.process_one()

    assert "claimed job_1" in (job_dir / "logs" / "windows-worker.log").read_text(encoding="utf-8")
    assert "transcribing" in (job_dir / "logs" / "whisper.log").read_text(encoding="utf-8")
    assert "translating" in (job_dir / "logs" / "translate.log").read_text(encoding="utf-8")
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
pytest tests/test_job_logs.py tests/test_mac_worker.py tests/test_windows_worker.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'orchestrator.job_logs'` or missing log file assertions.

- [ ] **Step 3: Add log helper**

Create `orchestrator/job_logs.py`:

```python
from pathlib import Path


def append_job_log(job_dir: Path, filename: str, message: str) -> Path:
    logs_dir = job_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / filename
    with log_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(message.rstrip() + "\n")
    return log_path
```

- [ ] **Step 4: Wire logs into Mac and Windows workers**

In `orchestrator/mac_worker.py`, add this import:

```python
from orchestrator.job_logs import append_job_log
```

In `MacDownloadWorker._process_job`, write the required Mac log entries:

```python
        Path(paths.job_dir_mac).mkdir(parents=True, exist_ok=True)
        append_job_log(paths.job_dir_mac, "mac-download.log", f"downloading_metadata {job.normalized_movie_number}")
        updated = self.store.update_download_status(job.id, JobStatus.DOWNLOADING_METADATA)
        write_job_snapshot(updated)
        self.adapter.download_metadata(job.normalized_movie_number, paths.metadata_path_mac)
        append_job_log(paths.job_dir_mac, "mac-download.log", f"downloading_audio {job.normalized_movie_number}")
        updated = self.store.update_download_status(
            job.id,
            JobStatus.DOWNLOADING_AUDIO,
            metadata_path_mac=str(paths.metadata_path_mac),
        )
        write_job_snapshot(updated)
        self.adapter.download_audio(job.normalized_movie_number, paths.audio_path_mac)
        append_job_log(paths.job_dir_mac, "mac-download.log", f"audio_ready {job.normalized_movie_number}")
```

In `orchestrator/windows_worker.py`, add this import:

```python
from orchestrator.job_logs import append_job_log
```

In `WindowsWorker.process_one`, write the required Windows log files:

```python
            job_dir = english_srt.parent
            append_job_log(job_dir, "windows-worker.log", f"claimed {job_id}")
            self.client.heartbeat(job_id, stage)
            append_job_log(job_dir, "whisper.log", f"transcribing {audio_path}")
            self.transcriber.transcribe_to_srt(audio_path, japanese_srt)
            stage = "transcription_done"
            self.client.heartbeat(job_id, stage)
            stage = "translating"
            self.client.heartbeat(job_id, stage)
            append_job_log(job_dir, "translate.log", f"translating {japanese_srt}")
            self.translator.translate_to_english(japanese_srt, english_srt)
            append_job_log(job_dir, "windows-worker.log", f"completed {job_id}")
```

In the `except` block, add:

```python
            append_job_log(Path(job["english_srt_path_windows"]).parent, "windows-worker.log", f"failed {job_id} {stage}: {exc}")
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest tests/test_job_logs.py tests/test_mac_worker.py tests/test_windows_worker.py -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/job_logs.py orchestrator/mac_worker.py orchestrator/windows_worker.py tests/test_job_logs.py tests/test_mac_worker.py tests/test_windows_worker.py
git commit -m "feat: add per-job log files"
```

## Task 12: CLI Entrypoints And Machine Setup Docs

**Files:**

- Modify: `orchestrator/__main__.py`
- Create: `orchestrator/logging_config.py`
- Create: `docs/setup/mac.md`
- Create: `docs/setup/windows.md`
- Create: `docs/setup/smb.md`
- Modify: `README.md`

- [ ] **Step 1: Write CLI smoke tests**

Append to `tests/test_config_paths.py`:

```python
import subprocess
import sys


def test_module_cli_prints_help():
    completed = subprocess.run(
        [sys.executable, "-m", "orchestrator", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "api" in completed.stdout
    assert "mac-worker" in completed.stdout
    assert "windows-worker" in completed.stdout
```

- [ ] **Step 2: Run smoke test to verify failure**

Run:

```bash
pytest tests/test_config_paths.py::test_module_cli_prints_help -v
```

Expected: FAIL because the CLI does not expose the required subcommands.

- [ ] **Step 3: Add CLI entrypoints**

Create `orchestrator/logging_config.py`:

```python
import logging


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
```

Replace `orchestrator/__main__.py` with:

```python
import argparse

import uvicorn

from orchestrator.api import create_app
from orchestrator.config import MacSettings, WindowsSettings
from orchestrator.logging_config import configure_logging
from orchestrator.mac_worker import MacDownloadWorker, run_forever as run_mac_forever
from orchestrator.missav_adapter import MissAVAdapter
from orchestrator.store import JobStore
from orchestrator.transcription import FasterWhisperTranscriber
from orchestrator.translation import SubtitleTranslator
from orchestrator.windows_worker import MacApiClient, WindowsWorker, run_forever as run_windows_forever


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m orchestrator")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("api")
    subcommands.add_parser("mac-worker")
    subcommands.add_parser("windows-worker")
    args = parser.parse_args()
    configure_logging()

    if args.command == "api":
        settings = MacSettings()
        store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
        store.initialize()
        app = create_app(
            store,
            worker_lease_seconds=settings.worker_lease_seconds,
            max_worker_attempts=settings.max_worker_attempts,
        )
        uvicorn.run(app, host=settings.host, port=settings.port)
    elif args.command == "mac-worker":
        settings = MacSettings()
        store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
        store.initialize()
        worker = MacDownloadWorker(
            store,
            MissAVAdapter(settings.missav_pipeline_root),
            settings.max_download_attempts,
        )
        run_mac_forever(worker)
    elif args.command == "windows-worker":
        settings = WindowsSettings()
        client = MacApiClient(settings.mac_api_base_url, settings.worker_id)
        transcriber = FasterWhisperTranscriber(
            settings.whisper_model,
            settings.whisper_device,
            settings.whisper_compute_type,
        )
        translator = SubtitleTranslator(settings.translate_script_path)
        worker = WindowsWorker(client, transcriber, translator)
        run_windows_forever(worker, settings.poll_interval_seconds)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Add setup docs**

Create `docs/setup/mac.md`:

~~~markdown
# Mac Setup

1. Create the SMB job root:

```bash
mkdir -p /Users/ytt/MissAVJobs
```

2. Create `.env` from `.env.example` and keep these values for version 1:

```text
ORCHESTRATOR_HOST=0.0.0.0
ORCHESTRATOR_PORT=8000
ORCHESTRATOR_DB_PATH=/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3
MISSAV_PIPELINE_ROOT=/Users/ytt/Documents/startup/MissAV-Pipeline
JOBS_ROOT_MAC=/Users/ytt/MissAVJobs
JOBS_ROOT_WINDOWS=M:\
MAC_DOWNLOAD_CONCURRENCY=1
WORKER_LEASE_SECONDS=1800
MAX_DOWNLOAD_ATTEMPTS=3
MAX_WORKER_ATTEMPTS=3
```

3. Install and run:

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python -m orchestrator api
```

4. In a second terminal:

```bash
cd /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator
source .venv/bin/activate
python -m orchestrator mac-worker
```

5. Submit a batch:

```bash
curl -X POST http://127.0.0.1:8000/jobs/batch \
  -H "Content-Type: application/json" \
  -d '{"movie_numbers":["ktb-096","ktb-095","ktb-093"],"priority":100,"force":false}'
```
~~~

Create `docs/setup/windows.md`:

~~~markdown
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
~~~

Create `docs/setup/smb.md`:

~~~markdown
# SMB Setup

The Mac owns the shared job folder:

```text
/Users/ytt/MissAVJobs
```

Windows maps that share to:

```text
M:\
```

The same job file must be visible at both paths:

```text
Mac:     /Users/ytt/MissAVJobs/ktb-096/audio.wav
Windows: M:\ktb-096\audio.wav
```

Keep SMB private to the home network. Do not expose SMB through Cloudflare Tunnel or the public internet.
~~~

Append to `README.md`:

~~~markdown

## Version 1 Run Commands

Mac API:

```bash
python -m orchestrator api
```

Mac downloader worker:

```bash
python -m orchestrator mac-worker
```

Windows worker:

```powershell
python -m orchestrator windows-worker
```

Setup details:

- `docs/setup/mac.md`
- `docs/setup/windows.md`
- `docs/setup/smb.md`
~~~

- [ ] **Step 5: Run tests and commit**

Run:

```bash
pytest -v
```

Expected: PASS.

Commit:

```bash
git add orchestrator/__main__.py orchestrator/logging_config.py docs/setup/mac.md docs/setup/windows.md docs/setup/smb.md README.md tests/test_config_paths.py
git commit -m "docs: add machine setup and cli entrypoints"
```

## Task 13: Integration Verification And One-Movie LAN Runbook

**Files:**

- Create: `docs/setup/e2e-lan-test.md`
- Test: all tests and one manual LAN run

- [ ] **Step 1: Add end-to-end LAN runbook**

Create `docs/setup/e2e-lan-test.md`:

~~~markdown
# End-to-End LAN Test

Use this after all unit and integration tests pass.

## Preconditions

- Mac API is running on `http://0.0.0.0:8000`.
- Mac downloader worker is running.
- Windows worker is running.
- Windows can read and write `M:\`.
- `M:\` points to `/Users/ytt/MissAVJobs`.
- `.env` exists on Mac.
- `.env` exists on Windows.
- `OPENAI_API_KEY` is set on Windows.
- `TRANSLATE_SCRIPT_PATH` points to the existing `subtitle_translate.py` script.

## Submit One Movie

```bash
curl -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"movie_number":"ktb-096","priority":100,"force":false}'
```

Expected response:

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

```bash
ls -la /Users/ytt/MissAVJobs/ktb-096
```

Expected files after the Mac worker finishes:

```text
job.json
metadata.json
audio.wav
```

## Confirm Windows SMB Visibility

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

Expected final files:

```text
M:\ktb-096\ktb-096.Japanese.srt
M:\ktb-096\ktb-096.English.srt
```

## Confirm API Status

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
~~~

- [ ] **Step 2: Run full automated tests**

Run:

```bash
pytest -v
```

Expected: PASS.

- [ ] **Step 3: Run one real movie through the LAN flow**

Run on Mac:

```bash
python -m orchestrator api
python -m orchestrator mac-worker
curl -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"movie_number":"ktb-096","priority":100,"force":false}'
```

Run on Windows:

```powershell
python -m orchestrator windows-worker
```

Expected final Mac files:

```text
/Users/ytt/MissAVJobs/ktb-096/audio.wav
/Users/ytt/MissAVJobs/ktb-096/ktb-096.Japanese.srt
/Users/ytt/MissAVJobs/ktb-096/ktb-096.English.srt
```

Expected final API status:

```text
english_srt_ready
```

- [ ] **Step 4: Commit runbook**

Commit:

```bash
git add docs/setup/e2e-lan-test.md
git commit -m "docs: add e2e lan test runbook"
```

## Self-Review

Spec coverage:

- Mac API/downloader: covered by Tasks 5, 7, and 12.
- SQLite queue: covered by Tasks 3 and 4.
- SMB job folder: covered by Tasks 2, 6, 12, and 13.
- Windows worker: covered by Tasks 8, 9, and 10.
- faster-whisper transcription: covered by Task 8.
- Existing SRT translation script: covered by Task 9.
- Conservative concurrency of one Mac download and one Windows worker: preserved by single-job `process_one()` loops and config defaults in Tasks 1, 7, and 10.
- Status visibility through API endpoints: covered by Task 5.
- Batch submission and duplicate handling: covered by Task 3 and Task 5.
- Worker claim and lease: covered by Task 4 and Task 5.
- Per-job logs under `logs/`: covered by Task 11.
- Setup documentation and end-to-end LAN test: covered by Tasks 12 and 13.

Placeholder scan:

- No task uses unresolved placeholder tokens, unspecified validation, or unspecified test coverage.
- Where real MissAV CLI flags may differ, Task 7 pins the adapter method signatures and limits inspection to the two exact scripts named in the spec.

Type consistency:

- `JobStatus` values match the design spec.
- `JobStore` method names used by API and workers are introduced before use.
- Path properties use the same names from the SQLite schema and API contract.
- Windows path outputs use `M:\<movie>\<file>` strings, while Mac file writes use `Path` objects under `/Users/ytt/MissAVJobs`.
