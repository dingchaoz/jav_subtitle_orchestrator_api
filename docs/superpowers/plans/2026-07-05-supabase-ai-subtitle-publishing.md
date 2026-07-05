# Supabase AI Subtitle Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish newly generated English SRT files from the orchestrator to javsubtitle.com's Supabase catalog as `English_AI` as soon as Windows translation completes.

**Architecture:** Add a focused Mac-side publisher that uses Supabase Storage and PostgREST through the existing `requests` dependency. The FastAPI worker completion endpoint calls the publisher after `english_srt_ready`, and a CLI command can publish already completed jobs such as `ktb-112`.

**Tech Stack:** Python 3.11, FastAPI, SQLite, `requests`, Supabase Storage REST API, Supabase PostgREST.

---

## File Structure

- Create `orchestrator/supabase_publisher.py`: Pure publisher logic, storage path naming, movie parsing, Storage upload, movie row upsert, `movie_languages` insert/update.
- Modify `orchestrator/config.py`: Add Supabase publisher settings.
- Modify `orchestrator/api.py`: Accept optional publisher and call it after worker completion.
- Modify `orchestrator/__main__.py`: Wire publisher into API and add `publish-job` CLI command for one-shot publication.
- Modify `.env.example`: Document Supabase publication env vars.
- Add `tests/test_supabase_publisher.py`: Unit tests for path naming, DB/storage calls, and existing language-row behavior.
- Add `tests/test_api_publish.py`: API-level test proving completion triggers publication after `english_srt_ready`.

## Task 1: Publisher Path and Payload Rules

**Files:**
- Create: `tests/test_supabase_publisher.py`
- Create: `orchestrator/supabase_publisher.py`

- [ ] **Step 1: Write failing tests for path and movie parsing**

```python
from pathlib import Path

from orchestrator.supabase_publisher import (
    SupabaseSubtitlePublisher,
    build_ai_subtitle_storage_path,
    parse_movie_code,
)


def test_parse_movie_code_splits_series_and_number():
    assert parse_movie_code("ktb-112") == ("ktb", 112)
    assert parse_movie_code("KTB-007") == ("ktb", 7)


def test_build_ai_subtitle_storage_path_uses_existing_site_convention():
    assert (
        build_ai_subtitle_storage_path("ktb-112")
        == "ktb/ktb-112/ktb-112-English_AI.srt"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/python -m pytest tests/test_supabase_publisher.py -q
```

Expected: fail because `orchestrator.supabase_publisher` does not exist.

- [ ] **Step 3: Implement minimal helpers**

Create `orchestrator/supabase_publisher.py` with:

```python
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


MOVIE_CODE_RE = re.compile(r"^([a-zA-Z]+)-?(\d+)$")
AI_ENGLISH_LANGUAGE = "English_AI"


def parse_movie_code(movie_code: str) -> tuple[str, int]:
    match = MOVIE_CODE_RE.match(movie_code.strip())
    if not match:
        raise ValueError(f"invalid movie code: {movie_code}")
    series, number = match.groups()
    return series.lower(), int(number)


def canonical_movie_code(movie_code: str) -> str:
    series, number = parse_movie_code(movie_code)
    return f"{series}-{number:03d}"


def build_ai_subtitle_storage_path(movie_code: str) -> str:
    canonical = canonical_movie_code(movie_code)
    series, _ = parse_movie_code(canonical)
    return f"{series}/{canonical}/{canonical}-English_AI.srt"


@dataclass(frozen=True)
class SupabasePublishResult:
    movie_code: str
    language: str
    storage_path: str
    movie_uuid: str
    subtitle_id: str


class SupabaseSubtitlePublisher:
    def __init__(
        self,
        supabase_url: str,
        service_role_key: str,
        *,
        bucket: str = "subtitles",
        timeout_seconds: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        self.supabase_url = supabase_url.rstrip("/")
        self.service_role_key = service_role_key
        self.bucket = bucket
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    @property
    def headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
.venv/bin/python -m pytest tests/test_supabase_publisher.py -q
```

Expected: pass.

## Task 2: Publish to Supabase Storage and Tables

**Files:**
- Modify: `tests/test_supabase_publisher.py`
- Modify: `orchestrator/supabase_publisher.py`

- [ ] **Step 1: Add failing publisher behavior tests**

Append:

```python
class FakeResponse:
    def __init__(self, status_code=200, payload=None, text="OK"):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}: {self.text}")


class FakeSession:
    def __init__(self):
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if "/rest/v1/movies" in url and method == "GET":
            return FakeResponse(payload=[])
        if "/rest/v1/movies" in url and method == "POST":
            return FakeResponse(payload=[{"id": "movie-uuid", "movie_id": "ktb-112"}])
        if "/rest/v1/movie_languages" in url and method == "GET":
            return FakeResponse(payload=[])
        if "/rest/v1/movie_languages" in url and method == "POST":
            return FakeResponse(payload=[{"id": "subtitle-uuid"}])
        if "/storage/v1/object/" in url and method == "POST":
            return FakeResponse(payload={"Key": "ktb/ktb-112/ktb-112-English_AI.srt"})
        raise AssertionError(f"unexpected call: {method} {url}")


def test_publish_uploads_english_ai_and_inserts_language_row(tmp_path):
    srt = tmp_path / "ktb-112.English.srt"
    srt.write_text("1\n00:00:00,000 --> 00:00:01,000\nhello\n", encoding="utf-8")
    session = FakeSession()
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
    )

    result = publisher.publish_english_ai("ktb-112", srt)

    assert result.storage_path == "ktb/ktb-112/ktb-112-English_AI.srt"
    assert result.language == "English_AI"
    assert result.movie_uuid == "movie-uuid"
    assert result.subtitle_id == "subtitle-uuid"
    post_language = [
        call for call in session.calls
        if call[0] == "POST" and "/rest/v1/movie_languages" in call[1]
    ][0]
    assert post_language[2]["json"]["language"] == "English_AI"
    assert post_language[2]["json"]["subtitle_source"] == "human"
    assert post_language[2]["json"]["file_path"] == "ktb/ktb-112/ktb-112-English_AI.srt"


def test_publish_updates_existing_english_ai_row(tmp_path):
    srt = tmp_path / "ktb-112.English.srt"
    srt.write_text("1\n00:00:00,000 --> 00:00:01,000\nhello\n", encoding="utf-8")

    class ExistingSession(FakeSession):
        def request(self, method, url, **kwargs):
            if "/rest/v1/movies" in url and method == "GET":
                return FakeResponse(payload=[{"id": "movie-uuid", "movie_id": "ktb-112"}])
            if "/rest/v1/movie_languages" in url and method == "GET":
                return FakeResponse(payload=[{"id": "subtitle-uuid"}])
            if "/rest/v1/movie_languages" in url and method == "PATCH":
                self.calls.append((method, url, kwargs))
                return FakeResponse(payload=[{"id": "subtitle-uuid"}])
            return super().request(method, url, **kwargs)

    session = ExistingSession()
    publisher = SupabaseSubtitlePublisher("https://example.supabase.co", "service-key", session=session)

    result = publisher.publish_english_ai("ktb-112", srt)

    assert result.subtitle_id == "subtitle-uuid"
    assert any(call[0] == "PATCH" and "/rest/v1/movie_languages" in call[1] for call in session.calls)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
.venv/bin/python -m pytest tests/test_supabase_publisher.py -q
```

Expected: fail because `publish_english_ai` is not implemented.

- [ ] **Step 3: Implement publisher methods**

Add methods to `SupabaseSubtitlePublisher`:

```python
    def publish_english_ai(self, movie_code: str, srt_path: Path) -> SupabasePublishResult:
        if not srt_path.exists():
            raise FileNotFoundError(srt_path)
        canonical = canonical_movie_code(movie_code)
        storage_path = build_ai_subtitle_storage_path(canonical)
        self._upload_storage_object(storage_path, srt_path)
        movie_uuid = self._ensure_movie(canonical)
        subtitle_id = self._upsert_language_row(movie_uuid, AI_ENGLISH_LANGUAGE, storage_path, srt_path.stat().st_size)
        return SupabasePublishResult(
            movie_code=canonical,
            language=AI_ENGLISH_LANGUAGE,
            storage_path=storage_path,
            movie_uuid=movie_uuid,
            subtitle_id=subtitle_id,
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = {**self.headers, **kwargs.pop("headers", {})}
        response = self.session.request(
            method,
            f"{self.supabase_url}{path}",
            headers=headers,
            timeout=self.timeout_seconds,
            **kwargs,
        )
        if not response.ok:
            raise RuntimeError(f"Supabase {method} {path} failed ({response.status_code}): {response.text}")
        if response.text:
            return response.json()
        return None

    def _upload_storage_object(self, storage_path: str, srt_path: Path) -> None:
        with srt_path.open("rb") as handle:
            self._request(
                "POST",
                f"/storage/v1/object/{self.bucket}/{storage_path}",
                headers={
                    "Content-Type": "text/plain; charset=utf-8",
                    "x-upsert": "true",
                    "cache-control": "no-cache",
                },
                data=handle.read(),
            )

    def _ensure_movie(self, canonical: str) -> str:
        rows = self._request(
            "GET",
            "/rest/v1/movies",
            params={"select": "id,movie_id", "standard_movie_id": f"eq.{canonical}", "limit": "1"},
        )
        if rows:
            return rows[0]["id"]
        series, number = parse_movie_code(canonical)
        inserted = self._request(
            "POST",
            "/rest/v1/movies",
            headers={"Prefer": "return=representation"},
            json={
                "series": series,
                "movie_number": number,
                "title": canonical.upper(),
            },
        )
        return inserted[0]["id"]

    def _upsert_language_row(
        self,
        movie_uuid: str,
        language: str,
        storage_path: str,
        file_size: int,
    ) -> str:
        rows = self._request(
            "GET",
            "/rest/v1/movie_languages",
            params={
                "select": "id",
                "movie_id": f"eq.{movie_uuid}",
                "language": f"eq.{language}",
                "limit": "1",
            },
        )
        payload = {
            "file_path": storage_path,
            "file_size": file_size,
            "subtitle_quality": "auto",
            "subtitle_source": "human",
            "is_premium": False,
        }
        if rows:
            subtitle_id = rows[0]["id"]
            updated = self._request(
                "PATCH",
                "/rest/v1/movie_languages",
                params={"id": f"eq.{subtitle_id}"},
                headers={"Prefer": "return=representation"},
                json=payload,
            )
            return updated[0]["id"]
        inserted = self._request(
            "POST",
            "/rest/v1/movie_languages",
            headers={"Prefer": "return=representation"},
            json={
                **payload,
                "movie_id": movie_uuid,
                "language": language,
            },
        )
        return inserted[0]["id"]
```

- [ ] **Step 4: Run publisher tests**

Run:

```bash
.venv/bin/python -m pytest tests/test_supabase_publisher.py -q
```

Expected: all tests pass.

## Task 3: Wire Publisher into API Completion

**Files:**
- Modify: `tests/test_api_publish.py`
- Modify: `orchestrator/api.py`
- Modify: `orchestrator/config.py`
- Modify: `orchestrator/__main__.py`

- [ ] **Step 1: Write failing API completion test**

Create `tests/test_api_publish.py`:

```python
from pathlib import Path

from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


class RecordingPublisher:
    def __init__(self):
        self.calls = []

    def publish_english_ai(self, movie_code, srt_path):
        self.calls.append((movie_code, Path(srt_path)))


def test_worker_complete_publishes_english_ai(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    result = store.submit_job("ktb-112", priority=100, force=False)
    job = result.job
    store.mark_downloading(job.id, JobStatus.DOWNLOADING_METADATA)
    store.mark_downloading(job.id, JobStatus.DOWNLOADING_AUDIO)
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    assert claimed is not None

    english_srt = mac_jobs_root / "ktb-112" / "ktb-112.English.srt"
    japanese_srt = mac_jobs_root / "ktb-112" / "ktb-112.Japanese.srt"
    english_srt.parent.mkdir(parents=True, exist_ok=True)
    english_srt.write_text("translated\n", encoding="utf-8")
    japanese_srt.write_text("japanese\n", encoding="utf-8")
    publisher = RecordingPublisher()
    app = create_app(store, publisher=publisher)
    client = TestClient(app)

    response = client.post(
        f"/worker/jobs/{job.id}/complete",
        json={
            "worker_id": "windows-gpu-1",
            "japanese_srt_path_windows": "M:\\ktb-112\\ktb-112.Japanese.srt",
            "english_srt_path_windows": "M:\\ktb-112\\ktb-112.English.srt",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "english_srt_ready"
    assert publisher.calls == [("ktb-112", english_srt)]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
.venv/bin/python -m pytest tests/test_api_publish.py -q
```

Expected: fail because `create_app()` does not accept `publisher`.

- [ ] **Step 3: Implement API publisher hook**

Modify `orchestrator/api.py`:

```python
def create_app(
    store: JobStore,
    *,
    worker_lease_seconds: int = 1800,
    max_worker_attempts: int = 3,
    final_file_exists: Callable[[str], bool] | None = None,
    publisher=None,
) -> FastAPI:
```

Inside `/worker/jobs/{job_id}/complete`, after `complete_worker_job(...)` returns:

```python
        if publisher is not None and job.english_srt_path_mac:
            publisher.publish_english_ai(job.normalized_movie_number, Path(job.english_srt_path_mac))
```

- [ ] **Step 4: Add settings and API wiring**

Modify `orchestrator/config.py` `MacSettings`:

```python
    publish_to_supabase: bool = Field(default=False, alias="PUBLISH_TO_SUPABASE")
    supabase_url: str | None = Field(default=None, alias="SUPABASE_URL")
    supabase_service_role_key: str | None = Field(default=None, alias="SUPABASE_SERVICE_ROLE_KEY")
    supabase_storage_bucket: str = Field(default="subtitles", alias="SUPABASE_STORAGE_BUCKET")
```

Modify `orchestrator/__main__.py` `run_api()`:

```python
    publisher = None
    if settings.publish_to_supabase:
        from orchestrator.supabase_publisher import SupabaseSubtitlePublisher

        if not settings.supabase_url or not settings.supabase_service_role_key:
            raise RuntimeError("PUBLISH_TO_SUPABASE requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        publisher = SupabaseSubtitlePublisher(
            settings.supabase_url,
            settings.supabase_service_role_key,
            bucket=settings.supabase_storage_bucket,
        )
```

Pass `publisher=publisher` to `create_app(...)`.

- [ ] **Step 5: Run API publish test**

Run:

```bash
.venv/bin/python -m pytest tests/test_api_publish.py -q
```

Expected: pass.

## Task 4: Add One-Shot Publish Command

**Files:**
- Modify: `orchestrator/__main__.py`
- Modify: `.env.example`

- [ ] **Step 1: Add CLI command**

Add:

```python
def run_publish_job(movie_code: str) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.supabase_publisher import SupabaseSubtitlePublisher, canonical_movie_code

    settings = MacSettings()
    if not settings.supabase_url or not settings.supabase_service_role_key:
        raise RuntimeError("publish-job requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
    canonical = canonical_movie_code(movie_code)
    srt_path = settings.jobs_root_mac / canonical / f"{canonical}.English.srt"
    publisher = SupabaseSubtitlePublisher(
        settings.supabase_url,
        settings.supabase_service_role_key,
        bucket=settings.supabase_storage_bucket,
    )
    result = publisher.publish_english_ai(canonical, srt_path)
    print(f"published {result.movie_code} {result.language} {result.storage_path} {result.subtitle_id}")
```

Add subcommand:

```python
    publish_parser = subcommands.add_parser("publish-job")
    publish_parser.add_argument("movie_code")
```

Dispatch:

```python
    elif args.command == "publish-job":
        run_publish_job(args.movie_code)
```

- [ ] **Step 2: Document env**

Append to `.env.example`:

```text
PUBLISH_TO_SUPABASE=false
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=replace-with-service-role-key
SUPABASE_STORAGE_BUCKET=subtitles
```

- [ ] **Step 3: Run focused tests**

Run:

```bash
.venv/bin/python -m pytest tests/test_supabase_publisher.py tests/test_api_publish.py -q
```

Expected: pass.

## Task 5: Verify and Publish `ktb-112`

**Files:**
- No code changes after this task unless verification exposes a bug.

- [ ] **Step 1: Run full tests**

Run:

```bash
.venv/bin/python -m pytest -q
```

Expected: all tests pass.

- [ ] **Step 2: Confirm env is configured locally without printing secrets**

Run:

```bash
.venv/bin/python - <<'PY'
from orchestrator.config import MacSettings
s = MacSettings()
print("publish_to_supabase", s.publish_to_supabase)
print("supabase_url_set", bool(s.supabase_url))
print("supabase_service_role_key_set", bool(s.supabase_service_role_key))
print("bucket", s.supabase_storage_bucket)
PY
```

Expected: URL and service key are set before attempting production upload.

- [ ] **Step 3: Publish completed `ktb-112`**

Run:

```bash
.venv/bin/python -m orchestrator publish-job ktb-112
```

Expected: prints `published ktb-112 English_AI ktb/ktb-112/ktb-112-English_AI.srt ...`.

- [ ] **Step 4: Verify production rows read-only**

Run a read-only Supabase query for `ktb-112`:

```sql
select m.movie_id, ml.language, ml.file_path, ml.file_size, ml.subtitle_source
from public.movies m
join public.movie_languages ml on ml.movie_id = m.id
where m.standard_movie_id = 'ktb-112'
order by ml.language;
```

Expected: includes `English_AI` with `ktb/ktb-112/ktb-112-English_AI.srt`.

## Self-Review Notes

- The plan uses the existing production differentiation pattern: `language='English_AI'`.
- It does not require a database migration because `English_AI` coexists with `English` under the current unique `(movie_id, language)` index.
- It keeps publishing on the Mac side, after Windows worker completion, so Windows remains focused on GPU transcription and translation.
- It uses existing `requests` dependency instead of adding `supabase-py`.
