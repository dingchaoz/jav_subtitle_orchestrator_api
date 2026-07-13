# Metadata-Resilient Subtitle Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish every quality-approved English SRT through a valid `public.movies` row even when full MissAV metadata is unavailable, without retranslation, premature Storage overwrite, or false ready states.

**Architecture:** Add a service-role-only, concurrency-safe Supabase RPC that bridges `missav.movies` into `public.movies` and falls back to a code-only movie row. Add a focused Python catalog client, move catalog resolution before Storage upload, and split Mac translation success from publication retry with a durable `publish_pending` state. Existing local `metadata.json` is optional enrichment input; its absence never blocks a quality-approved SRT from publication.

**Tech Stack:** Python 3.11, FastAPI, SQLite, requests, pytest, PostgreSQL, Supabase PostgREST/Storage, SQL migrations.

---

## Confirmed invariants

- `public.movie_languages.movie_id` references `public.movies.id`; `public.missav_movies` is only a view over `missav.movies`.
- A MissAV `has_subtitles=true` flag or an `-english-subtitle` movie variant is not proof that a usable SRT exists.
- A missing `public.movies` row must be repaired before any Storage upload or overwrite.
- Full metadata is best effort. The canonical movie code is sufficient to create a publishable placeholder.
- A placeholder uses parsed `series`, numeric `movie_number`, and `title=canonical_code`; all other optional metadata remains null.
- Quality failures remain permanent, keep `audio.wav`, quarantine the rejected English SRT, and never reach catalog or Storage calls.
- Publication failures do not consume translation attempts and do not invoke TranslateLocally again.
- No historical requeue, Storage overwrite, database migration, or production process restart occurs without an explicit execution/deployment approval.

## File structure

- Create `orchestrator/movie_code.py`: canonical movie-code parsing shared by catalog and publisher modules, preventing a circular import.
- Create `orchestrator/movie_catalog.py`: bounded metadata sanitization plus the Supabase `ensure_subtitle_movie` RPC client.
- Modify `orchestrator/supabase_publisher.py`: quality gate, catalog ensure, Storage upload, language-row upsert, and verification in safe order.
- Modify `orchestrator/models.py`: add `publish_pending` and publication/catalog observability fields.
- Modify `orchestrator/store.py`: migrate SQLite, claim publication work separately, and record publication success/failure without changing translation attempts.
- Modify `orchestrator/mac_worker.py`: finish translation at `publish_pending`; publish existing validated SRT on a separate claim path.
- Modify `orchestrator/config.py` and `orchestrator/__main__.py`: wire the catalog client and bounded publication retry settings.
- Modify `orchestrator/dashboard.py`: show catalog source/status, movie UUID, and publication attempts.
- Create `orchestrator/catalog_repair.py`: read-only historical catalog/publication repair planner.
- Modify `orchestrator/__main__.py`: expose only a dry-run catalog repair command initially.
- Create `supabase/migrations/<CLI-generated timestamp>_ensure_subtitle_movie.sql`: service-role-only idempotent bridge/placeholder RPC. The implementation must create this file with `supabase migration new ensure_subtitle_movie`; do not hand-invent the timestamp.
- Create/modify the focused test files named in each task.
- Modify `docs/setup/mac.md`: document state flow, placeholder semantics, dry-run command, and approval gates.

### Task 1: Freeze the catalog RPC contract with unit tests

**Files:**
- Create: `orchestrator/movie_code.py`
- Create: `tests/test_movie_catalog.py`
- Create: `orchestrator/movie_catalog.py`

- [ ] **Step 1: Write failing tests for metadata sanitization and RPC results**

```python
import json
from pathlib import Path

import pytest

from orchestrator.movie_catalog import (
    MovieCatalogResult,
    SupabaseMovieCatalogEnsurer,
    load_publish_metadata,
)
from orchestrator.movie_code import canonical_movie_code


class Response:
    ok = True
    status_code = 200

    def __init__(self, payload):
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class Session:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return Response(self.payload)


def test_load_publish_metadata_accepts_only_bounded_fields(tmp_path: Path):
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps({
        "number": "MIST-166",
        "title": "Example title",
        "release_date": "2024-06-03",
        "duration": "120 min",
        "description": "must not be forwarded",
        "unexpected": {"nested": "value"},
    }), encoding="utf-8")

    assert load_publish_metadata(path, "mist-166") == {
        "number": "mist-166",
        "title": "Example title",
        "release_date": "2024-06-03",
        "duration_minutes": 120,
    }


def test_missing_metadata_becomes_empty_rpc_metadata(tmp_path: Path):
    assert load_publish_metadata(tmp_path / "missing.json", "mist-166") == {}


def test_ensure_movie_returns_placeholder_as_success(tmp_path: Path):
    session = Session({
        "movie_uuid": "00000000-0000-0000-0000-000000000001",
        "canonical_code": "mist-166",
        "metadata_status": "placeholder",
        "metadata_source": "placeholder",
    })
    client = SupabaseMovieCatalogEnsurer(
        "https://example.supabase.co", "service-key", session=session
    )

    result = client.ensure_movie("mist-166", tmp_path / "missing.json")

    assert result == MovieCatalogResult(
        movie_uuid="00000000-0000-0000-0000-000000000001",
        canonical_code="mist-166",
        metadata_status="placeholder",
        metadata_source="placeholder",
    )
    assert session.calls[0][0] == "POST"
    assert session.calls[0][1].endswith("/rest/v1/rpc/ensure_subtitle_movie")
    assert session.calls[0][2]["json"] == {
        "p_movie_code": "mist-166",
        "p_local_metadata": {},
    }


@pytest.mark.parametrize("payload", [None, [], {}, {"movie_uuid": "bad"}])
def test_ensure_movie_rejects_malformed_rpc_response(payload, tmp_path: Path):
    client = SupabaseMovieCatalogEnsurer(
        "https://example.supabase.co", "service-key", session=Session(payload)
    )
    with pytest.raises(RuntimeError, match="catalog ensure returned invalid response"):
        client.ensure_movie("mist-166", tmp_path / "metadata.json")
```

- [ ] **Step 2: Run the tests and verify the new module is missing**

Run: `pytest tests/test_movie_catalog.py -q`

Expected: collection fails with `ModuleNotFoundError: orchestrator.movie_catalog`.

- [ ] **Step 3: Implement the bounded metadata loader and RPC client**

```python
# orchestrator/movie_code.py
import re


MOVIE_CODE_RE = re.compile(r"^([a-zA-Z]+)-?(\d+)$")


def canonical_movie_code(movie_code: str) -> str:
    match = MOVIE_CODE_RE.fullmatch(movie_code.strip())
    if match is None:
        raise ValueError(f"invalid movie code: {movie_code}")
    series, number = match.groups()
    return f"{series.lower()}-{int(number):03d}"
```

Move the existing `canonical_movie_code` implementation out of `supabase_publisher.py` into this module, and import it from both `movie_catalog.py` and `supabase_publisher.py`.

```python
# orchestrator/movie_catalog.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
import re
from typing import Any, Literal
from uuid import UUID

import requests

from orchestrator.movie_code import canonical_movie_code


MetadataStatus = Literal["complete", "partial", "placeholder"]
MetadataSource = Literal["public", "missav", "local", "placeholder"]


@dataclass(frozen=True)
class MovieCatalogResult:
    movie_uuid: str
    canonical_code: str
    metadata_status: MetadataStatus
    metadata_source: MetadataSource


def _duration_minutes(value: object) -> int | None:
    match = re.search(r"\d+", str(value or ""))
    if match is None:
        return None
    minutes = int(match.group())
    return minutes if 1 <= minutes <= 1440 else None


def load_publish_metadata(path: Path, movie_code: str) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    canonical = canonical_movie_code(movie_code)
    if canonical_movie_code(str(payload.get("number") or canonical)) != canonical:
        return {}
    result: dict[str, object] = {"number": canonical}
    title = str(payload.get("title") or "").strip()[:500]
    if title:
        result["title"] = title
    release_date = str(payload.get("release_date") or "").strip()
    try:
        parsed_date = date.fromisoformat(release_date)
    except ValueError:
        parsed_date = None
    if parsed_date is not None:
        result["release_date"] = parsed_date.isoformat()
    duration = _duration_minutes(payload.get("duration"))
    if duration is not None:
        result["duration_minutes"] = duration
    return result if len(result) > 1 else {}


class SupabaseMovieCatalogEnsurer:
    def __init__(self, url: str, service_key: str, *, timeout_seconds: int = 30, session=None):
        self.url = url.rstrip("/")
        self.service_key = service_key
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    def ensure_movie(self, movie_code: str, metadata_path: Path) -> MovieCatalogResult:
        canonical = canonical_movie_code(movie_code)
        response = self.session.request(
            "POST",
            f"{self.url}/rest/v1/rpc/ensure_subtitle_movie",
            headers={
                "apikey": self.service_key,
                "Authorization": f"Bearer {self.service_key}",
                "Content-Type": "application/json",
            },
            json={
                "p_movie_code": canonical,
                "p_local_metadata": load_publish_metadata(metadata_path, canonical),
            },
            timeout=self.timeout_seconds,
            allow_redirects=False,
        )
        if not response.ok:
            raise RuntimeError(f"catalog ensure failed ({response.status_code})")
        try:
            payload = response.json()
            movie_uuid = str(UUID(payload["movie_uuid"]))
            source = payload["metadata_source"]
            status = payload["metadata_status"]
            returned_code = payload["canonical_code"]
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise RuntimeError("catalog ensure returned invalid response") from exc
        if returned_code != canonical or source not in {"public", "missav", "local", "placeholder"}:
            raise RuntimeError("catalog ensure returned invalid response")
        if status not in {"complete", "partial", "placeholder"}:
            raise RuntimeError("catalog ensure returned invalid response")
        return MovieCatalogResult(movie_uuid, canonical, status, source)
```

- [ ] **Step 4: Run the focused tests**

Run: `pytest tests/test_movie_catalog.py -q`

Expected: all tests pass.

- [ ] **Step 5: Commit the contract**

```bash
git add orchestrator/movie_code.py orchestrator/movie_catalog.py tests/test_movie_catalog.py \
  orchestrator/supabase_publisher.py
git commit -m "feat: add subtitle movie catalog client"
```

### Task 2: Add the service-role-only bridge and placeholder RPC

**Files:**
- Create: `supabase/migrations/<CLI-generated timestamp>_ensure_subtitle_movie.sql`
- Create: `tests/test_ensure_subtitle_movie_migration.py`

- [ ] **Step 1: Fetch current Supabase guidance before writing SQL**

Run:

```bash
curl -fsSL https://supabase.com/changelog.md -o /tmp/supabase-changelog.md
rg -n "breaking-change|PostgREST|function|RLS|Storage" /tmp/supabase-changelog.md | head -n 50
supabase --version
supabase migration new ensure_subtitle_movie
```

Expected: the changelog fetch succeeds, the CLI prints its version, and exactly one new migration ending in `_ensure_subtitle_movie.sql` is created.

- [ ] **Step 2: Write a failing static security/contract test for the generated migration**

```python
from pathlib import Path


def migration_sql() -> str:
    files = list(Path("supabase/migrations").glob("*_ensure_subtitle_movie.sql"))
    assert len(files) == 1
    return files[0].read_text(encoding="utf-8").lower()


def test_ensure_subtitle_movie_rpc_is_service_role_only():
    sql = migration_sql()
    assert "create or replace function public.ensure_subtitle_movie" in sql
    assert "security definer" in sql
    assert "set search_path = ''" in sql
    assert "revoke execute on function public.ensure_subtitle_movie" in sql
    assert "from public, anon, authenticated" in sql
    assert "grant execute on function public.ensure_subtitle_movie" in sql
    assert "to service_role" in sql


def test_ensure_subtitle_movie_has_placeholder_and_conflict_paths():
    sql = migration_sql()
    assert "from missav.movies" in sql
    assert "on conflict (movie_id)" in sql
    assert "'placeholder'" in sql
    assert "p_local_metadata" in sql
```

- [ ] **Step 3: Run the static test and verify it fails on the empty migration**

Run: `pytest tests/test_ensure_subtitle_movie_migration.py -q`

Expected: failure because the generated migration does not yet contain the required function.

- [ ] **Step 4: Implement the RPC in the CLI-generated migration**

```sql
create or replace function public.ensure_subtitle_movie(
  p_movie_code text,
  p_local_metadata jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_code text := lower(btrim(p_movie_code));
  v_series text;
  v_number integer;
  v_existing public.movies%rowtype;
  v_missav record;
  v_movie public.movies%rowtype;
  v_title text;
  v_release_year integer;
  v_duration integer;
  v_source text;
  v_status text;
begin
  if v_code !~ '^[a-z]+-[0-9]+$' then
    raise exception 'invalid_movie_code' using errcode = '22023';
  end if;
  v_series := split_part(v_code, '-', 1);
  v_number := split_part(v_code, '-', 2)::integer;
  if v_number < 0 then
    raise exception 'invalid_movie_code' using errcode = '22023';
  end if;

  select * into v_existing
  from public.movies
  where standard_movie_id = v_code
  limit 1;

  select m.id, m.title, m.release_date, m.extra, mk.name as maker_name
  into v_missav
  from missav.movies m
  left join missav.makers mk on mk.id = m.maker_id
  where lower(m.number) = v_code
  order by m.published desc, m.release_date desc nulls last, m.id desc
  limit 1;

  v_title := nullif(btrim(coalesce(v_missav.title, p_local_metadata->>'title')), '');
  v_release_year := coalesce(
    extract(year from v_missav.release_date)::integer,
    extract(year from nullif(p_local_metadata->>'release_date', '')::date)::integer
  );
  v_duration := case
    when coalesce(p_local_metadata->>'duration_minutes', '') ~ '^[0-9]+$'
      then (p_local_metadata->>'duration_minutes')::integer
    else null
  end;
  v_source := case
    when v_missav.id is not null then 'missav'
    when nullif(p_local_metadata->>'title', '') is not null then 'local'
    when v_existing.id is not null and v_existing.title is distinct from v_code then 'public'
    else 'placeholder'
  end;
  v_status := case
    when v_source in ('missav', 'public') and v_title is not null then 'complete'
    when v_source = 'local' then 'partial'
    else 'placeholder'
  end;

  insert into public.movies (
    series, movie_number, title, studio, release_year, duration_minutes
  ) values (
    v_series,
    v_number,
    coalesce(v_title, v_code),
    nullif(v_missav.maker_name, ''),
    v_release_year,
    v_duration
  )
  on conflict (movie_id) do update
  set title = case
        when public.movies.title is null or public.movies.title = v_code
          then excluded.title
        else public.movies.title
      end,
      studio = coalesce(public.movies.studio, excluded.studio),
      release_year = coalesce(public.movies.release_year, excluded.release_year),
      duration_minutes = coalesce(public.movies.duration_minutes, excluded.duration_minutes),
      updated_at = now()
  returning * into v_movie;

  return jsonb_build_object(
    'movie_uuid', v_movie.id,
    'canonical_code', v_movie.standard_movie_id,
    'metadata_status', v_status,
    'metadata_source', v_source
  );
end;
$$;

revoke execute on function public.ensure_subtitle_movie(text, jsonb)
from public, anon, authenticated;
grant execute on function public.ensure_subtitle_movie(text, jsonb)
to service_role;
```

- [ ] **Step 5: Run static tests and lint the SQL locally**

Run:

```bash
pytest tests/test_ensure_subtitle_movie_migration.py -q
supabase db lint --help
```

Expected: pytest passes; the CLI help confirms the available lint invocation. Do not apply the migration remotely in this task.

- [ ] **Step 6: Commit the migration without deploying it**

```bash
git add supabase/migrations/*_ensure_subtitle_movie.sql tests/test_ensure_subtitle_movie_migration.py
git commit -m "feat: add idempotent subtitle movie catalog rpc"
```

### Task 3: Put catalog resolution before every Storage upload

**Files:**
- Modify: `orchestrator/supabase_publisher.py:82-215`
- Modify: `tests/test_supabase_publisher.py`

- [ ] **Step 1: Replace the fake movie GET with a recording catalog ensurer**

```python
from orchestrator.movie_catalog import MovieCatalogResult


class RecordingCatalogEnsurer:
    def __init__(self, events=None, error=None, source="missav"):
        self.events = events if events is not None else []
        self.error = error
        self.source = source

    def ensure_movie(self, movie_code, metadata_path):
        self.events.append(("ensure", movie_code, metadata_path))
        if self.error:
            raise self.error
        return MovieCatalogResult(
            movie_uuid="movie-uuid",
            canonical_code=movie_code,
            metadata_status="placeholder" if self.source == "placeholder" else "complete",
            metadata_source=self.source,
        )
```

- [ ] **Step 2: Add failing ordering and placeholder tests**

```python
def test_catalog_failure_cannot_reach_storage_upload(tmp_path):
    english = _write_pair(tmp_path)
    session = FakeSession()
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        catalog_ensurer=RecordingCatalogEnsurer(error=RuntimeError("catalog unavailable")),
    )

    with pytest.raises(RuntimeError, match="catalog unavailable"):
        publisher.publish_english_ai("ktb-112", english, tmp_path / "metadata.json")

    assert not any("/storage/v1/object/" in call[1] for call in session.calls)


def test_placeholder_movie_still_publishes_quality_approved_srt(tmp_path):
    english = _write_pair(tmp_path)
    events = []
    session = FakeSession(events=events)
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        catalog_ensurer=RecordingCatalogEnsurer(events, source="placeholder"),
    )

    result = publisher.publish_english_ai(
        "ktb-112", english, tmp_path / "missing-metadata.json"
    )

    assert result.metadata_status == "placeholder"
    assert result.metadata_source == "placeholder"
    assert [event[0] for event in events[:2]] == ["ensure", "upload"]
```

- [ ] **Step 3: Run the two tests and verify constructor/signature failures**

Run:

```bash
pytest tests/test_supabase_publisher.py::test_catalog_failure_cannot_reach_storage_upload \
       tests/test_supabase_publisher.py::test_placeholder_movie_still_publishes_quality_approved_srt -q
```

Expected: failures because `catalog_ensurer`, `metadata_path`, and result metadata fields are not implemented.

- [ ] **Step 4: Update the publisher contract and safe operation order**

```python
@dataclass(frozen=True)
class SupabasePublishResult:
    movie_code: str
    storage_path: str
    movie_uuid: str
    subtitle_id: str
    content_sha256: str
    file_size: int
    verified: bool
    metadata_status: str
    metadata_source: str


def publish_english_ai(
    self,
    movie_code: str,
    english_srt_path: Path,
    metadata_path: Path | None = None,
) -> SupabasePublishResult:
    canonical = canonical_movie_code(movie_code)
    japanese_srt_path = english_srt_path.with_name(f"{canonical}.Japanese.srt")
    report = validate_translation_quality(japanese_srt_path, english_srt_path)
    if not report.passed:
        raise RuntimeError("quality_gate_failed:" + ",".join(report.reason_codes))

    catalog = self.catalog_ensurer.ensure_movie(
        canonical,
        metadata_path or english_srt_path.with_name("metadata.json"),
    )
    subtitle_bytes = english_srt_path.read_bytes()
    content_sha256 = hashlib.sha256(subtitle_bytes).hexdigest()
    storage_path = build_ai_subtitle_storage_path(canonical)
    self._upload_storage_object(storage_path, subtitle_bytes)
    subtitle_id = self._upsert_language_row(
        catalog.movie_uuid, storage_path, len(subtitle_bytes)
    )
    self._verify_storage(storage_path, subtitle_bytes, content_sha256)
    self._verify_catalog(
        subtitle_id, catalog.movie_uuid, storage_path, len(subtitle_bytes)
    )
    return SupabasePublishResult(
        movie_code=canonical,
        storage_path=storage_path,
        movie_uuid=catalog.movie_uuid,
        subtitle_id=subtitle_id,
        content_sha256=content_sha256,
        file_size=len(subtitle_bytes),
        verified=True,
        metadata_status=catalog.metadata_status,
        metadata_source=catalog.metadata_source,
    )
```

Initialize `self.catalog_ensurer` from an injected object; when omitted, construct `SupabaseMovieCatalogEnsurer` with the same URL, key, timeout, and HTTP session. Delete `_find_movie()` so no code path can upload before catalog resolution.

- [ ] **Step 5: Update all existing publisher tests to pass `metadata_path` only where needed and run the file**

Run: `pytest tests/test_supabase_publisher.py -q`

Expected: all publisher tests pass, including bad-English upload prevention and repaired-subtitle Storage upsert.

- [ ] **Step 6: Commit the safe publisher order**

```bash
git add orchestrator/supabase_publisher.py tests/test_supabase_publisher.py
git commit -m "fix: ensure movie catalog before subtitle upload"
```

### Task 4: Add durable publication state without consuming translation attempts

**Files:**
- Modify: `orchestrator/models.py:43-55,195-225`
- Modify: `orchestrator/store.py:18-45,80-145,680-950,1100-1130`
- Modify: `tests/test_models.py`
- Modify: `tests/test_store_worker_claims.py`

- [ ] **Step 1: Add failing enum and SQLite migration assertions**

```python
def test_job_statuses_include_publication_boundary():
    assert JobStatus.PUBLISH_PENDING.value == "publish_pending"
    assert JobStatus.PUBLISHING.value == "publishing"


def test_store_migrates_publication_observability(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    with store.connection() as conn:
        columns = {row["name"] for row in conn.execute("pragma table_info(jobs)")}
    assert {
        "publish_attempt_count",
        "next_publish_attempt_at",
        "catalog_movie_uuid",
        "metadata_status",
        "metadata_source",
    } <= columns
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `pytest tests/test_models.py tests/test_store_worker_claims.py -q`

Expected: missing enum members/columns.

- [ ] **Step 3: Add enum, record fields, CREATE TABLE columns, and additive migrations**

```python
class JobStatus(StrEnum):
    QUEUED = "queued"
    DOWNLOADING_METADATA = "downloading_metadata"
    DOWNLOADING_AUDIO = "downloading_audio"
    AUDIO_READY = "audio_ready"
    TRANSCRIPTION_CLAIMED = "transcription_claimed"
    TRANSCRIBING = "transcribing"
    TRANSCRIPTION_DONE = "transcription_done"
    TRANSLATING = "translating"
    PUBLISH_PENDING = "publish_pending"
    PUBLISHING = "publishing"
    ENGLISH_SRT_READY = "english_srt_ready"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class JobRecord:
    id: str
    movie_number: str
    normalized_movie_number: str
    status: JobStatus
    priority: int
    attempt_count: int
    worker_attempt_count: int
    translation_attempt_count: int
    publish_attempt_count: int
    next_publish_attempt_at: str | None
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
    catalog_movie_uuid: str | None
    metadata_status: str | None
    metadata_source: str | None
```

Add the corresponding SQLite columns with `publish_attempt_count INTEGER NOT NULL DEFAULT 0` and nullable text columns. Extend every INSERT, SELECT-to-record conversion, force reset, and historical translation reset deliberately: historical translation reset clears publication metadata and sets `publish_attempt_count=0`; normal process retries preserve the validated English path.

- [ ] **Step 4: Add failing store transition tests**

```python
def _prepare_transcription_done(store, mac_jobs_root, movie="ktb-096"):
    job = store.submit_job(movie, priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=60)
    japanese = mac_jobs_root / movie / f"{movie}.Japanese.srt"
    japanese.parent.mkdir(parents=True, exist_ok=True)
    japanese.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\n日本語\n",
        encoding="utf-8",
    )
    return store.complete_worker_transcription(
        claimed.id,
        "windows-gpu-1",
        f"M:\\{movie}\\{movie}.Japanese.srt",
        lambda path: Path(path).exists(),
    )


def _prepare_publish_pending(store, mac_jobs_root):
    job = _prepare_transcription_done(store, mac_jobs_root)
    claimed = store.claim_translation_job(job.id, "mac-translation-1", 60)
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    english.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nEnglish\n",
        encoding="utf-8",
    )
    return store.complete_mac_translation_quality(
        claimed.id,
        "mac-translation-1",
        lambda path: Path(path).exists(),
    )


def test_quality_success_moves_to_publish_pending(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    translated_job = _prepare_transcription_done(store, mac_jobs_root)
    claimed = store.claim_translation_job(translated_job.id, "mac-translation-1", 60)
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    english.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nEnglish\n",
        encoding="utf-8",
    )
    pending = store.complete_mac_translation_quality(
        claimed.id,
        "mac-translation-1",
        lambda path: Path(path).exists(),
    )
    assert pending.status is JobStatus.PUBLISH_PENDING
    assert pending.translation_attempt_count == 0
    assert pending.english_srt_path_mac is not None


def test_publish_failure_returns_to_pending_without_translation_attempt(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending_job = _prepare_publish_pending(store, mac_jobs_root)
    claimed = store.claim_publication_job("mac-translation-1", 60)
    failed = store.fail_publication(
        claimed.id,
        "mac-translation-1",
        "Supabase unavailable",
        max_publish_attempts=10,
        retry_seconds=30,
    )
    assert failed.status is JobStatus.PUBLISH_PENDING
    assert failed.publish_attempt_count == 1
    assert failed.next_publish_attempt_at is not None
    assert failed.translation_attempt_count == 0
    assert failed.english_srt_path_mac is not None


def test_publish_success_is_the_only_path_to_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending_job = _prepare_publish_pending(store, mac_jobs_root)
    claimed = store.claim_publication_job("mac-translation-1", 60)
    ready = store.complete_publication(
        claimed.id,
        "mac-translation-1",
        movie_uuid="00000000-0000-0000-0000-000000000001",
        metadata_status="placeholder",
        metadata_source="placeholder",
    )
    assert ready.status is JobStatus.ENGLISH_SRT_READY
    assert ready.metadata_status == "placeholder"
```

- [ ] **Step 5: Implement the four atomic store methods**

Implement:

```python
complete_mac_translation_quality(job_id, worker_id, final_file_exists) -> JobRecord
claim_publication_job(worker_id, lease_seconds, *, job_id=None) -> JobRecord | None
fail_publication(
    job_id,
    worker_id,
    error,
    *,
    max_publish_attempts,
    retry_seconds,
) -> JobRecord
complete_publication(
    job_id,
    worker_id,
    *,
    movie_uuid,
    metadata_status,
    metadata_source,
) -> JobRecord
```

Each method must use `BEGIN IMMEDIATE`, compare the expected prior status and claimant in its `WHERE` clause, clear leases on completion/failure, and never update `translation_attempt_count` from a publication method. `fail_publication` increments only `publish_attempt_count`, schedules `next_publish_attempt_at=now+retry_seconds`, and keeps `publish_pending`; on the configured final attempt it moves to `failed` while preserving the validated English SRT. `claim_publication_job` filters out future `next_publish_attempt_at` values. Add `recover_expired_publication_leases(max_publish_attempts, retry_seconds)` with the same counter semantics.

- [ ] **Step 6: Run store/model tests**

Run: `pytest tests/test_models.py tests/test_store_worker_claims.py tests/test_store_submit.py -q`

Expected: all tests pass.

- [ ] **Step 7: Commit the state boundary**

```bash
git add orchestrator/models.py orchestrator/store.py tests/test_models.py \
  tests/test_store_worker_claims.py tests/test_store_submit.py
git commit -m "feat: persist subtitle publication state"
```

### Task 5: Make the Mac worker retry publication without retranslating

**Files:**
- Modify: `orchestrator/mac_worker.py:137-310`
- Modify: `tests/test_mac_worker.py:250-440`

- [ ] **Step 1: Rewrite the happy-path test around the two durable stages**

```python
class RecordingPublisher:
    def __init__(
        self,
        events=None,
        *,
        errors=None,
        metadata_status="complete",
        metadata_source="missav",
    ):
        self.events = events if events is not None else []
        self.errors = iter(errors or [])
        self.metadata_status = metadata_status
        self.metadata_source = metadata_source

    def publish_english_ai(self, movie, path, metadata_path):
        self.events.append(("publish", movie, path.name, metadata_path.name))
        error = next(self.errors, None)
        if error is not None:
            raise error
        return SimpleNamespace(
            movie_uuid="00000000-0000-0000-0000-000000000001",
            content_sha256="a" * 64,
            file_size=path.stat().st_size,
            verified=True,
            metadata_status=self.metadata_status,
            metadata_source=self.metadata_source,
        )


def test_good_translation_becomes_pending_then_publishes_without_retranslation(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
    )

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert [event[0] for event in events] == ["translate"]

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert [event[0] for event in events] == ["translate", "publish"]
```

- [ ] **Step 2: Add the required metadata-missing and transient retry tests**

```python
def test_placeholder_metadata_still_reaches_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(
            metadata_status="placeholder",
            metadata_source="placeholder",
        ),
    )

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_job(job.id).metadata_status == "placeholder"


def test_publish_retry_never_invokes_translator_again(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(
            events,
            errors=[RuntimeError("publish unavailable"), None],
        ),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert worker.process_one() is True
    assert [event[0] for event in events].count("translate") == 1
    assert store.get_job(job.id).translation_attempt_count == 0
    assert store.get_job(job.id).publish_attempt_count == 1
```

- [ ] **Step 3: Preserve the existing deterministic quality-failure assertions**

Keep and run the existing test proving:

```python
assert refreshed.status is JobStatus.FAILED
assert refreshed.error.startswith("translating: quality_gate_failed:")
assert not english.exists()
assert audio.read_bytes() == b"keep-audio"
assert publisher.events == []
```

- [ ] **Step 4: Run the new worker tests and verify the current one-stage worker fails**

Run: `pytest tests/test_mac_worker.py -q`

Expected: the new two-stage expectations fail before implementation.

- [ ] **Step 5: Implement publication-first polling and stage-specific handlers**

```python
def process_one(self) -> bool:
    if self.consecutive_quality_failures >= self.quality_failure_limit:
        raise MacTranslationUnhealthyError(
            "Mac translation worker stopped after "
            f"{self.consecutive_quality_failures} consecutive quality failures"
        )
    self.store.recover_expired_translation_leases(self.max_translation_attempts)
    self.store.recover_expired_publication_leases(
        self.max_publish_attempts,
        self.publish_retry_seconds,
    )
    publication = self.store.claim_publication_job(self.worker_id, self.lease_seconds)
    if publication is not None:
        return self._process_claimed_publication(publication)
    job = self.store.claim_next_translation_job(self.worker_id, self.lease_seconds)
    if job is None:
        self._record_idle()
        return False
    return self._process_claimed_translation(job)
```

Translation handler responsibilities:

```text
translate → validate pair → write sanitized quality.log → quarantine on failure
→ complete_mac_translation_quality → publish_pending
```

Publication handler responsibilities:

```text
revalidate pair → publisher.publish_english_ai(movie, English.srt, metadata.json)
→ verify result → complete_publication → english_srt_ready
```

On publication exceptions call only `fail_publication(job.id, self.worker_id, str(exc), max_publish_attempts=self.max_publish_attempts, retry_seconds=self.publish_retry_seconds)`. Add `max_publish_attempts` and `publish_retry_seconds` constructor arguments and wire them into both continuous and exact-job workers. Do not quarantine the validated English file, do not increment `consecutive_quality_failures`, do not increment translation attempts, and do not delete audio.

For `process_job_id(job_id)`, finish both stages for that exact job only: claim/translate it when it is `transcription_done`, then claim that same ID from `publish_pending` and publish it. Never claim a different pending publication from the exact-job path. Update `run_mac_translation_worker_once` tests so the command still exits only after the requested job reaches `english_srt_ready`.

- [ ] **Step 6: Run the complete worker test file**

Run: `pytest tests/test_mac_worker.py -q`

Expected: all tests pass, including the three-consecutive-quality-failure circuit breaker.

- [ ] **Step 7: Commit the two-stage worker**

```bash
git add orchestrator/mac_worker.py tests/test_mac_worker.py
git commit -m "feat: retry subtitle publication without retranslation"
```

### Task 6: Wire settings and the catalog client

**Files:**
- Modify: `orchestrator/config.py:35-80`
- Modify: `orchestrator/__main__.py:25-55,180-235`
- Modify: `tests/test_config_paths.py`
- Modify: `.env.example`

- [ ] **Step 1: Add failing configuration/factory assertions**

```python
def test_publisher_factory_wires_catalog_ensurer(tmp_path):
    settings = MacSettings(
        MAC_TRANSLATION_PUBLISH_ENABLED=True,
        SUPABASE_URL="https://example.supabase.co",
        SUPABASE_SERVICE_ROLE_KEY="service-key",
        SUPABASE_SUBTITLE_BUCKET="subtitles",
    )
    publisher = build_supabase_publisher(settings)
    assert isinstance(publisher.catalog_ensurer, SupabaseMovieCatalogEnsurer)


def test_publish_retry_configuration_defaults_are_bounded():
    settings = MacSettings(_env_file=None)
    assert settings.max_publish_attempts == 10
    assert settings.mac_publish_retry_seconds == 30
```

- [ ] **Step 2: Run the focused config tests and verify missing settings**

Run: `pytest tests/test_config_paths.py -q`

Expected: failures for missing settings/catalog client wiring.

- [ ] **Step 3: Add explicit settings and factory wiring**

```python
max_publish_attempts: int = Field(default=10, ge=1, alias="MAX_PUBLISH_ATTEMPTS")
mac_publish_retry_seconds: int = Field(
    default=30, ge=1, le=3600, alias="MAC_PUBLISH_RETRY_SECONDS"
)
```

Construct one `SupabaseMovieCatalogEnsurer` inside `build_supabase_publisher` and inject it into `SupabaseSubtitlePublisher`. Do not export or log the service-role key.

Pass `settings.max_publish_attempts` and `settings.mac_publish_retry_seconds` into every `MacTranslationWorker` constructor in `run_mac_translation_worker` and `run_mac_translation_worker_once`.

- [ ] **Step 4: Document non-secret environment keys**

```dotenv
MAX_PUBLISH_ATTEMPTS=10
MAC_PUBLISH_RETRY_SECONDS=30
```

- [ ] **Step 5: Run config and CLI construction tests**

Run: `pytest tests/test_config_paths.py -q`

Expected: all tests pass.

- [ ] **Step 6: Commit wiring**

```bash
git add orchestrator/config.py orchestrator/__main__.py tests/test_config_paths.py .env.example
git commit -m "feat: configure metadata resilient publication"
```

### Task 7: Expose catalog and publication status in the dashboard

**Files:**
- Modify: `orchestrator/models.py:190-235`
- Modify: `orchestrator/dashboard.py:20-60,83-112,1401-1435`
- Modify: `tests/test_dashboard_state.py`

- [ ] **Step 1: Add failing detail-model assertions**

```python
def test_job_detail_shows_publication_and_catalog_state(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    with store.connection() as conn:
        conn.execute(
            """
            update jobs
            set status = ?, publish_attempt_count = 1,
                catalog_movie_uuid = ?, metadata_status = ?, metadata_source = ?
            where id = ?
            """,
            (
                JobStatus.ENGLISH_SRT_READY.value,
                "00000000-0000-0000-0000-000000000001",
                "placeholder",
                "placeholder",
                job.id,
            ),
        )

    detail = build_job_detail(store.get_job(job.id))

    assert detail.publish_attempt_count == 1
    assert detail.next_publish_attempt_at is None
    assert detail.catalog_movie_uuid == "00000000-0000-0000-0000-000000000001"
    assert detail.metadata_status == "placeholder"
    assert detail.metadata_source == "placeholder"
```

- [ ] **Step 2: Run the test and verify missing response fields**

Run: `pytest tests/test_dashboard_state.py::test_job_detail_shows_publication_and_catalog_state -q`

Expected: failure because the detail model does not expose the fields.

- [ ] **Step 3: Add response fields and active-state classification**

```python
class JobDetailResponse(BaseModel):
    id: str
    movie_number: str
    normalized_movie_number: str
    status: JobStatus
    priority: int
    attempt_count: int
    worker_attempt_count: int
    translation_attempt_count: int
    publish_attempt_count: int
    next_publish_attempt_at: str | None = None
    catalog_movie_uuid: str | None = None
    metadata_status: str | None = None
    metadata_source: str | None = None
```

Add `PUBLISH_PENDING` and `PUBLISHING` to Mac/subtitle processing and active browser sets. Map all four new fields in `build_job_detail`.

- [ ] **Step 4: Add dashboard detail rows without metadata text**

```javascript
["Publish attempts", detail.publish_attempt_count],
["Next publish attempt", formatDate(detail.next_publish_attempt_at)],
["Catalog movie UUID", detail.catalog_movie_uuid],
["Metadata status", detail.metadata_status],
["Metadata source", detail.metadata_source],
```

Do not display title, description, performers, raw metadata payloads, service keys, or subtitle text.

- [ ] **Step 5: Run dashboard/API tests**

Run: `pytest tests/test_dashboard_state.py tests/test_api_dashboard.py -q`

Expected: all tests pass and `publish_pending` appears in active counts.

- [ ] **Step 6: Commit observability**

```bash
git add orchestrator/models.py orchestrator/dashboard.py tests/test_dashboard_state.py
git commit -m "feat: show publication metadata status in dashboard"
```

### Task 8: Add a dry-run-only historical catalog repair report

**Files:**
- Create: `orchestrator/catalog_repair.py`
- Create: `tests/test_catalog_repair.py`
- Modify: `orchestrator/__main__.py`

- [ ] **Step 1: Write failing planner tests**

```python
def test_catalog_repair_plan_is_read_only_and_allowlisted(store, monkeypatch):
    calls = []
    monkeypatch.setattr("requests.Session.request", lambda *a, **k: calls.append((a, k)))
    plans = plan_catalog_repairs(
        store,
        allowlist={"mist-166", "vec-777"},
        limit=1,
    )
    assert len(plans) == 1
    assert plans[0].movie_code in {"mist-166", "vec-777"}
    assert plans[0].action == "would_ensure_catalog_then_publish"
    assert calls == []


def test_catalog_repair_report_names_overwrite_without_authorizing_it(store):
    report = render_catalog_repair_report(plan_catalog_repairs(store, allowlist=None, limit=5))
    assert "DRY RUN" in report
    assert "would overwrite Storage" in report
    assert "force=True" not in report
```

- [ ] **Step 2: Run the tests and verify the module is missing**

Run: `pytest tests/test_catalog_repair.py -q`

Expected: collection fails with `ModuleNotFoundError`.

- [ ] **Step 3: Implement a filesystem/SQLite-only planner**

Define:

```python
@dataclass(frozen=True)
class CatalogRepairPlan:
    job_id: str
    movie_code: str
    current_status: str
    japanese_srt: str
    english_srt: str
    metadata_path: str | None
    metadata_available: bool
    action: str
    storage_effect: str
```

Eligibility must require existing Japanese and English SRT paths and a current quality pass. The planner may hash files and read `quality.log`, but it must not instantiate a publisher/catalog client, perform HTTP calls, mutate SQLite, move files, delete audio, requeue jobs, or use `force=True`.

- [ ] **Step 4: Add the dry-run CLI only**

```text
python -m orchestrator plan-catalog-repairs \
  --allowlist mist-166,vec-777 \
  --limit 5
```

The CLI output must include job ID, movie code, metadata available yes/no, expected catalog source (`missav/local/placeholder/unknown`), whether an existing Storage path would be overwritten, and the total affected count. It must not include subtitle text or secrets.

- [ ] **Step 5: Run planner and CLI tests**

Run: `pytest tests/test_catalog_repair.py tests/test_cli_historical_repair.py -q`

Expected: all tests pass and no network call is recorded.

- [ ] **Step 6: Commit the dry-run report**

```bash
git add orchestrator/catalog_repair.py orchestrator/__main__.py \
  tests/test_catalog_repair.py tests/test_cli_historical_repair.py
git commit -m "feat: add dry run catalog repair planner"
```

### Task 9: Document operations and verify locally

**Files:**
- Modify: `docs/setup/mac.md`
- Modify: `README.md`

- [ ] **Step 1: Document the exact state flow**

```text
transcription_done
→ translating
→ quality gate
→ publish_pending
→ publishing
→ ensure public.movies (MissAV/local metadata or code-only placeholder)
→ upload/upsert Storage
→ upsert and verify movie_languages
→ english_srt_ready
```

Document that metadata status `placeholder` is a successful publication outcome, not a translation failure.

- [ ] **Step 2: Document failure semantics**

```text
quality failure: failed/permanent; rejected/ English; audio kept; no catalog/Storage call
metadata unavailable: create code-only public.movies row; continue publishing
Supabase unavailable: publish_pending; validated English and audio kept; no retranslation
verification failure: publish_pending; never english_srt_ready
```

- [ ] **Step 3: Run the complete local test suite**

Run:

```bash
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Expected: exit code 0. Record the exact pass count and duration in the implementation handoff, not in source-controlled docs.

- [ ] **Step 4: Run the smoke test without starting the worker**

Run: `python -m orchestrator mac-translation-smoke-test`

Expected: exit 0, 10 English cues, unique ratio at least 0.5, and known_bad=0. Do not start a worker in this task.

- [ ] **Step 5: Run the dry-run report against an explicit canary allowlist**

Run:

```bash
python -m orchestrator plan-catalog-repairs \
  --allowlist iesp-744,mist-166,mist-268,roe-379,skmj-097,vec-777 \
  --limit 6
```

Expected: a `DRY RUN` report with zero mutations. Record the affected count. Do not requeue, upload, overwrite, or delete anything.

- [ ] **Step 6: Commit documentation**

```bash
git add docs/setup/mac.md README.md
git commit -m "docs: explain metadata resilient subtitle publication"
```

### Task 10: Production deployment and one-job canary — explicit approval gate

**Files:**
- No code files; this is a controlled deployment checklist.

- [ ] **Step 1: Stop at the approval gate**

Present:

```text
- branch/commit to deploy
- pytest result
- smoke result
- migration filename and SQL review summary
- dry-run affected count and exact canary movie code
- whether the canary would overwrite an existing Storage object
```

Expected: no production action until the user explicitly approves migration deployment and one canary publication.

- [ ] **Step 2: After approval, apply only the reviewed migration**

Use the connected Supabase migration tool or the repository's approved deployment workflow. Immediately verify with read-only SQL:

```sql
select has_function_privilege('anon', 'public.ensure_subtitle_movie(text,jsonb)', 'execute') as anon_execute,
       has_function_privilege('authenticated', 'public.ensure_subtitle_movie(text,jsonb)', 'execute') as authenticated_execute,
       has_function_privilege('service_role', 'public.ensure_subtitle_movie(text,jsonb)', 'execute') as service_execute;
```

Expected: `false, false, true`.

- [ ] **Step 3: Run database security/performance advisors**

Expected: report any new migration-related security/performance finding before proceeding. Do not dismiss unrelated existing findings as caused by this change.

- [ ] **Step 4: Restart only the Mac API and Mac translation worker after approval**

Run the documented process-manager commands for this host. Do not start an additional duplicate worker process.

- [ ] **Step 5: Process exactly one approved canary**

Verify this ordered evidence without logging subtitle text:

```text
quality.log passed=true
status publish_pending before publication
catalog UUID exists
metadata source/status recorded (placeholder is allowed)
Storage SHA-256 equals local English SRT SHA-256
movie_languages points to that UUID/path/size
status english_srt_ready only after both verifications
audio.wav unchanged if present
Japanese SRT unchanged
```

- [ ] **Step 6: Stop before historical repair**

Report the canary outcome and request separate approval for any batch. Never infer bulk authorization from approval of the migration or one canary.

## Self-review checklist

- Requirement coverage: Tasks 1–3 guarantee metadata-independent publication and safe upload ordering; Tasks 4–6 prevent retranslation; Task 7 exposes status; Task 8 supplies dry-run repair; Tasks 9–10 enforce verification and approvals.
- Placeholder behavior is explicit: canonical code creates `series`, numeric `movie_number`, code title, stable UUID, and permits SRT publication.
- Type consistency: RPC and Python both use `metadata_status` values `complete/partial/placeholder` and `metadata_source` values `public/missav/local/placeholder`.
- Retry consistency: translation failures affect `translation_attempt_count`; publication failures affect only `publish_attempt_count`.
- Security consistency: the `SECURITY DEFINER` RPC has an empty search path and execute is revoked from `public`, `anon`, and `authenticated`.
- No plan step authorizes bulk requeue, historical overwrite, audio deletion, or `force=True`.

