# Normal-First Historical Subtitle Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover `roe-291` as a full production canary, automatically synchronize every verified Supabase subtitle into javsubtitle.com D1/KV, and exhaust the approved historical allowlist through a bounded normal-first repair lane.

**Architecture:** Keep one TranslateLocally execution slot. Normal Windows-originated publication and translation claims run first; historical intent remains in a separate SQLite queue until the worker is otherwise idle. Split verified Supabase publication from website catalog synchronization so catalog retries never retranslate or re-upload, and make interrupted-audio recovery exact-job, hash-bound, and crash-resumable.

**Tech Stack:** Python 3.11, SQLite/WAL, Pydantic/FastAPI, requests, pytest, TranslateLocally, Supabase Storage/PostgREST, Cloudflare Worker D1/KV admin API, macOS launchd.

---

## File Map

- Create `orchestrator/audio_recovery.py`: exact staged-WAV validation/finalization.
- Create `orchestrator/catalog_sync.py`: javsubtitle.com catalog client.
- Create `orchestrator/historical_batch.py`: immutable batch plans and controller.
- Create `orchestrator/process_lock.py`: non-blocking single-instance locks.
- Modify `orchestrator/models.py`, `config.py`, `store.py`, `mac_worker.py`, and `__main__.py`.
- Modify `orchestrator/dashboard.py` and `api.py` for safe repair progress.
- Create two launchd plists and `scripts/install_mac_worker_launchd.sh`.
- Modify `.env.example`, `docs/setup/mac.md`, and `README.md`.
- Create focused tests for every new component and extend worker/store/dashboard tests.

## Task 1: Extend the durable state model

**Files:**
- Modify: `orchestrator/models.py`
- Modify: `orchestrator/store.py`
- Test: `tests/test_models.py`
- Test: `tests/test_store_worker_claims.py`

- [ ] **Step 1: Write failing enum and schema tests**

```python
def test_catalog_sync_statuses_are_explicit():
    assert JobStatus.CATALOG_SYNC_PENDING.value == "catalog_sync_pending"
    assert JobStatus.CATALOG_SYNCING.value == "catalog_syncing"


def test_initialize_adds_receipt_and_repair_queue(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\\\")
    store.initialize()
    with store.connection() as conn:
        jobs = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
        repairs = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(historical_translation_repairs)"
            )
        }
    assert {
        "translation_origin", "published_subtitle_id", "published_storage_path",
        "published_content_sha256", "published_file_size",
        "catalog_sync_attempt_count", "next_catalog_sync_attempt_at",
    } <= jobs
    assert {
        "id", "batch_id", "job_id", "movie_code", "allowlist_sha256",
        "state", "attempt_count", "next_attempt_at", "reason_code",
        "japanese_sha256", "audio_sha256", "english_sha256",
        "created_at", "updated_at",
    } <= repairs
```

- [ ] **Step 2: Run the tests and confirm the red state**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_models.py tests/test_store_worker_claims.py -q
```

Expected: failure because the enum members/table/columns do not exist.

- [ ] **Step 3: Add statuses, records, and forward-only initialization**

Add to `JobStatus`:

```python
CATALOG_SYNC_PENDING = "catalog_sync_pending"
CATALOG_SYNCING = "catalog_syncing"
```

Add to `orchestrator/store.py`:

```python
from enum import StrEnum


class HistoricalRepairState(StrEnum):
    PLANNED = "planned"
    PENDING = "pending"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    PERMANENT_FAILED = "permanent_failed"
    PAUSED = "paused"


@dataclass(frozen=True)
class HistoricalRepairRecord:
    id: str
    batch_id: str
    job_id: str
    movie_code: str
    allowlist_sha256: str
    state: HistoricalRepairState
    attempt_count: int
    next_attempt_at: str | None
    reason_code: str | None
    japanese_sha256: str
    audio_sha256: str | None
    english_sha256: str | None
    created_at: str
    updated_at: str
```

Use the existing `PRAGMA table_info` migration pattern:

```python
catalog_columns = {
    "translation_origin": "TEXT NOT NULL DEFAULT 'normal'",
    "published_subtitle_id": "TEXT",
    "published_storage_path": "TEXT",
    "published_content_sha256": "TEXT",
    "published_file_size": "INTEGER",
    "catalog_sync_attempt_count": "INTEGER NOT NULL DEFAULT 0",
    "next_catalog_sync_attempt_at": "TEXT",
}
for column, definition in catalog_columns.items():
    if column not in columns:
        conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")

conn.execute("""
CREATE TABLE IF NOT EXISTS historical_translation_repairs (
  id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
  movie_code TEXT NOT NULL,
  allowlist_sha256 TEXT NOT NULL,
  state TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  reason_code TEXT,
  japanese_sha256 TEXT NOT NULL,
  audio_sha256 TEXT,
  english_sha256 TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
)
""")
conn.execute("""
CREATE INDEX IF NOT EXISTS idx_historical_repairs_state_created
ON historical_translation_repairs(state, created_at)
""")
```

Extend `JobRecord` and `_row_to_job` with all seven job columns.

- [ ] **Step 4: Run focused and full tests**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_models.py tests/test_store_worker_claims.py -q
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest -q
```

Expected: focused tests pass; full suite remains at least 420 passing tests.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/models.py orchestrator/store.py \
  tests/test_models.py tests/test_store_worker_claims.py
git commit -m "feat: add catalog and repair states"
```

## Task 2: Implement exact interrupted-audio recovery

**Files:**
- Create: `orchestrator/audio_recovery.py`
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/__main__.py`
- Create: `tests/test_audio_recovery.py`

- [ ] **Step 1: Write safety and crash-resume tests**

```python
def test_recover_exact_staged_audio_moves_and_marks_ready(store, staged_wav):
    expected = sha256(staged_wav.read_bytes()).hexdigest()
    receipt = recover_interrupted_audio(
        store, job_id="job_exact", movie="roe-291",
        expected_sha256=expected,
    )
    assert receipt.status == "audio_ready"
    assert receipt.sha256 == expected
    assert receipt.reused_final is False
    assert receipt.final_path.name == "audio.wav"
    assert not staged_wav.exists()


@pytest.mark.parametrize("mutation", ["wrong_hash", "symlink", "partial", "wrong_job"])
def test_recovery_rejects_unsafe_input_without_state_change(
    store, staged_wav, mutation
):
    before = snapshot_job_and_files(store, "job_exact")
    with pytest.raises(AudioRecoveryError):
        attempt_unsafe_recovery(store, staged_wav, mutation)
    assert snapshot_job_and_files(store, "job_exact") == before


def test_recovery_resumes_after_move_before_database_commit(store, final_wav):
    receipt = recover_interrupted_audio(
        store, job_id="job_exact", movie="roe-291",
        expected_sha256=sha256(final_wav.read_bytes()).hexdigest(),
    )
    assert receipt.reused_final is True
    assert store.get_job("job_exact").status is JobStatus.AUDIO_READY
```

- [ ] **Step 2: Run the missing-module failure**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_audio_recovery.py -q
```

Expected: collection fails because `orchestrator.audio_recovery` is absent.

- [ ] **Step 3: Implement exact recovery**

Create these public types:

```python
@dataclass(frozen=True)
class AudioRecoveryReceipt:
    job_id: str
    movie_code: str
    status: str
    final_path: Path
    sha256: str
    size_bytes: int
    duration_seconds: float
    reused_final: bool


class AudioRecoveryError(RuntimeError):
    pass
```

Implement:

```python
def recover_interrupted_audio(
    store: JobStore, *, job_id: str, movie: str, expected_sha256: str
) -> AudioRecoveryReceipt:
    job = store.get_job(job_id)
    canonical = canonical_movie_code(movie)
    if job is None or canonical_movie_code(job.normalized_movie_number) != canonical:
        raise AudioRecoveryError("audio_recovery_job_mismatch")
    if job.status is not JobStatus.DOWNLOADING_AUDIO:
        raise AudioRecoveryError("audio_recovery_status_changed")
    paths = build_job_paths(canonical, store.jobs_root_mac, store.jobs_root_windows)
    staged = paths.job_dir_mac / "audio" / f"{canonical}.wav"
    source = paths.audio_path_mac if paths.audio_path_mac.exists() else staged
    reused_final = source == paths.audio_path_mac
    probe = validate_pcm_wav(source, paths.job_dir_mac, expected_sha256)
    if not reused_final:
        os.replace(source, paths.audio_path_mac)
        probe = validate_pcm_wav(
            paths.audio_path_mac, paths.job_dir_mac, expected_sha256
        )
    updated = store.finalize_interrupted_audio(
        job_id,
        expected_status=JobStatus.DOWNLOADING_AUDIO,
        metadata_path=str(paths.metadata_path_mac),
        audio_path=str(paths.audio_path_mac),
        audio_path_windows=paths.audio_path_windows,
    )
    return AudioRecoveryReceipt(
        updated.id, canonical, updated.status.value, paths.audio_path_mac,
        probe.sha256, probe.size_bytes, probe.duration_seconds, reused_final,
    )
```

`validate_pcm_wav` opens with `O_NOFOLLOW`, requires the exact final or adapter
staged path, hashes the same descriptor snapshot, and requires uncompressed PCM,
16 kHz, mono, 16-bit samples, positive frames, and file size consistent with frames.

Add a compare-and-set `finalize_interrupted_audio` from `downloading_audio` to
`audio_ready`. Add CLI:

```python
recover = subcommands.add_parser("recover-interrupted-audio")
recover.add_argument("--job-id", required=True)
recover.add_argument("--movie", required=True)
recover.add_argument("--expected-sha256", required=True)
```

Print job/movie/status/hash/size/duration/reused-final only.

- [ ] **Step 4: Verify tests and CLI contract**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_audio_recovery.py -q
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/python \
  -m orchestrator recover-interrupted-audio --help
```

Expected: tests pass; help has no force or batch option.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/audio_recovery.py orchestrator/store.py \
  orchestrator/__main__.py tests/test_audio_recovery.py
git commit -m "feat: recover exact staged audio"
```

## Task 3: Add the safe website catalog-sync client

**Files:**
- Create: `orchestrator/catalog_sync.py`
- Modify: `orchestrator/config.py`
- Modify: `.env.example`
- Create: `tests/test_catalog_sync.py`
- Modify: `tests/test_config_paths.py`

- [ ] **Step 1: Write request, response, and secret-safety tests**

```python
def test_sync_exact_code_uses_bounded_payload(fake_session):
    fake_session.response(200, {
        "success": True, "requested": 1, "synced": 1, "failed": [],
        "results": [{
            "canonicalCode": "roe-291", "d1RowsUpdated": 1,
            "subtitleCount": 1,
            "kvKeysDeleted": [
                "movie:full:roe-291", "movie:light:roe-291"
            ],
            "dryRun": False,
        }],
    })
    result = CatalogSyncClient(
        "https://javsubtitle.com", "secret", session=fake_session
    ).sync("roe-291")
    assert result.canonical_code == "roe-291"
    request = fake_session.requests[0]
    assert request.json["canonicalCodes"] == ["roe-291"]
    assert request.json["dryRun"] is False
    assert request.allow_redirects is False


@pytest.mark.parametrize("status,reason", [
    (401, "catalog_auth_failed"),
    (302, "catalog_redirect_rejected"),
    (500, "catalog_sync_failed"),
])
def test_sync_fails_closed_without_leaking_token(status, reason):
    with pytest.raises(CatalogSyncError) as error:
        sync_with_response(status, token="never-log-this")
    assert error.value.reason_code == reason
    assert "never-log-this" not in str(error.value)
```

- [ ] **Step 2: Run and confirm missing client/config**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_catalog_sync.py tests/test_config_paths.py -q
```

Expected: failure because client/config fields are absent.

- [ ] **Step 3: Implement exact response validation**

Create:

```python
@dataclass(frozen=True)
class CatalogSyncResult:
    canonical_code: str
    d1_rows_updated: int
    subtitle_count: int
    kv_keys_deleted: tuple[str, ...]


class CatalogSyncError(RuntimeError):
    def __init__(self, reason_code: str):
        self.reason_code = reason_code
        super().__init__(reason_code)


class CatalogSyncClient:
    def __init__(self, base_url: str, admin_token: str, *,
                 timeout_seconds: int = 30, session=None):
        self.endpoint = (
            base_url.rstrip("/") + "/api/admin/catalog/sync-subtitles"
        )
        self.admin_token = admin_token
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    def sync(self, movie_code: str) -> CatalogSyncResult:
        canonical = canonical_movie_code(movie_code)
        response = self.session.post(
            self.endpoint,
            headers={
                "Authorization": f"Bearer {self.admin_token}",
                "Content-Type": "application/json",
            },
            json={
                "canonicalCodes": [canonical],
                "reason": "subtitle_ingest",
                "source": "jav-subtitle-orchestrator",
                "dryRun": False,
            },
            timeout=self.timeout_seconds,
            allow_redirects=False,
        )
        if 300 <= response.status_code < 400:
            raise CatalogSyncError("catalog_redirect_rejected")
        if response.status_code in {401, 403}:
            raise CatalogSyncError("catalog_auth_failed")
        if response.status_code != 200:
            raise CatalogSyncError("catalog_sync_failed")
        try:
            body = response.json()
        except ValueError as exc:
            raise CatalogSyncError("catalog_response_invalid") from exc
        if not isinstance(body, dict):
            raise CatalogSyncError("catalog_response_invalid")
        results = body.get("results")
        if (
            body.get("success") is not True
            or body.get("synced") != 1
            or body.get("failed") != []
            or not isinstance(results, list)
            or len(results) != 1
            or results[0].get("canonicalCode") != canonical
            or results[0].get("dryRun") is not False
        ):
            raise CatalogSyncError("catalog_response_mismatch")
        row = results[0]
        expected_keys = {
            f"movie:full:{canonical}", f"movie:light:{canonical}"
        }
        if (
            not isinstance(row.get("d1RowsUpdated"), int)
            or row["d1RowsUpdated"] < 1
            or not isinstance(row.get("subtitleCount"), int)
            or row["subtitleCount"] < 1
            or set(row.get("kvKeysDeleted", [])) != expected_keys
        ):
            raise CatalogSyncError("catalog_response_mismatch")
        return CatalogSyncResult(
            canonical, int(row["d1RowsUpdated"]), int(row["subtitleCount"]),
            tuple(row["kvKeysDeleted"]),
        )
```

Add settings:

```python
javsubtitle_api_base: str | None = Field(
    default=None, alias="JAVSUBTITLE_API_BASE"
)
javsubtitle_admin_api_token: str | None = Field(
    default=None, alias="JAVSUBTITLE_ADMIN_API_TOKEN"
)
catalog_sync_retry_seconds: int = Field(
    default=30, ge=1, le=3600, alias="CATALOG_SYNC_RETRY_SECONDS"
)
max_catalog_sync_attempts: int = Field(
    default=10, ge=1, alias="MAX_CATALOG_SYNC_ATTEMPTS"
)
```

Document names only in `.env.example`; never commit a token.

- [ ] **Step 4: Verify focused tests**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_catalog_sync.py tests/test_config_paths.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/catalog_sync.py orchestrator/config.py .env.example \
  tests/test_catalog_sync.py tests/test_config_paths.py
git commit -m "feat: add website catalog sync client"
```

## Task 4: Split Supabase verification from D1/KV readiness

**Files:**
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/mac_worker.py`
- Modify: `orchestrator/__main__.py`
- Modify: `tests/test_mac_worker.py`
- Modify: `tests/test_store_worker_claims.py`

- [ ] **Step 1: Write transition and no-repeat tests**

```python
def test_verified_supabase_moves_to_catalog_pending_not_ready(harness):
    harness.worker.process_one()
    refreshed = harness.store.get_job(harness.job.id)
    assert refreshed.status is JobStatus.CATALOG_SYNC_PENDING
    assert refreshed.published_subtitle_id == harness.publisher.result.subtitle_id


def test_catalog_failure_retries_without_retranslation_or_reupload(harness):
    harness.catalog.fail("catalog_sync_failed")
    harness.worker.process_one()
    harness.worker.process_one()
    assert harness.translator.calls == 1
    assert harness.publisher.calls == 1
    assert harness.store.get_job(harness.job.id).status is JobStatus.CATALOG_SYNC_PENDING


def test_ready_only_after_exact_catalog_sync_success(harness):
    harness.worker.process_one()
    harness.worker.process_one()
    job = harness.store.get_job(harness.job.id)
    assert job.status is JobStatus.ENGLISH_SRT_READY
    assert harness.catalog.codes == [job.normalized_movie_number]
```

- [ ] **Step 2: Run and confirm current code marks ready too early**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_mac_worker.py tests/test_store_worker_claims.py -q
```

Expected: new assertions fail.

- [ ] **Step 3: Add receipt and catalog-sync transitions**

Replace the final publisher transition with:

```python
store.complete_supabase_publication(
    job.id, worker_id,
    movie_uuid=published.movie_uuid,
    metadata_status=published.metadata_status,
    metadata_source=published.metadata_source,
    subtitle_id=published.subtitle_id,
    storage_path=published.storage_path,
    content_sha256=published.content_sha256,
    file_size=published.file_size,
)
```

This compare-and-set changes `publishing` to `catalog_sync_pending`. Add
`claim_catalog_sync_job`, `fail_catalog_sync`, `complete_catalog_sync`, and
`recover_expired_catalog_sync_leases`. Catalog failure changes only
`catalog_sync_attempt_count` and `next_catalog_sync_attempt_at`.

Construct `CatalogSyncClient` only when publication is enabled; refuse worker
startup if publication is enabled but catalog URL/token are absent. Process a due
catalog-sync job before claiming unrelated work. On success only,
`complete_catalog_sync` assigns `english_srt_ready`.

- [ ] **Step 4: Verify quality, overwrite, and retry regressions**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_mac_worker.py tests/test_store_worker_claims.py \
  tests/test_supabase_publisher.py tests/test_subtitle_quality.py -q
```

Expected: bad English reaches neither Supabase nor catalog client; repaired English
retains verified same-path `x-upsert: true`; catalog retry does not re-upload.
The implementation must remain consistent with Supabase's official
[standard upload overwrite behavior](https://supabase.com/docs/guides/storage/uploads/standard-uploads)
and server-side service-role use; no public Storage policy is added.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/store.py orchestrator/mac_worker.py orchestrator/__main__.py \
  tests/test_mac_worker.py tests/test_store_worker_claims.py
git commit -m "feat: require website sync before ready"
```


## Task 5: Add immutable historical batch planning and enqueue

**Files:**
- Create: `orchestrator/historical_batch.py`
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/__main__.py`
- Create: `tests/test_historical_batch.py`

- [ ] **Step 1: Write dry-run, digest, and allowlist tests**

```python
def test_plan_is_read_only_bounded_and_digest_stable(store, allowlist, tmp_path):
    before = snapshot_database_and_files(store)
    plan = plan_historical_batch(store, allowlist, limit=5)
    write_private_plan(tmp_path / "plan.json", plan)
    assert len(plan.items) <= 5
    assert plan.allowlist_sha256 == sha256(allowlist.read_bytes()).hexdigest()
    assert plan.plan_sha256 == plan.recalculate_sha256()
    assert snapshot_database_and_files(store) == before


def test_enqueue_rejects_changed_plan_without_writes(store, plan):
    before = snapshot_database(store)
    with pytest.raises(ValueError, match="historical_plan_changed"):
        enqueue_historical_batch(
            store, changed_plan(plan), plan.allowlist_path,
            confirm_plan_sha256=plan.plan_sha256,
        )
    assert snapshot_database(store) == before


def test_enqueue_never_selects_outside_allowlist_or_limit(store, allowlist):
    plan = plan_historical_batch(store, allowlist, limit=5)
    records = enqueue_historical_batch(
        store, plan, allowlist, confirm_plan_sha256=plan.plan_sha256
    )
    assert len(records) <= 5
    assert {
        record.movie_code for record in records
    } <= load_repair_allowlist(allowlist)
    assert all(
        store.get_job(record.job_id).status is not JobStatus.TRANSCRIPTION_DONE
        for record in records
    )
```

- [ ] **Step 2: Run and confirm the missing batch module**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_historical_batch.py -q
```

Expected: collection fails for `orchestrator.historical_batch`.

- [ ] **Step 3: Implement immutable plans and pending repair records**

Define frozen `HistoricalBatchItem` and `HistoricalBatchPlan` dataclasses. Hash
canonical JSON produced by:

```python
def canonical_plan_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
```

Implement:

```python
def plan_historical_batch(
    store: JobStore, allowlist_path: Path, *, limit: int
) -> HistoricalBatchPlan:
    if not 1 <= limit <= 20:
        raise ValueError("historical batch limit must be between 1 and 20")
    allowlist = load_repair_allowlist(allowlist_path)
    candidates = select_all_eligible_historical_repairs(store, allowlist)
    return HistoricalBatchPlan.build(allowlist_path, candidates[:limit])


def enqueue_historical_batch(
    store: JobStore,
    plan: HistoricalBatchPlan,
    allowlist_path: Path,
    *,
    confirm_plan_sha256: str,
) -> list[HistoricalRepairRecord]:
    plan.verify_unchanged(store, allowlist_path, confirm_plan_sha256)
    return store.enqueue_historical_repairs(plan)
```

`enqueue_historical_repairs` uses one `BEGIN IMMEDIATE`, rechecks every job and
snapshot, inserts records as `pending`, and changes neither job status nor files.
The plan scans the full allowlist and includes `eligible_total`, `already_repaired`,
`ineligible`, and `blocked` counts, while serializing at most `limit` actionable
items. A five-item plan therefore still reports the authoritative full count.

Add CLIs:

```text
plan-historical-repair-batch --allowlist-file PATH --limit 1..20 --output PATH
enqueue-historical-repair-batch --allowlist-file PATH --plan-file PATH
  --confirm-plan-sha256 SHA
```

Write plan files with mode 0600. Outputs contain identifiers, counts, and digests
only. Neither parser contains force, delete, upload, overwrite, or an unrestricted
selector.

- [ ] **Step 4: Verify batch and legacy canary tests**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_historical_batch.py tests/test_historical_repair.py -q
```

Expected: all pass and the existing exact one-job canary remains compatible.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/historical_batch.py orchestrator/store.py \
  orchestrator/__main__.py tests/test_historical_batch.py
git commit -m "feat: enqueue bounded historical repairs"
```

## Task 6: Implement normal-first historical scheduling

**Files:**
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/mac_worker.py`
- Modify: `tests/test_mac_worker.py`
- Modify: `tests/test_store_worker_claims.py`

- [ ] **Step 1: Write ordering and failure-isolation tests**

```python
def test_normal_translation_wins_over_pending_historical(harness):
    normal = harness.add_normal_transcription_done("new-001")
    repair = harness.enqueue_historical("old-001")
    harness.worker.process_one()
    assert harness.translator.job_ids == [normal.id]
    assert harness.store.get_repair(repair.id).state is HistoricalRepairState.PENDING


def test_historical_runs_only_when_normal_backlog_is_empty(harness):
    repair = harness.enqueue_historical("old-001")
    harness.worker.process_one()
    assert harness.translator.job_ids == [repair.job_id]
    assert harness.store.get_repair(repair.id).state is HistoricalRepairState.RUNNING


def test_three_historical_quality_failures_leave_normal_lane_running(harness):
    for code in ("old-001", "old-002", "old-003"):
        harness.enqueue_bad_historical(code)
        harness.worker.process_one()
    normal = harness.add_normal_transcription_done("new-001")
    harness.worker.process_one()
    assert harness.translator.job_ids[-1] == normal.id
    assert harness.worker.historical_quality_failures == 3
    assert (
        harness.store.historical_lane_state().reason_code
        == "quality_failure_limit"
    )


def test_bad_historical_english_is_quarantined_without_publication(harness):
    repair = harness.enqueue_bad_historical("old-001")
    japanese_before = harness.sha256("old-001", "Japanese")
    audio_before = harness.sha256_optional("old-001", "audio.wav")
    harness.worker.process_one()
    assert harness.publisher.calls == 0
    assert harness.catalog.calls == 0
    assert harness.sha256("old-001", "Japanese") == japanese_before
    assert harness.sha256_optional("old-001", "audio.wav") == audio_before
    assert harness.rejected_files("old-001")
    assert (
        harness.store.get_repair(repair.id).state
        is HistoricalRepairState.PERMANENT_FAILED
    )
```

- [ ] **Step 2: Run and observe current oldest-job behavior**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_mac_worker.py tests/test_store_worker_claims.py -q
```

Expected: ordering tests fail because the current worker has no repair lane.

- [ ] **Step 3: Add atomic historical activation and origin-aware claims**

Add store methods with these exact contracts:

```python
claim_next_translation_job(worker_id, lease_seconds, *, origin="normal")
claim_next_historical_repair(worker_id, lease_seconds)
claim_inflight_historical_stage(worker_id, lease_seconds)
mark_historical_retry(job_id, reason_code, retry_seconds)
mark_historical_permanent_failure(job_id, reason_code, english_sha256=None)
mark_historical_success(job_id, english_sha256)
pause_historical_lane(reason_code)
resume_historical_lane()
has_claimable_normal_work()
```

`claim_next_historical_repair` uses one `BEGIN IMMEDIATE` to select one due
`pending/retry_wait` record, recheck the job, mark the repair `running`, set
`translation_origin='historical'`, reset only translation/publication/catalog
counters, and claim the exact job as `translating`. It never exposes an unclaimed
historical `transcription_done` row.

`claim_inflight_historical_stage` is restricted to a historical job already in
`publish_pending` or `catalog_sync_pending`; it must not claim a historical
translation retry ahead of normal work.

Refactor `process_one` into explicit status dispatch:

```python
self._recover_leases()
if job := self.store.claim_inflight_historical_stage(
    self.worker_id, self.lease_seconds
):
    return self._process_claimed_stage(job)
if job := self.store.claim_normal_catalog_or_publication(
    self.worker_id, self.lease_seconds
):
    return self._process_claimed_stage(job)
if job := self.store.claim_next_translation_job(
    self.worker_id, self.lease_seconds, origin="normal"
):
    return self._process_claimed_translation(job)
if self.historical_quality_failures < self.quality_failure_limit:
    if job := self.store.claim_next_historical_repair(
        self.worker_id, self.lease_seconds
    ):
        return self._process_claimed_translation(job)
self._record_idle()
return False
```

An in-flight historical unit may finish publication/catalog sync. After it reaches a
terminal/retry-wait state, a normal job wins before another repair. Historical
deterministic failures update the repair counter and pause at three; normal failure
behavior remains unchanged. Reuse the existing collision-safe `rejected/` quarantine;
compare Japanese/audio snapshots before marking either historical success or failure.

- [ ] **Step 4: Verify all scheduling regressions**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_mac_worker.py tests/test_store_worker_claims.py \
  tests/test_historical_batch.py tests/test_subtitle_quality.py -q
```

Expected: normal-first and failure-isolation tests pass.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/store.py orchestrator/mac_worker.py \
  tests/test_mac_worker.py tests/test_store_worker_claims.py
git commit -m "feat: schedule normal work before repairs"
```

## Task 7: Add the bounded historical controller

**Files:**
- Modify: `orchestrator/historical_batch.py`
- Modify: `orchestrator/__main__.py`
- Modify: `tests/test_historical_batch.py`

- [ ] **Step 1: Write controller decision tests**

```python
def test_controller_enqueues_five_then_twenty(store, allowlist):
    controller = HistoricalRepairController(
        store, allowlist, initial_batch_size=5, batch_size=20
    )
    first = controller.run_once()
    assert first.enqueued == 5
    complete_batch(store, first.batch_id)
    second = controller.run_once()
    assert 1 <= second.enqueued <= 20


@pytest.mark.parametrize("blocker", [
    "quality_failure_limit", "catalog_auth_failed",
    "supabase_verification_failed", "preservation_hash_changed",
])
def test_controller_pauses_without_enqueue(store, allowlist, blocker):
    arrange_blocker(store, blocker)
    result = HistoricalRepairController(store, allowlist).run_once()
    assert result.enqueued == 0
    assert result.reason_code == blocker


def test_controller_yields_to_normal_backlog_without_pausing(store, allowlist):
    add_normal_transcription_done(store, "new-001")
    result = HistoricalRepairController(store, allowlist).run_once()
    assert result.enqueued == 0
    assert result.reason_code == "normal_backlog"
    assert result.hard_pause is False
```

- [ ] **Step 2: Run and confirm the missing controller**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_historical_batch.py -q
```

Expected: failure because `HistoricalRepairController` is absent.

- [ ] **Step 3: Implement one-cycle decisions and the finite command**

`run_once()` may enqueue the next immutable batch only when the previous batch is
terminal, no normal work is claimable, the repair lane is healthy, and the allowlist
digest is unchanged. It never translates or changes job stages directly.

Add:

```text
historical-repair-controller --allowlist-file PATH --initial-batch-size 5
  --batch-size 20 --poll-interval-seconds 30
```

Require `1 <= initial_batch_size <= 5`, `1 <= batch_size <= 20`, and no force
flag. A normal backlog is a yield condition: the command stays healthy and rechecks
after the configured interval. Exit 0 with `complete=true` only when every currently
eligible allowlisted record has a terminal result. Exit non-zero only on a hard
safety pause.

- [ ] **Step 4: Verify controller tests**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_historical_batch.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/historical_batch.py orchestrator/__main__.py \
  tests/test_historical_batch.py
git commit -m "feat: control bounded repair batches"
```

## Task 8: Expose safe repair progress in the dashboard

**Files:**
- Modify: `orchestrator/models.py`
- Modify: `orchestrator/dashboard.py`
- Modify: `orchestrator/api.py`
- Modify: `tests/test_dashboard_state.py`
- Modify: `tests/test_api_dashboard.py`

- [ ] **Step 1: Write safe progress tests**

```python
def test_dashboard_separates_normal_and_historical_activity(store):
    enqueue_running_repair(store, "old-001")
    state = build_dashboard_state(store)
    assert (
        state.activity["historical_translation"]["movie_number"] == "old-001"
    )
    assert state.historical_repairs["running"] == 1


def test_dashboard_repair_payload_has_no_text_or_secret(store):
    rendered = json.dumps(build_dashboard_state(store).model_dump())
    for forbidden in (
        "subtitle_text", "service_role", "admin_api_token", "description"
    ):
        assert forbidden not in rendered
```

- [ ] **Step 2: Run and confirm missing response fields**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_dashboard_state.py tests/test_api_dashboard.py -q
```

Expected: failure because historical progress/activity are absent.

- [ ] **Step 3: Add aggregate progress**

Extend `DashboardStateResponse`:

```python
historical_repairs: dict[str, int | str | None] = Field(default_factory=dict)
```

Use one grouped store query plus the active repair job ID/code. Add
`activity["historical_translation"]`. Expose counts, batch ID, state, and structured
reason only; do not expose hashes, paths, plan contents, subtitle text, or tokens.
Add both catalog-sync states to the dashboard translation/processing status sets.

- [ ] **Step 4: Verify dashboard/API tests**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_dashboard_state.py tests/test_api_dashboard.py tests/test_api_jobs.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/models.py orchestrator/dashboard.py orchestrator/api.py \
  tests/test_dashboard_state.py tests/test_api_dashboard.py
git commit -m "feat: show historical repair progress"
```

## Task 9: Make Mac workers durable and single-instance

**Files:**
- Create: `orchestrator/process_lock.py`
- Modify: `orchestrator/config.py`
- Modify: `orchestrator/__main__.py`
- Create: `deployment/launchd/com.javsubtitle.mac-worker.plist`
- Create: `deployment/launchd/com.javsubtitle.mac-translation-worker.plist`
- Create: `scripts/install_mac_worker_launchd.sh`
- Create: `tests/test_process_lock.py`

- [ ] **Step 1: Write process-lock and plist tests**

```python
def test_second_lock_for_same_worker_is_rejected(tmp_path):
    first = SingleInstanceLock(tmp_path / "worker.lock").acquire()
    with pytest.raises(AlreadyRunningError):
        SingleInstanceLock(tmp_path / "worker.lock").acquire()
    first.release()


def test_launchd_plists_keep_workers_separate():
    downloader = load_plist(
        "deployment/launchd/com.javsubtitle.mac-worker.plist"
    )
    translator = load_plist(
        "deployment/launchd/com.javsubtitle.mac-translation-worker.plist"
    )
    assert downloader["ProgramArguments"][-1] == "mac-worker"
    assert translator["ProgramArguments"][-1] == "mac-translation-worker"
    assert downloader["Label"] != translator["Label"]
    assert downloader["KeepAlive"] is True
    assert translator["KeepAlive"] is True
```

- [ ] **Step 2: Run and confirm missing module/plists**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_process_lock.py -q
```

Expected: missing files.

- [ ] **Step 3: Implement non-blocking flock and launchd definitions**

Create:

```python
class AlreadyRunningError(RuntimeError):
    pass


class SingleInstanceLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(
                self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
            )
        except BlockingIOError as exc:
            self.handle.close()
            self.handle = None
            raise AlreadyRunningError("worker_already_running") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(f"{os.getpid()}\\n")
        self.handle.flush()
        return self

    def release(self):
        if self.handle is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None
```

Wrap downloader and translator runtime functions in distinct locks under `data/`.
The plists use the production checkout and `.venv/bin/python`, separate logs,
`RunAtLoad=true`, `KeepAlive=true`, and `ThrottleInterval=10`.

The installer validates with `plutil -lint`, copies only the two exact plists to
`~/Library/LaunchAgents`, bootouts only those labels if present, bootstraps them,
and prints label/PID state without environment values.

- [ ] **Step 4: Verify locks and plists**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_process_lock.py -q
plutil -lint deployment/launchd/com.javsubtitle.mac-worker.plist
plutil -lint deployment/launchd/com.javsubtitle.mac-translation-worker.plist
```

Expected: tests and lint pass.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/process_lock.py orchestrator/config.py orchestrator/__main__.py \
  deployment/launchd scripts/install_mac_worker_launchd.sh tests/test_process_lock.py
git commit -m "feat: supervise unique Mac workers"
```

## Task 10: Complete docs and pre-deploy verification

**Files:**
- Modify: `docs/setup/mac.md`
- Modify: `README.md`
- Modify: `.env.example`
- Test: full suite

- [ ] **Step 1: Document final flow and commands**

Document:

```text
audio_ready
→ transcription_claimed → transcribing → transcription_done
→ translating → publish_pending → publishing
→ catalog_sync_pending → catalog_syncing → english_srt_ready
```

Add recovery, dry-run plan, enqueue, controller, launchd status, pause/resume, and
rollback commands. State that the 340-line allowlist is input evidence, not the
affected count; the dry-run eligibility result is authoritative.

- [ ] **Step 2: Run static and automated verification**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/python \
  -m compileall -q orchestrator
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest -q
git diff --check
```

Expected: compilation and all tests pass; diff check is silent.

- [ ] **Step 3: Verify production config without printing secrets**

```bash
set -a; source .env; set +a
test -n "$SUPABASE_URL"
test -n "$SUPABASE_SERVICE_ROLE_KEY"
test -n "$JAVSUBTITLE_API_BASE"
test -n "$JAVSUBTITLE_ADMIN_API_TOKEN"
.venv/bin/python -m orchestrator mac-translation-smoke-test
```

Expected: checks exit 0; smoke reports 10 cues, unique ratio at least 0.5, known_bad
0. No secret is printed.

- [ ] **Step 4: Commit docs**

```bash
git add docs/setup/mac.md README.md .env.example
git commit -m "docs: run normal-first historical repairs"
```

## Task 11: Deploy and complete the exact `roe-291` canary

**Files:**
- Production checkout: `/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator`
- Exact job: `job_1e59eef416c64f6a9b51312242539010`
- Exact job directory: `/Users/ytt/MissAVJobs/roe-291`

- [ ] **Step 1: Require a clean reviewed deployment boundary**

Merge reviewed implementation into the production checkout and run the full suite
there. Review, but do not overwrite, unrelated user files. Identify and stop only the
current terminal translation worker before launchd migration. Keep API, Cloudflare
tunnel, and Windows worker running.

- [ ] **Step 2: Capture exact preflight evidence**

```bash
JOB_ID=job_1e59eef416c64f6a9b51312242539010
STAGED=/Users/ytt/MissAVJobs/roe-291/audio/roe-291.wav
STAGED_SHA=$(shasum -a 256 "$STAGED" | awk '{print $1}')
ffprobe -v error \
  -show_entries format=duration,size:stream=codec_name,sample_rate,channels \
  -of json "$STAGED" | jq '{streams,format}'
curl -fsS "http://127.0.0.1:8010/jobs/$JOB_ID/detail" |
  jq '{id,movie_number,status,claimed_by,updated_at}'
```

Require exact movie/status, no claim, valid PCM, and no final `audio.wav`.

- [ ] **Step 3: Recover exact audio and verify**

```bash
.venv/bin/python -m orchestrator recover-interrupted-audio \
  --job-id "$JOB_ID" --movie roe-291 --expected-sha256 "$STAGED_SHA"
test ! -e "$STAGED"
test "$(shasum -a 256 /Users/ytt/MissAVJobs/roe-291/audio.wav |
  awk '{print $1}')" = "$STAGED_SHA"
curl -fsS "http://127.0.0.1:8010/jobs/$JOB_ID/detail" |
  jq -e '.status == "audio_ready"
    and .audio_path_mac != null and .audio_path_windows != null'
```

Expected: recovery succeeds once and a verification rerun is safe.

- [ ] **Step 4: Install exactly one downloader and translator**

```bash
./scripts/install_mac_worker_launchd.sh
launchctl print gui/$(id -u)/com.javsubtitle.mac-worker | rg 'state =|pid ='
launchctl print gui/$(id -u)/com.javsubtitle.mac-translation-worker |
  rg 'state =|pid ='
```

Require one PID for each label and no terminal duplicate.

- [ ] **Step 5: Observe the full condition sequence**

Poll `/jobs/$JOB_ID/detail` and record timestamps for:

```text
audio_ready
transcription_claimed/transcribing
transcription_done
translating
publish_pending/publishing
catalog_sync_pending/catalog_syncing
english_srt_ready
```

Require Windows-created Japanese SRT, no Windows-created English SRT, Mac quality
`passed=true`, and unchanged audio/Japanese hashes after publication.

- [ ] **Step 6: Verify Supabase, D1/KV, API, and browser**

Use existing publisher verification for Storage SHA and the exact
`movie_languages` row. Then require:

```bash
curl -fsS \
  "https://javsubtitle.com/api/movie/roe-291?cacheNonce=$(date +%s)" |
  jq -e '.code == "roe-291"
    and .hasSubtitles == true and (.subtitles | length) >= 1'
```

In a real browser verify subtitle controls without reading/reporting subtitle text.
Run existing catalog and two playback regression scripts. Any failure pauses history.

- [ ] **Step 7: Write the private canary report**

Record only job/movie IDs, timestamps, hashes, sizes, quality statistics, Supabase
IDs/paths, D1/KV counts, and reason codes. Include no secrets, subtitle text, or adult
metadata.

## Task 12: Execute the approved historical allowlist

**Files:**
- Allowlist: `/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt`
- Private reports: `/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/reports/historical-repair-20260713/`

- [ ] **Step 1: Produce the authoritative dry-run count**

```bash
mkdir -p -m 700 reports/historical-repair-20260713
.venv/bin/python -m orchestrator plan-historical-repair-batch \
  --allowlist-file \
  reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --limit 5 \
  --output reports/historical-repair-20260713/batch-001-plan.json
```

Also generate the read-only full eligibility totals: eligible, already repaired,
ineligible, and blocked. Do not use the 340 file lines as affected count.

- [ ] **Step 2: Enqueue only the first five-job batch**

```bash
PLAN=reports/historical-repair-20260713/batch-001-plan.json
PLAN_SHA=$(jq -r .plan_sha256 "$PLAN")
.venv/bin/python -m orchestrator enqueue-historical-repair-batch \
  --allowlist-file \
  reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --plan-file "$PLAN" --confirm-plan-sha256 "$PLAN_SHA"
```

Require at most five pending repair records and zero normal job-stage changes before
one is claimed.

- [ ] **Step 3: Verify the first batch completely**

For every record require preserved Japanese/audio hashes, retained rejected English,
quality outcome, verified Supabase and website state for successes, and structured
permanent reasons for failures. Pause on any design stop condition.

- [ ] **Step 4: Start the bounded controller**

```bash
.venv/bin/python -m orchestrator historical-repair-controller \
  --allowlist-file \
  reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --initial-batch-size 5 --batch-size 20 --poll-interval-seconds 30
```

It processes one translation at a time, yields to normal work, exits non-zero on a
safety pause, and never uses `force=True`.

- [ ] **Step 5: Prove normal-lane priority during repairs**

Use `/dashboard/state` to show that a new Windows `transcription_done` job is
selected before the next historical record. If normal work waits behind more than
one historical unit, stop the controller and investigate.

- [ ] **Step 6: Produce the final report**

Report:

```text
allowlist_lines
eligible_at_start
already_repaired_or_ineligible
succeeded
permanent_failed_by_reason_code
retry_exhausted_by_reason_code
remaining_pending
normal_jobs_processed_during_repair
supabase_verified
website_catalog_verified
```

Require `remaining_pending=0` unless a named safety gate stopped the controller.
Include no secrets, subtitle text, or adult metadata.

## Final Verification

```bash
.venv/bin/python -m compileall -q orchestrator
.venv/bin/pytest -q
.venv/bin/python -m orchestrator mac-translation-smoke-test
pgrep -fal '^(.*/)?python(3(\.[0-9]+)?)? -m orchestrator mac-worker$'
pgrep -fal \
  '^(.*/)?python(3(\.[0-9]+)?)? -m orchestrator mac-translation-worker$'
curl -fsS http://127.0.0.1:8010/dashboard/state |
  jq '{counts,activity,historical_repairs}'
```

Require all tests/smokes to pass, exactly one downloader and translator, Windows
heartbeat online, and no unnamed blocker. Run javsubtitle.com catalog and playback
regressions one final time.
