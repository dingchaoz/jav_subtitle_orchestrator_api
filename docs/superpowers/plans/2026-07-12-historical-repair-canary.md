# Historical Subtitle Repair Canary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate the completed local audit, implement a controlled translation-only historical repair path with verified Supabase publication, execute exactly one eligible canary, verify it on `javsubtitle.com`, and stop before any small batch.

**Architecture:** A read-only selector chooses one eligible allowlisted local job, and a confirmation-bound prepare command atomically resets only its translation stage. An exact-job one-shot Mac worker quarantines the old English SRT, translates, runs the existing pair quality gate, publishes with same-path Supabase upsert, verifies Storage SHA-256 plus catalog metadata, and only then marks the job ready. Normal polling is restored only after the canary succeeds.

**Tech Stack:** Python 3.11, SQLite, `requests`, Pydantic Settings, TranslateLocally, Supabase Storage/PostgREST, `pytest`, Git/GitHub CLI, Codex browser verification.

---

## File map

- Modify `orchestrator/store.py`: idempotent translation-attempt schema migration, translation-only reset, exact-job claim, and Mac-only retry accounting.
- Modify `orchestrator/models.py`: expose `translation_attempt_count` in job detail data.
- Modify `orchestrator/dashboard.py`: include the translation-attempt count in dashboard detail without changing Windows attempt display.
- Create `orchestrator/historical_repair.py`: allowlist reader, eligibility selector, sanitized JSON result, and confirmation-bound prepare operation.
- Modify `orchestrator/supabase_publisher.py`: same-path upsert plus bounded Storage SHA-256 and catalog verification.
- Modify `orchestrator/mac_worker.py`: publish after quality pass and before ready; add exact-job one-shot processing.
- Modify `orchestrator/config.py`: explicit publishing enable and verification timeout settings.
- Modify `orchestrator/__main__.py`: selector, prepare, and exact-job one-shot CLI commands; construct the publisher only under explicit configuration.
- Modify `.env.example`: document non-secret publishing settings.
- Modify `docs/setup/mac.md`: operator sequence and approval boundary.
- Create `tests/test_historical_repair.py`: selector, prepare, allowlist, preservation, and confirmation tests.
- Modify `tests/test_store_worker_claims.py`: migration, translation counter, exact claim, and reset tests.
- Modify `tests/test_supabase_publisher.py`: stale CDN, SHA-256, catalog, upsert, and sanitized failure tests.
- Modify `tests/test_mac_worker.py`: publish ordering, failure retry, permanent quality failure, quarantine, and exact-job isolation tests.
- Modify `tests/test_config_paths.py`, `tests/test_dashboard_state.py`: settings and new counter visibility.

### Task 1: Integrate the completed GET-only audit branch

**Files:**
- Merge source: `codex/local-english-ai-audit`
- Merge target: `codex/windows-transcription-mac-translation`

- [ ] **Step 1: Verify the audit feature branch before integration**

Run from `.worktrees/local-english-ai-audit`:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.worktrees/.venvs/local-english-ai-audit/bin/pytest -q
git status --short
```

Expected: `228 passed`, one known Starlette warning, and clean Git status.

- [ ] **Step 2: Merge into the orchestration branch**

Run from the main worktree:

```bash
git switch codex/windows-transcription-mac-translation
git merge --no-ff codex/local-english-ai-audit -m "merge: add local English AI audit"
```

Expected: merge succeeds without touching the unrelated untracked benchmark/model directories.

- [ ] **Step 3: Verify the merged branch**

```bash
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Expected: `228 passed`, zero failures.

- [ ] **Step 4: Push and verify the existing draft PR**

```bash
git push origin codex/windows-transcription-mac-translation
gh pr view 1 --json number,isDraft,headRefName,baseRefName,url
```

Expected: PR 1 remains draft, its head is `codex/windows-transcription-mac-translation`, and the pushed merge commit is included.

### Task 2: Create the isolated repair implementation worktree

**Files:**
- Worktree: `.worktrees/historical-repair-canary`
- Branch: `codex/historical-repair-canary`

- [ ] **Step 1: Verify worktree safety and create the branch**

```bash
git check-ignore -q .worktrees
git worktree add .worktrees/historical-repair-canary \
  -b codex/historical-repair-canary \
  codex/windows-transcription-mac-translation
```

Expected: worktree is created from the pushed orchestration branch.

- [ ] **Step 2: Create an isolated Python 3.11 environment**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/python -m venv \
  /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.worktrees/.venvs/historical-repair-canary
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.worktrees/.venvs/historical-repair-canary/bin/python \
  -m pip install -e ".[dev]"
```

Expected: installation succeeds without repointing the production `.venv`.

- [ ] **Step 3: Run the clean baseline**

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.worktrees/.venvs/historical-repair-canary/bin/pytest -q
```

Expected: `228 passed`, zero failures.

### Task 3: Separate Mac translation attempts and add exact state operations

**Files:**
- Modify: `orchestrator/store.py`
- Modify: `orchestrator/models.py`
- Modify: `orchestrator/dashboard.py`
- Modify: `tests/test_store_worker_claims.py`
- Modify: `tests/test_dashboard_state.py`

- [ ] **Step 1: Write failing schema, reset, retry, and exact-claim tests**

Add this safe fixture helper and the tests below:

```python
def write_historical_files(root: Path, movie: str) -> tuple[JobPaths, bytes, bytes]:
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    japanese = "\n\n".join(
        f"{i}\n00:00:{i - 1:02d},000 --> 00:00:{i:02d},000\nJapanese source {i}"
        for i in range(1, 26)
    ).encode()
    bad_english = "\n\n".join(
        f"{i}\n00:00:{i - 1:02d},000 --> 00:00:{i:02d},000\nCannot translate"
        for i in range(1, 26)
    ).encode()
    paths.audio_path_mac.write_bytes(b"synthetic-audio")
    paths.japanese_srt_path_mac.write_bytes(japanese)
    paths.english_srt_path_mac.write_bytes(bad_english)
    return paths, japanese, bad_english


def test_historical_reset_preserves_windows_attempts_and_paths(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("abc-001", priority=100, force=False).job
    paths, japanese, bad_english = write_historical_files(
        mac_jobs_root, "abc-001"
    )
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, worker_attempt_count = 2, "
            "translation_attempt_count = 2, audio_path_mac = ?, "
            "japanese_srt_path_mac = ?, english_srt_path_mac = ? WHERE id = ?",
            (
                JobStatus.ENGLISH_SRT_READY.value,
                str(paths.audio_path_mac),
                str(paths.japanese_srt_path_mac),
                str(paths.english_srt_path_mac),
                job.id,
            ),
        )

    reset = store.prepare_historical_translation_repair(
        job.id, expected_status=JobStatus.ENGLISH_SRT_READY
    )

    assert reset.status is JobStatus.TRANSCRIPTION_DONE
    assert reset.worker_attempt_count == 2
    assert reset.translation_attempt_count == 0
    assert reset.audio_path_mac == str(paths.audio_path_mac)
    assert reset.japanese_srt_path_mac == str(paths.japanese_srt_path_mac)
    assert reset.english_srt_path_mac is None
    assert paths.audio_path_mac.read_bytes() == b"synthetic-audio"
    assert paths.japanese_srt_path_mac.read_bytes() == japanese
    assert paths.english_srt_path_mac.read_bytes() == bad_english
    assert paths.english_srt_path_mac.exists()


def test_exact_translation_claim_cannot_claim_another_job(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-001")
    second = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-002")

    claimed = store.claim_translation_job(
        second.id, "mac-translation-canary", lease_seconds=60
    )

    assert claimed.id == second.id
    assert store.get_job(first.id).status is JobStatus.TRANSCRIPTION_DONE
    assert store.get_job(second.id).status is JobStatus.TRANSLATING
```

Also change the existing transient Mac failure assertion from
`worker_attempt_count == 1` to:

```python
assert refreshed.worker_attempt_count == 0
assert refreshed.translation_attempt_count == 1
```

- [ ] **Step 2: Run the focused tests and verify RED**

```bash
pytest -q tests/test_store_worker_claims.py tests/test_mac_worker.py -k \
  "historical_reset or exact_translation_claim or retries_transient"
```

Expected: failures report missing `translation_attempt_count`,
`prepare_historical_translation_repair`, and `claim_translation_job`.

- [ ] **Step 3: Implement the idempotent schema migration and record field**

Add `translation_attempt_count: int` to `JobRecord`. Include it in new-table DDL and
new-job inserts. Immediately after `CREATE TABLE IF NOT EXISTS jobs`, migrate old
databases:

```python
columns = {
    row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
}
if "translation_attempt_count" not in columns:
    conn.execute(
        "ALTER TABLE jobs ADD COLUMN translation_attempt_count "
        "INTEGER NOT NULL DEFAULT 0"
    )
```

Map `row["translation_attempt_count"]` in `_row_to_job`. Add the field to
`JobDetailResponse`, `build_job_detail`, and the dashboard detail display as
`Translation attempts`; retain the existing Windows `Worker attempts` field.

- [ ] **Step 4: Implement exact claim and translation-only reset**

Add these store methods:

```python
def claim_translation_job(
    self, job_id: str, worker_id: str, lease_seconds: int
) -> JobRecord | None:
    now = utc_now_iso()
    lease = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).replace(
        microsecond=0
    ).isoformat()
    with self.connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        cursor = conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = ?, lease_expires_at = ?, "
            "updated_at = ? WHERE id = ? AND status = ? AND claimed_by IS NULL",
            (
                JobStatus.TRANSLATING.value,
                worker_id,
                lease,
                now,
                job_id,
                JobStatus.TRANSCRIPTION_DONE.value,
            ),
        )
        return self.get_job(job_id, conn=conn) if cursor.rowcount == 1 else None


def prepare_historical_translation_repair(
    self, job_id: str, *, expected_status: JobStatus
) -> JobRecord:
    if expected_status not in {
        JobStatus.QUEUED, JobStatus.FAILED, JobStatus.ENGLISH_SRT_READY
    }:
        raise ValueError("historical repair status is not eligible")
    now = utc_now_iso()
    with self.connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        cursor = conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = NULL, lease_expires_at = NULL, "
            "translation_attempt_count = 0, updated_at = ?, "
            "error = NULL, english_srt_path_mac = NULL, "
            "english_srt_path_windows = NULL "
            "WHERE id = ? AND status = ? AND claimed_by IS NULL",
            (
                JobStatus.TRANSCRIPTION_DONE.value,
                now,
                job_id,
                expected_status.value,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("historical repair state changed before prepare")
        return self.get_job(job_id, conn=conn)
```

- [ ] **Step 5: Move Mac retry accounting to the new counter**

In `fail_mac_translation` and `recover_expired_translation_leases`, calculate and
update `translation_attempt_count`; never modify `worker_attempt_count`. Windows
failure and expired-transcription recovery continue using `worker_attempt_count`.

- [ ] **Step 6: Run focused and regression tests**

```bash
pytest -q tests/test_store_worker_claims.py tests/test_mac_worker.py \
  tests/test_dashboard_state.py
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit the state model**

```bash
git add orchestrator/store.py orchestrator/models.py orchestrator/dashboard.py \
  tests/test_store_worker_claims.py tests/test_mac_worker.py \
  tests/test_dashboard_state.py
git commit -m "feat: isolate Mac translation repair state"
```

### Task 4: Add the read-only selector and confirmation-bound prepare command

**Files:**
- Create: `orchestrator/historical_repair.py`
- Create: `tests/test_historical_repair.py`
- Modify: `orchestrator/__main__.py`

- [ ] **Step 1: Write failing selector and prepare tests**

Create this fixture wrapper around the same safe synthetic SRT builder, then add the
tests:

```python
def write_historical_files(root: Path, movie: str):
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    japanese = "\n\n".join(
        f"{i}\n00:00:{i - 1:02d},000 --> 00:00:{i:02d},000\nJapanese source {i}"
        for i in range(1, 26)
    ).encode()
    bad_english = "\n\n".join(
        f"{i}\n00:00:{i - 1:02d},000 --> 00:00:{i:02d},000\nCannot translate"
        for i in range(1, 26)
    ).encode()
    paths.audio_path_mac.write_bytes(b"synthetic-audio")
    paths.japanese_srt_path_mac.write_bytes(japanese)
    paths.english_srt_path_mac.write_bytes(bad_english)
    return paths, japanese, bad_english


@dataclass
class PreparedStore:
    store: JobStore
    root: Path

    def bad_job(
        self,
        movie: str,
        *,
        status: JobStatus = JobStatus.FAILED,
        audio: bool = True,
        claimed: bool = False,
    ) -> JobRecord:
        job = self.store.submit_job(movie, priority=100, force=False).job
        paths, japanese, bad_english = write_historical_files(self.root, movie)
        if not audio:
            paths.audio_path_mac.unlink()
        with self.store.connection() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, claimed_by = ?, audio_path_mac = ?, "
                "japanese_srt_path_mac = ?, english_srt_path_mac = ? WHERE id = ?",
                (
                    status.value,
                    "active-worker" if claimed else None,
                    str(paths.audio_path_mac),
                    str(paths.japanese_srt_path_mac),
                    str(paths.english_srt_path_mac),
                    job.id,
                ),
            )
        return self.store.get_job(job.id)

    def good_job(self, movie: str) -> JobRecord:
        job = self.bad_job(movie)
        paths = build_job_paths(movie, self.root, "M:\\")
        good = "\n\n".join(
            f"{i}\n00:00:{i - 1:02d},000 --> 00:00:{i:02d},000\nDistinct translation {i}"
            for i in range(1, 26)
        )
        paths.english_srt_path_mac.write_text(good, encoding="utf-8")
        return job


@pytest.fixture
def prepared_store(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    return PreparedStore(store, mac_jobs_root)


def test_selector_prefers_eligible_movie_and_is_read_only(tmp_path, prepared_store):
    preferred = prepared_store.bad_job("abf-279", status=JobStatus.FAILED)
    fallback = prepared_store.bad_job("abc-001", status=JobStatus.ENGLISH_SRT_READY)
    allowlist = tmp_path / "repair-allowlist.txt"
    allowlist.write_text("abc-001\nabf-279\n", encoding="utf-8")

    candidate = select_historical_repair_canary(
        prepared_store.store,
        allowlist,
        preferred_movie="abf-279",
    )

    assert candidate.job_id == preferred.id
    assert candidate.movie_number == "abf-279"
    assert candidate.reason_codes
    assert prepared_store.store.get_job(preferred.id).status is JobStatus.FAILED


def test_selector_skips_missing_audio_good_english_active_or_unlisted_jobs(
    tmp_path, prepared_store
):
    prepared_store.bad_job("abc-001", audio=False)
    prepared_store.good_job("abc-002")
    prepared_store.bad_job("abc-003", status=JobStatus.TRANSLATING, claimed=True)
    prepared_store.bad_job("abc-004")
    allowlist = tmp_path / "repair-allowlist.txt"
    allowlist.write_text("abc-001\nabc-002\nabc-003\n", encoding="utf-8")

    assert select_historical_repair_canary(
        prepared_store.store, allowlist
    ) is None


def test_prepare_requires_exact_limit_movie_and_job_id(tmp_path, prepared_store):
    job = prepared_store.bad_job("abc-001", status=JobStatus.FAILED)
    allowlist = tmp_path / "repair-allowlist.txt"
    allowlist.write_text("abc-001\n", encoding="utf-8")

    with pytest.raises(ValueError, match="limit must be exactly 1"):
        prepare_historical_repair_canary(
            prepared_store.store, allowlist, movie="abc-001", limit=2,
            confirm_job_id=job.id,
        )
    with pytest.raises(ValueError, match="confirmed job does not match"):
        prepare_historical_repair_canary(
            prepared_store.store, allowlist, movie="abc-001", limit=1,
            confirm_job_id="wrong-job",
        )
```

Add assertions that invalid/mismatched calls preserve status, Japanese bytes,
English bytes, and audio bytes.

- [ ] **Step 2: Run tests and verify RED**

```bash
pytest -q tests/test_historical_repair.py
```

Expected: collection fails because `orchestrator.historical_repair` does not exist.

- [ ] **Step 3: Implement bounded allowlist loading and candidate selection**

Create:

```python
@dataclass(frozen=True, slots=True)
class HistoricalRepairCandidate:
    job_id: str
    movie_number: str
    expected_status: JobStatus
    reason_codes: tuple[str, ...]
    japanese_path: str
    english_path: str
    audio_path: str
    audio_preexisting: bool
    quarantine_directory: str

    def to_safe_dict(self) -> dict[str, object]:
        return asdict(self)
```

`load_repair_allowlist(path)` must reject symlinks/non-files, blank or invalid movie
codes, duplicates, and more than 10,000 lines. Normalize through
`normalize_movie_number` and return a frozen set.

`select_historical_repair_canary(...)` must iterate `store.list_jobs()` and accept
only allowlisted, unclaimed jobs in `queued`, `failed`, or `english_srt_ready` with
non-empty Japanese and English files whose production pair gate fails. Audio is not
required for historical translation; record whether it existed before selection.
Sort by normalized movie then job ID, moving the eligible preferred movie to the
front. Return one candidate or `None`. Never serialize subtitle content.

- [ ] **Step 4: Implement confirmation-bound prepare**

```python
def prepare_historical_repair_canary(
    store: JobStore,
    allowlist_path: Path,
    *,
    movie: str,
    limit: int,
    confirm_job_id: str,
) -> JobRecord:
    if limit != 1:
        raise ValueError("limit must be exactly 1")
    normalized = normalize_movie_number(movie)
    if normalized is None:
        raise ValueError("movie is invalid")
    candidate = select_historical_repair_canary(
        store, allowlist_path, preferred_movie=normalized
    )
    if candidate is None or candidate.movie_number != normalized:
        raise ValueError("movie is not an eligible allowlisted canary")
    if candidate.job_id != confirm_job_id:
        raise ValueError("confirmed job does not match selected canary")
    return store.prepare_historical_translation_repair(
        candidate.job_id, expected_status=candidate.expected_status
    )
```

- [ ] **Step 5: Add selector and prepare CLI commands**

Add parsers with no force/delete/bulk flags:

```python
selector = subcommands.add_parser("select-historical-repair-canary")
selector.add_argument("--allowlist-file", type=Path, required=True)
selector.add_argument("--preferred-movie")
selector.add_argument("--output", type=Path, required=True)

prepare = subcommands.add_parser("prepare-historical-repair-canary")
prepare.add_argument("--allowlist-file", type=Path, required=True)
prepare.add_argument("--movie", required=True)
prepare.add_argument("--limit", type=int, required=True)
prepare.add_argument("--confirm-job-id", required=True)
```

Selector atomically writes one sanitized JSON object with mode `0600`. Prepare prints
only job ID, movie, prior status, new status, and `prepared=true`.

- [ ] **Step 6: Verify focused tests and CLI surfaces**

```bash
pytest -q tests/test_historical_repair.py
python -m orchestrator select-historical-repair-canary --help
python -m orchestrator prepare-historical-repair-canary --help
```

Expected: tests pass; help exposes no `force`, `delete`, wildcard, or batch option.

- [ ] **Step 7: Commit selector and prepare**

```bash
git add orchestrator/historical_repair.py orchestrator/__main__.py \
  tests/test_historical_repair.py
git commit -m "feat: add controlled historical repair canary"
```

### Task 5: Verify Supabase Storage bytes and catalog after upsert

**Files:**
- Modify: `orchestrator/supabase_publisher.py`
- Modify: `tests/test_supabase_publisher.py`

- [ ] **Step 1: Write failing verification tests**

Extend the fake session to return stale bytes once, then repaired bytes. Assert:

```python
def test_publish_waits_for_matching_storage_hash_and_catalog(tmp_path):
    repaired = _write_pair(tmp_path)
    session = VerifyingSession(
        existing=True,
        storage_downloads=[b"stale", repaired.read_bytes()],
    )
    sleeps = []
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        verification_timeout_seconds=90,
        verification_interval_seconds=2,
        sleeper=sleeps.append,
        nonce_factory=iter(["nonce-1", "nonce-2"]).__next__,
    )

    result = publisher.publish_english_ai("ktb-112", repaired)

    assert result.verified is True
    assert result.content_sha256 == hashlib.sha256(repaired.read_bytes()).hexdigest()
    assert sleeps == [2]
    download_calls = [call for call in session.calls if call[0] == "GET" and "/storage/" in call[1]]
    assert [call[2]["params"]["cacheNonce"] for call in download_calls] == [
        "nonce-1", "nonce-2"
    ]


def test_publish_never_accepts_stale_bytes_or_wrong_catalog(tmp_path):
    repaired = _write_pair(tmp_path)
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=VerifyingSession(existing=True, storage_downloads=[b"stale"] * 3),
        verification_timeout_seconds=4,
        verification_interval_seconds=2,
        sleeper=lambda _: None,
        clock=iter([0, 0, 2, 4]).__next__,
    )
    with pytest.raises(RuntimeError, match="Supabase verification failed"):
        publisher.publish_english_ai("ktb-112", repaired)
```

Keep the existing test that bad English produces `quality_gate_failed:` before any
Storage call.

- [ ] **Step 2: Run publisher tests and verify RED**

```bash
pytest -q tests/test_supabase_publisher.py
```

Expected: failures report missing verification constructor parameters/result fields.

- [ ] **Step 3: Split JSON and binary request handling**

Retain one `_request_raw(method, path, **kwargs)` that adds server-side auth, timeout,
and `allow_redirects=False` and raises sanitized status-only errors. Build
`_request_json` on top of it with bounded JSON shape checks. Do not include response
bodies, headers, paths containing credentials, or subtitle bytes in exceptions.

- [ ] **Step 4: Implement bounded post-upload verification**

Extend the result:

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
```

After upsert and catalog PATCH/POST, poll authenticated Storage GET with
`params={"cacheNonce": nonce_factory()}` and `Accept-Encoding: identity`. Read at
most `local_size + 1` bytes. Return only when SHA-256 and size match. Then GET the
exact `movie_languages` row by subtitle ID and require expected movie ID, language,
path, and size. Timeout raises
`RuntimeError("Supabase verification failed: storage_hash_timeout")`; catalog
mismatch raises `RuntimeError("Supabase verification failed: catalog_mismatch")`
with reason codes but no content.

- [ ] **Step 5: Run publisher tests and existing quality-gate tests**

```bash
pytest -q tests/test_supabase_publisher.py tests/test_subtitle_quality.py
```

Expected: all pass, including same-path `x-upsert:true`, catalog PATCH, stale-byte
rejection, later-byte acceptance, and pre-upload quality rejection.

- [ ] **Step 6: Commit verified publication**

```bash
git add orchestrator/supabase_publisher.py tests/test_supabase_publisher.py
git commit -m "feat: verify repaired Supabase subtitles"
```

### Task 6: Publish before ready and add exact-job one-shot worker

**Files:**
- Modify: `orchestrator/mac_worker.py`
- Modify: `orchestrator/__main__.py`
- Modify: `orchestrator/config.py`
- Modify: `.env.example`
- Modify: `tests/test_mac_worker.py`
- Modify: `tests/test_config_paths.py`

- [ ] **Step 1: Write failing worker-ordering and isolation tests**

Add a fake publisher and assert:

```python
class RecordingPublisher:
    def __init__(self, events, *, error=None):
        self.events = events
        self.error = error

    def publish_english_ai(self, movie, path):
        self.events.append(("publish", movie, path.name))
        if self.error:
            raise self.error
        return SimpleNamespace(
            content_sha256="a" * 64, file_size=path.stat().st_size, verified=True
        )


class RecordingTranslator(DiverseMacTranslator):
    def __init__(self, events):
        self.events = events

    def translate_to_english(self, input_srt, output_srt):
        self.events.append(("translate", input_srt.name, output_srt.name))
        super().translate_to_english(input_srt, output_srt)


def test_good_translation_publishes_before_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    events = []
    worker = MacTranslationWorker(
        store, RecordingTranslator(events), 3, "mac-translation-1", 60,
        publisher=RecordingPublisher(events),
    )
    worker.process_one()
    assert events[0][0] == "translate"
    assert events[1][0] == "publish"
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY


def test_publish_failure_is_transient_and_never_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / job.normalized_movie_number / "audio.wav"
    audio.write_bytes(b"keep-audio")
    worker = MacTranslationWorker(
        store, GoodTranslator(), 3, "mac-translation-1", 60,
        publisher=RecordingPublisher([], error=RuntimeError("publish unavailable")),
    )
    worker.process_one()
    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.TRANSCRIPTION_DONE
    assert refreshed.translation_attempt_count == 1
    assert audio.exists()


def test_exact_job_worker_does_not_claim_other_translation(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-001")
    second = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-002")
    worker = MacTranslationWorker(
        store, GoodTranslator(), 3, "mac-canary", 60,
        publisher=RecordingPublisher([]),
    )
    assert worker.process_job_id(second.id) is True
    assert store.get_job(first.id).status is JobStatus.TRANSCRIPTION_DONE
    assert store.get_job(second.id).status is JobStatus.ENGLISH_SRT_READY
```

Extend the existing permanent quality-failure test to assert zero publisher calls,
audio preservation, one new `rejected/*.srt`, and no retry.

- [ ] **Step 2: Run focused worker tests and verify RED**

```bash
pytest -q tests/test_mac_worker.py -k \
  "publishes_before_ready or publish_failure or exact_job or quality_failure"
```

Expected: failures report missing publisher support and `process_job_id`.

- [ ] **Step 3: Refactor one claimed-job processing path**

Keep `process_one()` for polling and add:

```python
def process_job_id(self, job_id: str) -> bool:
    if self.consecutive_quality_failures >= self.quality_failure_limit:
        raise MacTranslationUnhealthyError(
            "Mac translation worker stopped after consecutive quality failures"
        )
    job = self.store.claim_translation_job(job_id, self.worker_id, self.lease_seconds)
    if job is None:
        raise RuntimeError("exact translation job is not claimable")
    self._process_claimed_job(job)
    return True
```

Extract the current try/fail/idle logic into `_process_claimed_job(job)` so polling
and exact-job modes have identical quality and retry behavior. Exact mode never
calls `claim_next_translation_job`.

- [ ] **Step 4: Publish and verify before ready**

Add `publisher=None` to the constructor. After a passing local quality report and
before `complete_mac_translation`, call publisher when configured. Require
`result.verified is True`; append one `mac-translation.log` line containing only
job ID, movie, SHA-256, and size. Publisher exceptions remain non-quality transient
failures, so the existing bounded translation retry applies.

- [ ] **Step 5: Add explicit publishing settings and factory**

Add:

```python
mac_translation_publish_enabled: bool = Field(
    default=False, alias="MAC_TRANSLATION_PUBLISH_ENABLED"
)
supabase_publish_verify_timeout_seconds: int = Field(
    default=90, ge=60, le=300,
    alias="SUPABASE_PUBLISH_VERIFY_TIMEOUT_SECONDS",
)
```

`build_supabase_publisher(settings)` returns `None` only when publishing is disabled.
When enabled, missing URL/key/bucket raises `RuntimeError` before smoke or claim.
Both `run_mac_translation_worker()` and `run_mac_translation_worker_once(job_id)`
must call this factory and pass its result into `MacTranslationWorker`.
Document in `.env.example`:

```text
MAC_TRANSLATION_PUBLISH_ENABLED=false
SUPABASE_SUBTITLE_BUCKET=subtitles
SUPABASE_PUBLISH_VERIFY_TIMEOUT_SECONDS=90
```

- [ ] **Step 6: Add the exact-job one-shot CLI**

```python
one_shot = subcommands.add_parser("mac-translation-worker-once")
one_shot.add_argument("--job-id", required=True)
```

`run_mac_translation_worker_once(job_id)` loads settings, constructs translator and
publisher, runs the same startup smoke, initializes the store migration, processes
that exact ID once, and exits zero only if the final job is
`english_srt_ready`. It must not enter `run_translation_forever`.

- [ ] **Step 7: Run focused and full tests**

```bash
pytest -q tests/test_mac_worker.py tests/test_supabase_publisher.py \
  tests/test_config_paths.py tests/test_store_worker_claims.py
pytest -q
```

Expected: focused tests and full suite pass with zero failures.

- [ ] **Step 8: Commit worker publication and one-shot mode**

```bash
git add orchestrator/mac_worker.py orchestrator/__main__.py \
  orchestrator/config.py .env.example tests/test_mac_worker.py \
  tests/test_config_paths.py
git commit -m "feat: publish verified Mac translations before ready"
```

### Task 7: Document, review, merge, and push the repair implementation

**Files:**
- Modify: `docs/setup/mac.md`
- Merge source: `codex/historical-repair-canary`
- Merge target: `codex/windows-transcription-mac-translation`

- [ ] **Step 1: Document the exact safe operator flow**

Add commands for selector, prepare, smoke, exact-job one-shot, verification, and
general-worker restart. State that `rejected/` is retained, prepare supports exactly
one job, and no small batch is authorized by the canary procedure.

- [ ] **Step 2: Run final feature-branch verification**

```bash
git diff --check
python -m compileall -q orchestrator tests
pytest -q
```

Expected: no diff errors, compilation succeeds, full suite has zero failures.

- [ ] **Step 3: Commit documentation**

```bash
git add docs/setup/mac.md
git commit -m "docs: add historical repair canary runbook"
```

- [ ] **Step 4: Perform an inline code review and fix only verified findings**

Review the diff from the orchestration merge base through HEAD for authorization
bypass, unsafe file deletion, status races, secret leakage, upload-before-quality,
ready-before-verification, and unbounded retry. Re-run focused/full tests after any
fix and commit each fix separately.

- [ ] **Step 5: Merge into the orchestration branch and verify again**

```bash
git switch codex/windows-transcription-mac-translation
git merge --no-ff codex/historical-repair-canary \
  -m "merge: add controlled historical repair canary"
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Expected: merge succeeds and the full suite passes on the deployment branch.

- [ ] **Step 6: Push and verify draft PR 1**

```bash
git push origin codex/windows-transcription-mac-translation
gh pr view 1 --json number,isDraft,headRefName,commits,url
```

Expected: PR 1 remains draft and contains the repair implementation commits.

### Task 8: Deploy and execute exactly one historical repair canary

**Files/state:**
- Modify ignored production file: `.env`
- Read allowlist: `reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt`
- Create local sanitized selection: `reports/subtitle-audit/english-ai-local-20260712/canary-selection.json`
- Mutate exactly one selected local job and one corresponding Supabase subtitle

- [ ] **Step 1: Update only non-secret publishing settings in `.env`**

Ensure these exact values exist without printing the file:

```text
MAC_TRANSLATION_PUBLISH_ENABLED=true
SUPABASE_SUBTITLE_BUCKET=subtitles
SUPABASE_PUBLISH_VERIFY_TIMEOUT_SECONDS=90
```

Do not change or display the existing Supabase URL/key or translation runtime paths.

- [ ] **Step 2: Restart the Mac API on the merged code**

Stop only the process serving `python -m orchestrator api`, restart it from the
production `.venv`, and require local `/dashboard` and `/dashboard/state` to return
HTTP 200. POST the disabled legacy complete route with a non-production dummy job ID
and require HTTP 409. Do not restart the Windows worker or Mac downloader.

- [ ] **Step 3: Stop the current general translation worker only**

Identify the single process whose command is exactly
`python -m orchestrator mac-translation-worker`, send `SIGTERM`, and verify it exits.
Do not stop the API, Mac downloader, or Windows worker.

- [ ] **Step 4: Run fresh tests and the real startup smoke**

```bash
source .venv/bin/activate
pytest -q
python -m orchestrator mac-translation-smoke-test
```

Expected: full tests pass; smoke exits zero with 10 English cues, unique ratio at
least 0.5, and known_bad 0. If either fails, do not select or prepare a canary.

- [ ] **Step 5: Run the read-only selector**

```bash
python -m orchestrator select-historical-repair-canary \
  --allowlist-file reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --preferred-movie abf-279 \
  --output reports/subtitle-audit/english-ai-local-20260712/canary-selection.json
jq '{job_id,movie_number,expected_status,reason_codes,japanese_path,english_path,audio_path,audio_preexisting,quarantine_directory}' \
  reports/subtitle-audit/english-ai-local-20260712/canary-selection.json
```

Expected: one eligible candidate and no subtitle text. If none is eligible, stop
without mutation and report the missing eligibility condition.

- [ ] **Step 6: Capture pre-prepare hashes and exact identifiers**

```bash
SELECTION=reports/subtitle-audit/english-ai-local-20260712/canary-selection.json
JOB_ID=$(jq -r .job_id "$SELECTION")
MOVIE=$(jq -r .movie_number "$SELECTION")
JA_PATH=$(jq -r .japanese_path "$SELECTION")
EN_PATH=$(jq -r .english_path "$SELECTION")
AUDIO_PATH=$(jq -r .audio_path "$SELECTION")
shasum -a 256 "$JA_PATH" "$EN_PATH" \
  > reports/subtitle-audit/english-ai-local-20260712/canary-pre-hashes.txt
if [ "$(jq -r .audio_preexisting "$SELECTION")" = true ]; then
  shasum -a 256 "$AUDIO_PATH" >> \
    reports/subtitle-audit/english-ai-local-20260712/canary-pre-hashes.txt
fi
```

Expected: Japanese and English hashes are recorded; audio is hashed only when it
preexisted. No file content is printed.

- [ ] **Step 7: Prepare exactly the selected canary**

```bash
python -m orchestrator prepare-historical-repair-canary \
  --allowlist-file reports/subtitle-audit/english-ai-local-20260712/repair-allowlist.txt \
  --movie "$MOVIE" \
  --limit 1 \
  --confirm-job-id "$JOB_ID"
```

Expected: `prepared=true`, exact job status becomes `transcription_done`, Japanese
and audio hashes remain unchanged, and no Supabase request occurs in this command.

- [ ] **Step 8: Run the exact-job one-shot worker**

```bash
python -m orchestrator mac-translation-worker-once --job-id "$JOB_ID"
```

Expected: startup smoke passes, only the selected job transitions through
`translating`, the old English SRT moves to `rejected/`, quality passes, Supabase
upsert and verification pass, status becomes `english_srt_ready`, and the process
exits zero without claiming another job.

- [ ] **Step 9: Verify local preservation and logs without subtitle text**

Compare current Japanese/audio hashes to their pre-hashes; require equality. Require
at least one timestamped historical/stale English SRT in `rejected/`. Parse the final
line of `logs/quality.log` and require `passed=true`. Parse `mac-translation.log` and
require a verified publication SHA-256 line. Do not print raw SRT or log fields that
could contain subtitle text.

- [ ] **Step 10: Independently verify Supabase and website**

Use authenticated GET-only verification to confirm the catalog row and Storage bytes
match the new local English SHA-256. Then use the Codex browser against
`https://javsubtitle.com`, locate `$MOVIE`, and verify its English subtitle request
resolves to the repaired object/bytes with a cache nonce. Record only movie, job ID,
subtitle ID, reason codes, sizes, hashes, status transitions, and pass/fail.

- [ ] **Step 11: Restart the normal Mac translation worker after canary success**

Run `python -m orchestrator mac-translation-worker` in its dedicated terminal only
after every canary verification passes. Confirm the API and downloader were never
stopped and the worker reports idle/polling or a normal new-job state.

- [ ] **Step 12: Stop at the small-batch approval gate**

Report code commits, merged/pushed branch, PR URL, full pytest output, smoke output,
selected canary, state transitions, rejected-file preservation, Japanese/audio hash
preservation, quality result, Supabase verification, and website verification. Do
not prepare, requeue, translate, upload, overwrite, or delete a second historical
candidate. Ask whether to process a batch of five to ten.
