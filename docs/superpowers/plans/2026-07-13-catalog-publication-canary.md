# Catalog Publication-Only Canary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one allowlisted, exact-job command that moves an already quality-passed legacy subtitle from `failed` or unverified legacy ready state to `publish_pending` without retranslation or artifact mutation, then safely publish only `mist-166`.

**Architecture:** Keep the mutation boundary in `JobStore`, where one SQLite transaction verifies exact job/movie/status binding and resets publication fields only. Keep file and quality eligibility in `catalog_repair.py`, which derives canonical paths, rereads a regular-file allowlist, runs the shared quality gate, and returns a statistics-only receipt. Wire a dedicated CLI command to that service and reuse the existing exact-job one-shot worker for publication.

**Tech Stack:** Python 3.11, argparse, sqlite3, pathlib/hashlib, pytest, existing subtitle quality gate, existing Supabase publisher and MCP deployment tools.

---

## File map

- Modify `orchestrator/store.py`: atomic publication-only preparation transition.
- Modify `orchestrator/catalog_repair.py`: exact allowlist/job eligibility, quality validation, hash receipt, and preparation service.
- Modify `orchestrator/__main__.py`: dedicated CLI parser and command runner.
- Modify `tests/test_store_worker_claims.py`: transaction and field-preservation tests.
- Modify `tests/test_catalog_repair.py`: allowlist, quality, filesystem, and network isolation tests.
- Modify `tests/test_mac_worker.py`: exact-job publication proves translator and other jobs are untouched.
- Modify `docs/setup/mac.md`: production one-job publication-only runbook.

### Task 1: Atomic publication-only state transition

**Files:**
- Modify: `orchestrator/store.py`
- Test: `tests/test_store_worker_claims.py`

- [ ] **Step 1: Write a failing preservation test**

Add a test that creates `abc-021`, writes non-empty canonical Japanese and English
files plus `audio.wav`, changes the row to `failed` with
`translation_attempt_count=3`, `publish_attempt_count=4`, stale catalog fields and
an error, then calls the not-yet-existing method:

```python
prepared = store.prepare_catalog_publication_repair(
    job.id,
    expected_status=JobStatus.FAILED,
    expected_movie="abc-021",
)

assert prepared.status is JobStatus.PUBLISH_PENDING
assert prepared.translation_attempt_count == 3
assert prepared.publish_attempt_count == 0
assert prepared.next_publish_attempt_at is None
assert prepared.catalog_movie_uuid is None
assert prepared.metadata_status is None
assert prepared.metadata_source is None
assert prepared.error is None
assert prepared.english_srt_path_mac == str(paths.english_srt_path_mac)
assert prepared.english_srt_path_windows == paths.english_srt_path_windows
assert paths.japanese_srt_path_mac.read_bytes() == japanese_before
assert paths.english_srt_path_mac.read_bytes() == english_before
assert paths.audio_path_mac.read_bytes() == audio_before
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_store_worker_claims.py::test_prepare_catalog_publication_preserves_translation_and_artifacts -q
```

Expected: FAIL with `AttributeError` because
`prepare_catalog_publication_repair` does not exist.

- [ ] **Step 3: Add the minimal transactional method**

Add this public method to `JobStore`, using existing `get_job`, `connection`,
`build_job_paths`, `canonical_movie_code`, and `utc_now_iso` helpers:

```python
def prepare_catalog_publication_repair(
    self,
    job_id: str,
    *,
    expected_status: JobStatus,
    expected_movie: str,
) -> JobRecord:
    if expected_status not in {JobStatus.FAILED, JobStatus.ENGLISH_SRT_READY}:
        raise ValueError("catalog publication status is not eligible")
    expected_canonical = canonical_movie_code(expected_movie)
    now = utc_now_iso()
    with self.connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        job = self.get_job(job_id, conn=conn)
        if job is None:
            raise KeyError(job_id)
        if canonical_movie_code(job.normalized_movie_number) != expected_canonical:
            raise ValueError("confirmed job movie changed before prepare")
        if job.status is not expected_status or job.claimed_by is not None:
            raise RuntimeError("catalog publication state changed before prepare")
        if (
            job.status is JobStatus.ENGLISH_SRT_READY
            and job.catalog_movie_uuid
            and job.metadata_status in METADATA_STATUSES
            and job.metadata_source in METADATA_SOURCES
        ):
            raise ValueError("verified publication is not eligible")
        paths = build_job_paths(
            job.normalized_movie_number,
            self.jobs_root_mac,
            self.jobs_root_windows,
        )
        if not paths.english_srt_path_mac.is_file() or paths.english_srt_path_mac.stat().st_size == 0:
            raise FileNotFoundError(paths.english_srt_path_mac)
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                publish_attempt_count = 0, next_publish_attempt_at = NULL,
                catalog_movie_uuid = NULL, metadata_status = NULL,
                metadata_source = NULL, updated_at = ?, error = NULL,
                english_srt_path_mac = ?, english_srt_path_windows = ?
            WHERE id = ? AND status = ? AND claimed_by IS NULL
            """,
            (
                JobStatus.PUBLISH_PENDING.value,
                now,
                str(paths.english_srt_path_mac),
                paths.english_srt_path_windows,
                job_id,
                expected_status.value,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("catalog publication state changed before prepare")
        prepared = self.get_job(job_id, conn=conn)
        assert prepared is not None
        return prepared
```

Import `canonical_movie_code`, `METADATA_SOURCES`, and `METADATA_STATUSES` without
creating an import cycle. If `movie_catalog.py` imports `store.py`, define the two
immutable value sets locally in `store.py` with the same values and cover them in
tests instead of introducing the cycle.

- [ ] **Step 4: Add rejection tests**

Add focused tests proving the method rejects:

```text
- a mismatched expected movie;
- a claimed row;
- `queued`, `transcription_done`, `translating`, or `publishing`;
- modern english_srt_ready with UUID plus valid metadata source/status;
- missing or empty canonical English file.
```

Each test snapshots the row and relevant files before the call and asserts they are
unchanged afterward.

- [ ] **Step 5: Run Task 1 tests and verify GREEN**

Run:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_store_worker_claims.py -q
```

Expected: all store claim tests pass.

- [ ] **Step 6: Commit Task 1**

```bash
git add orchestrator/store.py tests/test_store_worker_claims.py
git commit -m "feat: prepare catalog publication state"
```

### Task 2: Allowlisted quality validation and safe receipt

**Files:**
- Modify: `orchestrator/catalog_repair.py`
- Test: `tests/test_catalog_repair.py`

- [ ] **Step 1: Write failing exact-binding and preservation tests**

Add `CatalogPublicationCanaryReceipt` expectations and call:

```python
receipt = prepare_catalog_publication_canary(
    store,
    allowlist_path,
    movie="ABC21",
    limit=1,
    confirm_job_id=job.id,
)

assert receipt.job_id == job.id
assert receipt.movie_code == "abc-021"
assert receipt.prior_status is JobStatus.FAILED
assert receipt.new_status is JobStatus.PUBLISH_PENDING
assert receipt.translation_attempt_count == 3
assert receipt.english_sha256 == hashlib.sha256(english_before).hexdigest()
assert receipt.quality_passed is True
assert receipt.known_bad_phrase_count == 0
```

Also assert database/file snapshots are unchanged except for the expected row fields.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_catalog_repair.py::test_prepare_catalog_publication_canary_is_exact_and_preserves_translation -q
```

Expected: collection or import FAIL because the receipt/function is absent.

- [ ] **Step 3: Implement the preparation service**

Add a frozen receipt containing only operational evidence:

```python
@dataclass(frozen=True, slots=True)
class CatalogPublicationCanaryReceipt:
    job_id: str
    movie_code: str
    prior_status: JobStatus
    new_status: JobStatus
    translation_attempt_count: int
    english_sha256: str
    quality_passed: bool
    english_cue_count: int
    english_unique_ratio: float
    known_bad_phrase_count: int
```

Implement `prepare_catalog_publication_canary` in this order:

```python
def prepare_catalog_publication_canary(
    store: JobStore,
    allowlist_path: Path,
    *,
    movie: str,
    limit: int,
    confirm_job_id: str,
) -> CatalogPublicationCanaryReceipt:
    if limit != 1:
        raise ValueError("limit must be exactly 1")
    requested = canonical_movie_code(movie)
    allowlist = load_repair_allowlist(allowlist_path)
    if requested not in allowlist:
        raise ValueError("movie is not in the explicit allowlist")
    prior = store.get_job(confirm_job_id)
    if prior is None:
        raise ValueError("confirmed catalog publication job does not exist")
    if canonical_movie_code(prior.normalized_movie_number) != requested:
        raise ValueError("confirmed job does not match requested movie")
    if prior.status not in {JobStatus.FAILED, JobStatus.ENGLISH_SRT_READY}:
        raise ValueError("confirmed job status is not eligible")
    paths = build_job_paths(
        prior.normalized_movie_number,
        store.jobs_root_mac,
        store.jobs_root_windows,
    )
    report = validate_translation_quality(
        paths.japanese_srt_path_mac,
        paths.english_srt_path_mac,
    )
    if not report.passed:
        reasons = ",".join(report.reason_codes) or "unknown"
        raise ValueError(f"quality_gate_failed:{reasons}")
    english_sha256 = hashlib.sha256(
        paths.english_srt_path_mac.read_bytes()
    ).hexdigest()
    allowlist_after_validation = load_repair_allowlist(allowlist_path)
    if requested not in allowlist_after_validation:
        raise RuntimeError("allowlist changed before prepare")
    prepared = store.prepare_catalog_publication_repair(
        prior.id,
        expected_status=prior.status,
        expected_movie=requested,
    )
    return CatalogPublicationCanaryReceipt(
        job_id=prepared.id,
        movie_code=requested,
        prior_status=prior.status,
        new_status=prepared.status,
        translation_attempt_count=prepared.translation_attempt_count,
        english_sha256=english_sha256,
        quality_passed=report.passed,
        english_cue_count=report.english_cue_count,
        english_unique_ratio=report.english_unique_ratio,
        known_bad_phrase_count=report.known_bad_phrase_count,
    )
```

Reuse `load_repair_allowlist` from `historical_repair.py`. Do not accept caller file
paths and do not print or store subtitle text.

- [ ] **Step 4: Add negative and isolation tests**

Tests must prove:

```text
- limit other than one fails;
- symlink/empty/duplicate/invalid allowlist fails through the shared loader;
- movie absent from allowlist or job ID/movie mismatch fails;
- bad quality raises quality_gate_failed:<reason codes>;
- missing Japanese or English file fails without row mutation;
- verified modern ready publication fails;
- requests.Session.request, translator, and publisher are never invoked;
- English/Japanese/audio bytes and rejected/ directory contents are unchanged.
```

- [ ] **Step 5: Run Task 2 tests and verify GREEN**

Run:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_catalog_repair.py tests/test_store_worker_claims.py -q
```

Expected: all focused tests pass.

- [ ] **Step 6: Commit Task 2**

```bash
git add orchestrator/catalog_repair.py tests/test_catalog_repair.py
git commit -m "feat: prepare publication-only canary"
```

### Task 3: Dedicated CLI command and exact-job integration

**Files:**
- Modify: `orchestrator/__main__.py`
- Modify: `tests/test_catalog_repair.py`
- Modify: `tests/test_mac_worker.py`
- Modify: `docs/setup/mac.md`

- [ ] **Step 1: Write failing parser and runner tests**

Add parser assertions for:

```python
args = build_parser().parse_args([
    "prepare-catalog-publication-canary",
    "--allowlist-file", "canary.txt",
    "--movie", "mist-166",
    "--limit", "1",
    "--confirm-job-id", "job_exact",
])
assert args.command == "prepare-catalog-publication-canary"
assert args.limit == 1
```

Test the runner with temporary settings/store and assert stdout contains only:

```text
prepared=true job_id=... movie=... prior_status=failed new_status=publish_pending translation_attempt_count=3 english_sha256=<64 hex> quality_passed=true cues=<number> unique_ratio=<number> known_bad=0
```

- [ ] **Step 2: Run parser/runner tests and verify RED**

Run:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_catalog_repair.py -k 'prepare_catalog_publication_cli' -q
```

Expected: FAIL because the subcommand is not registered.

- [ ] **Step 3: Wire the command**

Add `run_prepare_catalog_publication_canary` beside the historical preparation
runner. It must call `store.initialize()`, then the service, and print only receipt
fields with `english_unique_ratio` formatted to three decimals. Register all four
required arguments and dispatch the new command in `main()`.

Do not add `--force`, an optional job ID, an automatic selector, or a batch mode.

- [ ] **Step 4: Write a failing exact publication integration test**

Create two jobs. Prepare the first directly into `publish_pending`, leave the second
claimable, and use spies:

```python
translator = RecordingTranslator()
publisher = RecordingPublisher(verified_result)
worker = MacTranslationWorker(..., translator=translator, publisher=publisher)

assert worker.process_job_id(first.id) is True
assert translator.calls == []
assert publisher.movies == ["abc-021"]
assert store.get_job(first.id).status is JobStatus.ENGLISH_SRT_READY
assert store.get_job(second.id).status is original_second_status
```

Assert Japanese/audio hashes are unchanged and the accepted English file was not
moved into `rejected/`.

- [ ] **Step 5: Run the integration test and verify behavior**

Run:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_mac_worker.py -k 'publication_only_exact_job' -q
```

Expected: PASS using the existing exact-job publication path. If it fails because
the worker translates or claims another job, fix only that exact-job behavior and
add the failing assertion before modifying production code.

- [ ] **Step 6: Document the command**

Add this ordered runbook to `docs/setup/mac.md`:

```bash
python -m orchestrator prepare-catalog-publication-canary \
  --allowlist-file /absolute/path/catalog-canary-allowlist.txt \
  --movie mist-166 \
  --limit 1 \
  --confirm-job-id job_5ca44399d21c40168821397f10c04538
python -m orchestrator mac-translation-worker-once \
  --job-id job_5ca44399d21c40168821397f10c04538
```

State explicitly that preparation does not translate or call Supabase, the one-shot
worker reruns quality before upload, and any second job requires new approval.

- [ ] **Step 7: Run Task 3 tests and verify GREEN**

Run:

```bash
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest \
  tests/test_catalog_repair.py tests/test_mac_worker.py -q
```

Expected: all tests pass.

- [ ] **Step 8: Commit Task 3**

```bash
git add orchestrator/__main__.py tests/test_catalog_repair.py \
  tests/test_mac_worker.py docs/setup/mac.md
git commit -m "feat: add catalog publication canary command"
```

### Task 4: Full verification, review, deployment, and `mist-166`

**Files:**
- No new production code unless review finds a tested defect.
- Runtime evidence stays under ignored `reports/`; do not commit hashes or secrets.

- [ ] **Step 1: Run complete local verification**

Run:

```bash
git diff --check 911ccf85..HEAD
/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/.venv/bin/pytest -q
```

Expected: diff check clean and every test passes.

- [ ] **Step 2: Run two-stage review**

Dispatch a spec reviewer against
`docs/superpowers/specs/2026-07-13-catalog-publication-canary-design.md`, then a code
quality reviewer. Fix every Critical/Important finding with RED/GREEN tests and
repeat the affected review.

- [ ] **Step 3: Push the reviewed commits and update draft PR #2**

```bash
git push origin codex/metadata-resilient-publication
```

Expected: PR #2 contains the design, implementation, tests, and runbook.

- [ ] **Step 4: Integrate the reviewed code into the production checkout**

The LaunchAgent runs from
`/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator`. Before fast-forwarding its
current `codex/windows-transcription-mac-translation` branch, compare the untracked
plan file byte-for-byte with the tracked feature file. Preserve all unrelated
untracked reports and environments. Move only the identical conflicting plan to a
temporary backup, fast-forward, verify the tracked replacement has the same
SHA-256, then remove only that temporary backup.

Expected: no unrelated untracked file changes and production checkout HEAD equals
the reviewed feature HEAD.

- [ ] **Step 5: Restart only the Mac API and run health checks**

Use the existing `com.javsubtitle.orchestrator-api` LaunchAgent; do not touch the
Windows worker, downloader plist, Cloudflare tunnel, or unrelated audit process.
Require local `/dashboard` and `/dashboard/state` HTTP 200 and legacy `/complete`
HTTP 409 with a dummy non-production ID.

- [ ] **Step 6: Run the real smoke test**

From the production checkout and `.venv`:

```bash
python -m orchestrator mac-translation-smoke-test
```

Require exit 0, 10 English cues, unique ratio at least 0.5, and known_bad=0. If it
fails, do not prepare the canary.

- [ ] **Step 7: Capture pre-canary evidence**

Create a private mode-0600 one-line allowlist containing only `mist-166`. Record the
current SQLite row and SHA-256 of canonical Japanese, English, and `audio.wav` into
an ignored report without printing subtitle text. Confirm quality statistics are
passed and that the exact job is unclaimed.

- [ ] **Step 8: Prepare and publish exactly `mist-166`**

Run the two documented commands with exact job
`job_5ca44399d21c40168821397f10c04538`. Do not start the general translation worker
before the one-shot process exits and all verification succeeds.

- [ ] **Step 9: Verify local and Supabase evidence**

Require:

```text
quality.log final passed=true
Japanese SHA-256 unchanged
audio.wav SHA-256 unchanged
accepted English was not moved during preparation
Storage SHA-256 equals final local English SHA-256
movie_languages UUID/path/size matches the ensured catalog row/object
metadata source/status is valid (placeholder is allowed)
job status is english_srt_ready
translation_attempt_count remains 3
```

Use statistics/hashes only; never output subtitle text, service keys, or adult
metadata.

- [ ] **Step 10: Restart the normal translation worker only after success**

Confirm no existing duplicate process, then start one dedicated
`python -m orchestrator mac-translation-worker` process from the production checkout.
The downloader remains separate. Confirm the worker is idle/polling or handling a
normal new transcription job.

- [ ] **Step 11: Stop at the batch approval gate**

Report PR, commits, test/smoke results, ACL/advisor result, exact canary state
transition and hashes, catalog/Storage verification, and preservation of Japanese,
English, and audio. Do not process a second job or any historical batch without a
new explicit approval.
