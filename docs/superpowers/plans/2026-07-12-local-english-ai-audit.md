# Local English_AI Audit Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a resumable, GET-only local audit of every Supabase `English_AI` subtitle, producing a text-free hard-failure allowlist without changing Supabase, job state, audio, or subtitle objects.

**Architecture:** Add one focused module containing a bounded GET-only Supabase reader, a pure historical English SRT inspector, and a resumable report runner. Wire it to a new CLI command using existing Mac settings; keep the production Japanese/English pair gate unchanged. Verify with synthetic fixtures and fake sessions before a one-record preflight and the already-authorized full read-only scan.

**Tech Stack:** Python 3.11+, `requests`, `argparse`, standard-library SRT parsing/CSV/JSON/hashing/concurrency, `pytest`.

---

## File map

- Create `orchestrator/historical_english_ai_audit.py`: catalog models, GET-only reader, bounded Storage download, pure English SRT inspection, resume checkpoint validation, deterministic report generation, and scan orchestration.
- Modify `orchestrator/__main__.py`: add `audit-english-ai-local`, validate arguments, construct the reader/runner, and print sanitized progress/completion information.
- Modify `orchestrator/config.py`: add non-secret defaults for bucket and audit HTTP timeout while continuing to source the existing Supabase URL/service key from `.env`.
- Create `tests/test_historical_english_ai_audit.py`: pure threshold, GET-only, bounds, redaction, resume, completeness, report, and CLI tests using synthetic non-sensitive subtitles.
- Modify `docs/setup/mac.md`: document the read-only command, files produced, resume semantics, and the explicit separation between auditing and repair/publication.

### Task 1: Lock the historical hard-failure inspector

**Files:**
- Create: `orchestrator/historical_english_ai_audit.py`
- Create: `tests/test_historical_english_ai_audit.py`

- [ ] **Step 1: Write failing tests for the pure inspector and locked thresholds**

Add synthetic SRT builders and assertions for these exact reason codes:

```python
from orchestrator.historical_english_ai_audit import inspect_english_srt


def srt(lines: list[str]) -> bytes:
    return "\n\n".join(
        f"{index}\n00:00:{index - 1:02d},000 --> 00:00:{index:02d},000\n{line}"
        for index, line in enumerate(lines, 1)
    ).encode()


def test_inspector_accepts_diverse_valid_english():
    report = inspect_english_srt(srt([f"Distinct sentence {i}" for i in range(1, 26)]))
    assert report.status == "passed"
    assert report.reason_codes == ()
    assert report.metrics["cue_count"] == 25


def test_inspector_locks_historical_collapse_thresholds():
    refusal = inspect_english_srt(srt(["Cannot translate"] * 3 + [f"Line {i}" for i in range(147)]))
    dominant = inspect_english_srt(srt(["Repeated output"] * 10 + [f"Line {i}" for i in range(10)]))
    low_diversity = inspect_english_srt(srt(["Same output"] * 25 + [f"Variant {i % 10}" for i in range(75)]))
    assert "KNOWN_BAD_TRANSLATION" in refusal.reason_codes
    assert "DOMINANT_TEXT_COLLAPSE" in dominant.reason_codes
    assert "LOW_DIVERSITY_COLLAPSE" in low_diversity.reason_codes


def test_inspector_rejects_invalid_or_corrupted_srt_without_returning_text():
    empty = inspect_english_srt(b"")
    invalid = inspect_english_srt(b"not an srt")
    mojibake = inspect_english_srt(srt(["bad \ufffd \ufffd \ufffd"] * 5))
    assert empty.reason_codes == ("EMPTY_FILE", "NO_VALID_CUES")
    assert invalid.reason_codes == ("NO_VALID_CUES",)
    assert "SEVERE_MOJIBAKE" in mojibake.reason_codes
    assert "dominant_normalized_text" not in mojibake.metrics
    assert "bad" not in repr(mojibake)
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `pytest -q tests/test_historical_english_ai_audit.py`

Expected: collection fails with `ModuleNotFoundError: orchestrator.historical_english_ai_audit`.

- [ ] **Step 3: Implement the bounded parser and inspector**

Create these public immutable types and entry point:

```python
@dataclass(frozen=True, slots=True)
class LocalInspection:
    status: str
    reason_codes: tuple[str, ...]
    metrics: dict[str, int | float | str | bool | None]


def inspect_english_srt(data: bytes) -> LocalInspection:
    if len(data) > MAX_OBJECT_BYTES:
        raise ObjectLimitExceeded("storage object exceeds 33554432 bytes")
    inspection = _inspect_srt_bytes(data)
    reasons: list[str] = []
    if not data:
        reasons.append("EMPTY_FILE")
    if inspection.cue_count == 0:
        reasons.append("NO_VALID_CUES")
    if inspection.cue_count and (
        inspection.invalid_interval_count / inspection.cue_count > 0.01
        or inspection.timeline_regression_count > 0
    ):
        reasons.append("INVALID_TIMELINE")
    if (
        inspection.replacement_character_count >= 2
        or inspection.nul_count >= 2
        or inspection.control_character_count >= 3
        or inspection.mojibake_marker_count >= 3
    ):
        reasons.append("SEVERE_MOJIBAKE")
    if inspection.known_bad_occurrence_count >= 3 and (
        inspection.known_bad_occurrence_count / max(inspection.text_line_count, 1) >= 0.02
    ):
        reasons.append("KNOWN_BAD_TRANSLATION")
    if inspection.text_line_count >= 20 and inspection.dominant_text_ratio >= 0.50:
        reasons.append("DOMINANT_TEXT_COLLAPSE")
    if (
        inspection.text_line_count >= 100
        and inspection.unique_text_ratio < 0.15
        and inspection.dominant_text_ratio >= 0.25
    ):
        reasons.append("LOW_DIVERSITY_COLLAPSE")
    return LocalInspection(
        status="hard_failure" if reasons else "passed",
        reason_codes=tuple(reasons),
        metrics=_safe_metrics(inspection),
    )
```

Port the bounded decoding and strict/tolerant parsing primitives from the historical `srt_inspection.py`, retaining these limits: 40,000 logical lines, 10,000 cues, 20,000 text lines, 16,384 characters per line, and 10,000 unique normalized lines. Hash the dominant normalized line with SHA-256 into `dominant_text_sha256`; never expose the normalized line itself. Use the historical known-bad phrase tuple and count phrase occurrences. Treat an interval with `end <= start` as invalid and a start-time regression greater than two seconds as a regression.

- [ ] **Step 4: Run inspector tests and verify GREEN**

Run: `pytest -q tests/test_historical_english_ai_audit.py`

Expected: all Task 1 tests pass.

- [ ] **Step 5: Commit the inspector**

```bash
git add orchestrator/historical_english_ai_audit.py tests/test_historical_english_ai_audit.py
git commit -m "feat: add historical English subtitle inspector"
```

### Task 2: Add the bounded GET-only Supabase reader

**Files:**
- Modify: `orchestrator/historical_english_ai_audit.py`
- Modify: `tests/test_historical_english_ai_audit.py`

- [ ] **Step 1: Write failing reader tests**

Add a fake session that deliberately has no generic `request`, `post`, `put`, `patch`, or `delete` implementation:

```python
class GetOnlySession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


def test_reader_filters_exact_english_ai_and_uses_get_only():
    session = GetOnlySession([
        FakeResponse([catalog_row("00000000-0000-0000-0000-000000000001")]),
        FakeResponse([]),
    ])
    reader = SupabaseEnglishAiReader(
        "https://example.supabase.co", "SERVICE-SECRET", session=session, page_size=1
    )
    rows = list(reader.iter_catalog())
    assert len(rows) == 1
    assert session.calls[0][1]["params"]["language"] == "eq.English_AI"
    assert session.calls[0][1]["params"]["limit"] == "1"
    assert all(call[1]["allow_redirects"] is False for call in session.calls)


def test_storage_download_is_streamed_and_bounded():
    session = GetOnlySession([FakeStreamResponse(b"abc")])
    reader = SupabaseEnglishAiReader(
        "https://example.supabase.co", "SERVICE-SECRET", session=session
    )
    assert reader.download_object("aa/aa-001/aa-001-English_AI.srt", max_bytes=3) == b"abc"
    assert "/storage/v1/object/subtitles/aa/aa-001/aa-001-English_AI.srt" in session.calls[0][0]


def test_reader_rejects_redirect_oversize_and_malformed_catalog_payload():
    with pytest.raises(ObjectLimitExceeded):
        SupabaseEnglishAiReader(
            "https://example.supabase.co", "key",
            session=GetOnlySession([FakeStreamResponse(b"abcd", content_length=4)]),
        ).download_object("safe.srt", max_bytes=3)
    with pytest.raises(ValueError, match="catalog payload"):
        list(SupabaseEnglishAiReader(
            "https://example.supabase.co", "key",
            session=GetOnlySession([FakeResponse({"unexpected": True})]),
        ).iter_catalog())
```

- [ ] **Step 2: Run the reader tests and verify RED**

Run: `pytest -q tests/test_historical_english_ai_audit.py -k reader`

Expected: failures report that `SupabaseEnglishAiReader` is undefined.

- [ ] **Step 3: Implement the GET-only reader**

Add:

```python
class StorageObjectMissing(FileNotFoundError):
    pass


class ObjectLimitExceeded(RuntimeError):
    pass


class RequestRateLimiter:
    def __init__(self, requests_per_second: float, *, clock=time.monotonic, sleeper=time.sleep):
        if not 0 < requests_per_second <= 10:
            raise ValueError("requests_per_second must be greater than 0 and at most 10")
        self._interval = 1.0 / requests_per_second
        self._clock = clock
        self._sleeper = sleeper
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            wait_seconds = max(0.0, self._next_at - now)
            if wait_seconds:
                self._sleeper(wait_seconds)
                now = self._clock()
            self._next_at = max(now, self._next_at) + self._interval


@dataclass(frozen=True, slots=True)
class CatalogRecord:
    subtitle_id: str
    movie_id: str
    movie_code: str
    language: str
    file_path: str
    file_size: int


class SupabaseEnglishAiReader:
    def __init__(self, url, service_role_key, *, bucket="subtitles", page_size=500,
                 timeout_seconds=30, session=None, rate_limiter=None):
        if not 1 <= page_size <= 500:
            raise ValueError("page_size must be between 1 and 500")
        if not service_role_key:
            raise ValueError("Supabase service role key is required")
        self.url = url.rstrip("/")
        self._key = service_role_key
        self.bucket = _safe_bucket(bucket)
        self.page_size = page_size
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()
        self.rate_limiter = rate_limiter or RequestRateLimiter(2.0)

    def iter_catalog(self, *, limit=None):
        emitted = 0
        last_id = None
        while limit is None or emitted < limit:
            page_limit = min(self.page_size, limit - emitted) if limit is not None else self.page_size
            params = {
                "select": "id,movie_id,language,file_path,file_size,movies!inner(standard_movie_id)",
                "language": "eq.English_AI",
                "order": "id.asc",
                "limit": str(page_limit),
            }
            if last_id is not None:
                params["id"] = f"gt.{last_id}"
            payload = self._get_json("/rest/v1/movie_languages", params=params)
            page = _parse_catalog_page(payload, page_limit, last_id)
            for record in page:
                yield record
                emitted += 1
                last_id = record.subtitle_id
            if len(page) < page_limit:
                return

    def download_object(self, file_path, *, max_bytes=32 * 1024 * 1024):
        encoded = quote(_safe_object_path(file_path), safe="/")
        bucket = quote(self.bucket, safe="")
        response = self._get(
            f"/storage/v1/object/{bucket}/{encoded}", stream=True,
            headers={"Accept-Encoding": "identity"},
        )
        if response.status_code == 404:
            raise StorageObjectMissing("storage object missing")
        _require_2xx(response)
        return _read_bounded_body(response, max_bytes)
```

All requests must go through a private `_get` that calls only `session.get`, adds `apikey`/Bearer headers, applies the shared rate limiter, sets the timeout and `allow_redirects=False`, and emits sanitized errors containing only operation and status code. Validate UUIDs, exact `English_AI`, bounded strings, nonnegative file sizes, inner movie relation, page order, bucket, and object paths. Reject non-identity `Content-Encoding`, declared oversize bodies, and streamed bodies once the accumulated bytes exceed the limit.

Add a deterministic rate-limiter test with an injected fake clock/sleeper and two
threads; assert the recorded acquisition times are at least `0.5` seconds apart at
the default two requests per second. This proves catalog and Storage GETs share one
global rate rather than each worker receiving its own allowance.

- [ ] **Step 4: Run reader and inspector tests**

Run: `pytest -q tests/test_historical_english_ai_audit.py`

Expected: all tests pass.

- [ ] **Step 5: Commit the reader**

```bash
git add orchestrator/historical_english_ai_audit.py tests/test_historical_english_ai_audit.py
git commit -m "feat: add GET-only English AI reader"
```

### Task 3: Add resumable scanning and deterministic text-free reports

**Files:**
- Modify: `orchestrator/historical_english_ai_audit.py`
- Modify: `tests/test_historical_english_ai_audit.py`

- [ ] **Step 1: Write failing runner/report tests**

```python
def test_scan_resumes_terminal_ids_and_writes_text_free_reports(tmp_path):
    first = catalog_record("00000000-0000-0000-0000-000000000001", "ok-001")
    bad = catalog_record("00000000-0000-0000-0000-000000000002", "bad-002")
    checkpoint = tmp_path / "audit-results.jsonl"
    checkpoint.write_text(json.dumps(terminal_row(first, status="passed")) + "\n")
    reader = FakeReader([first, bad], {bad.file_path: srt(["Cannot translate"] * 20)})
    summary = LocalEnglishAiAuditRunner(reader, workers=2).scan(tmp_path)
    assert reader.downloaded_paths == [bad.file_path]
    assert summary.discovered == 2
    assert summary.hard_failure == 1
    assert summary.complete is True
    assert (tmp_path / "repair-allowlist.txt").read_text() == "bad-002\n"
    combined = "".join(path.read_text() for path in tmp_path.iterdir() if path.is_file())
    assert "Cannot translate" not in combined


def test_scan_marks_missing_and_errors_terminal_but_partial_on_catalog_failure(tmp_path):
    missing = catalog_record("00000000-0000-0000-0000-000000000001", "gone-001")
    reader = FakeReader([missing], {missing.file_path: StorageObjectMissing("missing")})
    summary = LocalEnglishAiAuditRunner(reader).scan(tmp_path)
    assert summary.hard_failure == 1
    assert json.loads((tmp_path / "audit-results.jsonl").read_text())["reason_codes"] == ["STORAGE_OBJECT_MISSING"]
    broken = CatalogFailingReader()
    partial = LocalEnglishAiAuditRunner(broken).scan(tmp_path / "partial")
    assert partial.complete is False


def test_checkpoint_rejects_truncation_duplicates_identity_change_and_secrets(tmp_path):
    checkpoint = tmp_path / "audit-results.jsonl"
    checkpoint.write_text('{"subtitle_id":')
    with pytest.raises(ValueError, match="checkpoint line"):
        load_checkpoint(checkpoint)
    secret = "SERVICE-SECRET"
    assert secret not in sanitize_error(RuntimeError(f"Authorization: Bearer {secret}"), secret)
```

- [ ] **Step 2: Run runner tests and verify RED**

Run: `pytest -q tests/test_historical_english_ai_audit.py -k "scan or checkpoint"`

Expected: failures report that runner/checkpoint functions are undefined.

- [ ] **Step 3: Implement result records, checkpoint validation, and scan orchestration**

Use these stable row fields:

```python
REPORT_FIELDS = (
    "subtitle_id", "movie_id", "movie_code", "language", "file_path", "file_size",
    "status", "reason_codes", "content_sha256", "byte_count", "encoding", "parse_mode",
    "cue_count", "text_line_count", "text_character_count", "unique_text_count",
    "unique_text_ratio", "dominant_text_sha256", "dominant_text_count",
    "dominant_text_ratio", "invalid_interval_count", "timeline_regression_count",
    "replacement_character_count", "nul_count", "control_character_count",
    "mojibake_marker_count", "error",
)


@dataclass(frozen=True, slots=True)
class AuditSummary:
    discovered: int
    passed: int
    hard_failure: int
    errors: int
    skipped: int
    reason_counts: dict[str, int]
    complete: bool
    bounded: bool
    catalog_error: str | None


class LocalEnglishAiAuditRunner:
    def __init__(self, reader, *, workers=4, max_object_bytes=32 * 1024 * 1024):
        if not 1 <= workers <= 4:
            raise ValueError("workers must be between 1 and 4")
        self.reader = reader
        self.workers = workers
        self.max_object_bytes = max_object_bytes

    def inspect_record(self, record):
        try:
            data = self.reader.download_object(record.file_path, max_bytes=self.max_object_bytes)
            inspection = inspect_english_srt(data)
            return _result_row(record, inspection, hashlib.sha256(data).hexdigest())
        except StorageObjectMissing:
            return _hard_failure_row(record, "STORAGE_OBJECT_MISSING")
        except Exception as exc:
            return _error_row(record, sanitize_error(exc, self.reader.service_key_for_redaction))
```

`scan(output_dir, limit=None)` must create the directory with mode `0700`, validate every existing JSONL line against `REPORT_FIELDS`, retain a `subtitle_id -> identity tuple` map, skip only terminal matching identities, and append each new row with `flush()` plus `os.fsync()`. Use at most four threads for object inspection while preserving deterministic catalog/result ordering. Catch per-object errors into sanitized terminal `error` rows; catch catalog traversal failure at the scan boundary, store only its sanitized message in `catalog_error`, and set `complete=false`. After checkpointing, atomically derive CSV, summary JSON, and the sorted unique movie-code allowlist from `hard_failure` rows only. Set `bounded=true` when a non-null limit intentionally stops traversal. Set `complete=true` only when catalog iteration exhausted without a limit or catalog error and every discovered ID has one terminal row.

- [ ] **Step 4: Run focused tests**

Run: `pytest -q tests/test_historical_english_ai_audit.py`

Expected: all tests pass and no report contains synthetic subtitle text or service keys.

- [ ] **Step 5: Commit resumable reports**

```bash
git add orchestrator/historical_english_ai_audit.py tests/test_historical_english_ai_audit.py
git commit -m "feat: add resumable local audit reports"
```

### Task 4: Wire the CLI and safe configuration

**Files:**
- Modify: `orchestrator/config.py`
- Modify: `orchestrator/__main__.py`
- Modify: `tests/test_historical_english_ai_audit.py`
- Modify: `docs/setup/mac.md`

- [ ] **Step 1: Write failing CLI/configuration tests**

```python
def test_cli_parser_exposes_only_read_only_audit_flags(monkeypatch, tmp_path):
    parser = build_parser()
    args = parser.parse_args([
        "audit-english-ai-local", "--output", str(tmp_path), "--limit", "1",
        "--workers", "2", "--requests-per-second", "2",
    ])
    assert args.command == "audit-english-ai-local"
    assert not hasattr(args, "persist")
    assert not hasattr(args, "apply")
    assert not hasattr(args, "force")
    assert not hasattr(args, "upload")


def test_cli_refuses_missing_supabase_credentials(monkeypatch, tmp_path):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    with pytest.raises(SystemExit, match="SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY"):
        run_local_english_ai_audit(output=tmp_path, limit=1, workers=1, requests_per_second=2)
```

- [ ] **Step 2: Run CLI tests and verify RED**

Run: `pytest -q tests/test_historical_english_ai_audit.py -k cli`

Expected: failures report missing parser/runner wiring.

- [ ] **Step 3: Refactor parser construction and add the command**

Extract `build_parser() -> argparse.ArgumentParser` from `main()` without changing existing commands. Add:

```python
audit_parser = subcommands.add_parser(
    "audit-english-ai-local",
    help="GET-only local audit of exact English_AI catalog subtitles",
)
audit_parser.add_argument("--output", type=Path, required=True)
audit_parser.add_argument("--limit", type=int)
audit_parser.add_argument("--workers", type=int, choices=range(1, 5), default=4)
audit_parser.add_argument("--requests-per-second", type=float, default=2.0)
```

Implement `run_local_english_ai_audit(...)` to load `MacSettings`, require both credentials, construct one shared `RequestRateLimiter`, reader, and runner, execute the scan, and print only counts/output paths/resume command. An intentionally bounded `--limit` run exits zero with `bounded=true` and `complete=false`; exit nonzero only when `summary.catalog_error` is non-null or local setup/report validation raises. Do not add persistence, apply, force, upload, requeue, or deletion arguments.

Add these settings:

```python
supabase_subtitle_bucket: str = Field(default="subtitles", alias="SUPABASE_SUBTITLE_BUCKET")
local_audit_timeout_seconds: int = Field(default=30, ge=1, le=120, alias="LOCAL_AUDIT_TIMEOUT_SECONDS")
```

- [ ] **Step 4: Document safe operation**

Add a “Historical English_AI read-only audit” section to `docs/setup/mac.md` showing:

```bash
python -m orchestrator audit-english-ai-local \
  --output reports/subtitle-audit/english-ai-local-20260712 \
  --limit 1 \
  --workers 1
```

State explicitly that rerunning the same command resumes JSONL; the full run omits `--limit`; the command never writes Supabase; and `repair-allowlist.txt` is a candidate list, not authorization to repair or publish.

- [ ] **Step 5: Run focused and full tests**

Run: `pytest -q tests/test_historical_english_ai_audit.py`

Expected: all focused tests pass.

Run: `pytest -q`

Expected: the full suite passes with no new failures.

- [ ] **Step 6: Commit CLI and documentation**

```bash
git add orchestrator/config.py orchestrator/__main__.py tests/test_historical_english_ai_audit.py docs/setup/mac.md
git commit -m "feat: expose GET-only English AI audit"
```

### Task 5: Verify safety, run the approved read-only audit, and report counts

**Files:**
- Local output only: `reports/subtitle-audit/english-ai-local-20260712/`

- [ ] **Step 1: Verify the command surface contains no mutation flag**

Run: `python -m orchestrator audit-english-ai-local --help`

Expected: help contains `--output`, `--limit`, `--workers`, and `--requests-per-second`; it contains no `persist`, `apply`, `force`, `upload`, `overwrite`, or `requeue` option.

- [ ] **Step 2: Run final static and test verification**

Run: `git diff --check && pytest -q`

Expected: `git diff --check` is silent and the full suite passes.

- [ ] **Step 3: Run the one-record GET-only preflight**

Run:

```bash
python -m orchestrator audit-english-ai-local \
  --output reports/subtitle-audit/english-ai-local-20260712 \
  --limit 1 \
  --workers 1 \
  --requests-per-second 2
```

Expected: exit 0; exactly one terminal JSONL row; no subtitle text or key in any report; summary is partial because `--limit 1` intentionally bounds catalog traversal; no Supabase mutation and no local job/audio changes.

- [ ] **Step 4: Inspect preflight report shape without printing subtitle text**

Run:

```bash
jq '{discovered, passed, hard_failure, errors, skipped, reason_counts, complete, bounded, catalog_error}' \
  reports/subtitle-audit/english-ai-local-20260712/audit-summary.json
```

Expected: only counts and reason codes are printed. If the row is an error, fix only the local reader/validation defect and rerun the unit/full suites before repeating the preflight.

- [ ] **Step 5: Resume as the approved full GET-only audit**

Run:

```bash
python -m orchestrator audit-english-ai-local \
  --output reports/subtitle-audit/english-ai-local-20260712 \
  --workers 4 \
  --requests-per-second 2
```

Expected: exit 0, `complete=true`, catalog exhausted, and every exact `English_AI` row represented once. This command performs GET requests and local report writes only.

- [ ] **Step 6: Produce the decision report without repair actions**

Report the exact discovered count, passed count, hard-failure count, error count, reason-code counts, allowlist count, tests result, and output directory. Recommend one canary from the allowlist using sanitized movie code/ID only. Do not translate, quarantine, upload, overwrite, requeue, reset a job, delete audio, or invoke `force=True`.
