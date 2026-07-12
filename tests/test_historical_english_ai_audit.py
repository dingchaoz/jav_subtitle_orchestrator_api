from __future__ import annotations

import pytest
import json
from pathlib import Path


def _timestamp(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},000"


def _srt(lines: list[str]) -> bytes:
    return "\n\n".join(
        (
            f"{index}\n"
            f"{_timestamp(index - 1)} --> {_timestamp(index)}\n"
            f"{line}"
        )
        for index, line in enumerate(lines, 1)
    ).encode("utf-8")


def test_inspector_accepts_diverse_valid_english():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    report = inspect_english_srt(
        _srt([f"Distinct sentence {index}" for index in range(1, 26)])
    )

    assert report.status == "passed"
    assert report.reason_codes == ()
    assert report.metrics["cue_count"] == 25
    assert report.metrics["unique_text_ratio"] == 1.0


def test_inspector_locks_refusal_threshold():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    report = inspect_english_srt(
        _srt(["Cannot translate"] * 3 + [f"Line {index}" for index in range(147)])
    )

    assert report.status == "hard_failure"
    assert "KNOWN_BAD_TRANSLATION" in report.reason_codes


def test_inspector_locks_dominant_line_threshold():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    report = inspect_english_srt(
        _srt(["Repeated output"] * 10 + [f"Line {index}" for index in range(10)])
    )

    assert "DOMINANT_TEXT_COLLAPSE" in report.reason_codes
    assert report.metrics["dominant_text_ratio"] == 0.5


def test_inspector_locks_low_diversity_threshold():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    report = inspect_english_srt(
        _srt(["Same output"] * 25 + [f"Variant {index % 10}" for index in range(75)])
    )

    assert "LOW_DIVERSITY_COLLAPSE" in report.reason_codes
    assert report.metrics["unique_text_ratio"] < 0.15


def test_inspector_rejects_empty_invalid_and_corrupted_srt_without_text():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    empty = inspect_english_srt(b"")
    invalid = inspect_english_srt(b"not an srt")
    mojibake = inspect_english_srt(_srt(["synthetic \ufffd \ufffd \ufffd"] * 5))

    assert empty.reason_codes == ("EMPTY_FILE", "NO_VALID_CUES")
    assert invalid.reason_codes == ("NO_VALID_CUES",)
    assert "SEVERE_MOJIBAKE" in mojibake.reason_codes
    assert "dominant_normalized_text" not in mojibake.metrics
    assert "synthetic" not in repr(mojibake)


def test_inspector_rejects_invalid_timeline_beyond_locked_tolerance():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    data = (
        "1\n00:00:10,000 --> 00:00:09,000\nFirst\n\n"
        "2\n00:00:01,000 --> 00:00:02,000\nSecond\n"
    ).encode("utf-8")

    report = inspect_english_srt(data)

    assert "INVALID_TIMELINE" in report.reason_codes


class _FakeResponse:
    def __init__(
        self,
        payload=None,
        *,
        status_code: int = 200,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}
        self.closed = False

    def json(self):
        return self._payload

    def iter_content(self, chunk_size: int):
        for offset in range(0, len(self._body), chunk_size):
            yield self._body[offset : offset + chunk_size]

    def close(self):
        self.closed = True


class _GetOnlySession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = iter(responses)
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        return next(self._responses)


def _catalog_row(subtitle_id: str) -> dict[str, object]:
    return {
        "id": subtitle_id,
        "movie_id": "10000000-0000-0000-0000-000000000001",
        "language": "English_AI",
        "file_path": "aa/aa-001/aa-001-English_AI.srt",
        "file_size": 123,
        "movies": {"standard_movie_id": "aa-001"},
    }


def test_reader_filters_exact_english_ai_and_uses_get_only():
    from orchestrator.historical_english_ai_audit import SupabaseEnglishAiReader

    subtitle_id = "00000000-0000-0000-0000-000000000001"
    session = _GetOnlySession(
        [_FakeResponse([_catalog_row(subtitle_id)]), _FakeResponse([])]
    )
    reader = SupabaseEnglishAiReader(
        "https://example.supabase.co",
        "SERVICE-SECRET",
        session=session,
        page_size=1,
        rate_limiter=None,
    )

    rows = list(reader.iter_catalog())

    assert len(rows) == 1
    assert rows[0].movie_code == "aa-001"
    first_params = session.calls[0][1]["params"]
    assert first_params["language"] == "eq.English_AI"
    assert first_params["limit"] == "1"
    assert first_params["order"] == "id.asc"
    assert session.calls[1][1]["params"]["id"] == f"gt.{subtitle_id}"
    assert all(call[1]["allow_redirects"] is False for call in session.calls)


def test_storage_download_is_streamed_bounded_and_path_encoded():
    from orchestrator.historical_english_ai_audit import SupabaseEnglishAiReader

    response = _FakeResponse(
        body=b"abc", headers={"Content-Length": "3", "Content-Encoding": "identity"}
    )
    session = _GetOnlySession([response])
    reader = SupabaseEnglishAiReader(
        "https://example.supabase.co", "SERVICE-SECRET", session=session
    )

    data = reader.download_object("aa/folder name/subtitle.srt", max_bytes=3)

    assert data == b"abc"
    assert "/storage/v1/object/subtitles/aa/folder%20name/subtitle.srt" in session.calls[0][0]
    assert session.calls[0][1]["stream"] is True
    assert session.calls[0][1]["headers"]["Accept-Encoding"] == "identity"
    assert response.closed is True


@pytest.mark.parametrize(
    "response",
    [
        _FakeResponse(body=b"abcd", headers={"Content-Length": "4"}),
        _FakeResponse(body=b"abcd"),
    ],
)
def test_storage_download_rejects_declared_and_streamed_oversize(response):
    from orchestrator.historical_english_ai_audit import (
        ObjectLimitExceeded,
        SupabaseEnglishAiReader,
    )

    reader = SupabaseEnglishAiReader(
        "https://example.supabase.co",
        "SERVICE-SECRET",
        session=_GetOnlySession([response]),
    )

    with pytest.raises(ObjectLimitExceeded, match="exceeds configured byte limit"):
        reader.download_object("safe.srt", max_bytes=3)

    assert response.closed is True


def test_reader_reports_missing_object_without_path_or_secret():
    from orchestrator.historical_english_ai_audit import (
        StorageObjectMissing,
        SupabaseEnglishAiReader,
    )

    reader = SupabaseEnglishAiReader(
        "https://example.supabase.co",
        "SERVICE-SECRET",
        session=_GetOnlySession([_FakeResponse(status_code=404)]),
    )

    with pytest.raises(StorageObjectMissing) as raised:
        reader.download_object("private/movie/subtitle.srt")

    assert "private/movie" not in str(raised.value)
    assert "SERVICE-SECRET" not in str(raised.value)


def test_reader_rejects_malformed_catalog_and_sanitizes_http_failure():
    from orchestrator.historical_english_ai_audit import SupabaseEnglishAiReader

    malformed = SupabaseEnglishAiReader(
        "https://example.supabase.co",
        "SERVICE-SECRET",
        session=_GetOnlySession([_FakeResponse({"unexpected": True})]),
    )
    with pytest.raises(ValueError, match="catalog payload"):
        list(malformed.iter_catalog())

    failed = SupabaseEnglishAiReader(
        "https://example.supabase.co",
        "SERVICE-SECRET",
        session=_GetOnlySession([_FakeResponse(status_code=500)]),
    )
    with pytest.raises(RuntimeError) as raised:
        list(failed.iter_catalog())
    assert "500" in str(raised.value)
    assert "SERVICE-SECRET" not in str(raised.value)


def test_request_rate_limiter_serializes_global_rate():
    from orchestrator.historical_english_ai_audit import RequestRateLimiter

    now = [10.0]
    sleeps: list[float] = []

    def clock() -> float:
        return now[0]

    def sleeper(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    limiter = RequestRateLimiter(2.0, clock=clock, sleeper=sleeper)
    limiter.acquire()
    limiter.acquire()

    assert sleeps == [0.5]


def _catalog_record(subtitle_id: str, movie_code: str, *, suffix: str = ""):
    from orchestrator.historical_english_ai_audit import CatalogRecord

    return CatalogRecord(
        subtitle_id=subtitle_id,
        movie_id="10000000-0000-0000-0000-000000000001",
        movie_code=movie_code,
        language="English_AI",
        file_path=f"audit/{movie_code}{suffix}.srt",
        file_size=123,
    )


class _FakeAuditReader:
    def __init__(self, records, objects, *, secret: str = "SERVICE-SECRET") -> None:
        self.records = records
        self.objects = objects
        self.downloaded_paths: list[str] = []
        self._service_role_key = secret

    def iter_catalog(self, *, limit=None):
        records = self.records if limit is None else self.records[:limit]
        yield from records

    def download_object(self, path, *, max_bytes):
        self.downloaded_paths.append(path)
        value = self.objects[path]
        if isinstance(value, BaseException):
            raise value
        return value


def test_scan_resumes_terminal_ids_and_writes_text_free_reports(tmp_path: Path):
    from orchestrator.historical_english_ai_audit import LocalEnglishAiAuditRunner

    first = _catalog_record("00000000-0000-0000-0000-000000000001", "ok-001")
    bad = _catalog_record("00000000-0000-0000-0000-000000000002", "bad-002")
    reader = _FakeAuditReader(
        [first, bad],
        {
            first.file_path: _srt([f"Safe line {index}" for index in range(25)]),
            bad.file_path: _srt(["Cannot translate"] * 20),
        },
    )

    preflight = LocalEnglishAiAuditRunner(reader, workers=1).scan(tmp_path, limit=1)
    summary = LocalEnglishAiAuditRunner(reader, workers=2).scan(tmp_path)

    assert preflight.complete is False
    assert preflight.bounded is True
    assert summary.complete is True
    assert summary.bounded is False
    assert summary.discovered == 2
    assert summary.hard_failure == 1
    assert summary.skipped == 1
    assert reader.downloaded_paths == [first.file_path, bad.file_path]
    assert (tmp_path / "repair-allowlist.txt").read_text() == "bad-002\n"
    combined = "".join(
        path.read_text() for path in tmp_path.iterdir() if path.is_file()
    )
    assert "Cannot translate" not in combined
    assert "SERVICE-SECRET" not in combined


def test_scan_marks_missing_as_hard_failure_without_exposing_path(tmp_path: Path):
    from orchestrator.historical_english_ai_audit import (
        LocalEnglishAiAuditRunner,
        StorageObjectMissing,
    )

    missing = _catalog_record(
        "00000000-0000-0000-0000-000000000001", "gone-001"
    )
    reader = _FakeAuditReader(
        [missing], {missing.file_path: StorageObjectMissing("private subtitle path")}
    )

    summary = LocalEnglishAiAuditRunner(reader).scan(tmp_path)
    row = json.loads((tmp_path / "audit-results.jsonl").read_text())

    assert summary.complete is True
    assert summary.hard_failure == 1
    assert row["reason_codes"] == ["STORAGE_OBJECT_MISSING"]
    assert "private subtitle path" not in json.dumps(row)


def test_scan_sanitizes_per_object_error_and_excludes_it_from_allowlist(tmp_path: Path):
    from orchestrator.historical_english_ai_audit import LocalEnglishAiAuditRunner

    record = _catalog_record(
        "00000000-0000-0000-0000-000000000001", "error-001"
    )
    secret = "SERVICE-SECRET"
    reader = _FakeAuditReader(
        [record],
        {
            record.file_path: RuntimeError(
                f"Authorization: Bearer {secret}\nupstream failed"
            )
        },
        secret=secret,
    )

    summary = LocalEnglishAiAuditRunner(reader).scan(tmp_path)
    row = json.loads((tmp_path / "audit-results.jsonl").read_text())

    assert summary.errors == 1
    assert row["status"] == "error"
    assert secret not in row["error"]
    assert "Authorization" not in row["error"]
    assert "\n" not in row["error"]
    assert (tmp_path / "repair-allowlist.txt").read_text() == ""


def test_scan_records_catalog_failure_as_partial_summary(tmp_path: Path):
    from orchestrator.historical_english_ai_audit import LocalEnglishAiAuditRunner

    record = _catalog_record(
        "00000000-0000-0000-0000-000000000001", "partial-001"
    )

    class CatalogFailingReader(_FakeAuditReader):
        def iter_catalog(self, *, limit=None):
            yield record
            raise RuntimeError("Authorization: Bearer SERVICE-SECRET\ncatalog failed")

    reader = CatalogFailingReader(
        [record], {record.file_path: _srt([f"Line {index}" for index in range(25)])}
    )

    summary = LocalEnglishAiAuditRunner(reader).scan(tmp_path)

    assert summary.complete is False
    assert summary.bounded is False
    assert summary.catalog_error is not None
    assert "SERVICE-SECRET" not in summary.catalog_error
    on_disk = json.loads((tmp_path / "audit-summary.json").read_text())
    assert on_disk["complete"] is False


@pytest.mark.parametrize(
    "checkpoint",
    [
        '{"subtitle_id":',
        "{}\n",
    ],
)
def test_checkpoint_rejects_truncated_or_incomplete_rows(tmp_path: Path, checkpoint):
    from orchestrator.historical_english_ai_audit import load_checkpoint

    path = tmp_path / "audit-results.jsonl"
    path.write_text(checkpoint)

    with pytest.raises(ValueError, match="checkpoint line"):
        load_checkpoint(path)


def test_checkpoint_rejects_duplicate_subtitle_ids(tmp_path: Path):
    from orchestrator.historical_english_ai_audit import (
        LocalEnglishAiAuditRunner,
        load_checkpoint,
    )

    record = _catalog_record(
        "00000000-0000-0000-0000-000000000001", "duplicate-001"
    )
    reader = _FakeAuditReader(
        [record], {record.file_path: _srt([f"Line {index}" for index in range(25)])}
    )
    LocalEnglishAiAuditRunner(reader).scan(tmp_path)
    path = tmp_path / "audit-results.jsonl"
    path.write_text(path.read_text() * 2)

    with pytest.raises(ValueError, match="duplicate subtitle_id"):
        load_checkpoint(path)


def test_resume_rejects_catalog_identity_change(tmp_path: Path):
    from orchestrator.historical_english_ai_audit import LocalEnglishAiAuditRunner

    original = _catalog_record(
        "00000000-0000-0000-0000-000000000001", "identity-001"
    )
    first_reader = _FakeAuditReader(
        [original], {original.file_path: _srt([f"Line {index}" for index in range(25)])}
    )
    LocalEnglishAiAuditRunner(first_reader).scan(tmp_path, limit=1)
    changed = _catalog_record(
        original.subtitle_id, original.movie_code, suffix="-changed"
    )
    changed_reader = _FakeAuditReader(
        [changed], {changed.file_path: _srt([f"Line {index}" for index in range(25)])}
    )

    with pytest.raises(ValueError, match="catalog identity changed"):
        LocalEnglishAiAuditRunner(changed_reader).scan(tmp_path)
