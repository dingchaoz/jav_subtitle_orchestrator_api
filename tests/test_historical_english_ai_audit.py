from __future__ import annotations

import pytest


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
