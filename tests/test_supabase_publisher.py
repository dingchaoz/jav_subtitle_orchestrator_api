from pathlib import Path

from orchestrator.supabase_publisher import (
    AI_SUBTITLE_SOURCE,
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
    storage_call = [call for call in session.calls if "/storage/v1/object/" in call[1]][0]
    assert storage_call[2]["headers"]["x-upsert"] == "true"
    assert storage_call[2]["headers"]["Content-Type"] == "text/plain; charset=utf-8"
    post_language = [
        call
        for call in session.calls
        if call[0] == "POST" and "/rest/v1/movie_languages" in call[1]
    ][0]
    assert post_language[2]["json"]["language"] == "English_AI"
    assert post_language[2]["json"]["subtitle_source"] == AI_SUBTITLE_SOURCE
    assert post_language[2]["json"]["subtitle_source"] == "human"
    assert post_language[2]["json"]["file_path"] == "ktb/ktb-112/ktb-112-English_AI.srt"


def test_publish_updates_existing_english_ai_row(tmp_path):
    srt = tmp_path / "ktb-112.English.srt"
    srt.write_text("1\n00:00:00,000 --> 00:00:01,000\nhello\n", encoding="utf-8")

    class ExistingSession(FakeSession):
        def request(self, method, url, **kwargs):
            if "/rest/v1/movies" in url and method == "GET":
                self.calls.append((method, url, kwargs))
                return FakeResponse(payload=[{"id": "movie-uuid", "movie_id": "ktb-112"}])
            if "/rest/v1/movie_languages" in url and method == "GET":
                self.calls.append((method, url, kwargs))
                return FakeResponse(payload=[{"id": "subtitle-uuid"}])
            if "/rest/v1/movie_languages" in url and method == "PATCH":
                self.calls.append((method, url, kwargs))
                return FakeResponse(payload=[{"id": "subtitle-uuid"}])
            return super().request(method, url, **kwargs)

    session = ExistingSession()
    publisher = SupabaseSubtitlePublisher("https://example.supabase.co", "service-key", session=session)

    result = publisher.publish_english_ai("ktb-112", srt)

    assert result.subtitle_id == "subtitle-uuid"
    assert any(
        call[0] == "PATCH" and "/rest/v1/movie_languages" in call[1]
        for call in session.calls
    )
