from pathlib import Path

import pytest

from orchestrator.supabase_publisher import SupabaseSubtitlePublisher


class FakeResponse:
    def __init__(self, payload):
        self.status_code = 200
        self._payload = payload
        self.text = "OK"

    @property
    def ok(self):
        return True

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, *, existing: bool = False):
        self.calls = []
        self.existing = existing

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if "/storage/v1/object/" in url:
            return FakeResponse({"Key": "subtitle"})
        if "/rest/v1/movies" in url and method == "GET":
            return FakeResponse([{"id": "movie-uuid"}])
        if "/rest/v1/movie_languages" in url and method == "GET":
            return FakeResponse([{"id": "subtitle-uuid"}] if self.existing else [])
        if "/rest/v1/movie_languages" in url and method in {"POST", "PATCH"}:
            return FakeResponse([{"id": "subtitle-uuid"}])
        raise AssertionError(f"unexpected call: {method} {url}")


def _write_pair(root: Path, *, bad: bool = False) -> Path:
    japanese = root / "ktb-112.Japanese.srt"
    english = root / "ktb-112.English.srt"
    ja_blocks = []
    en_blocks = []
    for index in range(1, 26):
        timing = f"{index}\n00:00:{index - 1:02d},000 --> 00:00:{index:02d},000\n"
        ja_blocks.append(timing + f"日本語{index}\n")
        translated = "Cannot translate" if bad else f"Good translation {index}"
        en_blocks.append(timing + translated + "\n")
    japanese.write_text("\n".join(ja_blocks), encoding="utf-8")
    english.write_text("\n".join(en_blocks), encoding="utf-8")
    return english


def test_bad_english_cannot_reach_supabase_upload(tmp_path):
    english = _write_pair(tmp_path, bad=True)
    session = FakeSession()
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co", "service-key", session=session
    )

    with pytest.raises(RuntimeError, match=r"^quality_gate_failed:"):
        publisher.publish_english_ai("ktb-112", english)

    assert not any("/storage/v1/object/" in call[1] for call in session.calls)


def test_good_translation_uploads_normally(tmp_path):
    english = _write_pair(tmp_path)
    session = FakeSession()
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co", "service-key", session=session
    )

    result = publisher.publish_english_ai("ktb-112", english)

    assert result.subtitle_id == "subtitle-uuid"
    assert any("/storage/v1/object/" in call[1] for call in session.calls)
    assert any(call[0] == "POST" and "/movie_languages" in call[1] for call in session.calls)


def test_repaired_subtitle_uses_storage_upsert_and_updates_catalog_row(tmp_path):
    repaired = _write_pair(tmp_path)
    session = FakeSession(existing=True)
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co", "service-key", session=session
    )

    publisher.publish_english_ai("ktb-112", repaired)

    storage_call = next(call for call in session.calls if "/storage/v1/object/" in call[1])
    assert storage_call[2]["headers"]["x-upsert"] == "true"
    assert storage_call[2]["data"] == repaired.read_bytes()
    assert any(call[0] == "PATCH" and "/movie_languages" in call[1] for call in session.calls)
