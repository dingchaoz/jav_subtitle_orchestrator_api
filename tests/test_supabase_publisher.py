import hashlib
from pathlib import Path

import pytest

from orchestrator.movie_catalog import MovieCatalogResult
from orchestrator.supabase_publisher import SupabaseSubtitlePublisher


CATALOG_MOVIE_UUID = "catalog-movie-uuid"


class RecordingCatalogEnsurer:
    def __init__(
        self,
        events=None,
        error=None,
        source="missav",
        movie_uuid=CATALOG_MOVIE_UUID,
    ):
        self.events = events if events is not None else []
        self.error = error
        self.source = source
        self.movie_uuid = movie_uuid

    def ensure_movie(self, movie_code, metadata_path):
        self.events.append(("ensure", movie_code, metadata_path))
        if self.error:
            raise self.error
        return MovieCatalogResult(
            movie_uuid=self.movie_uuid,
            canonical_code=movie_code,
            metadata_status=(
                "placeholder" if self.source == "placeholder" else "complete"
            ),
            metadata_source=self.source,
        )


class FakeResponse:
    def __init__(self, payload):
        self.status_code = 200
        self._payload = payload
        self.text = "OK"
        self.content = b""
        self.headers = {}

    @property
    def ok(self):
        return True

    def json(self):
        return self._payload

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset : offset + chunk_size]

    def close(self):
        return None


class FakeSession:
    def __init__(self, *, existing: bool = False, events=None):
        self.calls = []
        self.existing = existing
        self.uploaded = b""
        self.events = events if events is not None else []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if "/storage/v1/object/" in url and method == "POST":
            self.events.append(("upload", url))
            self.uploaded = kwargs["data"]
            return FakeResponse({"Key": "subtitle"})
        if "/storage/v1/object/" in url and method == "GET":
            return BinaryResponse(self.uploaded)
        if "/rest/v1/movie_languages" in url and method == "GET":
            select = kwargs.get("params", {}).get("select", "")
            if select == "id":
                return FakeResponse([{"id": "subtitle-uuid"}] if self.existing else [])
            return FakeResponse(
                [
                    {
                        "id": "subtitle-uuid",
                        "movie_id": CATALOG_MOVIE_UUID,
                        "language": "English_AI",
                        "file_path": "ktb/ktb-112/ktb-112-English_AI.srt",
                        "file_size": len(self.uploaded),
                    }
                ]
            )
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
    catalog = RecordingCatalogEnsurer()
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        catalog_ensurer=catalog,
    )

    with pytest.raises(RuntimeError, match=r"^quality_gate_failed:"):
        publisher.publish_english_ai("ktb-112", english)

    assert not any("/storage/v1/object/" in call[1] for call in session.calls)
    assert catalog.events == []


@pytest.mark.parametrize("metadata_source", ["missav", "local", "public"])
def test_good_translation_uploads_normally(tmp_path, metadata_source):
    english = _write_pair(tmp_path)
    session = FakeSession()
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        catalog_ensurer=RecordingCatalogEnsurer(source=metadata_source),
    )

    result = publisher.publish_english_ai("ktb-112", english)

    assert result.subtitle_id == "subtitle-uuid"
    assert result.metadata_source == metadata_source
    assert any("/storage/v1/object/" in call[1] for call in session.calls)
    assert any(call[0] == "POST" and "/movie_languages" in call[1] for call in session.calls)


def test_repaired_subtitle_uses_storage_upsert_and_updates_catalog_row(tmp_path):
    repaired = _write_pair(tmp_path)
    session = FakeSession(existing=True)
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        catalog_ensurer=RecordingCatalogEnsurer(),
    )

    publisher.publish_english_ai("ktb-112", repaired)

    storage_call = next(call for call in session.calls if "/storage/v1/object/" in call[1])
    assert storage_call[2]["headers"]["x-upsert"] == "true"
    assert storage_call[2]["data"] == repaired.read_bytes()
    assert any(call[0] == "PATCH" and "/movie_languages" in call[1] for call in session.calls)


class BinaryResponse(FakeResponse):
    def __init__(self, content: bytes):
        super().__init__(None)
        self.content = content
        self.text = ""
        self.headers = {"Content-Length": str(len(content))}


class VerifyingSession(FakeSession):
    def __init__(self, storage_downloads: list[bytes]):
        super().__init__(existing=True)
        self.storage_downloads = iter(storage_downloads)
        self.uploaded_size = 0

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if method == "POST" and "/storage/v1/object/" in url:
            self.events.append(("upload", url))
            self.uploaded_size = len(kwargs["data"])
            return FakeResponse({"Key": "subtitle"})
        if method == "GET" and "/storage/v1/object/" in url:
            return BinaryResponse(next(self.storage_downloads))
        if method == "GET" and "/rest/v1/movie_languages" in url:
            select = kwargs.get("params", {}).get("select", "")
            if select == "id":
                return FakeResponse([{"id": "subtitle-uuid"}])
            return FakeResponse(
                [
                    {
                        "id": "subtitle-uuid",
                        "movie_id": CATALOG_MOVIE_UUID,
                        "language": "English_AI",
                        "file_path": "ktb/ktb-112/ktb-112-English_AI.srt",
                        "file_size": self.uploaded_size,
                    }
                ]
            )
        if method == "PATCH" and "/rest/v1/movie_languages" in url:
            return FakeResponse([{"id": "subtitle-uuid"}])
        raise AssertionError(f"unexpected call: {method} {url}")


def test_publish_waits_for_matching_storage_hash_and_catalog(tmp_path):
    repaired = _write_pair(tmp_path)
    session = VerifyingSession([b"stale", repaired.read_bytes()])
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        verification_timeout_seconds=90,
        verification_interval_seconds=2,
        clock=lambda: now[0],
        sleeper=sleep,
        nonce_factory=iter(["nonce-1", "nonce-2"]).__next__,
        catalog_ensurer=RecordingCatalogEnsurer(),
    )

    result = publisher.publish_english_ai("ktb-112", repaired)

    assert result.verified is True
    assert result.content_sha256 == hashlib.sha256(repaired.read_bytes()).hexdigest()
    assert result.file_size == repaired.stat().st_size
    assert sleeps == [2]
    downloads = [
        call
        for call in session.calls
        if call[0] == "GET" and "/storage/v1/object/" in call[1]
    ]
    assert [call[2]["params"]["cacheNonce"] for call in downloads] == [
        "nonce-1",
        "nonce-2",
    ]


def test_publish_never_accepts_stale_storage_bytes(tmp_path):
    repaired = _write_pair(tmp_path)
    session = VerifyingSession([b"stale", b"still-stale"])
    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        verification_timeout_seconds=4,
        verification_interval_seconds=2,
        clock=lambda: now[0],
        sleeper=sleep,
        nonce_factory=iter(["nonce-1", "nonce-2"]).__next__,
        catalog_ensurer=RecordingCatalogEnsurer(),
    )

    with pytest.raises(
        RuntimeError,
        match="Supabase verification failed: storage_hash_timeout",
    ):
        publisher.publish_english_ai("ktb-112", repaired)


def test_catalog_failure_cannot_reach_storage_or_languages(tmp_path):
    english = _write_pair(tmp_path)
    session = FakeSession()
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        catalog_ensurer=RecordingCatalogEnsurer(
            error=RuntimeError("catalog unavailable")
        ),
    )

    with pytest.raises(RuntimeError, match=r"^catalog unavailable$"):
        publisher.publish_english_ai(
            "ktb-112", english, tmp_path / "explicit-metadata.json"
        )

    assert not any("/storage/v1/object/" in call[1] for call in session.calls)
    assert not any("/movie_languages" in call[1] for call in session.calls)


def test_placeholder_movie_is_ensured_before_quality_approved_upload(tmp_path):
    english = _write_pair(tmp_path)
    metadata_path = tmp_path / "missing-metadata.json"
    events = []
    session = FakeSession(events=events)
    catalog = RecordingCatalogEnsurer(events, source="placeholder")
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        catalog_ensurer=catalog,
    )

    result = publisher.publish_english_ai("ktb-112", english, metadata_path)

    assert result.metadata_status == "placeholder"
    assert result.metadata_source == "placeholder"
    assert events[:2] == [
        ("ensure", "ktb-112", metadata_path),
        (
            "upload",
            "https://example.supabase.co/storage/v1/object/subtitles/"
            "ktb/ktb-112/ktb-112-English_AI.srt",
        ),
    ]
    assert any(
        call[0] in {"POST", "PATCH"} and "/movie_languages" in call[1]
        for call in session.calls
    )


def test_catalog_movie_uuid_is_used_without_movies_lookup(tmp_path):
    english = _write_pair(tmp_path)
    session = FakeSession()
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=session,
        catalog_ensurer=RecordingCatalogEnsurer(),
    )

    result = publisher.publish_english_ai("ktb-112", english)

    assert result.movie_uuid == CATALOG_MOVIE_UUID
    language_lookup = next(
        call
        for call in session.calls
        if call[0] == "GET"
        and "/movie_languages" in call[1]
        and call[2]["params"]["select"] == "id"
    )
    assert language_lookup[2]["params"]["movie_id"] == f"eq.{CATALOG_MOVIE_UUID}"
    assert not any("/rest/v1/movies" in call[1] for call in session.calls)


def test_metadata_path_defaults_next_to_english_srt(tmp_path):
    english = _write_pair(tmp_path)
    catalog = RecordingCatalogEnsurer()
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=FakeSession(),
        catalog_ensurer=catalog,
    )

    publisher.publish_english_ai("ktb-112", english)

    assert catalog.events == [("ensure", "ktb-112", tmp_path / "metadata.json")]
