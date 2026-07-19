import pytest

from orchestrator.site_audio.errors import (
    BrowserResolutionTimeout,
    DownloadFailure,
    SourceUnavailable,
    UnsupportedSiteURL,
)
from orchestrator.site_audio.models import Provider, ResolvedStream
from orchestrator.site_audio.urls import default_output_path, detect_provider


@pytest.mark.parametrize(
    ("url", "provider"),
    [
        ("https://jable.tv/videos/abf-367/", Provider.JABLE),
        ("https://www.jable.tv/videos/abf-367/?ref=home", Provider.JABLE),
        (
            "https://www.bestjavporn.com/video/420hpt-060-sakine-24/",
            Provider.BESTJAVPORN,
        ),
    ],
)
def test_detect_provider_accepts_supported_movie_pages(url, provider):
    assert detect_provider(url) is provider


@pytest.mark.parametrize(
    "url",
    [
        "http://jable.tv/videos/abf-367/",
        "https://evil.example/videos/abf-367/",
        "https://jable.tv.evil.example/videos/abf-367/",
        "https://jable.tv/",
        "https://bestjavporn.com/videos/not-a-movie-page/",
    ],
)
def test_detect_provider_rejects_unsupported_or_unsafe_urls(url):
    with pytest.raises(UnsupportedSiteURL):
        detect_provider(url)


def test_default_output_path_uses_sanitized_movie_slug(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = default_output_path("https://jable.tv/videos/ABF%20367/?token=secret")

    assert result == tmp_path / "ABF_367.wav"


def test_resolved_stream_freezes_request_headers():
    source = {"User-Agent": "Browser", "Cookie": "session=secret"}
    stream = ResolvedStream(
        provider=Provider.JABLE,
        page_url="https://jable.tv/videos/abf-367/",
        manifest_url="https://cdn.example/movie/index.m3u8?token=secret",
        headers=source,
        expected_duration=60.0,
        refreshable=True,
    )
    source["Cookie"] = "changed=yes"

    assert stream.headers["Cookie"] == "session=secret"
    with pytest.raises(TypeError):
        stream.headers["Cookie"] = "mutated=yes"


@pytest.mark.parametrize(
    ("error", "exit_code"),
    [
        (UnsupportedSiteURL("bad URL"), 2),
        (SourceUnavailable("missing"), 3),
        (BrowserResolutionTimeout("timed out"), 4),
        (DownloadFailure("ffmpeg failed"), 5),
    ],
)
def test_public_errors_define_stable_exit_codes(error, exit_code):
    assert error.exit_code == exit_code
