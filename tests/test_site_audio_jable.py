import pytest

from orchestrator.site_audio.errors import SourceUnavailable
from orchestrator.site_audio.hls import InspectedPlaylist, TextResponse
from orchestrator.site_audio.jable import JableResolver, extract_jable_manifest_url
from orchestrator.site_audio.models import Provider, ResolvedStream


def test_extract_jable_manifest_preserves_signed_query_string():
    html = """
    <script>
      var hlsUrl = 'https://edge.example/hls/movie/index.m3u8?token=a&amp;expires=9';
    </script>
    """

    assert extract_jable_manifest_url(html) == (
        "https://edge.example/hls/movie/index.m3u8?token=a&expires=9"
    )


def test_jable_resolver_uses_browser_headers_and_inspected_media_playlist():
    calls = []

    def get_page(url, headers):
        calls.append((url, headers))
        return TextResponse(
            url=url,
            status_code=200,
            text="var hlsUrl='https://cdn.example/master.m3u8?token=secret'",
        )

    class FakeInspector:
        def inspect(self, url, headers):
            assert url == "https://cdn.example/master.m3u8?token=secret"
            assert headers["Referer"] == "https://jable.tv/videos/abf-367/"
            return InspectedPlaylist(
                media_url="https://cdn.example/media.m3u8?token=secret",
                duration=8095.0,
                resource_urls=(),
            )

    resolver = JableResolver(get_page=get_page, inspector=FakeInspector())
    result = resolver.resolve("https://jable.tv/videos/abf-367/")

    assert result.provider is Provider.JABLE
    assert result.manifest_url == "https://cdn.example/media.m3u8?token=secret"
    assert result.expected_duration == 8095.0
    assert "Mozilla/5.0" in calls[0][1]["User-Agent"]


@pytest.mark.parametrize(
    "response",
    [
        TextResponse(url="https://jable.tv/videos/x/", status_code=403, text="forbidden"),
        TextResponse(
            url="https://jable.tv/videos/x/",
            status_code=200,
            text="<title>Just a moment...</title><script>cf-chl</script>",
        ),
        TextResponse(url="https://jable.tv/videos/x/", status_code=200, text="no manifest"),
    ],
)
def test_jable_resolver_falls_back_to_browser(response):
    expected = ResolvedStream(
        provider=Provider.JABLE,
        page_url="https://jable.tv/videos/x/",
        manifest_url="https://cdn.example/media.m3u8",
        headers={},
        expected_duration=10,
    )

    class BrowserFallback:
        def resolve(self, page_url, provider):
            assert provider is Provider.JABLE
            return expected

    resolver = JableResolver(
        get_page=lambda url, headers: response,
        inspector=pytest.fail,
        browser_resolver=BrowserFallback(),
    )

    assert resolver.resolve("https://jable.tv/videos/x/") is expected


def test_jable_resolver_reports_missing_source_without_browser_fallback():
    resolver = JableResolver(
        get_page=lambda url, headers: TextResponse(url, 404, "missing"),
        inspector=pytest.fail,
    )

    with pytest.raises(SourceUnavailable, match="HTTP 404"):
        resolver.resolve("https://jable.tv/videos/missing/")

