import pytest

from orchestrator.site_audio.errors import ManifestValidationError
from orchestrator.site_audio.hls import HLSInspector, TextResponse


def PUBLIC_IPS(hostname):
    return ["93.184.216.34"]


def test_inspector_selects_highest_bandwidth_variant_with_audio():
    playlists = {
        "https://cdn.example/master.m3u8": """#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=500000,CODECS="avc1.42e01e"
video-only.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=900000,CODECS="avc1.4d401f,mp4a.40.2"
media/high.m3u8?token=signed
#EXT-X-STREAM-INF:BANDWIDTH=700000,CODECS="avc1.4d401e,mp4a.40.2"
media/mid.m3u8
""",
        "https://cdn.example/media/high.m3u8?token=signed": """#EXTM3U
#EXT-X-TARGETDURATION:10
#EXTINF:10.0,
seg-1.ts
#EXTINF:9.5,
seg-2.ts
#EXT-X-ENDLIST
""",
    }

    def fetch(url, headers):
        return TextResponse(url=url, status_code=200, text=playlists[url])

    result = HLSInspector(fetch, resolve_host=PUBLIC_IPS).inspect(
        "https://cdn.example/master.m3u8",
        {"Referer": "https://jable.tv/videos/example/"},
    )

    assert result.media_url == "https://cdn.example/media/high.m3u8?token=signed"
    assert result.duration == 19.5


def test_inspector_selects_referenced_alternate_audio_rendition():
    playlists = {
        "https://cdn.example/master.m3u8": """#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="English",DEFAULT=YES,URI="audio/en.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1500000,CODECS="avc1.4d401f",AUDIO="audio"
video/main.m3u8
""",
        "https://cdn.example/audio/en.m3u8": """#EXTM3U
#EXTINF:5.0,
audio-1.aac
#EXT-X-ENDLIST
""",
    }

    def fetch(url, headers):
        return TextResponse(url=url, status_code=200, text=playlists[url])

    result = HLSInspector(fetch, resolve_host=PUBLIC_IPS).inspect(
        "https://cdn.example/master.m3u8",
        {},
    )

    assert result.media_url == "https://cdn.example/audio/en.m3u8"
    assert result.duration == 5.0


def test_inspector_allows_relative_aes_128_key_map_and_segments():
    playlist = """#EXTM3U
#EXT-X-KEY:METHOD=AES-128,URI="../keys/movie.key",IV=0x1
#EXT-X-MAP:URI="init.mp4"
#EXTINF:4.0,
segment-1.m4s
#EXT-X-ENDLIST
"""

    def fetch(url, headers):
        return TextResponse(url=url, status_code=200, text=playlist)

    result = HLSInspector(fetch, resolve_host=PUBLIC_IPS).inspect(
        "https://media.example/hls/movie/index.m3u8",
        {},
    )

    assert result.duration == 4.0
    assert set(result.resource_urls) == {
        "https://media.example/hls/keys/movie.key",
        "https://media.example/hls/movie/init.mp4",
        "https://media.example/hls/movie/segment-1.m4s",
    }


@pytest.mark.parametrize(
    ("playlist", "message"),
    [
        (
            "#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"key\"\n"
            "#EXTINF:4,\nseg.ts\n#EXT-X-ENDLIST\n",
            "unsupported encryption",
        ),
        (
            "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\",KEYFORMAT=\"com.apple.streamingkeydelivery\"\n"
            "#EXTINF:4,\nseg.ts\n#EXT-X-ENDLIST\n",
            "KEYFORMAT",
        ),
        ("#EXTM3U\n#EXTINF:4,\nseg.ts\n", "ENDLIST"),
    ],
)
def test_inspector_rejects_drm_or_non_vod_playlists(playlist, message):
    def fetch(url, headers):
        return TextResponse(url=url, status_code=200, text=playlist)

    with pytest.raises(ManifestValidationError, match=message):
        HLSInspector(fetch, resolve_host=PUBLIC_IPS).inspect("https://cdn.example/a.m3u8", {})


@pytest.mark.parametrize(
    "resource",
    [
        "http://cdn.example/segment.ts",
        "file:///etc/passwd",
        "https://127.0.0.1/segment.ts",
        "https://192.168.1.4/segment.ts",
        "https://[::1]/segment.ts",
    ],
)
def test_inspector_rejects_non_https_or_non_public_resources(resource):
    playlist = f"#EXTM3U\n#EXTINF:4,\n{resource}\n#EXT-X-ENDLIST\n"

    def fetch(url, headers):
        return TextResponse(url=url, status_code=200, text=playlist)

    def resolver(hostname):
        if hostname in {"127.0.0.1", "192.168.1.4", "::1"}:
            return [hostname]
        return PUBLIC_IPS(hostname)

    with pytest.raises(ManifestValidationError, match="public HTTPS"):
        HLSInspector(fetch, resolve_host=resolver).inspect("https://cdn.example/a.m3u8", {})


def test_inspector_rejects_http_error_and_non_hls_body():
    responses = iter(
        [
            TextResponse(url="https://cdn.example/a.m3u8", status_code=403, text="forbidden"),
            TextResponse(url="https://cdn.example/a.m3u8", status_code=200, text="<html>nope</html>"),
        ]
    )

    def fetch(url, headers):
        return next(responses)

    inspector = HLSInspector(fetch, resolve_host=PUBLIC_IPS)
    with pytest.raises(ManifestValidationError, match="HTTP 403"):
        inspector.inspect("https://cdn.example/a.m3u8", {})
    with pytest.raises(ManifestValidationError, match="not an HLS"):
        inspector.inspect("https://cdn.example/a.m3u8", {})


def test_inspector_rejects_cross_origin_resources_when_cookie_header_is_required():
    playlist = """#EXTM3U
#EXTINF:4,
https://other.example/segment.ts
#EXT-X-ENDLIST
"""

    def fetch(url, headers):
        return TextResponse(url=url, status_code=200, text=playlist)

    with pytest.raises(ManifestValidationError, match="cross-origin"):
        HLSInspector(fetch, resolve_host=PUBLIC_IPS).inspect(
            "https://media.example/index.m3u8",
            {"Cookie": "media_session=private"},
        )
