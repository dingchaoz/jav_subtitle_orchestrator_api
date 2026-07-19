from contextlib import contextmanager

import pytest

from orchestrator.site_audio.browser import (
    BrowserCapture,
    BrowserResolver,
    ChromeBrowserDriver,
    ManifestCollector,
    build_stream_headers,
    scoped_cookie_header,
)
from orchestrator.site_audio.errors import BrowserResolutionTimeout, SourceUnavailable
from orchestrator.site_audio.hls import InspectedPlaylist
from orchestrator.site_audio.models import Provider


def test_manifest_collector_accepts_bestjavporn_player_frame_and_rejects_ad_frame():
    collector = ManifestCollector(
        Provider.BESTJAVPORN,
        "https://www.bestjavporn.com/video/movie/",
    )

    assert collector.record(
        url="https://ads.example/pre-roll.m3u8",
        content_type="application/vnd.apple.mpegurl",
        frame_url="https://ads.example/player",
        request_headers={"referer": "https://ads.example/"},
    ) is False
    assert collector.record(
        url="https://media.example/movie/master.m3u8?token=signed",
        content_type="application/vnd.apple.mpegurl",
        frame_url="https://video1.bestjavporn.com/p/player-token",
        request_headers={"referer": "https://video1.bestjavporn.com/p/player-token"},
    ) is True
    assert [capture.manifest_url for capture in collector.captures] == [
        "https://media.example/movie/master.m3u8?token=signed"
    ]


def test_manifest_collector_accepts_m3u8_by_url_when_content_type_is_generic():
    collector = ManifestCollector(Provider.JABLE, "https://jable.tv/videos/movie/")

    accepted = collector.record(
        url="https://cdn.example/index.m3u8?expires=1",
        content_type="application/octet-stream",
        frame_url="https://jable.tv/videos/movie/",
        request_headers={},
    )

    assert accepted is True


def test_scoped_cookie_header_only_includes_manifest_host_cookies():
    cookies = [
        {"name": "page", "value": "private", "domain": ".jable.tv", "secure": True},
        {"name": "cdn", "value": "allowed", "domain": ".media.example", "secure": True},
        {"name": "exact", "value": "yes", "domain": "edge.media.example", "secure": True},
        {"name": "wrong", "value": "no", "domain": "notmedia.example", "secure": True},
    ]

    result = scoped_cookie_header(cookies, "https://edge.media.example/movie/index.m3u8")

    assert result == "cdn=allowed; exact=yes"


def test_build_stream_headers_allows_only_required_headers_and_scoped_cookie():
    capture = BrowserCapture(
        manifest_url="https://media.example/index.m3u8",
        frame_url="https://video1.bestjavporn.com/p/token",
        request_headers={
            "user-agent": "Captured Browser",
            "referer": "https://video1.bestjavporn.com/p/token",
            "origin": "https://video1.bestjavporn.com",
            "authorization": "must-not-leak",
            "x-private": "must-not-leak",
        },
        cookies=[
            {"name": "media_session", "value": "ok", "domain": "media.example", "secure": True},
            {"name": "site_session", "value": "no", "domain": "bestjavporn.com", "secure": True},
        ],
        user_agent="Fallback Browser",
    )

    assert build_stream_headers(capture) == {
        "User-Agent": "Captured Browser",
        "Referer": "https://video1.bestjavporn.com/p/token",
        "Origin": "https://video1.bestjavporn.com",
        "Cookie": "media_session=ok",
    }


def test_browser_resolver_inspects_capture_and_returns_media_playlist():
    capture = BrowserCapture(
        manifest_url="https://media.example/master.m3u8",
        frame_url="https://video1.bestjavporn.com/p/token",
        request_headers={"referer": "https://video1.bestjavporn.com/p/token"},
        cookies=[],
        user_agent="Browser",
    )

    class FakeDriver:
        def capture(self, page_url, provider):
            assert provider is Provider.BESTJAVPORN
            return capture

    class FakeInspector:
        def inspect(self, manifest_url, headers):
            assert manifest_url == capture.manifest_url
            assert headers["Referer"] == capture.frame_url
            return InspectedPlaylist(
                media_url="https://media.example/media.m3u8",
                duration=3600.5,
                resource_urls=(),
            )

    result = BrowserResolver(FakeDriver(), FakeInspector()).resolve(
        "https://www.bestjavporn.com/video/movie/",
        Provider.BESTJAVPORN,
    )

    assert result.manifest_url == "https://media.example/media.m3u8"
    assert result.expected_duration == 3600.5


@pytest.mark.parametrize(
    ("body", "error_type"),
    [
        ("Sorry streaming service is unavailable", SourceUnavailable),
        ("This video has been banned", SourceUnavailable),
        ("A normal page without a stream", BrowserResolutionTimeout),
    ],
)
def test_chrome_driver_classifies_unavailable_page_and_timeout(body, error_type, tmp_path):
    class FakePage:
        frames = []

        def on(self, event, callback):
            assert event == "response"

        def goto(self, url, **kwargs):
            return None

        def inner_text(self, selector):
            assert selector == "body"
            return body

        def locator(self, selector):
            return FakeLocator()

        def wait_for_timeout(self, milliseconds):
            return None

        def evaluate(self, expression):
            return "Browser UA"

    class FakeLocator:
        @property
        def first(self):
            return self

        def click(self, **kwargs):
            return None

    class FakeContext:
        pages = [FakePage()]

        def cookies(self, urls):
            return []

        def on(self, event, callback):
            return None

    @contextmanager
    def context_factory(profile_dir, headless):
        yield FakeContext()

    monotonic_values = iter([0.0, 0.0, 2.0])
    driver = ChromeBrowserDriver(
        profile_dir=tmp_path / "profile",
        timeout_seconds=1,
        context_factory=context_factory,
        monotonic=lambda: next(monotonic_values),
    )

    with pytest.raises(error_type):
        driver.capture(
            "https://www.bestjavporn.com/video/movie/",
            Provider.BESTJAVPORN,
        )


def test_chrome_driver_uses_one_timeout_budget_for_navigation_and_capture(tmp_path):
    clock = [0.0]
    wait_calls = []

    class FakePage:
        frames = []

        def on(self, event, callback):
            return None

        def goto(self, url, **kwargs):
            clock[0] = 0.8

        def inner_text(self, selector):
            return "normal page"

        def locator(self, selector):
            return FakeLocator()

        def wait_for_timeout(self, milliseconds):
            wait_calls.append(milliseconds)
            clock[0] += milliseconds / 1000

    class FakeLocator:
        @property
        def first(self):
            return self

        def click(self, **kwargs):
            return None

    class FakeContext:
        pages = [FakePage()]

        def on(self, event, callback):
            return None

    @contextmanager
    def context_factory(profile_dir, headless):
        yield FakeContext()

    driver = ChromeBrowserDriver(
        profile_dir=tmp_path / "profile",
        timeout_seconds=1,
        context_factory=context_factory,
        monotonic=lambda: clock[0],
    )

    with pytest.raises(BrowserResolutionTimeout):
        driver.capture("https://www.bestjavporn.com/video/movie/", Provider.BESTJAVPORN)

    assert len(wait_calls) == 1
    assert wait_calls[0] <= 200


def test_chrome_driver_retries_player_click_when_frame_dom_initializes_late(tmp_path):
    clock = [0.0]
    clicks = []

    class FakeLocator:
        def __init__(self, source):
            self.source = source

        @property
        def first(self):
            return self

        def click(self, **kwargs):
            clicks.append(self.source)

    class FakeFrame:
        url = "https://video1.bestjavporn.com/p/token"

        def locator(self, selector):
            return FakeLocator("frame")

    class FakePage:
        frames = [FakeFrame()]

        def on(self, event, callback):
            return None

        def goto(self, url, **kwargs):
            return None

        def inner_text(self, selector):
            return "normal page"

        def locator(self, selector):
            return FakeLocator("page")

        def wait_for_timeout(self, milliseconds):
            clock[0] += milliseconds / 1000

    class FakeContext:
        pages = [FakePage()]

        def on(self, event, callback):
            return None

    @contextmanager
    def context_factory(profile_dir, headless):
        yield FakeContext()

    driver = ChromeBrowserDriver(
        profile_dir=tmp_path / "profile",
        timeout_seconds=0.6,
        context_factory=context_factory,
        monotonic=lambda: clock[0],
    )

    with pytest.raises(BrowserResolutionTimeout):
        driver.capture("https://www.bestjavporn.com/video/movie/", Provider.BESTJAVPORN)

    assert clicks.count("frame") > 4


def test_chrome_driver_maps_launch_failure_to_browser_timeout(tmp_path):
    @contextmanager
    def context_factory(profile_dir, headless):
        raise OSError("Chrome executable missing")
        yield

    driver = ChromeBrowserDriver(
        profile_dir=tmp_path / "profile",
        context_factory=context_factory,
    )

    with pytest.raises(BrowserResolutionTimeout, match="could not start Chrome"):
        driver.capture("https://jable.tv/videos/movie/", Provider.JABLE)


def test_player_click_timeouts_never_exceed_global_browser_budget(tmp_path):
    clock = [0.0]

    class SlowMissingLocator:
        @property
        def first(self):
            return self

        def click(self, **kwargs):
            clock[0] += kwargs["timeout"] / 1000
            raise RuntimeError("control is not ready")

    class FakeFrame:
        url = "https://video1.bestjavporn.com/p/token"

        def locator(self, selector):
            return SlowMissingLocator()

    class FakePage:
        frames = [FakeFrame()]

        def on(self, event, callback):
            return None

        def goto(self, url, **kwargs):
            return None

        def inner_text(self, selector):
            return "normal page"

        def locator(self, selector):
            return SlowMissingLocator()

        def wait_for_timeout(self, milliseconds):
            clock[0] += milliseconds / 1000

    class FakeContext:
        pages = [FakePage()]

        def on(self, event, callback):
            return None

    @contextmanager
    def context_factory(profile_dir, headless):
        yield FakeContext()

    driver = ChromeBrowserDriver(
        profile_dir=tmp_path / "profile",
        timeout_seconds=1,
        context_factory=context_factory,
        monotonic=lambda: clock[0],
    )

    with pytest.raises(BrowserResolutionTimeout):
        driver.capture("https://www.bestjavporn.com/video/movie/", Provider.BESTJAVPORN)

    assert clock[0] <= 1.01
