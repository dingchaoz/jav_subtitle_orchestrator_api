from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable, Mapping, Protocol
from urllib.parse import urlsplit

from .errors import BrowserResolutionTimeout, SiteAudioError, SourceUnavailable
from .models import Provider, ResolvedStream


_HLS_CONTENT_TYPES = ("application/vnd.apple.mpegurl", "application/x-mpegurl", "audio/mpegurl")
_PLAY_SELECTORS = (
    "video",
    ".vjs-big-play-button",
    ".plyr__control--overlaid",
    "button[aria-label*='play' i]",
)
_UNAVAILABLE_MARKERS = (
    "streaming service is unavailable",
    "service is unavailable",
    "video has been banned",
    "video has been ban",
)


@dataclass(frozen=True)
class BrowserCapture:
    manifest_url: str
    frame_url: str
    request_headers: Mapping[str, str]
    cookies: list[dict]
    user_agent: str


class BrowserDriver(Protocol):
    def capture(self, page_url: str, provider: Provider) -> BrowserCapture: ...


class ManifestCollector:
    def __init__(self, provider: Provider, page_url: str) -> None:
        self.provider = provider
        self.page_url = page_url
        self.captures: list[BrowserCapture] = []

    def record(
        self,
        *,
        url: str,
        content_type: str,
        frame_url: str,
        request_headers: Mapping[str, str],
    ) -> bool:
        path = urlsplit(url).path.lower()
        is_hls = path.endswith(".m3u8") or any(
            media_type in content_type.lower() for media_type in _HLS_CONTENT_TYPES
        )
        if not is_hls or not _is_primary_player_frame(self.provider, self.page_url, frame_url):
            return False
        self.captures.append(
            BrowserCapture(
                manifest_url=url,
                frame_url=frame_url,
                request_headers=dict(request_headers),
                cookies=[],
                user_agent="",
            )
        )
        return True


def _is_primary_player_frame(provider: Provider, page_url: str, frame_url: str) -> bool:
    frame = urlsplit(frame_url)
    hostname = (frame.hostname or "").lower()
    if frame.scheme != "https":
        return False
    if provider is Provider.JABLE:
        return hostname in {"jable.tv", "www.jable.tv"}
    return (
        (hostname == "bestjavporn.com" or hostname.endswith(".bestjavporn.com"))
        and frame.path.startswith("/p/")
    )


def scoped_cookie_header(cookies: Iterable[Mapping[str, object]], manifest_url: str) -> str:
    parsed = urlsplit(manifest_url)
    hostname = (parsed.hostname or "").lower()
    path = parsed.path or "/"
    values: list[str] = []
    for cookie in cookies:
        domain = str(cookie.get("domain", "")).lstrip(".").lower()
        cookie_path = str(cookie.get("path", "/"))
        secure = bool(cookie.get("secure", False))
        domain_matches = hostname == domain or hostname.endswith(f".{domain}")
        if not domain_matches or not path.startswith(cookie_path) or (secure and parsed.scheme != "https"):
            continue
        name = str(cookie.get("name", ""))
        value = str(cookie.get("value", ""))
        if name:
            values.append(f"{name}={value}")
    return "; ".join(values)


def build_stream_headers(capture: BrowserCapture) -> dict[str, str]:
    captured = {key.lower(): value for key, value in capture.request_headers.items()}
    headers = {
        "User-Agent": captured.get("user-agent") or capture.user_agent,
        "Referer": captured.get("referer") or capture.frame_url,
    }
    origin = captured.get("origin")
    if not origin:
        frame = urlsplit(capture.frame_url)
        origin = f"{frame.scheme}://{frame.netloc}"
    if origin:
        headers["Origin"] = origin
    cookie_header = scoped_cookie_header(capture.cookies, capture.manifest_url)
    if cookie_header:
        headers["Cookie"] = cookie_header
    return {key: value for key, value in headers.items() if value}


class BrowserResolver:
    def __init__(self, driver: BrowserDriver, inspector) -> None:
        self.driver = driver
        self.inspector = inspector

    def resolve(self, page_url: str, provider: Provider) -> ResolvedStream:
        capture = self.driver.capture(page_url, provider)
        headers = build_stream_headers(capture)
        inspected = self.inspector.inspect(capture.manifest_url, headers)
        return ResolvedStream(
            provider=provider,
            page_url=page_url,
            manifest_url=inspected.media_url,
            headers=headers,
            expected_duration=inspected.duration,
            refreshable=True,
            resource_urls=inspected.resource_urls,
        )


class ChromeBrowserDriver:
    def __init__(
        self,
        *,
        profile_dir: Path,
        timeout_seconds: float = 120.0,
        headless: bool = False,
        context_factory=None,
        monotonic=time.monotonic,
    ) -> None:
        self.profile_dir = profile_dir
        self.timeout_seconds = timeout_seconds
        self.headless = headless
        self.context_factory = context_factory or _playwright_context
        self.monotonic = monotonic

    def capture(self, page_url: str, provider: Provider) -> BrowserCapture:
        try:
            return self._capture_unsafe(page_url, provider)
        except SiteAudioError:
            raise
        except Exception as exc:
            raise BrowserResolutionTimeout("could not start Chrome or inspect the page") from exc

    def _capture_unsafe(self, page_url: str, provider: Provider) -> BrowserCapture:
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        with self.context_factory(self.profile_dir, self.headless) as context:
            deadline = self.monotonic() + self.timeout_seconds
            page = context.pages[0] if context.pages else context.new_page()
            collector = ManifestCollector(provider, page_url)

            def handle_response(response) -> None:
                try:
                    request = response.request
                    frame_url = request.frame.url
                    request_headers = request.all_headers()
                    content_type = response.headers.get("content-type", "")
                    collector.record(
                        url=response.url,
                        content_type=content_type,
                        frame_url=frame_url,
                        request_headers=request_headers,
                    )
                except Exception:
                    return

            page.on("response", handle_response)
            context.on("page", lambda popup: popup.close() if popup is not page else None)
            try:
                page.goto(
                    page_url,
                    wait_until="domcontentloaded",
                    timeout=max(1, int((deadline - self.monotonic()) * 1000)),
                )
            except Exception:
                # A challenge page can outlive the navigation timeout; keep the visible page open.
                pass
            body_text = self._body_text(page).lower()
            if any(marker in body_text for marker in _UNAVAILABLE_MARKERS):
                raise SourceUnavailable("streaming service is unavailable or the video is banned")
            if provider is Provider.BESTJAVPORN:
                self._click(
                    page.locator("#video-player[data-mpu]").first,
                    timeout_ms=min(250, self._remaining_ms(deadline)),
                )

            while self.monotonic() < deadline:
                if collector.captures:
                    capture = collector.captures[0]
                    cookies = context.cookies([capture.manifest_url])
                    user_agent = str(page.evaluate("navigator.userAgent"))
                    return replace(capture, cookies=cookies, user_agent=user_agent)
                for frame in page.frames:
                    frame_url = getattr(frame, "url", "")
                    if not _is_primary_player_frame(provider, page_url, frame_url):
                        continue
                    for selector in _PLAY_SELECTORS:
                        remaining_ms = self._remaining_ms(deadline)
                        if remaining_ms <= 0:
                            break
                        self._click(
                            frame.locator(selector).first,
                            timeout_ms=min(250, remaining_ms),
                        )
                remaining_ms = self._remaining_ms(deadline)
                if remaining_ms <= 0:
                    break
                page.wait_for_timeout(min(250, remaining_ms))
            raise BrowserResolutionTimeout(
                "timed out waiting for the primary player HLS request; complete any visible challenge"
            )

    @staticmethod
    def _body_text(page) -> str:
        try:
            return page.inner_text("body")
        except Exception:
            return ""

    @staticmethod
    def _click(locator, *, timeout_ms: int = 250) -> None:
        try:
            locator.click(timeout=max(1, timeout_ms), force=True)
        except Exception:
            return

    def _remaining_ms(self, deadline: float) -> int:
        return max(0, int((deadline - self.monotonic()) * 1000))


@contextmanager
def _playwright_context(profile_dir: Path, headless: bool):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            channel="chrome",
            headless=headless,
            viewport={"width": 1440, "height": 1000},
            args=["--autoplay-policy=no-user-gesture-required"],
        )
        try:
            yield context
        finally:
            context.close()
