from __future__ import annotations

import html as html_module
import re
from typing import Callable, Mapping, Protocol
from urllib.parse import urlsplit

from .errors import SourceUnavailable
from .hls import HLSInspector, TextResponse
from .models import Provider, ResolvedStream
from .urls import detect_provider


CHROME_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_HLS_URL_RE = re.compile(
    r"(?:var\s+)?hlsUrl\s*=\s*['\"](?P<url>https://[^'\"]+?\.m3u8(?:\?[^'\"]*)?)['\"]",
    re.IGNORECASE,
)


class BrowserFallback(Protocol):
    def resolve(self, page_url: str, provider: Provider) -> ResolvedStream: ...


def extract_jable_manifest_url(page_html: str) -> str | None:
    match = _HLS_URL_RE.search(page_html)
    return html_module.unescape(match.group("url")) if match else None


def _is_cloudflare_page(page_html: str) -> bool:
    lowered = page_html.lower()
    return "cf-chl" in lowered or "just a moment" in lowered or "challenge-platform" in lowered


class JableResolver:
    def __init__(
        self,
        *,
        get_page: Callable[[str, Mapping[str, str]], TextResponse],
        inspector: HLSInspector,
        browser_resolver: BrowserFallback | None = None,
    ) -> None:
        self.get_page = get_page
        self.inspector = inspector
        self.browser_resolver = browser_resolver

    def resolve(self, page_url: str) -> ResolvedStream:
        if detect_provider(page_url) is not Provider.JABLE:
            raise SourceUnavailable("URL is not a Jable movie page")
        headers = {
            "User-Agent": CHROME_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": page_url,
            "Origin": f"https://{urlsplit(page_url).hostname}",
        }
        response = self.get_page(page_url, headers)
        if response.status_code == 404:
            raise SourceUnavailable("Jable page returned HTTP 404")
        manifest_url = extract_jable_manifest_url(response.text) if response.status_code == 200 else None
        needs_browser = response.status_code == 403 or _is_cloudflare_page(response.text) or not manifest_url
        if needs_browser:
            if self.browser_resolver is not None:
                return self.browser_resolver.resolve(page_url, Provider.JABLE)
            detail = f"HTTP {response.status_code}" if response.status_code != 200 else "no HLS source"
            raise SourceUnavailable(f"Jable source unavailable: {detail}")
        inspected = self.inspector.inspect(manifest_url, headers)
        return ResolvedStream(
            provider=Provider.JABLE,
            page_url=page_url,
            manifest_url=inspected.media_url,
            headers=headers,
            expected_duration=inspected.duration,
            refreshable=True,
            resource_urls=inspected.resource_urls,
        )
