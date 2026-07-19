from __future__ import annotations

from pathlib import Path

from .browser import BrowserResolver, ChromeBrowserDriver
from .errors import DownloadFailure, UnsupportedSiteURL
from .ffmpeg import FFmpegAudioWriter
from .hls import HLSInspector
from .http import CurlCffiTransport
from .jable import JableResolver
from .models import Provider
from .urls import detect_provider


class _BoundBrowserResolver:
    def __init__(self, resolver: BrowserResolver, provider: Provider) -> None:
        self.resolver = resolver
        self.provider = provider

    def resolve(self, page_url: str):
        return self.resolver.resolve(page_url, self.provider)


class SiteAudioPipeline:
    def __init__(self, options, *, resolvers=None, writer=None) -> None:
        self.options = options
        if resolvers is None:
            transport = CurlCffiTransport()
            inspector = HLSInspector(transport.get_text)
            driver = ChromeBrowserDriver(
                profile_dir=options.profile_dir,
                timeout_seconds=options.browser_timeout,
                headless=options.headless,
            )
            browser = BrowserResolver(driver, inspector)
            resolvers = {
                Provider.JABLE: JableResolver(
                    get_page=transport.get_text,
                    inspector=inspector,
                    browser_resolver=browser,
                ),
                Provider.BESTJAVPORN: _BoundBrowserResolver(browser, Provider.BESTJAVPORN),
            }
        self.resolvers = resolvers
        self.writer = writer or FFmpegAudioWriter()

    def download(self, page_url: str, output_path: Path, *, overwrite: bool) -> Path:
        output_path = Path(output_path)
        if output_path.exists() and not overwrite:
            raise UnsupportedSiteURL(f"output already exists: {output_path}")
        provider = detect_provider(page_url)
        resolver = self.resolvers.get(provider)
        if resolver is None:
            raise UnsupportedSiteURL(f"no resolver configured for {provider.value}")
        for attempt in range(2):
            stream = resolver.resolve(page_url)
            try:
                return self.writer.write(stream, output_path, overwrite=overwrite)
            except DownloadFailure as exc:
                if attempt == 0 and exc.retryable and stream.refreshable:
                    continue
                raise
        raise AssertionError("unreachable")

