from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

from .errors import UnsupportedSiteURL
from .models import Provider


_PROVIDERS = {
    "jable.tv": (Provider.JABLE, "/videos/"),
    "www.jable.tv": (Provider.JABLE, "/videos/"),
    "bestjavporn.com": (Provider.BESTJAVPORN, "/video/"),
    "www.bestjavporn.com": (Provider.BESTJAVPORN, "/video/"),
}


def detect_provider(url: str) -> Provider:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise UnsupportedSiteURL("movie URL must be a supported HTTPS page")
    match = _PROVIDERS.get(parsed.hostname.lower())
    if match is None:
        raise UnsupportedSiteURL(f"unsupported movie site: {parsed.hostname}")
    provider, path_prefix = match
    if not parsed.path.startswith(path_prefix) or parsed.path.rstrip("/") == path_prefix.rstrip("/"):
        raise UnsupportedSiteURL("URL is not a supported movie page")
    return provider


def default_output_path(url: str) -> Path:
    detect_provider(url)
    slug = unquote(urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1])
    safe_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", slug).strip("._")
    if not safe_slug:
        raise UnsupportedSiteURL("movie URL has no usable output name")
    return Path.cwd() / f"{safe_slug}.wav"

