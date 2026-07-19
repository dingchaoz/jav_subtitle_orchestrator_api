from __future__ import annotations

import ipaddress
import re
import socket
from dataclasses import dataclass
from typing import Callable, Iterable, Mapping
from urllib.parse import urljoin, urlsplit

from .errors import ManifestValidationError


@dataclass(frozen=True)
class TextResponse:
    url: str
    status_code: int
    text: str


@dataclass(frozen=True)
class InspectedPlaylist:
    media_url: str
    duration: float
    resource_urls: tuple[str, ...]


FetchText = Callable[[str, Mapping[str, str]], TextResponse]
ResolveHost = Callable[[str], Iterable[str]]
_ATTRIBUTE_RE = re.compile(r"([A-Z0-9-]+)=(\"[^\"]*\"|[^,]*)", re.IGNORECASE)
_AUDIO_CODECS = ("mp4a", "aac", "ac-3", "ec-3", "opus", "vorbis")


def _system_resolve_host(hostname: str) -> list[str]:
    return sorted({entry[4][0] for entry in socket.getaddrinfo(hostname, 443)})


def validate_public_https_url(url: str, resolve_host: ResolveHost = _system_resolve_host) -> None:
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise ManifestValidationError(f"URL is not public HTTPS: {url}") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
    ):
        raise ManifestValidationError(f"URL is not public HTTPS: {url}")
    try:
        addresses = [parsed.hostname] if _is_ip_literal(parsed.hostname) else list(resolve_host(parsed.hostname))
        if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
            raise ValueError("non-public address")
    except (OSError, ValueError) as exc:
        raise ManifestValidationError(f"URL is not public HTTPS: {url}") from exc


def _is_ip_literal(hostname: str) -> bool:
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return True


def _attributes(line: str) -> dict[str, str]:
    _, _, raw = line.partition(":")
    parsed: dict[str, str] = {}
    for key, value in _ATTRIBUTE_RE.findall(raw):
        parsed[key.upper()] = value[1:-1] if value.startswith('"') and value.endswith('"') else value
    return parsed


class HLSInspector:
    def __init__(
        self,
        fetch_text: FetchText,
        *,
        resolve_host: ResolveHost = _system_resolve_host,
        max_master_depth: int = 3,
    ) -> None:
        self.fetch_text = fetch_text
        self.resolve_host = resolve_host
        self.max_master_depth = max_master_depth

    def inspect(self, manifest_url: str, headers: Mapping[str, str]) -> InspectedPlaylist:
        return self._inspect(manifest_url, dict(headers), depth=0)

    def _inspect(
        self,
        manifest_url: str,
        headers: Mapping[str, str],
        *,
        depth: int,
    ) -> InspectedPlaylist:
        if depth > self.max_master_depth:
            raise ManifestValidationError("HLS master playlist nesting is too deep")
        validate_public_https_url(manifest_url, self.resolve_host)
        response = self.fetch_text(manifest_url, headers)
        validate_public_https_url(response.url, self.resolve_host)
        if response.status_code != 200:
            raise ManifestValidationError(f"manifest returned HTTP {response.status_code}")
        text = response.text.lstrip("\ufeff\r\n ")
        if not text.startswith("#EXTM3U"):
            raise ManifestValidationError("response is not an HLS playlist")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        variants = self._master_variants(lines, response.url)
        if variants:
            variants_with_audio = [variant for variant in variants if variant[2]]
            if not variants_with_audio:
                raise ManifestValidationError("master playlist has no audio-bearing variant")
            _, selected_url, _, audio_group = max(
                variants_with_audio, key=lambda variant: variant[0]
            )
            if audio_group:
                renditions = self._audio_renditions(lines, response.url).get(audio_group, [])
                external = [rendition for rendition in renditions if rendition[1]]
                if external:
                    _, selected_url = max(external, key=lambda rendition: rendition[0])
            self._reject_cookie_cross_origin(headers, response.url, selected_url)
            return self._inspect(selected_url, headers, depth=depth + 1)
        return self._inspect_media(lines, response.url, headers)

    def _master_variants(
        self, lines: list[str], base_url: str
    ) -> list[tuple[int, str, bool, str | None]]:
        variants: list[tuple[int, str, bool, str | None]] = []
        for index, line in enumerate(lines):
            if not line.upper().startswith("#EXT-X-STREAM-INF:"):
                continue
            attributes = _attributes(line)
            if index + 1 >= len(lines) or lines[index + 1].startswith("#"):
                raise ManifestValidationError("master playlist variant has no URI")
            uri = urljoin(base_url, lines[index + 1])
            validate_public_https_url(uri, self.resolve_host)
            codecs = attributes.get("CODECS", "").lower()
            audio_group = attributes.get("AUDIO")
            has_audio = bool(audio_group) or not codecs or any(
                codec in codecs for codec in _AUDIO_CODECS
            )
            try:
                bandwidth = int(attributes.get("BANDWIDTH", "0"))
            except ValueError as exc:
                raise ManifestValidationError("master playlist has invalid BANDWIDTH") from exc
            variants.append((bandwidth, uri, has_audio, audio_group))
        return variants

    def _audio_renditions(
        self, lines: list[str], base_url: str
    ) -> dict[str, list[tuple[int, str | None]]]:
        groups: dict[str, list[tuple[int, str | None]]] = {}
        for line in lines:
            if not line.upper().startswith("#EXT-X-MEDIA:"):
                continue
            attributes = _attributes(line)
            if attributes.get("TYPE", "").upper() != "AUDIO" or not attributes.get("GROUP-ID"):
                continue
            uri = attributes.get("URI")
            resolved_uri = urljoin(base_url, uri) if uri else None
            if resolved_uri:
                validate_public_https_url(resolved_uri, self.resolve_host)
            priority = 2 if attributes.get("DEFAULT", "").upper() == "YES" else 1
            groups.setdefault(attributes["GROUP-ID"], []).append((priority, resolved_uri))
        return groups

    def _inspect_media(
        self,
        lines: list[str],
        media_url: str,
        headers: Mapping[str, str],
    ) -> InspectedPlaylist:
        if "#EXT-X-ENDLIST" not in {line.upper() for line in lines}:
            raise ManifestValidationError("VOD playlist is missing EXT-X-ENDLIST")
        duration = 0.0
        resources: list[str] = []
        for line in lines:
            upper = line.upper()
            if upper.startswith("#EXTINF:"):
                raw_duration = line.partition(":")[2].partition(",")[0]
                try:
                    duration += float(raw_duration)
                except ValueError as exc:
                    raise ManifestValidationError("media playlist has invalid EXTINF") from exc
            elif upper.startswith("#EXT-X-KEY:"):
                attributes = _attributes(line)
                method = attributes.get("METHOD", "").upper()
                if method not in {"NONE", "AES-128"}:
                    raise ManifestValidationError(f"unsupported encryption method: {method or 'missing'}")
                key_format = attributes.get("KEYFORMAT", "identity")
                if key_format != "identity":
                    raise ManifestValidationError(f"unsupported KEYFORMAT: {key_format}")
                if method == "AES-128":
                    self._append_resource(resources, media_url, attributes.get("URI"), headers)
            elif upper.startswith("#EXT-X-MAP:"):
                self._append_resource(
                    resources, media_url, _attributes(line).get("URI"), headers
                )
            elif not line.startswith("#"):
                self._append_resource(resources, media_url, line, headers)
        if duration <= 0:
            raise ManifestValidationError("media playlist has no positive duration")
        return InspectedPlaylist(media_url=media_url, duration=duration, resource_urls=tuple(resources))

    def _append_resource(
        self,
        resources: list[str],
        base_url: str,
        uri: str | None,
        headers: Mapping[str, str],
    ) -> None:
        if not uri:
            raise ManifestValidationError("HLS resource is missing URI")
        resource_url = urljoin(base_url, uri)
        validate_public_https_url(resource_url, self.resolve_host)
        self._reject_cookie_cross_origin(headers, base_url, resource_url)
        resources.append(resource_url)

    @staticmethod
    def _reject_cookie_cross_origin(
        headers: Mapping[str, str], source_url: str, target_url: str
    ) -> None:
        if not headers.get("Cookie"):
            return
        source = urlsplit(source_url)
        target = urlsplit(target_url)
        if (source.scheme, source.hostname, source.port) != (
            target.scheme,
            target.hostname,
            target.port,
        ):
            raise ManifestValidationError(
                "cookie-authenticated HLS contains a cross-origin resource"
            )
