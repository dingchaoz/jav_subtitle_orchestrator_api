from __future__ import annotations

import ipaddress
import math
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol
from urllib.parse import quote, urlencode, urlsplit

import requests

from orchestrator.movie_code import canonical_movie_code


_LOCAL_HTTP_HOSTS = {"localhost", "127.0.0.1", "::1"}


class VisibilityStatus(str, Enum):
    VISIBLE = "visible"
    MISSING = "missing"
    NOT_FOUND = "not_found"
    FETCH_FAILED = "fetch_failed"
    RESPONSE_INVALID = "response_invalid"
    INVALID_RECEIPT = "invalid_receipt"


@dataclass(frozen=True, slots=True)
class PublicVisibilityResult:
    status: VisibilityStatus
    canonical_code: str
    expected_subtitle_id: str
    observed_subtitle_ids: tuple[str, ...] = ()
    reason_code: str | None = None


class VisibilitySession(Protocol):
    def get(self, url: str, **kwargs: Any) -> Any: ...


def _valid_hostname(hostname: str) -> bool:
    if ":" in hostname:
        try:
            ipaddress.IPv6Address(hostname)
        except ipaddress.AddressValueError:
            return False
        return True

    try:
        ascii_hostname = hostname.encode("idna").decode("ascii")
    except UnicodeError:
        return False
    if ascii_hostname != hostname:
        return False
    if ascii_hostname.endswith("."):
        ascii_hostname = ascii_hostname[:-1]
    labels = ascii_hostname.split(".")
    return (
        bool(ascii_hostname)
        and len(ascii_hostname) <= 253
        and all(
            label
            and len(label) <= 63
            and label[0].isalnum()
            and label[-1].isalnum()
            and all(character.isalnum() or character == "-" for character in label)
            for label in labels
        )
    )


def _authority_matches_hostname(
    authority: str,
    hostname: str,
    port: int | None,
) -> bool:
    host_authority = f"[{hostname}]" if ":" in hostname else hostname
    expected_authority = (
        f"{host_authority}:{port}" if port is not None else host_authority
    )
    return authority.lower() == expected_authority.lower()


def validate_catalog_timeout_seconds(timeout_seconds: object) -> int | float:
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or timeout_seconds <= 0
        or (isinstance(timeout_seconds, float) and not math.isfinite(timeout_seconds))
    ):
        raise ValueError("timeout_seconds must be positive")
    return timeout_seconds


def normalize_catalog_api_origin(base_url: str) -> str:
    if (
        not isinstance(base_url, str)
        or not base_url
        or any(character.isspace() for character in base_url)
    ):
        raise ValueError("catalog API base URL is invalid")
    parse_failed = False
    try:
        parsed = urlsplit(base_url)
        hostname = parsed.hostname
        has_credentials = parsed.username is not None or parsed.password is not None
        port = parsed.port
    except ValueError:
        parse_failed = True
        parsed = None
        hostname = None
        port = None
        has_credentials = True
    if parse_failed:
        raise ValueError("catalog API base URL is invalid")

    valid_transport = parsed.scheme == "https" or (
        parsed.scheme == "http" and hostname in _LOCAL_HTTP_HOSTS
    )
    if (
        not valid_transport
        or not parsed.netloc
        or not hostname
        or not _valid_hostname(hostname)
        or not _authority_matches_hostname(parsed.netloc, hostname, port)
        or has_credentials
        or parsed.netloc.endswith(":")
        or parsed.path not in {"", "/"}
        or "?" in base_url
        or "#" in base_url
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("catalog API base URL is invalid")
    return f"{parsed.scheme}://{parsed.netloc}"


class PublicCatalogVisibilityClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: int | float = 30,
        session: VisibilitySession | None = None,
    ) -> None:
        self.base_url = normalize_catalog_api_origin(base_url)
        self.timeout_seconds = validate_catalog_timeout_seconds(timeout_seconds)
        self.session = session or requests.Session()

    def check(
        self,
        movie_code: str,
        expected_subtitle_id: str,
        content_sha256: str,
    ) -> PublicVisibilityResult:
        try:
            canonical = canonical_movie_code(movie_code)
        except (AttributeError, TypeError, ValueError):
            return PublicVisibilityResult(
                VisibilityStatus.INVALID_RECEIPT,
                "",
                expected_subtitle_id if isinstance(expected_subtitle_id, str) else "",
                reason_code="invalid_receipt",
            )
        if (
            not isinstance(expected_subtitle_id, str)
            or not expected_subtitle_id
            or not isinstance(content_sha256, str)
            or len(content_sha256) != 64
            or any(character not in "0123456789abcdef" for character in content_sha256)
        ):
            return PublicVisibilityResult(
                VisibilityStatus.INVALID_RECEIPT,
                canonical,
                expected_subtitle_id if isinstance(expected_subtitle_id, str) else "",
                reason_code="invalid_receipt",
            )

        result_args = (canonical, expected_subtitle_id)
        try:
            response = self.session.get(
                f"{self.base_url}/api/movie/{quote(canonical, safe='')}?"
                f"{urlencode({'cacheNonce': content_sha256})}",
                timeout=self.timeout_seconds,
                allow_redirects=False,
            )
        except requests.RequestException:
            return PublicVisibilityResult(
                VisibilityStatus.FETCH_FAILED,
                *result_args,
                reason_code="public_visibility_fetch_failed",
            )

        if 300 <= response.status_code < 400:
            return PublicVisibilityResult(
                VisibilityStatus.FETCH_FAILED,
                *result_args,
                reason_code="public_visibility_redirect_rejected",
            )
        if response.status_code == 404:
            return PublicVisibilityResult(
                VisibilityStatus.NOT_FOUND,
                *result_args,
                reason_code="public_visibility_not_found",
            )
        if response.status_code != 200:
            return PublicVisibilityResult(
                VisibilityStatus.FETCH_FAILED,
                *result_args,
                reason_code="public_visibility_fetch_failed",
            )

        try:
            body = response.json()
        except (TypeError, ValueError):
            return PublicVisibilityResult(
                VisibilityStatus.RESPONSE_INVALID,
                *result_args,
                reason_code="public_visibility_response_invalid",
            )
        if (
            not isinstance(body, dict)
            or body.get("canonicalCode") != canonical
            or not isinstance(body.get("subtitles"), list)
            or any(
                not isinstance(row, dict) or not isinstance(row.get("id"), str)
                for row in body.get("subtitles", ())
            )
        ):
            return PublicVisibilityResult(
                VisibilityStatus.RESPONSE_INVALID,
                *result_args,
                reason_code="public_visibility_response_invalid",
            )

        observed_ids = tuple(row["id"] for row in body["subtitles"])
        if observed_ids.count(expected_subtitle_id) != 1:
            return PublicVisibilityResult(
                VisibilityStatus.MISSING,
                *result_args,
                observed_subtitle_ids=observed_ids,
                reason_code="public_visibility_mismatch",
            )
        return PublicVisibilityResult(
            VisibilityStatus.VISIBLE,
            *result_args,
            observed_subtitle_ids=observed_ids,
        )
