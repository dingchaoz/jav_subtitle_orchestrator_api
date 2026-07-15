from __future__ import annotations

import ipaddress
import math
import re
import unicodedata
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol
from urllib.parse import quote, urlencode, urlsplit

import requests

from orchestrator.movie_code import canonical_movie_code
from orchestrator.store import JobRecord, validate_verified_supabase_receipt


_LOCAL_HTTP_HOSTS = {"localhost", "127.0.0.1", "::1"}


class VisibilityStatus(str, Enum):
    VISIBLE = "visible"
    MISSING = "missing"
    NOT_FOUND = "not_found"
    FETCH_FAILED = "fetch_failed"
    RESPONSE_INVALID = "response_invalid"
    INVALID_RECEIPT = "invalid_receipt"


@dataclass(frozen=True, slots=True)
class AuditCandidateSnapshot:
    job_id: str
    movie_code: str
    movie_uuid: str | None
    metadata_status: str | None
    metadata_source: str | None
    subtitle_id: str | None
    storage_path: str | None
    content_sha256: str | None
    file_size: int | None
    job_updated_at: str

    @classmethod
    def from_job(cls, job: JobRecord) -> AuditCandidateSnapshot:
        return cls(
            job_id=job.id,
            movie_code=canonical_movie_code(job.normalized_movie_number),
            movie_uuid=job.catalog_movie_uuid,
            metadata_status=job.metadata_status,
            metadata_source=job.metadata_source,
            subtitle_id=job.published_subtitle_id,
            storage_path=job.published_storage_path,
            content_sha256=job.published_content_sha256,
            file_size=job.published_file_size,
            job_updated_at=job.updated_at,
        )

    def validated_receipt(self) -> PublicationReceiptSnapshot:
        return PublicationReceiptSnapshot.from_candidate(self)


@dataclass(frozen=True, slots=True)
class PublicationReceiptSnapshot:
    job_id: str
    movie_code: str
    movie_uuid: str
    metadata_status: str
    metadata_source: str
    subtitle_id: str
    storage_path: str
    content_sha256: str
    file_size: int
    job_updated_at: str

    @classmethod
    def from_candidate(
        cls, candidate: AuditCandidateSnapshot
    ) -> PublicationReceiptSnapshot:
        validate_verified_supabase_receipt(
            movie_code=candidate.movie_code,
            movie_uuid=candidate.movie_uuid,
            metadata_status=candidate.metadata_status,
            metadata_source=candidate.metadata_source,
            subtitle_id=candidate.subtitle_id,
            storage_path=candidate.storage_path,
            content_sha256=candidate.content_sha256,
            file_size=candidate.file_size,
        )
        assert isinstance(candidate.movie_uuid, str)
        assert isinstance(candidate.metadata_status, str)
        assert isinstance(candidate.metadata_source, str)
        assert isinstance(candidate.subtitle_id, str)
        assert isinstance(candidate.storage_path, str)
        assert isinstance(candidate.content_sha256, str)
        assert isinstance(candidate.file_size, int)
        return cls(
            job_id=candidate.job_id,
            movie_code=candidate.movie_code,
            movie_uuid=candidate.movie_uuid,
            metadata_status=candidate.metadata_status,
            metadata_source=candidate.metadata_source,
            subtitle_id=candidate.subtitle_id,
            storage_path=candidate.storage_path,
            content_sha256=candidate.content_sha256,
            file_size=candidate.file_size,
            job_updated_at=candidate.job_updated_at,
        )


@dataclass(frozen=True, slots=True)
class PublicVisibilityResult:
    status: VisibilityStatus
    canonical_code: str
    expected_subtitle_id: str
    observed_subtitle_ids: tuple[str, ...] = ()
    reason_code: str | None = None


class VisibilitySession(Protocol):
    def get(self, url: str, **kwargs: Any) -> Any: ...


def _normalize_bracketed_hostname(hostname: str) -> str | None:
    try:
        ipaddress.IPv6Address(hostname)
        return hostname
    except ipaddress.AddressValueError:
        pass
    if re.fullmatch(
        r"[vV][0-9A-Fa-f]+\.[A-Za-z0-9._~!$&'()*+,;=:-]+",
        hostname,
    ):
        return hostname
    return None


def _normalize_dns_hostname(hostname: str) -> str | None:
    if unicodedata.normalize("NFKC", hostname) != hostname:
        return None
    try:
        prepared_url = requests.Request("GET", f"https://{hostname}/").prepare().url
        prepared = urlsplit(prepared_url)
        ascii_hostname = prepared.hostname
        prepared_port = prepared.port
        has_credentials = (
            prepared.username is not None or prepared.password is not None
        )
    except (requests.RequestException, TypeError, UnicodeError, ValueError):
        return None
    if (
        not isinstance(prepared_url, str)
        or prepared.scheme != "https"
        or not ascii_hostname
        or not ascii_hostname.isascii()
        or prepared.netloc != ascii_hostname
        or prepared_port is not None
        or has_credentials
        or prepared.path != "/"
        or prepared.query
        or prepared.fragment
    ):
        return None
    hostname_for_validation = ascii_hostname.removesuffix(".")
    labels = hostname_for_validation.split(".")
    if (
        not hostname_for_validation
        or len(hostname_for_validation) > 253
        or any(
            not label
            or len(label) > 63
            or not label[0].isalnum()
            or not label[-1].isalnum()
            or any(
                not character.isalnum() and character != "-"
                for character in label
            )
            for label in labels
        )
    ):
        return None
    return ascii_hostname


def _normalize_authority(
    authority: str,
    hostname: str,
    port: int | None,
) -> str | None:
    bracketed = authority.startswith("[")
    if bracketed:
        closing_bracket = authority.find("]")
        if closing_bracket < 0:
            return None
        raw_hostname = authority[1:closing_bracket]
        suffix = authority[closing_bracket + 1 :]
        if "[" in raw_hostname or "]" in raw_hostname:
            return None
    else:
        if "[" in authority or "]" in authority or authority.count(":") > 1:
            return None
        raw_hostname, separator, raw_port = authority.rpartition(":")
        if not separator:
            raw_hostname = authority
            suffix = ""
        else:
            suffix = f":{raw_port}"

    if suffix:
        if not suffix.startswith(":") or not suffix[1:].isdigit():
            return None
        try:
            raw_port_value = int(suffix[1:])
        except ValueError:
            return None
        if port is None or raw_port_value != port:
            return None
    elif port is not None:
        return None

    if raw_hostname.casefold() != hostname.casefold():
        return None
    normalized_hostname = (
        _normalize_bracketed_hostname(raw_hostname)
        if bracketed
        else _normalize_dns_hostname(raw_hostname)
    )
    if normalized_hostname is None:
        return None
    host_authority = f"[{normalized_hostname}]" if bracketed else normalized_hostname
    return f"{host_authority}:{port}" if port is not None else host_authority


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

    normalized_authority = (
        _normalize_authority(parsed.netloc, hostname, port) if hostname else None
    )
    valid_transport = parsed.scheme == "https" or (
        parsed.scheme == "http" and hostname in _LOCAL_HTTP_HOSTS
    )
    if (
        not valid_transport
        or not parsed.netloc
        or not hostname
        or normalized_authority is None
        or has_credentials
        or parsed.netloc.endswith(":")
        or parsed.path not in {"", "/"}
        or "?" in base_url
        or "#" in base_url
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("catalog API base URL is invalid")
    return f"{parsed.scheme}://{normalized_authority}"


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
        except (requests.RequestException, OverflowError):
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
