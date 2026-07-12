from __future__ import annotations

import json
import math
import re
import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import urlsplit

import requests

from orchestrator.models import (
    AuditStatus,
    MAX_AUDIT_OFFSET,
    ReasonCode,
    SubtitleAuditItem,
    SubtitleAuditPageResponse,
    SubtitleAuditSummaryResponse,
)


_AUDIT_STATUSES = {status.value for status in AuditStatus}
_REASON_CODES = {reason.value for reason in ReasonCode}
_DURATION_CONFIDENCES = {"unknown", "low", "medium", "high"}
_DURATION_SOURCES = {
    "duration_manifest",
    "movies.duration_minutes",
    "sibling_median_last_end",
}
_LATEST_FIELDS = (
    "id,subtitle_id,audit_version,status,score,reason_codes,metrics,"
    "expected_duration_seconds,duration_source,duration_confidence,scanned_at,language"
)
_LATEST_CATALOG_RESOURCE = "subtitle_quality_latest_catalog"
_CATALOG_FIELDS = "id,movie_id,language,file_path,movies!inner(standard_movie_id)"
_SAFE_LANGUAGE = re.compile(r"^[A-Za-z][A-Za-z0-9 _.+-]{0,127}$")
_CONTENT_RANGE = re.compile(r"^(?:(\d+)-(\d+)|(\*))/(\d+)$")
MAX_AUDIT_RESPONSE_BYTES = 5 * 1024 * 1024
_STREAM_CHUNK_BYTES = 64 * 1024
_METRIC_INTEGER_FIELDS = {
    "byte_count",
    "cue_count",
    "text_line_count",
    "text_character_count",
    "unique_text_count",
    "dominant_text_count",
    "invalid_interval_count",
    "timeline_regression_count",
    "overlap_count",
    "long_cue_over_20s_count",
    "replacement_character_count",
    "nul_count",
    "control_character_count",
    "mojibake_marker_count",
}
_METRIC_NUMBER_FIELDS = {
    "unique_text_ratio",
    "dominant_text_ratio",
    "first_start_seconds",
    "last_end_seconds",
    "subtitle_span_seconds",
    "max_gap_seconds",
    "coverage_ratio",
}
_METRIC_BOOLEAN_FIELDS = {"used_fallback"}
_METRIC_STRING_FIELDS = {"encoding", "parse_mode", "dominant_text_sha256"}
_METRIC_ENCODINGS = {"utf-8-sig", "utf-16", "cp932", "gb18030", "big5", "latin-1"}
_METRIC_PARSE_MODES = {"strict", "tolerant", "failed"}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class AuditApiSession(Protocol):
    def get(self, url: str, **kwargs: Any) -> Any: ...
    def post(self, url: str, **kwargs: Any) -> Any: ...
    def close(self) -> None: ...


class SubtitleAuditApiService:
    """Server-side-only, bounded reader for persisted subtitle audit findings."""

    def __init__(
        self,
        url: str,
        service_role_key: str,
        *,
        timeout_seconds: int | float = 30,
        session: AuditApiSession | None = None,
    ) -> None:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username:
            raise ValueError("Supabase URL must be an HTTPS origin")
        if not isinstance(service_role_key, str) or not service_role_key.strip():
            raise ValueError("Supabase service role key is required")
        if isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._url = url.rstrip("/")
        self._key = service_role_key
        self._timeout = timeout_seconds
        self._owns_session = session is None
        self._session = session or requests.Session()

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self._key,
            "Authorization": f"Bearer {self._key}",
            "Accept-Encoding": "identity",
        }

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> SubtitleAuditApiService:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def summary(self) -> SubtitleAuditSummaryResponse:
        response = self._request(
            "post",
            "subtitle audit summary",
            f"{self._url}/rest/v1/rpc/subtitle_quality_latest_summary",
            headers=self._headers,
            json={},
            timeout=self._timeout,
            allow_redirects=False,
        )
        payload = self._payload(response, "subtitle audit summary")
        if not isinstance(payload, dict):
            raise ValueError("subtitle audit summary payload must be an object")
        status_counts = self._count_map(
            payload.get("status_counts"),
            _AUDIT_STATUSES,
            "status counts",
            exact_keys=True,
        )
        reason_counts = self._count_map(
            payload.get("reason_counts"),
            _REASON_CODES,
            "reason counts",
            exact_keys=False,
        )
        total = self._nonnegative_int(payload.get("total_audited"), "total audited")
        catalog = self._nonnegative_int(payload.get("catalog_total"), "catalog total")
        if sum(status_counts.values()) != total:
            raise ValueError("subtitle audit status counts do not match total")
        if total > catalog:
            raise ValueError("subtitle audit total exceeds catalog total")
        latest = payload.get("latest_scanned_at")
        if latest is not None:
            self._timestamp(latest, "latest scanned timestamp")
        progress = total / catalog if catalog else 0.0
        return SubtitleAuditSummaryResponse(
            status_counts=status_counts,
            reason_counts=reason_counts,
            total_audited=total,
            catalog_total=catalog,
            progress_ratio=progress,
            latest_scanned_at=latest,
        )

    def list_findings(
        self,
        *,
        status: str | None,
        language: str | None,
        page: int,
        page_size: int,
    ) -> SubtitleAuditPageResponse:
        if status is not None and status not in _AUDIT_STATUSES:
            raise ValueError("invalid subtitle audit status")
        self.validate_language(language)
        if isinstance(page, bool) or not 1 <= page <= MAX_AUDIT_OFFSET + 1:
            raise ValueError(f"page must be between 1 and {MAX_AUDIT_OFFSET + 1}")
        if isinstance(page_size, bool) or not 1 <= page_size <= 100:
            raise ValueError("page_size must be between 1 and 100")
        offset = (page - 1) * page_size
        if offset > MAX_AUDIT_OFFSET:
            raise ValueError("subtitle audit offset exceeds limit")
        select = _LATEST_FIELDS
        params: dict[str, str] = {
            "select": select,
            "order": "scanned_at.desc,id.desc",
        }
        if status is not None:
            params["status"] = f"eq.{status}"
        if language is not None:
            params["language"] = f"eq.{language}"
        response = self._request(
            "get",
            "subtitle audit findings",
            f"{self._url}/rest/v1/{_LATEST_CATALOG_RESOURCE}",
            params=params,
            headers={
                **self._headers,
                "Prefer": "count=exact",
                "Range": f"{offset}-{offset + page_size - 1}",
            },
            timeout=self._timeout,
            allow_redirects=False,
        )
        payload = self._payload(response, "subtitle audit findings")
        if not isinstance(payload, list) or len(payload) > page_size:
            raise ValueError("subtitle audit findings payload must be a bounded list")
        total = self._total_from_content_range(
            response, offset=offset, row_count=len(payload)
        )
        rows = [self._audit_row(row) for row in payload]
        subtitle_ids = [str(row["subtitle_id"]) for row in rows]
        if len(set(subtitle_ids)) != len(subtitle_ids):
            raise ValueError("subtitle audit page contains duplicate subtitle ids")
        if status is not None and any(row["status"] != status for row in rows):
            raise ValueError("subtitle audit row does not match requested filter")
        if language is not None and any(
            row["view_language"] != language for row in rows
        ):
            raise ValueError("subtitle audit row does not match requested filter")
        metadata = self._catalog_metadata(
            subtitle_ids, language=language
        )
        items = [self._merge_item(row, metadata) for row in rows]
        pages = max((total + page_size - 1) // page_size, 1)
        return SubtitleAuditPageResponse(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            pages=pages,
            accessible_pages=min(pages, MAX_AUDIT_OFFSET // page_size + 1),
        )

    def get_finding(self, subtitle_id: str) -> SubtitleAuditItem | None:
        normalized = str(uuid.UUID(subtitle_id))
        response = self._request(
            "get",
            "subtitle audit finding",
            f"{self._url}/rest/v1/{_LATEST_CATALOG_RESOURCE}",
            params={
                "select": _LATEST_FIELDS,
                "subtitle_id": f"eq.{normalized}",
                "order": "scanned_at.desc,id.desc",
                "limit": "1",
            },
            headers=self._headers,
            timeout=self._timeout,
            allow_redirects=False,
        )
        payload = self._payload(response, "subtitle audit finding")
        if not isinstance(payload, list) or len(payload) > 1:
            raise ValueError("subtitle audit finding payload must be a bounded list")
        if not payload:
            return None
        row = self._audit_row(payload[0])
        if row["subtitle_id"] != normalized:
            raise ValueError("subtitle audit row does not match requested subtitle")
        metadata = self._catalog_metadata([normalized], language=None)
        return self._merge_item(row, metadata)

    @staticmethod
    def validate_language(language: str | None) -> None:
        if language is not None and not _SAFE_LANGUAGE.fullmatch(language):
            raise ValueError("invalid subtitle language filter")

    def _catalog_metadata(
        self, subtitle_ids: list[str], *, language: str | None
    ) -> dict[str, dict[str, str]]:
        if not subtitle_ids:
            return {}
        params = {
            "select": _CATALOG_FIELDS,
            "id": f"in.({','.join(subtitle_ids)})",
            "order": "id.asc",
            "limit": str(len(subtitle_ids)),
        }
        if language is not None:
            params["language"] = f"eq.{language}"
        response = self._request(
            "get",
            "subtitle catalog metadata",
            f"{self._url}/rest/v1/movie_languages",
            params=params,
            headers=self._headers,
            timeout=self._timeout,
            allow_redirects=False,
        )
        payload = self._payload(response, "subtitle catalog metadata")
        if not isinstance(payload, list) or len(payload) > len(subtitle_ids):
            raise ValueError("subtitle catalog metadata payload must be a bounded list")
        parsed: dict[str, dict[str, str]] = {}
        for raw in payload:
            if not isinstance(raw, dict):
                raise ValueError("subtitle catalog metadata row is malformed")
            subtitle_id = self._uuid(raw.get("id"), "catalog metadata id")
            movie_id = self._uuid(raw.get("movie_id"), "catalog metadata movie id")
            language_value = self._nonblank(
                raw.get("language"), "catalog metadata language", 128
            )
            file_path = self._nonblank(
                raw.get("file_path"), "catalog metadata file path", 2048
            )
            movies = raw.get("movies")
            if isinstance(movies, list):
                movies = movies[0] if len(movies) == 1 else None
            if not isinstance(movies, dict):
                raise ValueError("subtitle catalog metadata movie relation is malformed")
            canonical = self._nonblank(
                movies.get("standard_movie_id"), "catalog metadata canonical code", 256
            )
            if subtitle_id in parsed:
                raise ValueError("subtitle catalog metadata contains duplicate ids")
            parsed[subtitle_id] = {
                "movie_id": movie_id,
                "canonical_code": canonical,
                "language": language_value,
                "file_path": file_path,
            }
        if set(parsed) != set(subtitle_ids):
            raise ValueError("subtitle catalog metadata is incomplete")
        return parsed

    def _audit_row(self, raw: object) -> dict[str, object]:
        if not isinstance(raw, dict):
            raise ValueError("subtitle audit row is malformed")
        row_id = self._nonnegative_int(raw.get("id"), "audit id")
        subtitle_id = self._uuid(raw.get("subtitle_id"), "audit subtitle id")
        audit_version = self._nonblank(raw.get("audit_version"), "audit version", 128)
        status = raw.get("status")
        if status not in _AUDIT_STATUSES:
            raise ValueError("subtitle audit row has invalid status")
        score = self._nonnegative_int(raw.get("score"), "audit score")
        if score > 100:
            raise ValueError("subtitle audit row has invalid score")
        reasons_raw = raw.get("reason_codes")
        if (
            not isinstance(reasons_raw, list)
            or any(
                not isinstance(reason, str) or reason not in _REASON_CODES
                for reason in reasons_raw
            )
        ):
            raise ValueError("subtitle audit row has invalid reason codes")
        metrics = self._sanitize_metrics(raw.get("metrics"))
        expected = self._optional_number(
            raw.get("expected_duration_seconds"), "expected duration"
        )
        if expected == 0:
            raise ValueError("subtitle audit expected duration must be positive")
        duration_source = raw.get("duration_source")
        if duration_source is not None and duration_source not in _DURATION_SOURCES:
            raise ValueError("subtitle audit duration source is invalid")
        confidence = raw.get("duration_confidence")
        if confidence not in _DURATION_CONFIDENCES:
            raise ValueError("subtitle audit row has invalid duration confidence")
        scanned_at = raw.get("scanned_at")
        self._timestamp(scanned_at, "audit scanned timestamp")
        view_language = self._nonblank(
            raw.get("language"), "audit view language", 128
        )
        return {
            "id": row_id,
            "subtitle_id": subtitle_id,
            "audit_version": audit_version,
            "status": status,
            "score": score,
            "reason_codes": reasons_raw,
            "metrics": metrics,
            "expected_duration_seconds": expected,
            "duration_source": duration_source,
            "duration_confidence": confidence,
            "scanned_at": scanned_at,
            "view_language": view_language,
        }

    def _merge_item(
        self, row: dict[str, object], metadata: Mapping[str, Mapping[str, str]]
    ) -> SubtitleAuditItem:
        subtitle_id = str(row["subtitle_id"])
        catalog = metadata.get(subtitle_id)
        if catalog is None:
            raise ValueError("subtitle catalog metadata is incomplete")
        if catalog["language"] != row["view_language"]:
            raise ValueError("subtitle catalog metadata does not match audit view")
        output_row = {key: value for key, value in row.items() if key != "view_language"}
        return SubtitleAuditItem(**output_row, **catalog)

    def _payload(self, response: Any, operation: str) -> object:
        try:
            status = getattr(response, "status_code", None)
            if not isinstance(status, int) or not 200 <= status < 300:
                raise RuntimeError(f"{operation} failed")
            headers = getattr(response, "headers", None) or {}
            raw_length = headers.get("Content-Length")
            declared_length: int | None = None
            if raw_length is not None:
                if not isinstance(raw_length, str) or not re.fullmatch(
                    r"[0-9]+", raw_length
                ):
                    raise ValueError(f"{operation} has invalid Content-Length") from None
                declared_length = int(raw_length)
                if declared_length > MAX_AUDIT_RESPONSE_BYTES:
                    raise ValueError(f"{operation} exceeds response size limit")

            chunks: list[bytes] = []
            observed = 0
            try:
                iterator = response.iter_content(chunk_size=_STREAM_CHUNK_BYTES)
                for chunk in iterator:
                    if not chunk:
                        continue
                    if not isinstance(chunk, bytes):
                        raise ValueError("response chunk is not bytes")
                    observed += len(chunk)
                    if observed > MAX_AUDIT_RESPONSE_BYTES:
                        raise ValueError(f"{operation} exceeds response size limit")
                    if declared_length is not None and observed > declared_length:
                        raise ValueError(
                            f"{operation} Content-Length does not match response body"
                        )
                    chunks.append(chunk)
            except ValueError:
                raise
            except Exception:
                raise ValueError(f"{operation} response stream failed") from None
            if declared_length is not None and observed != declared_length:
                raise ValueError(
                    f"{operation} Content-Length does not match response body"
                )
            try:
                decoded = b"".join(chunks).decode("utf-8", errors="strict")
                return json.loads(
                    decoded,
                    object_pairs_hook=self._reject_duplicate_json_keys,
                    parse_constant=self._reject_json_constant,
                )
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                raise ValueError(f"{operation} returned invalid JSON") from None
        finally:
            try:
                response.close()
            except Exception:
                pass

    def _request(self, method: str, operation: str, url: str, **kwargs: Any) -> Any:
        kwargs["allow_redirects"] = False
        kwargs["stream"] = True
        try:
            request = getattr(self._session, method)
            return request(url, **kwargs)
        except Exception:
            raise RuntimeError(f"{operation} request failed") from None

    @staticmethod
    def _total_from_content_range(
        response: Any, *, offset: int, row_count: int
    ) -> int:
        headers = getattr(response, "headers", None) or {}
        value = headers.get("Content-Range")
        match = _CONTENT_RANGE.fullmatch(value or "")
        if match is None:
            raise ValueError("subtitle audit response has invalid Content-Range")
        total = int(match.group(4))
        start_raw, end_raw, wildcard = match.group(1), match.group(2), match.group(3)
        if wildcard is not None:
            if row_count != 0 or not (total == 0 or offset >= total):
                raise ValueError("subtitle audit response has invalid Content-Range")
            return total
        assert start_raw is not None and end_raw is not None
        start, end = int(start_raw), int(end_raw)
        if (
            row_count == 0
            or start != offset
            or end != offset + row_count - 1
            or end >= total
        ):
            raise ValueError("subtitle audit response has invalid Content-Range")
        return total

    @staticmethod
    def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    @staticmethod
    def _reject_json_constant(_value: str) -> object:
        raise ValueError("non-finite JSON constant")

    @staticmethod
    def _count_map(
        value: object,
        allowed: set[str],
        label: str,
        *,
        exact_keys: bool,
    ) -> dict[str, int]:
        if not isinstance(value, dict):
            raise ValueError(f"subtitle audit {label} must be an object")
        if any(not isinstance(key, str) or key not in allowed for key in value):
            raise ValueError(f"subtitle audit {label} contains unknown keys")
        if exact_keys and set(value) != allowed:
            raise ValueError(f"subtitle audit {label} must include every status")
        result: dict[str, int] = {}
        for key, count in value.items():
            result[key] = SubtitleAuditApiService._nonnegative_int(count, label)
        return result

    @staticmethod
    def _nonnegative_int(value: object, label: str) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"subtitle audit {label} must be a non-negative integer")
        return value

    @staticmethod
    def _optional_number(value: object, label: str) -> float | None:
        if value is None:
            return None
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(f"subtitle audit {label} must be numeric")
        number = float(value)
        if not math.isfinite(number) or number < 0:
            raise ValueError(f"subtitle audit {label} must be finite and non-negative")
        return number

    @staticmethod
    def _nonblank(value: object, label: str, max_length: int) -> str:
        if not isinstance(value, str) or not value.strip() or len(value) > max_length:
            raise ValueError(f"subtitle {label} is malformed")
        return value

    @staticmethod
    def _uuid(value: object, label: str) -> str:
        if not isinstance(value, str):
            raise ValueError(f"subtitle {label} is malformed")
        try:
            return str(uuid.UUID(value))
        except ValueError as exc:
            raise ValueError(f"subtitle {label} is malformed") from exc

    @staticmethod
    def _timestamp(value: object, label: str) -> None:
        if not isinstance(value, str) or len(value) > 64:
            raise ValueError(f"subtitle audit {label} is malformed")
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"subtitle audit {label} is malformed") from exc

    @staticmethod
    def _sanitize_metrics(value: object) -> dict[str, int | float | bool | str | None]:
        if not isinstance(value, dict):
            raise ValueError("subtitle audit metrics must be an object")
        sanitized: dict[str, int | float | bool | str | None] = {}
        for key, metric in value.items():
            if key in _METRIC_INTEGER_FIELDS:
                sanitized[key] = SubtitleAuditApiService._nonnegative_int(metric, key)
            elif key in _METRIC_NUMBER_FIELDS:
                sanitized[key] = SubtitleAuditApiService._optional_number(metric, key)
            elif key in _METRIC_BOOLEAN_FIELDS:
                if not isinstance(metric, bool):
                    raise ValueError(f"subtitle audit metric {key} must be boolean")
                sanitized[key] = metric
            elif key in _METRIC_STRING_FIELDS:
                if key == "dominant_text_sha256":
                    if metric is not None and (
                        not isinstance(metric, str) or _SHA256.fullmatch(metric) is None
                    ):
                        raise ValueError("subtitle audit metric dominant hash is invalid")
                    sanitized[key] = metric
                elif key == "encoding":
                    if metric not in _METRIC_ENCODINGS:
                        raise ValueError("subtitle audit metric encoding is invalid")
                    sanitized[key] = metric
                else:
                    if metric not in _METRIC_PARSE_MODES:
                        raise ValueError("subtitle audit metric parse mode is invalid")
                    sanitized[key] = metric
        return sanitized
