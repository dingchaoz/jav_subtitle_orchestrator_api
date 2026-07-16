from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

import requests

from orchestrator.catalog_visibility import (
    PublicCatalogVisibilityClient,
    VisibilityStatus,
    normalize_catalog_api_origin,
    validate_catalog_timeout_seconds,
)
from orchestrator.movie_code import canonical_movie_code


_SYNC_PATH = "/api/admin/catalog/sync-subtitles"
_SAFE_REASON_CODES = {
    "catalog_fetch_failed",
    "catalog_redirect_rejected",
    "catalog_auth_failed",
    "catalog_sync_failed",
    "catalog_response_invalid",
    "catalog_response_mismatch",
    "public_visibility_fetch_failed",
    "public_visibility_redirect_rejected",
    "public_visibility_not_found",
    "public_visibility_response_invalid",
    "public_visibility_mismatch",
}
_CACHE_KEY_VARIANT_SUFFIXES = (
    "-uncensored-leak",
    "-uncensored",
    "-english-subtitle",
    "-chinese-subtitle",
    "-subtitle",
    "-leak",
)
_MAX_DIAGNOSTIC_BYTES = 4096
_SAFE_DIAGNOSTIC_CODE_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
)


def _safe_diagnostic_code(value: object) -> str | None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 96
        or any(character not in _SAFE_DIAGNOSTIC_CODE_CHARS for character in value)
    ):
        return None
    return value


def _safe_unknown_fields(mapping: dict[object, object], known: set[str]) -> list[str]:
    fields: list[str] = []
    for key in mapping:
        if isinstance(key, str) and key not in known:
            fields.append(_safe_diagnostic_code(key) or "<unsafe>")
    return sorted(set(fields))[:32]


def _body_fingerprint(text: object) -> dict[str, object]:
    encoded = text.encode("utf-8", errors="replace") if isinstance(text, str) else b""
    return {
        "bodyBytes": len(encoded),
        "bodySha256": hashlib.sha256(encoded).hexdigest(),
    }


def _sanitize_response_body(body: object, raw_text: object) -> str:
    if not isinstance(body, dict):
        return json.dumps(_body_fingerprint(raw_text), separators=(",", ":"), sort_keys=True)

    known_top = {
        "success",
        "requested",
        "synced",
        "failed",
        "results",
        "fence",
        "action",
        "dryRun",
    }
    safe: dict[str, object] = {}
    for field in ("success", "requested", "synced", "dryRun"):
        value = body.get(field)
        if isinstance(value, bool) or (
            isinstance(value, int) and not isinstance(value, bool)
        ):
            safe[field] = value

    failures: list[dict[str, str]] = []
    if isinstance(body.get("failed"), list):
        for item in body["failed"][:16]:
            if not isinstance(item, dict):
                continue
            failure: dict[str, str] = {}
            canonical = _safe_diagnostic_code(item.get("canonicalCode"))
            error = _safe_diagnostic_code(item.get("error"))
            if canonical is not None:
                failure["canonicalCode"] = canonical
            if error is not None:
                failure["error"] = error
            if failure:
                failures.append(failure)
    safe["failed"] = failures

    results: list[dict[str, object]] = []
    known_result = {
        "canonicalCode",
        "d1RowsUpdated",
        "subtitleCount",
        "kvKeysTouched",
        "kvKeysDeleted",
        "dryRun",
    }
    if isinstance(body.get("results"), list):
        for item in body["results"][:16]:
            if not isinstance(item, dict):
                continue
            result: dict[str, object] = {}
            canonical = _safe_diagnostic_code(item.get("canonicalCode"))
            if canonical is not None:
                result["canonicalCode"] = canonical
            for field in ("d1RowsUpdated", "subtitleCount", "dryRun"):
                value = item.get(field)
                if isinstance(value, bool) or (
                    isinstance(value, int) and not isinstance(value, bool)
                ):
                    result[field] = value
            for field in ("kvKeysTouched", "kvKeysDeleted"):
                value = item.get(field)
                if isinstance(value, list):
                    result[f"{field}Count"] = len(value)
            unknown = _safe_unknown_fields(item, known_result)
            if unknown:
                result["unknownFields"] = unknown
            results.append(result)
    safe["results"] = results

    fence = body.get("fence")
    if isinstance(fence, dict):
        safe_fence: dict[str, object] = {}
        value = fence.get("value")
        accepted = fence.get("accepted")
        if isinstance(value, int) and not isinstance(value, bool):
            safe_fence["value"] = value
        if isinstance(accepted, bool):
            safe_fence["accepted"] = accepted
        unknown = _safe_unknown_fields(fence, {"value", "accepted"})
        if unknown:
            safe_fence["unknownFields"] = unknown
        safe["fence"] = safe_fence
    action = _safe_diagnostic_code(body.get("action"))
    if action is not None:
        safe["action"] = action
    unknown = _safe_unknown_fields(body, known_top)
    if unknown:
        safe["unknownFields"] = unknown

    rendered = json.dumps(safe, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    if len(rendered.encode("utf-8")) <= _MAX_DIAGNOSTIC_BYTES:
        return rendered
    canonical = json.dumps(body, default=str, ensure_ascii=True, sort_keys=True)
    summary = _body_fingerprint(canonical)
    summary["diagnostic"] = "truncated"
    return json.dumps(summary, separators=(",", ":"), sort_keys=True)


_STRENGTHENED_CACHE_SCHEMA_VERSION = "v4"
_MAX_STRENGTHENED_CACHE_KEY_PAIRS = 64
_MAX_STRENGTHENED_CACHE_CODE_LENGTH = 128
_SAFE_CACHE_CODE_RE = re.compile(r"[a-z0-9]+(?:[-_][a-z0-9]+)*\Z")


def _matches_strengthened_v4_code(code: str, canonical: str) -> bool:
    if (
        not isinstance(code, str)
        or len(code) > _MAX_STRENGTHENED_CACHE_CODE_LENGTH
        or _SAFE_CACHE_CODE_RE.fullmatch(code) is None
    ):
        return False
    return code == canonical or code.startswith(f"{canonical}-") or code.startswith(
        f"{canonical}_"
    )


def _valid_strengthened_v4_cache_key_pairs(
    keys: object,
    canonical: str,
) -> bool:
    if (
        not isinstance(keys, list)
        or len(keys) < 2
        or len(keys) % 2 != 0
        or len(keys) > _MAX_STRENGTHENED_CACHE_KEY_PAIRS * 2
        or any(not isinstance(key, str) for key in keys)
        or len(set(keys)) != len(keys)
    ):
        return False

    codes: list[str] = []
    for index in range(0, len(keys), 2):
        full_key, light_key = keys[index : index + 2]
        full_prefix = "movie:full:v4:"
        light_prefix = "movie:light:"
        if not full_key.startswith(full_prefix) or not light_key.startswith(light_prefix):
            return False
        full_code = full_key[len(full_prefix) :]
        light_code = light_key[len(light_prefix) :]
        if full_code != light_code or not _matches_strengthened_v4_code(full_code, canonical):
            return False
        codes.append(full_code)

    return canonical in codes and len(set(codes)) == len(codes)


def _matches_cache_code(code: str, canonical: str, *, allow_aliases: bool) -> bool:
    return code == canonical or (
        allow_aliases
        and any(code == f"{canonical}{suffix}" for suffix in _CACHE_KEY_VARIANT_SUFFIXES)
    )


def _matches_full_cache_key(
    key: str,
    canonical: str,
    *,
    allow_aliases: bool = False,
) -> bool:
    prefix = "movie:full:"
    if not key.startswith(prefix):
        return False
    payload = key[len(prefix) :]
    if _matches_cache_code(payload, canonical, allow_aliases=allow_aliases):
        return True
    parts = payload.split(":")
    if len(parts) != 2:
        return False
    version, code = parts
    return (
        _matches_cache_code(code, canonical, allow_aliases=allow_aliases)
        and bool(version)
        and all(
            character.isascii()
            and (character.isalnum() or character in "._-")
            for character in version
        )
    )


def _matches_light_cache_key(
    key: str,
    canonical: str,
    *,
    allow_aliases: bool = False,
) -> bool:
    prefix = "movie:light:"
    if not key.startswith(prefix):
        return False
    payload = key[len(prefix) :]
    if _matches_cache_code(payload, canonical, allow_aliases=allow_aliases):
        return True
    parts = payload.split(":")
    if len(parts) != 2:
        return False
    version, code = parts
    return (
        _matches_cache_code(code, canonical, allow_aliases=allow_aliases)
        and bool(version)
        and all(
            character.isascii()
            and (character.isalnum() or character in "._-")
            for character in version
        )
    )


class CatalogSyncSession(Protocol):
    def post(self, url: str, **kwargs: Any) -> Any: ...

    def get(self, url: str, **kwargs: Any) -> Any: ...


@dataclass(frozen=True)
class CatalogSyncDiagnostic:
    http_status: int | None
    response_json: str | None


@dataclass(frozen=True)
class CatalogSyncResult:
    canonical_code: str
    d1_rows_updated: int
    subtitle_count: int
    kv_keys_deleted: tuple[str, ...]
    diagnostic: CatalogSyncDiagnostic


class CatalogSyncError(RuntimeError):
    def __init__(
        self,
        reason_code: str,
        *,
        retryable: bool | None = None,
        http_status: int | None = None,
        response_json: str | None = None,
    ) -> None:
        safe_reason = (
            reason_code if reason_code in _SAFE_REASON_CODES else "catalog_sync_failed"
        )
        self.reason_code = safe_reason
        self.retryable = (
            retryable
            if retryable is not None
            else safe_reason
            not in {
                "catalog_redirect_rejected",
                "catalog_auth_failed",
                "public_visibility_redirect_rejected",
            }
        )
        self.http_status = http_status
        self.response_json = response_json
        super().__init__(safe_reason)


class CatalogSyncClient:
    public_visibility_verification_enabled = True

    def __init__(
        self,
        base_url: str,
        admin_token: str,
        *,
        timeout_seconds: int | float = 30,
        session: CatalogSyncSession | None = None,
    ) -> None:
        self.base_url, self.endpoint = self._endpoints(base_url)
        if not isinstance(admin_token, str) or not admin_token.strip():
            raise ValueError("catalog admin token is required")
        self.admin_token = admin_token.strip()
        self.timeout_seconds = validate_catalog_timeout_seconds(timeout_seconds)
        self.session = session or requests.Session()
        self.public_visibility_client = PublicCatalogVisibilityClient(
            self.base_url,
            timeout_seconds=self.timeout_seconds,
            session=self.session,
        )

    def sync(
        self,
        movie_code: str,
        *,
        expected_subtitle_id: str,
        expected_content_sha256: str,
    ) -> CatalogSyncResult:
        invalid_movie_code = False
        try:
            canonical = canonical_movie_code(movie_code)
        except (AttributeError, TypeError, ValueError):
            invalid_movie_code = True
            canonical = ""
        if invalid_movie_code:
            raise ValueError("invalid movie code")
        if (
            not isinstance(expected_subtitle_id, str)
            or not expected_subtitle_id
            or not isinstance(expected_content_sha256, str)
            or len(expected_content_sha256) != 64
            or any(char not in "0123456789abcdef" for char in expected_content_sha256)
        ):
            raise ValueError("verified publication receipt is invalid")

        material = f"{canonical}\0{expected_subtitle_id}\0{expected_content_sha256}"
        idempotency_key = "jso-catalog-" + hashlib.sha256(
            material.encode("utf-8")
        ).hexdigest()
        request_failed = False
        try:
            response = self.session.post(
                self.endpoint,
                headers={
                    "Authorization": f"Bearer {self.admin_token}",
                    "Content-Type": "application/json",
                    "Idempotency-Key": idempotency_key,
                },
                json={
                    "canonicalCodes": [canonical],
                    "reason": "subtitle_ingest",
                    "source": "jav-subtitle-orchestrator",
                    "dryRun": False,
                },
                timeout=self.timeout_seconds,
                allow_redirects=False,
            )
        except (requests.RequestException, OverflowError):
            request_failed = True
            response = None
        if request_failed:
            raise CatalogSyncError(
                "catalog_fetch_failed",
                retryable=True,
            )

        assert response is not None
        status = response.status_code
        parsed = True
        try:
            body = response.json()
        except (TypeError, ValueError):
            parsed = False
            body = None
        response_json = _sanitize_response_body(
            body,
            getattr(response, "text", None),
        )
        diagnostic = CatalogSyncDiagnostic(
            http_status=status,
            response_json=response_json,
        )

        error_metadata = {
            "http_status": status,
            "response_json": response_json,
        }
        if 300 <= status < 400:
            raise CatalogSyncError(
                "catalog_redirect_rejected",
                **error_metadata,
            )
        if status in {401, 403}:
            raise CatalogSyncError(
                "catalog_auth_failed",
                **error_metadata,
            )
        if status == 207:
            raise CatalogSyncError(
                "catalog_sync_failed",
                retryable=True,
                **error_metadata,
            )
        if 500 <= status < 600:
            raise CatalogSyncError(
                "catalog_sync_failed",
                retryable=True,
                **error_metadata,
            )
        if status != 200:
            raise CatalogSyncError("catalog_sync_failed", **error_metadata)
        if not parsed:
            raise CatalogSyncError(
                "catalog_response_invalid",
                retryable=True,
                **error_metadata,
            )
        if not isinstance(body, dict):
            raise CatalogSyncError(
                "catalog_response_invalid",
                retryable=True,
                **error_metadata,
            )

        result_rows = body.get("results")
        if (
            body.get("success") is not True
            or not self._exact_int(body.get("requested"), 1)
            or not self._exact_int(body.get("synced"), 1)
            or body.get("failed") != []
            or not isinstance(result_rows, list)
            or len(result_rows) != 1
            or not isinstance(result_rows[0], dict)
        ):
            raise CatalogSyncError(
                "catalog_response_mismatch",
                retryable=True,
                **error_metadata,
            )

        row = result_rows[0]
        d1_rows_updated = row.get("d1RowsUpdated")
        subtitle_count = row.get("subtitleCount")
        strengthened_response = (
            "cacheSchemaVersion" in body
            or "d1Verified" in row
            or "kvAction" in row
        )
        if strengthened_response:
            allowed_body_keys = {
                "success",
                "requested",
                "synced",
                "failed",
                "results",
                "cacheSchemaVersion",
            }
            allowed_row_keys = {
                "canonicalCode",
                "d1RowsUpdated",
                "d1Verified",
                "subtitleCount",
                "kvAction",
                "kvKeysTouched",
                "kvKeysDeleted",
            }
            touched = row.get("kvKeysTouched")
            deleted = row.get("kvKeysDeleted")
            row_schema_valid = (
                set(body) == allowed_body_keys
                and body.get("cacheSchemaVersion")
                == _STRENGTHENED_CACHE_SCHEMA_VERSION
                and set(row) == allowed_row_keys
                and row.get("d1Verified") is True
                and isinstance(row.get("kvAction"), str)
                and row.get("kvAction")
                in {"written", "deleted_for_d1_fallback", "unchanged"}
                and isinstance(touched, list)
                and isinstance(deleted, list)
                and touched == deleted
            )
            kv_keys_deleted = touched
            kv_keys_valid = row_schema_valid and _valid_strengthened_v4_cache_key_pairs(
                kv_keys_deleted,
                canonical,
            )
            response_canonical = row.get("canonicalCode")
        else:
            dry_run_valid = "dryRun" not in body or body.get("dryRun") is False
            fence = body.get("fence")
            fence_valid = fence is None or (
                isinstance(fence, dict)
                and isinstance(fence.get("value"), int)
                and not isinstance(fence.get("value"), bool)
                and fence.get("value") > 0
                and fence.get("accepted") is True
            )
            action = body.get("action")
            action_valid = action is None or action == "sync"
            row_schema_valid = (
                dry_run_valid
                and fence_valid
                and action_valid
                and ("dryRun" not in row or row.get("dryRun") is False)
            )
            touched = row.get("kvKeysTouched")
            deleted = row.get("kvKeysDeleted")
            touched_valid = touched is None or self._valid_cache_keys(
                touched,
                canonical,
            )
            deleted_valid = deleted is None or self._valid_cache_keys(
                deleted,
                canonical,
            )
            keys_agree = (
                touched is None
                or deleted is None
                or (
                    touched_valid
                    and deleted_valid
                    and set(touched) == set(deleted)
                )
            )
            kv_keys_deleted = touched if isinstance(touched, list) else deleted
            kv_keys_valid = (
                touched_valid
                and deleted_valid
                and keys_agree
                and self._valid_cache_keys(kv_keys_deleted, canonical)
            )
            try:
                response_canonical = canonical_movie_code(row.get("canonicalCode"))
            except (AttributeError, TypeError, ValueError):
                response_canonical = None
        if (
            not row_schema_valid
            or response_canonical != canonical
            or not self._positive_int(d1_rows_updated)
            or not self._positive_int(subtitle_count)
            or not kv_keys_valid
        ):
            raise CatalogSyncError(
                "catalog_response_mismatch",
                retryable=True,
                **error_metadata,
            )

        try:
            self._verify_public_visibility(
                canonical,
                expected_subtitle_id=expected_subtitle_id,
                expected_content_sha256=expected_content_sha256,
            )
        except CatalogSyncError as exc:
            raise CatalogSyncError(
                exc.reason_code,
                retryable=exc.retryable,
                http_status=diagnostic.http_status,
                response_json=diagnostic.response_json,
            ) from None

        return CatalogSyncResult(
            canonical_code=canonical,
            d1_rows_updated=d1_rows_updated,
            subtitle_count=subtitle_count,
            kv_keys_deleted=tuple(kv_keys_deleted),
            diagnostic=diagnostic,
        )

    @staticmethod
    def _valid_cache_keys(value: object, canonical: str) -> bool:
        return (
            isinstance(value, list)
            and len(value) >= 2
            and all(isinstance(key, str) for key in value)
            and len(set(value)) == len(value)
            and any(_matches_light_cache_key(key, canonical, allow_aliases=True) for key in value)
            and any(_matches_full_cache_key(key, canonical, allow_aliases=True) for key in value)
            and all(
                _matches_full_cache_key(key, canonical, allow_aliases=True)
                or _matches_light_cache_key(key, canonical, allow_aliases=True)
                for key in value
            )
        )

    @staticmethod
    def _endpoints(base_url: str) -> tuple[str, str]:
        root = normalize_catalog_api_origin(base_url)
        return root, f"{root}{_SYNC_PATH}"

    def _verify_public_visibility(
        self,
        canonical: str,
        *,
        expected_subtitle_id: str,
        expected_content_sha256: str,
    ) -> None:
        result = self.public_visibility_client.check(
            canonical,
            expected_subtitle_id,
            expected_content_sha256,
        )
        if result.status is VisibilityStatus.VISIBLE:
            return
        if result.status is VisibilityStatus.NOT_FOUND:
            reason_code = "public_visibility_not_found"
        elif result.status is VisibilityStatus.FETCH_FAILED:
            reason_code = result.reason_code or "public_visibility_fetch_failed"
        elif result.status is VisibilityStatus.RESPONSE_INVALID:
            reason_code = "public_visibility_response_invalid"
        else:
            reason_code = "public_visibility_mismatch"
        raise CatalogSyncError(reason_code)

    @staticmethod
    def _exact_int(value: object, expected: int) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value == expected

    @staticmethod
    def _positive_int(value: object) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 1
