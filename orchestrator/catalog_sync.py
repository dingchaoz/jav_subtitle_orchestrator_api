from __future__ import annotations

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
    return key.startswith(prefix) and _matches_cache_code(
        key[len(prefix) :],
        canonical,
        allow_aliases=allow_aliases,
    )


class CatalogSyncSession(Protocol):
    def post(self, url: str, **kwargs: Any) -> Any: ...

    def get(self, url: str, **kwargs: Any) -> Any: ...


@dataclass(frozen=True)
class CatalogSyncResult:
    canonical_code: str
    d1_rows_updated: int
    subtitle_count: int
    kv_keys_deleted: tuple[str, ...]


class CatalogSyncError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        safe_reason = (
            reason_code if reason_code in _SAFE_REASON_CODES else "catalog_sync_failed"
        )
        self.reason_code = safe_reason
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

        request_failed = False
        try:
            response = self.session.post(
                self.endpoint,
                headers={
                    "Authorization": f"Bearer {self.admin_token}",
                    "Content-Type": "application/json",
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
        if request_failed:
            raise CatalogSyncError("catalog_fetch_failed")

        if 300 <= response.status_code < 400:
            raise CatalogSyncError("catalog_redirect_rejected")
        if response.status_code in {401, 403}:
            raise CatalogSyncError("catalog_auth_failed")
        if response.status_code != 200:
            raise CatalogSyncError("catalog_sync_failed")

        invalid_json = False
        try:
            body = response.json()
        except (TypeError, ValueError):
            invalid_json = True
            body = None
        if invalid_json:
            raise CatalogSyncError("catalog_response_invalid")
        if not isinstance(body, dict):
            raise CatalogSyncError("catalog_response_invalid")

        result_rows = body.get("results")
        body_keys = set(body)
        allowed_body_keys = {"success", "requested", "synced", "failed", "results"}
        if body_keys == allowed_body_keys:
            response_family = "legacy"
        elif body_keys == allowed_body_keys | {"dryRun"} and body.get("dryRun") is False:
            response_family = "current"
        elif body_keys == allowed_body_keys | {"cacheSchemaVersion"}:
            response_family = "strengthened"
        else:
            response_family = None
        if (
            response_family is None
            or body.get("success") is not True
            or not self._exact_int(body.get("requested"), 1)
            or not self._exact_int(body.get("synced"), 1)
            or body.get("failed") != []
            or not isinstance(result_rows, list)
            or len(result_rows) != 1
            or not isinstance(result_rows[0], dict)
        ):
            raise CatalogSyncError("catalog_response_mismatch")

        row = result_rows[0]
        d1_rows_updated = row.get("d1RowsUpdated")
        subtitle_count = row.get("subtitleCount")
        row_keys = set(row)
        if response_family == "legacy" and row_keys == {
            "canonicalCode",
            "d1RowsUpdated",
            "subtitleCount",
            "kvKeysDeleted",
            "dryRun",
        }:
            kv_keys_deleted = row.get("kvKeysDeleted")
            row_schema_valid = row.get("dryRun") is False
            allow_alias_cache_keys = False
        elif response_family == "current" and row_keys == {
            "canonicalCode",
            "d1RowsUpdated",
            "subtitleCount",
            "kvKeysTouched",
        }:
            kv_keys_deleted = row.get("kvKeysTouched")
            row_schema_valid = True
            allow_alias_cache_keys = True
        elif response_family == "strengthened" and row_keys == {
            "canonicalCode",
            "d1RowsUpdated",
            "d1Verified",
            "subtitleCount",
            "kvAction",
            "kvKeysTouched",
            "kvKeysDeleted",
        }:
            cache_schema_version = body.get("cacheSchemaVersion")
            kv_keys_touched = row.get("kvKeysTouched")
            kv_keys_deleted = row.get("kvKeysDeleted")
            kv_action = row.get("kvAction")
            row_schema_valid = (
                cache_schema_version == _STRENGTHENED_CACHE_SCHEMA_VERSION
                and row.get("d1Verified") is True
                and isinstance(kv_action, str)
                and kv_action in {"written", "deleted_for_d1_fallback", "unchanged"}
                and isinstance(kv_keys_touched, list)
                and isinstance(kv_keys_deleted, list)
                and kv_keys_touched == kv_keys_deleted
                and _valid_strengthened_v4_cache_key_pairs(kv_keys_deleted, canonical)
            )
            allow_alias_cache_keys = False
        else:
            kv_keys_deleted = None
            row_schema_valid = False
            allow_alias_cache_keys = False
        expected_kv_keys = {
            f"movie:full:{canonical}",
            f"movie:light:{canonical}",
        }
        expected_light_key = f"movie:light:{canonical}"
        if response_family == "strengthened":
            kv_keys_valid = row_schema_valid
        else:
            kv_keys_valid = (
                isinstance(kv_keys_deleted, list)
                and all(isinstance(key, str) for key in kv_keys_deleted)
                and (
                    (
                        allow_alias_cache_keys
                        and expected_kv_keys.issubset(set(kv_keys_deleted))
                        and all(
                            key.startswith(("movie:full:", "movie:light:"))
                            for key in kv_keys_deleted
                        )
                    )
                    or (
                        not allow_alias_cache_keys
                        and len(kv_keys_deleted) >= 2
                        and len(set(kv_keys_deleted)) == len(kv_keys_deleted)
                        and expected_light_key in set(kv_keys_deleted)
                        and sum(
                            _matches_full_cache_key(key, canonical)
                            for key in kv_keys_deleted
                        )
                        == 1
                        and all(
                            _matches_full_cache_key(
                                key,
                                canonical,
                                allow_aliases=True,
                            )
                            or _matches_light_cache_key(
                                key,
                                canonical,
                                allow_aliases=True,
                            )
                            for key in kv_keys_deleted
                        )
                    )
                )
            )
        if (
            not row_schema_valid
            or row.get("canonicalCode") != canonical
            or not self._positive_int(d1_rows_updated)
            or not self._positive_int(subtitle_count)
            or not kv_keys_valid
        ):
            raise CatalogSyncError("catalog_response_mismatch")

        self._verify_public_visibility(
            canonical,
            expected_subtitle_id=expected_subtitle_id,
            expected_content_sha256=expected_content_sha256,
        )

        return CatalogSyncResult(
            canonical_code=canonical,
            d1_rows_updated=d1_rows_updated,
            subtitle_count=subtitle_count,
            kv_keys_deleted=tuple(kv_keys_deleted),
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
