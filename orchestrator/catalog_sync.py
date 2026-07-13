from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote, urlencode, urlsplit

import requests

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
_LOCAL_HTTP_HOSTS = {"localhost", "127.0.0.1", "::1"}


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
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive")
        self.admin_token = admin_token.strip()
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

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
        except requests.RequestException:
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
        if (
            set(body) != {"success", "requested", "synced", "failed", "results"}
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
        kv_keys_deleted = row.get("kvKeysDeleted")
        expected_kv_keys = {
            f"movie:full:{canonical}",
            f"movie:light:{canonical}",
        }
        if (
            set(row)
            != {
                "canonicalCode",
                "d1RowsUpdated",
                "subtitleCount",
                "kvKeysDeleted",
                "dryRun",
            }
            or row.get("canonicalCode") != canonical
            or row.get("dryRun") is not False
            or not self._positive_int(d1_rows_updated)
            or not self._positive_int(subtitle_count)
            or not isinstance(kv_keys_deleted, list)
            or len(kv_keys_deleted) != 2
            or any(not isinstance(key, str) for key in kv_keys_deleted)
            or set(kv_keys_deleted) != expected_kv_keys
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
        if not isinstance(base_url, str) or not base_url or base_url != base_url.strip():
            raise ValueError("catalog API base URL is invalid")
        parse_failed = False
        try:
            parsed = urlsplit(base_url)
            hostname = parsed.hostname
            has_credentials = parsed.username is not None or parsed.password is not None
        except ValueError:
            parse_failed = True
            parsed = None
            hostname = None
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
            or has_credentials
            or parsed.path not in {"", "/"}
            or "?" in base_url
            or "#" in base_url
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("catalog API base URL is invalid")
        root = f"{parsed.scheme}://{parsed.netloc}"
        return root, f"{root}{_SYNC_PATH}"

    def _verify_public_visibility(
        self,
        canonical: str,
        *,
        expected_subtitle_id: str,
        expected_content_sha256: str,
    ) -> None:
        try:
            response = self.session.get(
                f"{self.base_url}/api/movie/{quote(canonical, safe='')}?"
                f"{urlencode({'cacheNonce': expected_content_sha256})}",
                timeout=self.timeout_seconds,
                allow_redirects=False,
            )
        except requests.RequestException:
            raise CatalogSyncError("public_visibility_fetch_failed") from None
        if 300 <= response.status_code < 400:
            raise CatalogSyncError("public_visibility_redirect_rejected")
        if response.status_code == 404:
            raise CatalogSyncError("public_visibility_not_found")
        if response.status_code != 200:
            raise CatalogSyncError("public_visibility_fetch_failed")
        try:
            body = response.json()
        except (TypeError, ValueError):
            raise CatalogSyncError("public_visibility_response_invalid") from None
        if not isinstance(body, dict) or not isinstance(body.get("subtitles"), list):
            raise CatalogSyncError("public_visibility_response_invalid")
        if body.get("canonicalCode") != canonical:
            raise CatalogSyncError("public_visibility_mismatch")
        matching = [
            row
            for row in body["subtitles"]
            if isinstance(row, dict) and row.get("id") == expected_subtitle_id
        ]
        if len(matching) != 1:
            raise CatalogSyncError("public_visibility_mismatch")

    @staticmethod
    def _exact_int(value: object, expected: int) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value == expected

    @staticmethod
    def _positive_int(value: object) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 1
