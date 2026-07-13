from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit

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
}
_LOCAL_HTTP_HOSTS = {"localhost", "127.0.0.1", "::1"}


class CatalogSyncSession(Protocol):
    def post(self, url: str, **kwargs: Any) -> Any: ...


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
    def __init__(
        self,
        base_url: str,
        admin_token: str,
        *,
        timeout_seconds: int | float = 30,
        session: CatalogSyncSession | None = None,
    ) -> None:
        self.endpoint = self._endpoint(base_url)
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

    def sync(self, movie_code: str) -> CatalogSyncResult:
        try:
            canonical = canonical_movie_code(movie_code)
        except (AttributeError, TypeError, ValueError):
            raise ValueError("invalid movie code") from None

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
            raise CatalogSyncError("catalog_fetch_failed") from None

        if 300 <= response.status_code < 400:
            raise CatalogSyncError("catalog_redirect_rejected")
        if response.status_code in {401, 403}:
            raise CatalogSyncError("catalog_auth_failed")
        if response.status_code != 200:
            raise CatalogSyncError("catalog_sync_failed")

        try:
            body = response.json()
        except (TypeError, ValueError):
            raise CatalogSyncError("catalog_response_invalid") from None
        if not isinstance(body, dict):
            raise CatalogSyncError("catalog_response_invalid")

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
            row.get("canonicalCode") != canonical
            or row.get("dryRun") is not False
            or not self._positive_int(d1_rows_updated)
            or not self._positive_int(subtitle_count)
            or not isinstance(kv_keys_deleted, list)
            or len(kv_keys_deleted) != 2
            or any(not isinstance(key, str) for key in kv_keys_deleted)
            or set(kv_keys_deleted) != expected_kv_keys
        ):
            raise CatalogSyncError("catalog_response_mismatch")

        return CatalogSyncResult(
            canonical_code=canonical,
            d1_rows_updated=d1_rows_updated,
            subtitle_count=subtitle_count,
            kv_keys_deleted=tuple(kv_keys_deleted),
        )

    @staticmethod
    def _endpoint(base_url: str) -> str:
        if not isinstance(base_url, str) or not base_url or base_url != base_url.strip():
            raise ValueError("catalog API base URL is invalid")
        try:
            parsed = urlsplit(base_url)
            hostname = parsed.hostname
            has_credentials = parsed.username is not None or parsed.password is not None
        except ValueError:
            raise ValueError("catalog API base URL is invalid") from None
        valid_transport = parsed.scheme == "https" or (
            parsed.scheme == "http" and hostname in _LOCAL_HTTP_HOSTS
        )
        if (
            not valid_transport
            or not parsed.netloc
            or not hostname
            or has_credentials
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("catalog API base URL is invalid")
        return base_url.rstrip("/") + _SYNC_PATH

    @staticmethod
    def _exact_int(value: object, expected: int) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value == expected

    @staticmethod
    def _positive_int(value: object) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 1
