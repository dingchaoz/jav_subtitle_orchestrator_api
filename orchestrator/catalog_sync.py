from __future__ import annotations

from typing import Any

import requests

from orchestrator.supabase_publisher import canonical_movie_code


DEFAULT_SOURCE = "jav-subtitle-orchestrator"
DEFAULT_REASON = "orchestrator_ai_subtitle_publish"


class CatalogSyncError(RuntimeError):
    def __init__(self, canonical_codes: list[str], message: str) -> None:
        self.canonical_codes = canonical_codes
        super().__init__(f"catalog sync failed for {', '.join(canonical_codes)}: {message}")


class CatalogSyncClient:
    def __init__(
        self,
        api_base_url: str,
        admin_api_token: str,
        *,
        enabled: bool = True,
        timeout_seconds: int = 30,
        max_attempts: int = 2,
        session: requests.Session | None = None,
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.admin_api_token = admin_api_token
        self.enabled = enabled
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max(1, max_attempts)
        self.session = session or requests.Session()

    def sync_subtitles(
        self,
        canonical_codes: list[str],
        *,
        source: str = DEFAULT_SOURCE,
        reason: str = DEFAULT_REASON,
    ) -> dict[str, Any] | None:
        if not self.enabled:
            return None

        normalized_codes = sorted(
            {canonical_movie_code(code) for code in canonical_codes if code.strip()}
        )
        if not normalized_codes:
            return None

        endpoint = f"{self.api_base_url}/api/admin/catalog/sync-subtitles"
        payload = {
            "canonicalCodes": normalized_codes,
            "source": source,
            "reason": reason,
        }
        headers = {
            "Authorization": f"Bearer {self.admin_api_token}",
            "Content-Type": "application/json",
        }
        last_error = "unknown error"
        for _attempt in range(self.max_attempts):
            try:
                response = self.session.post(
                    endpoint,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                continue

            if response.ok:
                result = response.json() if response.text else {}
                if isinstance(result, dict) and result.get("success") is False:
                    last_error = f"endpoint reported failure: {result}"
                    continue
                return result
            last_error = f"HTTP {response.status_code}: {response.text}"

        raise CatalogSyncError(normalized_codes, last_error)
