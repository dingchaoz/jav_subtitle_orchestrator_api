from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from orchestrator.models import RequestedSubtitle, RequestedSubtitleImportResponse
from orchestrator.supabase_publisher import canonical_movie_code


ENGLISH_AI_LANGUAGE = "English_AI"


@dataclass(frozen=True)
class RequestedSubtitleImportSelection:
    requested: list[RequestedSubtitle]
    imported: list[RequestedSubtitle]
    skipped_available: list[RequestedSubtitle]


class RequestedSubtitleImporter:
    def __init__(
        self,
        *,
        cloudflare_account_id: str,
        cloudflare_d1_api_token: str,
        cloudflare_d1_database_id: str,
        supabase_url: str,
        supabase_service_role_key: str,
        timeout_seconds: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        self.cloudflare_account_id = cloudflare_account_id
        self.cloudflare_d1_api_token = cloudflare_d1_api_token
        self.cloudflare_d1_database_id = cloudflare_d1_database_id
        self.supabase_url = supabase_url.rstrip("/")
        self.supabase_service_role_key = supabase_service_role_key
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    def fetch_requested_subtitles(
        self,
        *,
        min_count: int = 1,
        limit: int = 100,
    ) -> RequestedSubtitleImportSelection:
        requested = self._fetch_from_d1(min_count=min_count, limit=limit)
        available_codes = self._fetch_available_english_ai_codes(requested)
        imported: list[RequestedSubtitle] = []
        skipped_available: list[RequestedSubtitle] = []
        for item in requested:
            canonical = _canonical_or_none(item.code)
            if canonical and canonical in available_codes:
                skipped_available.append(item)
            else:
                imported.append(item)
        return RequestedSubtitleImportSelection(
            requested=requested,
            imported=imported,
            skipped_available=skipped_available,
        )

    def _fetch_from_d1(self, *, min_count: int, limit: int) -> list[RequestedSubtitle]:
        response = self.session.post(
            self._d1_query_url(),
            headers={
                "Authorization": f"Bearer {self.cloudflare_d1_api_token}",
                "Content-Type": "application/json",
            },
            json={
                "sql": (
                    "SELECT code, movie_id, request_count, last_requested_at "
                    "FROM subtitle_requests "
                    "WHERE request_count >= ? "
                    "ORDER BY request_count DESC, last_requested_at DESC "
                    "LIMIT ?"
                ),
                "params": [min_count, limit],
            },
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(
                "Cloudflare D1 requested subtitle query failed "
                f"({response.status_code}): {response.text}"
            )
        payload = response.json() if response.text else {}
        _raise_for_cloudflare_errors(payload)
        rows = _extract_d1_rows(payload)
        return [self._row_to_requested_subtitle(row) for row in rows]

    def _fetch_available_english_ai_codes(
        self,
        requested: list[RequestedSubtitle],
    ) -> set[str]:
        canonical_codes = sorted(
            {
                canonical
                for item in requested
                if (canonical := _canonical_or_none(item.code)) is not None
            }
        )
        if not canonical_codes:
            return set()
        response = self.session.get(
            f"{self.supabase_url}/rest/v1/movie_subtitle_catalog",
            headers={
                "apikey": self.supabase_service_role_key,
                "Authorization": f"Bearer {self.supabase_service_role_key}",
            },
            params={
                "select": "canonical_code,language",
                "canonical_code": f"in.({','.join(canonical_codes)})",
                "language": f"eq.{ENGLISH_AI_LANGUAGE}",
            },
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(
                "Supabase English_AI availability query failed "
                f"({response.status_code}): {response.text}"
            )
        rows = response.json() if response.text else []
        if not isinstance(rows, list):
            raise RuntimeError(f"invalid Supabase availability response: {rows!r}")
        return {
            str(row.get("canonical_code", "")).strip().lower()
            for row in rows
            if isinstance(row, dict)
            and str(row.get("language", "")).strip() == ENGLISH_AI_LANGUAGE
        }

    def _d1_query_url(self) -> str:
        return (
            "https://api.cloudflare.com/client/v4/accounts/"
            f"{self.cloudflare_account_id}/d1/database/"
            f"{self.cloudflare_d1_database_id}/query"
        )

    @staticmethod
    def _row_to_requested_subtitle(row: Any) -> RequestedSubtitle:
        if not isinstance(row, dict):
            raise RuntimeError(f"invalid requested subtitle item: {row!r}")
        return RequestedSubtitle(
            code=str(row.get("code", "")).strip(),
            movie_id=row.get("movie_id"),
            request_count=int(row.get("request_count", 0)),
            last_requested_at=row.get("last_requested_at"),
        )


def _canonical_or_none(movie_code: str) -> str | None:
    try:
        return canonical_movie_code(movie_code)
    except ValueError:
        return None


def _raise_for_cloudflare_errors(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid Cloudflare D1 response: {payload!r}")
    if payload.get("success") is False:
        raise RuntimeError(f"Cloudflare D1 requested subtitle query failed: {payload!r}")
    errors = payload.get("errors")
    if errors:
        raise RuntimeError(f"Cloudflare D1 requested subtitle query failed: {errors!r}")
    for result in _result_items(payload):
        if isinstance(result, dict) and result.get("success") is False:
            raise RuntimeError(f"Cloudflare D1 requested subtitle query failed: {result!r}")


def _extract_d1_rows(payload: Any) -> list[Any]:
    rows: list[Any] = []
    for result in _result_items(payload):
        if isinstance(result, dict):
            result_rows = result.get("results", [])
            if isinstance(result_rows, list):
                rows.extend(result_rows)
        elif isinstance(result, list):
            rows.extend(result)
    return rows


def _result_items(payload: dict[str, Any]) -> list[Any]:
    result = payload.get("result", [])
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return [result]
    return []


__all__ = [
    "RequestedSubtitle",
    "RequestedSubtitleImportResponse",
    "RequestedSubtitleImporter",
    "RequestedSubtitleImportSelection",
]
