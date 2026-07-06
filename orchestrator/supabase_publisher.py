import re
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import requests


MOVIE_CODE_RE = re.compile(r"^([a-zA-Z]+)-?(\d+)$")
AI_ENGLISH_LANGUAGE = "English_AI"
AI_SUBTITLE_SOURCE = "ai_orchestrator"

logger = logging.getLogger(__name__)


class CatalogSyncer(Protocol):
    def sync_subtitles(
        self,
        canonical_codes: list[str],
        *,
        source: str,
        reason: str,
    ) -> object | None:
        ...


def parse_movie_code(movie_code: str) -> tuple[str, int]:
    match = MOVIE_CODE_RE.match(movie_code.strip())
    if not match:
        raise ValueError(f"invalid movie code: {movie_code}")
    series, number = match.groups()
    return series.lower(), int(number)


def canonical_movie_code(movie_code: str) -> str:
    series, number = parse_movie_code(movie_code)
    return f"{series}-{number:03d}"


def build_ai_subtitle_storage_path(movie_code: str) -> str:
    canonical = canonical_movie_code(movie_code)
    series, _ = parse_movie_code(canonical)
    return f"{series}/{canonical}/{canonical}-{AI_ENGLISH_LANGUAGE}.srt"


@dataclass(frozen=True)
class SupabasePublishResult:
    movie_code: str
    language: str
    storage_path: str
    movie_uuid: str
    subtitle_id: str


class SupabaseSubtitlePublisher:
    def __init__(
        self,
        supabase_url: str,
        service_role_key: str,
        *,
        bucket: str = "subtitles",
        timeout_seconds: int = 30,
        session: requests.Session | None = None,
        catalog_sync: CatalogSyncer | None = None,
    ) -> None:
        self.supabase_url = supabase_url.rstrip("/")
        self.service_role_key = service_role_key
        self.bucket = bucket
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()
        self.catalog_sync = catalog_sync

    @property
    def headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
        }

    def publish_english_ai(self, movie_code: str, srt_path: Path) -> SupabasePublishResult:
        if not srt_path.exists():
            raise FileNotFoundError(srt_path)
        canonical = canonical_movie_code(movie_code)
        storage_path = build_ai_subtitle_storage_path(canonical)
        self._upload_storage_object(storage_path, srt_path)
        movie_uuid = self._ensure_movie(canonical)
        subtitle_id = self._upsert_language_row(
            movie_uuid,
            AI_ENGLISH_LANGUAGE,
            storage_path,
            srt_path.stat().st_size,
        )
        result = SupabasePublishResult(
            movie_code=canonical,
            language=AI_ENGLISH_LANGUAGE,
            storage_path=storage_path,
            movie_uuid=movie_uuid,
            subtitle_id=subtitle_id,
        )
        self._sync_catalog_after_publish(result.movie_code)
        return result

    def _sync_catalog_after_publish(self, canonical: str) -> None:
        if self.catalog_sync is None:
            return
        try:
            self.catalog_sync.sync_subtitles(
                [canonical],
                source="jav-subtitle-orchestrator",
                reason="orchestrator_ai_subtitle_publish",
            )
        except Exception as exc:
            logger.warning("Catalog sync failed for %s: %s", canonical, exc)

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = {**self.headers, **kwargs.pop("headers", {})}
        response = self.session.request(
            method,
            f"{self.supabase_url}{path}",
            headers=headers,
            timeout=self.timeout_seconds,
            **kwargs,
        )
        if not response.ok:
            raise RuntimeError(
                f"Supabase {method} {path} failed ({response.status_code}): {response.text}"
            )
        if response.text:
            return response.json()
        return None

    def _upload_storage_object(self, storage_path: str, srt_path: Path) -> None:
        with srt_path.open("rb") as handle:
            self._request(
                "POST",
                f"/storage/v1/object/{self.bucket}/{storage_path}",
                headers={
                    "Content-Type": "text/plain; charset=utf-8",
                    "x-upsert": "true",
                    "cache-control": "no-cache",
                },
                data=handle.read(),
            )

    def _ensure_movie(self, canonical: str) -> str:
        rows = self._request(
            "GET",
            "/rest/v1/movies",
            params={
                "select": "id,movie_id",
                "standard_movie_id": f"eq.{canonical}",
                "limit": "1",
            },
        )
        if rows:
            return rows[0]["id"]
        series, number = parse_movie_code(canonical)
        inserted = self._request(
            "POST",
            "/rest/v1/movies",
            headers={"Prefer": "return=representation"},
            json={
                "series": series,
                "movie_number": number,
                "title": canonical.upper(),
            },
        )
        return inserted[0]["id"]

    def _upsert_language_row(
        self,
        movie_uuid: str,
        language: str,
        storage_path: str,
        file_size: int,
    ) -> str:
        rows = self._request(
            "GET",
            "/rest/v1/movie_languages",
            params={
                "select": "id",
                "movie_id": f"eq.{movie_uuid}",
                "language": f"eq.{language}",
                "limit": "1",
            },
        )
        payload = {
            "file_path": storage_path,
            "file_size": file_size,
            "subtitle_quality": "auto",
            "subtitle_source": AI_SUBTITLE_SOURCE,
            "is_premium": False,
        }
        if rows:
            subtitle_id = rows[0]["id"]
            updated = self._request(
                "PATCH",
                "/rest/v1/movie_languages",
                params={"id": f"eq.{subtitle_id}"},
                headers={"Prefer": "return=representation"},
                json=payload,
            )
            return updated[0]["id"]
        inserted = self._request(
            "POST",
            "/rest/v1/movie_languages",
            headers={"Prefer": "return=representation"},
            json={
                **payload,
                "movie_id": movie_uuid,
                "language": language,
            },
        )
        return inserted[0]["id"]
