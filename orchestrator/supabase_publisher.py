from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import requests

from orchestrator.subtitle_quality import validate_translation_quality


MOVIE_CODE_RE = re.compile(r"^([a-zA-Z]+)-?(\d+)$")
AI_ENGLISH_LANGUAGE = "English_AI"


def canonical_movie_code(movie_code: str) -> str:
    match = MOVIE_CODE_RE.fullmatch(movie_code.strip())
    if match is None:
        raise ValueError(f"invalid movie code: {movie_code}")
    series, number = match.groups()
    return f"{series.lower()}-{int(number):03d}"


def build_ai_subtitle_storage_path(movie_code: str) -> str:
    canonical = canonical_movie_code(movie_code)
    series = canonical.split("-", 1)[0]
    return f"{series}/{canonical}/{canonical}-{AI_ENGLISH_LANGUAGE}.srt"


@dataclass(frozen=True)
class SupabasePublishResult:
    movie_code: str
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
    ) -> None:
        self.supabase_url = supabase_url.rstrip("/")
        self.service_role_key = service_role_key
        self.bucket = bucket
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    @property
    def headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
        }

    def publish_english_ai(
        self, movie_code: str, english_srt_path: Path
    ) -> SupabasePublishResult:
        canonical = canonical_movie_code(movie_code)
        japanese_srt_path = english_srt_path.with_name(
            f"{canonical}.Japanese.srt"
        )
        report = validate_translation_quality(japanese_srt_path, english_srt_path)
        if not report.passed:
            raise RuntimeError(
                "quality_gate_failed:" + ",".join(report.reason_codes)
            )

        storage_path = build_ai_subtitle_storage_path(canonical)
        self._upload_storage_object(storage_path, english_srt_path)
        movie_uuid = self._find_movie(canonical)
        subtitle_id = self._upsert_language_row(
            movie_uuid,
            storage_path,
            english_srt_path.stat().st_size,
        )
        return SupabasePublishResult(
            movie_code=canonical,
            storage_path=storage_path,
            movie_uuid=movie_uuid,
            subtitle_id=subtitle_id,
        )

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
                f"Supabase {method} {path} failed ({response.status_code})"
            )
        return response.json() if response.text else None

    def _upload_storage_object(self, storage_path: str, srt_path: Path) -> None:
        self._request(
            "POST",
            f"/storage/v1/object/{self.bucket}/{storage_path}",
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "x-upsert": "true",
                "cache-control": "no-cache",
            },
            data=srt_path.read_bytes(),
        )

    def _find_movie(self, canonical: str) -> str:
        rows = self._request(
            "GET",
            "/rest/v1/movies",
            params={
                "select": "id",
                "standard_movie_id": f"eq.{canonical}",
                "limit": "1",
            },
        )
        if not rows:
            raise RuntimeError(f"Supabase movie not found: {canonical}")
        return rows[0]["id"]

    def _upsert_language_row(
        self, movie_uuid: str, storage_path: str, file_size: int
    ) -> str:
        rows = self._request(
            "GET",
            "/rest/v1/movie_languages",
            params={
                "select": "id",
                "movie_id": f"eq.{movie_uuid}",
                "language": f"eq.{AI_ENGLISH_LANGUAGE}",
                "limit": "1",
            },
        )
        payload = {
            "file_path": storage_path,
            "file_size": file_size,
            "subtitle_quality": "auto",
            "subtitle_source": "human",
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
                "language": AI_ENGLISH_LANGUAGE,
            },
        )
        return inserted[0]["id"]
