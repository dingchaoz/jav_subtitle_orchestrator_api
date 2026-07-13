from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import time
from typing import Any
from uuid import uuid4

import requests

from orchestrator.movie_code import canonical_movie_code
from orchestrator.subtitle_quality import validate_translation_quality


AI_ENGLISH_LANGUAGE = "English_AI"


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
    content_sha256: str
    file_size: int
    verified: bool
    metadata_status: str
    metadata_source: str


class SupabaseSubtitlePublisher:
    def __init__(
        self,
        supabase_url: str,
        service_role_key: str,
        *,
        bucket: str = "subtitles",
        timeout_seconds: int = 30,
        verification_timeout_seconds: int = 90,
        verification_interval_seconds: float = 2.0,
        session: requests.Session | None = None,
        clock=time.monotonic,
        sleeper=time.sleep,
        nonce_factory=None,
        catalog_ensurer=None,
    ) -> None:
        if verification_timeout_seconds <= 0:
            raise ValueError("verification timeout must be positive")
        if verification_interval_seconds <= 0:
            raise ValueError("verification interval must be positive")
        self.supabase_url = supabase_url.rstrip("/")
        self.service_role_key = service_role_key
        self.bucket = bucket
        self.timeout_seconds = timeout_seconds
        self.verification_timeout_seconds = verification_timeout_seconds
        self.verification_interval_seconds = verification_interval_seconds
        self.session = session or requests.Session()
        self.clock = clock
        self.sleeper = sleeper
        self.nonce_factory = nonce_factory or (lambda: uuid4().hex)
        if catalog_ensurer is None:
            from orchestrator.movie_catalog import SupabaseMovieCatalogEnsurer

            catalog_ensurer = SupabaseMovieCatalogEnsurer(
                self.supabase_url,
                self.service_role_key,
                timeout_seconds=self.timeout_seconds,
                session=self.session,
            )
        self.catalog_ensurer = catalog_ensurer

    @property
    def headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
        }

    def publish_english_ai(
        self,
        movie_code: str,
        english_srt_path: Path,
        metadata_path: Path | None = None,
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

        catalog = self.catalog_ensurer.ensure_movie(
            canonical,
            metadata_path or english_srt_path.with_name("metadata.json"),
        )
        subtitle_bytes = english_srt_path.read_bytes()
        content_sha256 = hashlib.sha256(subtitle_bytes).hexdigest()
        storage_path = build_ai_subtitle_storage_path(canonical)
        self._upload_storage_object(storage_path, subtitle_bytes)
        subtitle_id = self._upsert_language_row(
            catalog.movie_uuid,
            storage_path,
            len(subtitle_bytes),
        )
        self._verify_storage(storage_path, subtitle_bytes, content_sha256)
        self._verify_catalog(
            subtitle_id,
            catalog.movie_uuid,
            storage_path,
            len(subtitle_bytes),
        )
        return SupabasePublishResult(
            movie_code=canonical,
            storage_path=storage_path,
            movie_uuid=catalog.movie_uuid,
            subtitle_id=subtitle_id,
            content_sha256=content_sha256,
            file_size=len(subtitle_bytes),
            verified=True,
            metadata_status=catalog.metadata_status,
            metadata_source=catalog.metadata_source,
        )

    def _request_raw(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = {**self.headers, **kwargs.pop("headers", {})}
        response = self.session.request(
            method,
            f"{self.supabase_url}{path}",
            headers=headers,
            timeout=self.timeout_seconds,
            allow_redirects=False,
            **kwargs,
        )
        if not response.ok:
            raise RuntimeError(
                f"Supabase {method} {path} failed ({response.status_code})"
            )
        return response

    def _request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self._request_raw(method, path, **kwargs)
        if not response.text:
            return None
        try:
            return response.json()
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Supabase {method} {path} returned invalid JSON"
            ) from exc

    def _upload_storage_object(self, storage_path: str, subtitle_bytes: bytes) -> None:
        self._request_raw(
            "POST",
            f"/storage/v1/object/{self.bucket}/{storage_path}",
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "x-upsert": "true",
                "cache-control": "no-cache",
            },
            data=subtitle_bytes,
        )

    def _upsert_language_row(
        self, movie_uuid: str, storage_path: str, file_size: int
    ) -> str:
        rows = self._request_json(
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
            updated = self._request_json(
                "PATCH",
                "/rest/v1/movie_languages",
                params={"id": f"eq.{subtitle_id}"},
                headers={"Prefer": "return=representation"},
                json=payload,
            )
            return updated[0]["id"]
        inserted = self._request_json(
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

    def _verify_storage(
        self,
        storage_path: str,
        expected_bytes: bytes,
        expected_sha256: str,
    ) -> None:
        deadline = self.clock() + self.verification_timeout_seconds
        while True:
            response = self._request_raw(
                "GET",
                f"/storage/v1/object/{self.bucket}/{storage_path}",
                headers={"Accept-Encoding": "identity"},
                params={"cacheNonce": self.nonce_factory()},
                stream=True,
            )
            try:
                downloaded = bytearray()
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    downloaded.extend(chunk)
                    if len(downloaded) > len(expected_bytes):
                        break
                actual = bytes(downloaded)
            finally:
                close = getattr(response, "close", None)
                if close is not None:
                    close()
            if (
                len(actual) == len(expected_bytes)
                and hashlib.sha256(actual).hexdigest() == expected_sha256
            ):
                return
            remaining = deadline - self.clock()
            if remaining <= self.verification_interval_seconds:
                raise RuntimeError(
                    "Supabase verification failed: storage_hash_timeout"
                )
            self.sleeper(self.verification_interval_seconds)

    def _verify_catalog(
        self,
        subtitle_id: str,
        movie_uuid: str,
        storage_path: str,
        file_size: int,
    ) -> None:
        rows = self._request_json(
            "GET",
            "/rest/v1/movie_languages",
            params={
                "select": "id,movie_id,language,file_path,file_size",
                "id": f"eq.{subtitle_id}",
                "limit": "1",
            },
        )
        expected = {
            "id": subtitle_id,
            "movie_id": movie_uuid,
            "language": AI_ENGLISH_LANGUAGE,
            "file_path": storage_path,
            "file_size": file_size,
        }
        if not isinstance(rows, list) or len(rows) != 1 or rows[0] != expected:
            raise RuntimeError("Supabase verification failed: catalog_mismatch")
