from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
import re
from typing import Literal
from uuid import UUID

import requests

from orchestrator.movie_code import canonical_movie_code


MetadataStatus = Literal["complete", "partial", "placeholder"]
MetadataSource = Literal["public", "missav", "local", "placeholder"]

METADATA_STATUSES = {"complete", "partial", "placeholder"}
METADATA_SOURCES = {"public", "missav", "local", "placeholder"}
DURATION_NUMBER_RE = re.compile(r"\d+")


@dataclass(frozen=True)
class MovieCatalogResult:
    movie_uuid: str
    canonical_code: str
    metadata_status: MetadataStatus
    metadata_source: MetadataSource


def load_publish_metadata(path: Path, movie_code: str) -> dict[str, object]:
    canonical = canonical_movie_code(movie_code)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeError):
        return {}
    if not isinstance(payload, dict):
        return {}

    raw_number = payload.get("number")
    if raw_number not in (None, ""):
        if not isinstance(raw_number, str):
            return {}
        try:
            metadata_code = canonical_movie_code(raw_number)
        except ValueError:
            return {}
        if metadata_code != canonical:
            return {}

    cleaned: dict[str, object] = {"number": canonical}

    raw_title = payload.get("title")
    if isinstance(raw_title, str):
        title = raw_title.strip()[:500]
        if title:
            cleaned["title"] = title

    raw_release_date = payload.get("release_date")
    if isinstance(raw_release_date, str):
        try:
            cleaned["release_date"] = date.fromisoformat(
                raw_release_date.strip()
            ).isoformat()
        except ValueError:
            pass

    raw_duration = payload.get("duration")
    if isinstance(raw_duration, str):
        match = DURATION_NUMBER_RE.search(raw_duration)
        if match is not None:
            duration = int(match.group())
            if 1 <= duration <= 1440:
                cleaned["duration_minutes"] = duration

    if len(cleaned) == 1:
        return {}
    return cleaned


class SupabaseMovieCatalogEnsurer:
    def __init__(
        self,
        url: str,
        service_key: str,
        timeout_seconds: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        self.url = url.rstrip("/")
        self.service_key = service_key
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    def ensure_movie(
        self, movie_code: str, metadata_path: Path
    ) -> MovieCatalogResult:
        canonical = canonical_movie_code(movie_code)
        response = self.session.request(
            "POST",
            f"{self.url}/rest/v1/rpc/ensure_subtitle_movie",
            headers={
                "apikey": self.service_key,
                "Authorization": f"Bearer {self.service_key}",
                "Content-Type": "application/json",
            },
            json={
                "p_movie_code": canonical,
                "p_local_metadata": load_publish_metadata(metadata_path, canonical),
            },
            timeout=self.timeout_seconds,
            allow_redirects=False,
        )
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"catalog ensure failed ({response.status_code})")
        try:
            payload = response.json()
        except (TypeError, ValueError) as exc:
            raise RuntimeError("catalog ensure returned invalid response") from exc
        if not self._valid_response(payload, canonical):
            raise RuntimeError("catalog ensure returned invalid response")
        return MovieCatalogResult(
            movie_uuid=payload["movie_uuid"],
            canonical_code=payload["canonical_code"],
            metadata_status=payload["metadata_status"],
            metadata_source=payload["metadata_source"],
        )

    @staticmethod
    def _valid_response(payload: object, canonical: str) -> bool:
        if not isinstance(payload, dict):
            return False
        movie_uuid = payload.get("movie_uuid")
        if not isinstance(movie_uuid, str):
            return False
        try:
            UUID(movie_uuid)
        except (ValueError, AttributeError):
            return False
        metadata_status = payload.get("metadata_status")
        metadata_source = payload.get("metadata_source")
        return (
            payload.get("canonical_code") == canonical
            and isinstance(metadata_status, str)
            and metadata_status in METADATA_STATUSES
            and isinstance(metadata_source, str)
            and metadata_source in METADATA_SOURCES
        )
