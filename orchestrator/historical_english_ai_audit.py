from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import re
import threading
import time
import unicodedata
from urllib.parse import quote, urlparse
import uuid

import requests


MAX_OBJECT_BYTES = 32 * 1024 * 1024
MAX_SRT_LOGICAL_LINES = 40_000
MAX_SRT_CUES = 10_000
MAX_SRT_TEXT_LINES = 20_000
MAX_SRT_TEXT_LINE_CHARACTERS = 16_384
MAX_SRT_UNIQUE_NORMALIZED_LINES = 10_000

STRICT_TIMING_RE = re.compile(
    r"^(\d{2}):(\d{2}):(\d{2}),(\d{3})\s+-->\s+"
    r"(\d{2}):(\d{2}):(\d{2}),(\d{3})(?:\s+.*)?$"
)
TOLERANT_TIMING_RE = re.compile(
    r"^(\d{1,3}):(\d{2}):(\d{2})[,.](\d{1,3})\s*-->\s*"
    r"(\d{1,3}):(\d{2}):(\d{2})[,.](\d{1,3})(?:\s+.*)?$"
)
_KNOWN_BAD_PHRASES = (
    "i don t know what to do",
    "i don t know what to say",
    "cannot translate",
    "cannot assist with translating",
    "unable to provide this translation",
    "translation omitted",
    "refused",
    "can t help translate",
)
_MOJIBAKE_MARKERS = (
    "ï»¿",
    "â€™",
    "â€œ",
    "â€",
    "ðÿ",
    "ã\x81",
    "ã\x82",
    "ã\x83",
    "�",
)
_MOJIBAKE_RE = re.compile(
    "|".join(
        re.escape(marker.casefold())
        for marker in sorted(_MOJIBAKE_MARKERS, key=len, reverse=True)
    )
)
_EXCLUDED_CONTROLS = {"\n", "\r", "\t"}


class ObjectLimitExceeded(RuntimeError):
    """Raised without object content when an audit bound is exceeded."""


class SubtitleStructureLimitExceeded(ValueError):
    """Raised without subtitle content when an SRT structure bound is exceeded."""


class StorageObjectMissing(FileNotFoundError):
    """Raised without a path when a catalog-referenced object does not exist."""


class RequestRateLimiter:
    def __init__(
        self,
        requests_per_second: float,
        *,
        clock=time.monotonic,
        sleeper=time.sleep,
    ) -> None:
        if not 0 < requests_per_second <= 10:
            raise ValueError(
                "requests_per_second must be greater than 0 and at most 10"
            )
        self._interval = 1.0 / requests_per_second
        self._clock = clock
        self._sleeper = sleeper
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            wait_seconds = max(0.0, self._next_at - now)
            if wait_seconds:
                self._sleeper(wait_seconds)
                now = self._clock()
            self._next_at = max(now, self._next_at) + self._interval


@dataclass(frozen=True, slots=True)
class LocalInspection:
    status: str
    reason_codes: tuple[str, ...]
    metrics: dict[str, int | float | str | bool | None]


@dataclass(frozen=True, slots=True)
class CatalogRecord:
    subtitle_id: str
    movie_id: str
    movie_code: str
    language: str
    file_path: str
    file_size: int


@dataclass(frozen=True, slots=True)
class _Cue:
    start: float
    end: float
    text: str


@dataclass(frozen=True, slots=True)
class _InspectionMetrics:
    byte_count: int
    encoding: str
    used_fallback: bool
    parse_mode: str
    cue_count: int
    text_line_count: int
    text_character_count: int
    unique_text_count: int
    unique_text_ratio: float
    dominant_text_sha256: str | None
    dominant_text_count: int
    dominant_text_ratio: float
    invalid_interval_count: int
    timeline_regression_count: int
    replacement_character_count: int
    nul_count: int
    control_character_count: int
    mojibake_marker_count: int
    known_bad_occurrence_count: int


def _timestamp_seconds(parts: tuple[str, ...]) -> float:
    hours, minutes, seconds, fraction = parts
    minute_value = int(minutes)
    second_value = int(seconds)
    if minute_value not in range(60) or second_value not in range(60):
        raise ValueError("invalid timestamp")
    milliseconds = int(fraction.ljust(3, "0")[:3])
    return int(hours) * 3600 + minute_value * 60 + second_value + milliseconds / 1000


def _parse_srt(text: str, timing_re: re.Pattern[str], *, strict: bool) -> tuple[_Cue, ...]:
    logical_lines = text.count("\n") + text.count("\r") - text.count("\r\n") + 1
    if logical_lines > MAX_SRT_LOGICAL_LINES:
        raise SubtitleStructureLimitExceeded("subtitle structural limit exceeded: logical lines")
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    if not normalized.strip():
        return ()

    cues: list[_Cue] = []
    for block in re.split(r"(?:\n[ \t]*){2,}", normalized.strip()):
        lines = block.splitlines()
        if strict:
            if len(lines) < 2 or not lines[0].strip().isdigit():
                continue
            timing_position = 1
        else:
            timing_position = next(
                (
                    position
                    for position, line in enumerate(lines[:2])
                    if timing_re.fullmatch(line.strip())
                ),
                -1,
            )
            if timing_position < 0:
                continue
        match = timing_re.fullmatch(lines[timing_position].strip())
        if match is None:
            continue
        try:
            start = _timestamp_seconds(match.groups()[:4])
            end = _timestamp_seconds(match.groups()[4:])
        except ValueError:
            continue
        cues.append(_Cue(start, end, "\n".join(lines[timing_position + 1 :])))
        if len(cues) > MAX_SRT_CUES:
            raise SubtitleStructureLimitExceeded("subtitle structural limit exceeded: cues")
    return tuple(cues)


def _decode(data: bytes) -> tuple[str, str, bool]:
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return data.decode("utf-16"), "utf-16", False
        except UnicodeError:
            pass
    try:
        return data.decode("utf-8-sig"), "utf-8-sig", False
    except UnicodeError:
        pass
    for encoding in ("cp932", "gb18030", "big5"):
        try:
            decoded = data.decode(encoding)
        except UnicodeError:
            continue
        if _parse_srt(decoded, STRICT_TIMING_RE, strict=True) or _parse_srt(
            decoded, TOLERANT_TIMING_RE, strict=False
        ):
            return decoded, encoding, False
    return data.decode("latin-1"), "latin-1", True


def _normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text).casefold()
    return " ".join(re.findall(r"\w+", normalized, flags=re.UNICODE))


def _inspect_srt_bytes(data: bytes) -> _InspectionMetrics:
    decoded, encoding, used_fallback = _decode(data)
    cues = _parse_srt(decoded, STRICT_TIMING_RE, strict=True)
    parse_mode = "strict"
    if not cues:
        cues = _parse_srt(decoded, TOLERANT_TIMING_RE, strict=False)
        parse_mode = "tolerant" if cues else "failed"

    normalized_counts: Counter[str] = Counter()
    text_line_count = 0
    text_character_count = 0
    known_bad_occurrence_count = 0
    normalized_line_count = 0
    for cue in cues:
        for raw_line in cue.text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            text_line_count += 1
            if text_line_count > MAX_SRT_TEXT_LINES:
                raise SubtitleStructureLimitExceeded(
                    "subtitle structural limit exceeded: text lines"
                )
            if len(line) > MAX_SRT_TEXT_LINE_CHARACTERS:
                raise SubtitleStructureLimitExceeded(
                    "subtitle structural limit exceeded: text line characters"
                )
            text_character_count += len(line)
            normalized = _normalize_text(line)
            if not normalized:
                continue
            normalized_line_count += 1
            if (
                normalized not in normalized_counts
                and len(normalized_counts) >= MAX_SRT_UNIQUE_NORMALIZED_LINES
            ):
                raise SubtitleStructureLimitExceeded(
                    "subtitle structural limit exceeded: unique normalized lines"
                )
            normalized_counts[normalized] += 1
            known_bad_occurrence_count += sum(
                normalized.count(phrase) for phrase in _KNOWN_BAD_PHRASES
            )

    dominant_text, dominant_count = (
        normalized_counts.most_common(1)[0] if normalized_counts else (None, 0)
    )
    timeline_regression_count = sum(
        current.start < previous.start - 2.0
        for previous, current in zip(cues, cues[1:])
    )
    casefolded = decoded.casefold()
    control_count = sum(
        unicodedata.category(character) == "Cc" and character not in _EXCLUDED_CONTROLS
        for character in decoded
    )
    return _InspectionMetrics(
        byte_count=len(data),
        encoding=encoding,
        used_fallback=used_fallback,
        parse_mode=parse_mode,
        cue_count=len(cues),
        text_line_count=text_line_count,
        text_character_count=text_character_count,
        unique_text_count=len(normalized_counts),
        unique_text_ratio=(
            len(normalized_counts) / normalized_line_count if normalized_line_count else 0.0
        ),
        dominant_text_sha256=(
            hashlib.sha256(dominant_text.encode("utf-8")).hexdigest()
            if dominant_text is not None
            else None
        ),
        dominant_text_count=dominant_count,
        dominant_text_ratio=(
            dominant_count / normalized_line_count if normalized_line_count else 0.0
        ),
        invalid_interval_count=sum(cue.end <= cue.start for cue in cues),
        timeline_regression_count=timeline_regression_count,
        replacement_character_count=decoded.count("�"),
        nul_count=decoded.count("\x00"),
        control_character_count=control_count,
        mojibake_marker_count=sum(1 for _ in _MOJIBAKE_RE.finditer(casefolded)),
        known_bad_occurrence_count=known_bad_occurrence_count,
    )


def _safe_metrics(inspection: _InspectionMetrics) -> dict[str, int | float | str | bool | None]:
    return {
        "byte_count": inspection.byte_count,
        "encoding": inspection.encoding,
        "used_fallback": inspection.used_fallback,
        "parse_mode": inspection.parse_mode,
        "cue_count": inspection.cue_count,
        "text_line_count": inspection.text_line_count,
        "text_character_count": inspection.text_character_count,
        "unique_text_count": inspection.unique_text_count,
        "unique_text_ratio": inspection.unique_text_ratio,
        "dominant_text_sha256": inspection.dominant_text_sha256,
        "dominant_text_count": inspection.dominant_text_count,
        "dominant_text_ratio": inspection.dominant_text_ratio,
        "invalid_interval_count": inspection.invalid_interval_count,
        "timeline_regression_count": inspection.timeline_regression_count,
        "replacement_character_count": inspection.replacement_character_count,
        "nul_count": inspection.nul_count,
        "control_character_count": inspection.control_character_count,
        "mojibake_marker_count": inspection.mojibake_marker_count,
        "known_bad_occurrence_count": inspection.known_bad_occurrence_count,
    }


def inspect_english_srt(data: bytes) -> LocalInspection:
    if len(data) > MAX_OBJECT_BYTES:
        raise ObjectLimitExceeded("storage object exceeds 33554432 bytes")
    inspection = _inspect_srt_bytes(data)
    reasons: list[str] = []
    if not data:
        reasons.append("EMPTY_FILE")
    if inspection.cue_count == 0:
        reasons.append("NO_VALID_CUES")
    if inspection.cue_count and (
        inspection.invalid_interval_count / inspection.cue_count > 0.01
        or inspection.timeline_regression_count > 0
    ):
        reasons.append("INVALID_TIMELINE")
    if (
        inspection.replacement_character_count >= 2
        or inspection.nul_count >= 2
        or inspection.control_character_count >= 3
        or inspection.mojibake_marker_count >= 3
    ):
        reasons.append("SEVERE_MOJIBAKE")
    if inspection.known_bad_occurrence_count >= 3 and (
        inspection.known_bad_occurrence_count
        / max(inspection.text_line_count, 1)
        >= 0.02
    ):
        reasons.append("KNOWN_BAD_TRANSLATION")
    if inspection.text_line_count >= 20 and inspection.dominant_text_ratio >= 0.50:
        reasons.append("DOMINANT_TEXT_COLLAPSE")
    if (
        inspection.text_line_count >= 100
        and inspection.unique_text_ratio < 0.15
        and inspection.dominant_text_ratio >= 0.25
    ):
        reasons.append("LOW_DIVERSITY_COLLAPSE")
    return LocalInspection(
        status="hard_failure" if reasons else "passed",
        reason_codes=tuple(reasons),
        metrics=_safe_metrics(inspection),
    )


_BUCKET_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_MOVIE_CODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")


def _safe_bucket(value: str) -> str:
    if value in {"", ".", ".."} or not _BUCKET_RE.fullmatch(value):
        raise ValueError("bucket must be a safe path segment")
    return value


def _safe_object_path(value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > 2048:
        raise ValueError("object path must be a bounded non-empty string")
    if value.startswith("/") or "\x00" in value:
        raise ValueError("object path is unsafe")
    if any(segment in {"", ".", ".."} for segment in value.split("/")):
        raise ValueError("object path is unsafe")
    return value


def _uuid(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be a UUID") from exc
    if str(parsed) != value.casefold():
        raise ValueError(f"{label} must use canonical UUID form")
    return str(parsed)


def _parse_catalog_page(
    payload: object,
    page_limit: int,
    last_id: str | None,
) -> list[CatalogRecord]:
    if not isinstance(payload, list) or len(payload) > page_limit:
        raise ValueError("catalog payload must be a bounded list")
    records: list[CatalogRecord] = []
    previous_id = last_id
    for raw in payload:
        if not isinstance(raw, dict):
            raise ValueError("catalog payload row is malformed")
        subtitle_id = _uuid(raw.get("id"), "subtitle id")
        movie_id = _uuid(raw.get("movie_id"), "movie id")
        if previous_id is not None and subtitle_id <= previous_id:
            raise ValueError("catalog payload is not strictly ordered")
        previous_id = subtitle_id
        if raw.get("language") != "English_AI":
            raise ValueError("catalog payload contains a non-English_AI row")
        file_path = _safe_object_path(raw.get("file_path"))
        file_size = raw.get("file_size")
        if (
            not isinstance(file_size, int)
            or isinstance(file_size, bool)
            or file_size < 0
        ):
            raise ValueError("catalog file size must be a non-negative integer")
        movie_relation = raw.get("movies")
        if isinstance(movie_relation, list):
            movie_relation = movie_relation[0] if len(movie_relation) == 1 else None
        if not isinstance(movie_relation, dict):
            raise ValueError("catalog movie relation is malformed")
        movie_code = movie_relation.get("standard_movie_id")
        if not isinstance(movie_code, str) or not _MOVIE_CODE_RE.fullmatch(movie_code):
            raise ValueError("catalog movie code is malformed")
        records.append(
            CatalogRecord(
                subtitle_id=subtitle_id,
                movie_id=movie_id,
                movie_code=movie_code,
                language="English_AI",
                file_path=file_path,
                file_size=file_size,
            )
        )
    return records


class SupabaseEnglishAiReader:
    def __init__(
        self,
        url: str,
        service_role_key: str,
        *,
        bucket: str = "subtitles",
        page_size: int = 500,
        timeout_seconds: int = 30,
        session=None,
        rate_limiter: RequestRateLimiter | None = None,
    ) -> None:
        parsed_url = urlparse(url)
        if parsed_url.scheme != "https" or not parsed_url.netloc or parsed_url.path not in {"", "/"}:
            raise ValueError("Supabase URL must be an HTTPS origin")
        if not service_role_key:
            raise ValueError("Supabase service role key is required")
        if not 1 <= page_size <= 500:
            raise ValueError("page_size must be between 1 and 500")
        if not 1 <= timeout_seconds <= 120:
            raise ValueError("timeout_seconds must be between 1 and 120")
        self.url = url.rstrip("/")
        self._service_role_key = service_role_key
        self.bucket = _safe_bucket(bucket)
        self.page_size = page_size
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()
        self.rate_limiter = rate_limiter or RequestRateLimiter(2.0)

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self._service_role_key,
            "Authorization": f"Bearer {self._service_role_key}",
        }

    def _get(self, path: str, **kwargs):
        headers = {**self._headers, **kwargs.pop("headers", {})}
        self.rate_limiter.acquire()
        response = self.session.get(
            f"{self.url}{path}",
            headers=headers,
            timeout=self.timeout_seconds,
            allow_redirects=False,
            **kwargs,
        )
        return response

    @staticmethod
    def _require_2xx(response, operation: str) -> None:
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"Supabase {operation} failed ({response.status_code})")

    def _get_json(self, path: str, *, params: dict[str, str]) -> object:
        response = self._get(path, params=params)
        self._require_2xx(response, "catalog GET")
        try:
            return response.json()
        except (TypeError, ValueError) as exc:
            raise ValueError("catalog payload is not valid JSON") from exc

    def iter_catalog(self, *, limit: int | None = None):
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit < 1
        ):
            raise ValueError("limit must be a positive integer")
        emitted = 0
        last_id: str | None = None
        while limit is None or emitted < limit:
            page_limit = (
                min(self.page_size, limit - emitted)
                if limit is not None
                else self.page_size
            )
            params = {
                "select": (
                    "id,movie_id,language,file_path,file_size,"
                    "movies!inner(standard_movie_id)"
                ),
                "language": "eq.English_AI",
                "order": "id.asc",
                "limit": str(page_limit),
            }
            if last_id is not None:
                params["id"] = f"gt.{last_id}"
            payload = self._get_json("/rest/v1/movie_languages", params=params)
            page = _parse_catalog_page(payload, page_limit, last_id)
            for record in page:
                yield record
                emitted += 1
                last_id = record.subtitle_id
            if len(page) < page_limit:
                return

    def download_object(
        self,
        file_path: str,
        *,
        max_bytes: int = MAX_OBJECT_BYTES,
    ) -> bytes:
        if (
            not isinstance(max_bytes, int)
            or isinstance(max_bytes, bool)
            or not 1 <= max_bytes <= MAX_OBJECT_BYTES
        ):
            raise ValueError("max_bytes must be between 1 and 33554432")
        encoded_path = quote(_safe_object_path(file_path), safe="/")
        encoded_bucket = quote(self.bucket, safe="")
        response = self._get(
            f"/storage/v1/object/{encoded_bucket}/{encoded_path}",
            stream=True,
            headers={"Accept-Encoding": "identity"},
        )
        try:
            if response.status_code == 404:
                raise StorageObjectMissing("storage object missing")
            self._require_2xx(response, "Storage GET")
            content_encoding = response.headers.get("Content-Encoding")
            if (
                content_encoding is not None
                and content_encoding.strip().casefold() != "identity"
            ):
                raise RuntimeError("Storage GET returned unsupported content encoding")
            raw_length = response.headers.get("Content-Length")
            if raw_length is not None:
                try:
                    content_length = int(raw_length)
                except ValueError as exc:
                    raise RuntimeError("Storage GET returned invalid content length") from exc
                if content_length < 0:
                    raise RuntimeError("Storage GET returned invalid content length")
                if content_length > max_bytes:
                    raise ObjectLimitExceeded("storage object exceeds configured byte limit")
            chunks: list[bytes] = []
            downloaded = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                downloaded += len(chunk)
                if downloaded > max_bytes:
                    raise ObjectLimitExceeded(
                        "storage object exceeds configured byte limit"
                    )
                chunks.append(chunk)
            if raw_length is not None and downloaded != content_length:
                raise RuntimeError("Storage GET ended before declared content length")
            return b"".join(chunks)
        finally:
            response.close()
