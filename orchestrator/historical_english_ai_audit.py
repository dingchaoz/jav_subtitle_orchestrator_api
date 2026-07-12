from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import re
import unicodedata


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


@dataclass(frozen=True, slots=True)
class LocalInspection:
    status: str
    reason_codes: tuple[str, ...]
    metrics: dict[str, int | float | str | bool | None]


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
