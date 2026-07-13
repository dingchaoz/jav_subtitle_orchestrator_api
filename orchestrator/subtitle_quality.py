from __future__ import annotations

import re
import statistics
import unicodedata
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path


TIMESTAMP_RE = re.compile(
    r"^(?P<sh>\d{2}):(?P<sm>\d{2}):(?P<ss>\d{2}),(?P<sms>\d{3})\s+-->\s+"
    r"(?P<eh>\d{2}):(?P<em>\d{2}):(?P<es>\d{2}),(?P<ems>\d{3})$"
)
KNOWN_BAD_TEXTS = {
    "i don t know what to do",
    "i don t know what to say",
}
REFUSAL_PHRASES = (
    "cannot translate",
    "cannot assist with translating",
    "unable to provide this translation",
    "translation omitted",
    "refused",
    "can t help translate",
)
SHORT_INTERJECTIONS = {
    "ah",
    "yes",
    "no",
    "oh",
    "oh yeah",
    "yeah",
    "hmm",
    "mm",
}
MOJIBAKE_MARKERS = ("Ã", "Â", "â€", "ðŸ", "ï¿½", "ã\x80", "ã\x81", "ã\x82")


class SubtitleQualityGateError(RuntimeError):
    def __init__(self, reason_codes: Iterable[str]) -> None:
        self.reason_codes = tuple(reason_codes)
        super().__init__("quality_gate_failed:" + ",".join(self.reason_codes))


@dataclass(frozen=True)
class Cue:
    index: int
    start_seconds: float
    end_seconds: float
    text_lines: tuple[str, ...]


@dataclass
class QualityReport:
    passed: bool
    reason_codes: list[str]
    japanese_cue_count: int = 0
    english_cue_count: int = 0
    english_text_line_count: int = 0
    english_unique_count: int = 0
    english_unique_ratio: float = 0.0
    dominant_normalized_text: str = ""
    dominant_text_count: int = 0
    dominant_text_ratio: float = 0.0
    known_bad_phrase_count: int = 0
    refusal_phrase_count: int = 0
    replacement_character_count: int = 0
    parse_errors: list[str] | None = None
    warning_codes: list[str] | None = None
    japanese_chars_per_minute: float | None = None
    median_japanese_text_length: float | None = None
    japanese_long_cue_over_10s_ratio: float | None = None
    japanese_long_cue_over_20s_ratio: float | None = None
    last_japanese_timestamp_seconds: float | None = None
    audio_duration_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.parse_errors is None:
            self.parse_errors = []
        if self.warning_codes is None:
            self.warning_codes = []

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _timestamp_seconds(hours: str, minutes: str, seconds: str, millis: str) -> float:
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds) + int(millis) / 1000


def _decode_utf8(snapshot: bytes, label: str) -> tuple[str | None, list[str]]:
    if not snapshot:
        return "", []
    try:
        return snapshot.decode("utf-8-sig"), []
    except UnicodeDecodeError as exc:
        return None, [f"{label}: file is not valid UTF-8: {exc}"]


def _read_utf8(path: Path, label: str) -> tuple[str | None, list[str]]:
    if not path.is_file():
        return None, [f"{label}: file does not exist"]
    try:
        snapshot = path.read_bytes()
    except OSError as exc:
        return None, [f"{label}: unable to read file: {exc}"]
    return _decode_utf8(snapshot, label)


def _parse_srt_text(
    text: str | None,
    errors: list[str],
    label: str,
) -> tuple[list[Cue], list[str], bool]:
    if text is None:
        return [], errors, False
    if not text.strip():
        return [], errors, True

    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    blocks = re.split(r"\n[ \t]*\n", normalized)
    cues: list[Cue] = []
    expected_index = 1
    for block_number, block in enumerate(blocks, start=1):
        lines = block.split("\n")
        if len(lines) < 3:
            errors.append(f"{label}: block {block_number} has fewer than 3 lines")
            continue
        try:
            index = int(lines[0].strip())
        except ValueError:
            errors.append(f"{label}: block {block_number} has a non-numeric cue index")
            continue
        if index != expected_index:
            errors.append(
                f"{label}: block {block_number} index {index} is not expected index {expected_index}"
            )
        expected_index = index + 1

        match = TIMESTAMP_RE.fullmatch(lines[1].strip())
        if match is None:
            errors.append(f"{label}: cue {index} has an invalid timestamp")
            continue
        start = _timestamp_seconds(
            match["sh"], match["sm"], match["ss"], match["sms"]
        )
        end = _timestamp_seconds(match["eh"], match["em"], match["es"], match["ems"])
        if end < start:
            errors.append(f"{label}: cue {index} ends before it starts")
            continue

        text_lines = tuple(line.strip() for line in lines[2:] if line.strip())
        if not text_lines:
            errors.append(f"{label}: cue {index} has no text")
            continue
        cues.append(Cue(index, start, end, text_lines))
    return cues, errors, False


def _parse_srt(path: Path, label: str) -> tuple[list[Cue], list[str], bool]:
    text, errors = _read_utf8(path, label)
    return _parse_srt_text(text, errors, label)


def _parse_srt_snapshot(
    snapshot: bytes,
    label: str,
) -> tuple[list[Cue], list[str], bool]:
    text, errors = _decode_utf8(snapshot, label)
    return _parse_srt_text(text, errors, label)


def normalize_text(text: str) -> str:
    folded = unicodedata.normalize("NFKC", text).casefold()
    return " ".join(re.sub(r"[^\w]+", " ", folded, flags=re.UNICODE).split())


def _append_once(items: list[str], value: str) -> None:
    if value not in items:
        items.append(value)


def _build_quality_report(
    japanese_cues: list[Cue],
    japanese_errors: list[str],
    _japanese_empty: bool,
    english_cues: list[Cue],
    english_errors: list[str],
    english_empty: bool,
    *,
    english_missing: bool,
    audio_duration_seconds: float | None = None,
) -> QualityReport:
    reasons: list[str] = []
    warnings: list[str] = []
    parse_errors = [*japanese_errors, *english_errors]

    if english_missing:
        _append_once(reasons, "english_srt_missing")
    elif english_empty:
        _append_once(reasons, "english_srt_empty")
    if japanese_errors:
        _append_once(reasons, "japanese_srt_parse_error")
    if english_errors:
        _append_once(reasons, "english_srt_parse_error")
    if not english_cues and not english_empty:
        _append_once(reasons, "no_valid_english_cues")
    if len(japanese_cues) != len(english_cues):
        _append_once(reasons, "cue_count_mismatch")

    english_lines = [line for cue in english_cues for line in cue.text_lines]
    normalized_lines = [normalize_text(line) for line in english_lines]
    normalized_lines = [line for line in normalized_lines if line]
    counts: dict[str, int] = {}
    for line in normalized_lines:
        counts[line] = counts.get(line, 0) + 1
    dominant_text, dominant_count = ("", 0)
    if counts:
        dominant_text, dominant_count = max(counts.items(), key=lambda item: item[1])
    line_count = len(normalized_lines)
    unique_count = len(counts)
    unique_ratio = unique_count / line_count if line_count else 0.0
    dominant_ratio = dominant_count / line_count if line_count else 0.0
    known_bad_count = sum(line in KNOWN_BAD_TEXTS for line in normalized_lines)
    known_bad_ratio = known_bad_count / line_count if line_count else 0.0
    refusal_count = sum(
        any(phrase in line for phrase in REFUSAL_PHRASES) for line in normalized_lines
    )

    raw_english = "\n".join(english_lines)
    replacement_count = raw_english.count("\ufffd")
    mojibake_count = sum(raw_english.count(marker) for marker in MOJIBAKE_MARKERS)
    if replacement_count >= 3 or mojibake_count >= 3:
        _append_once(reasons, "encoding_corruption")
    if known_bad_count >= 10 and known_bad_ratio >= 0.05:
        _append_once(reasons, "known_bad_collapse")
    refusal_ratio = refusal_count / line_count if line_count else 0.0
    if refusal_count >= 3 and refusal_ratio >= 0.02:
        _append_once(reasons, "refusal_template")
    if (
        len(english_cues) >= 20
        and dominant_ratio >= 0.50
        and dominant_text not in SHORT_INTERJECTIONS
    ):
        _append_once(reasons, "dominant_text_collapse")
    if (
        len(english_cues) >= 100
        and unique_ratio < 0.15
        and dominant_ratio >= 0.25
        and dominant_text not in SHORT_INTERJECTIONS
    ):
        _append_once(reasons, "low_diversity_collapse")

    japanese_lengths = [sum(len(line) for line in cue.text_lines) for cue in japanese_cues]
    last_timestamp = max((cue.end_seconds for cue in japanese_cues), default=0.0)
    japanese_char_count = sum(japanese_lengths)
    chars_per_minute = (
        japanese_char_count / (last_timestamp / 60) if last_timestamp > 0 else None
    )
    median_length = statistics.median(japanese_lengths) if japanese_lengths else None
    long_10_ratio = (
        sum(cue.end_seconds - cue.start_seconds > 10 for cue in japanese_cues)
        / len(japanese_cues)
        if japanese_cues
        else None
    )
    long_20_ratio = (
        sum(cue.end_seconds - cue.start_seconds > 20 for cue in japanese_cues)
        / len(japanese_cues)
        if japanese_cues
        else None
    )
    if chars_per_minute is not None and chars_per_minute < 30:
        warnings.append("low_japanese_chars_per_minute")
    if median_length is not None and median_length < 3:
        warnings.append("short_median_japanese_text")
    if long_10_ratio is not None and long_10_ratio >= 0.10:
        warnings.append("many_japanese_cues_over_10s")
    if long_20_ratio is not None and long_20_ratio >= 0.05:
        warnings.append("many_japanese_cues_over_20s")
    if audio_duration_seconds is not None and last_timestamp:
        gap = abs(audio_duration_seconds - last_timestamp)
        if gap > max(120.0, audio_duration_seconds * 0.10):
            warnings.append("audio_subtitle_duration_gap")

    return QualityReport(
        passed=not reasons,
        reason_codes=reasons,
        japanese_cue_count=len(japanese_cues),
        english_cue_count=len(english_cues),
        english_text_line_count=len(english_lines),
        english_unique_count=unique_count,
        english_unique_ratio=unique_ratio,
        dominant_normalized_text=dominant_text,
        dominant_text_count=dominant_count,
        dominant_text_ratio=dominant_ratio,
        known_bad_phrase_count=known_bad_count,
        refusal_phrase_count=refusal_count,
        replacement_character_count=replacement_count,
        parse_errors=parse_errors,
        warning_codes=warnings,
        japanese_chars_per_minute=chars_per_minute,
        median_japanese_text_length=median_length,
        japanese_long_cue_over_10s_ratio=long_10_ratio,
        japanese_long_cue_over_20s_ratio=long_20_ratio,
        last_japanese_timestamp_seconds=last_timestamp or None,
        audio_duration_seconds=audio_duration_seconds,
    )


def validate_translation_quality_snapshots(
    japanese_snapshot: bytes,
    english_snapshot: bytes,
    *,
    audio_duration_seconds: float | None = None,
) -> QualityReport:
    japanese_cues, japanese_errors, japanese_empty = _parse_srt_snapshot(
        japanese_snapshot,
        "japanese",
    )
    english_cues, english_errors, english_empty = _parse_srt_snapshot(
        english_snapshot,
        "english",
    )
    return _build_quality_report(
        japanese_cues,
        japanese_errors,
        japanese_empty,
        english_cues,
        english_errors,
        english_empty,
        english_missing=False,
        audio_duration_seconds=audio_duration_seconds,
    )


def validate_translation_quality(
    japanese_srt_path: str | Path,
    english_srt_path: str | Path,
    *,
    audio_duration_seconds: float | None = None,
) -> QualityReport:
    japanese_path = Path(japanese_srt_path)
    english_path = Path(english_srt_path)
    japanese_cues, japanese_errors, japanese_empty = _parse_srt(
        japanese_path,
        "japanese",
    )
    english_cues, english_errors, english_empty = _parse_srt(
        english_path,
        "english",
    )
    return _build_quality_report(
        japanese_cues,
        japanese_errors,
        japanese_empty,
        english_cues,
        english_errors,
        english_empty,
        english_missing=not english_path.is_file(),
        audio_duration_seconds=audio_duration_seconds,
    )
