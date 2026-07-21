"""Merge a stable base transcript with dialogue confirmed by two offset grids."""

from __future__ import annotations

import argparse
import json
import unicodedata
from dataclasses import asdict
from difflib import SequenceMatcher
from pathlib import Path

try:
    from scripts.benchmark_transcription_coverage import (
        Cue,
        _write_srt,
        compute_metrics,
        parse_srt,
    )
except ModuleNotFoundError:
    from benchmark_transcription_coverage import (
        Cue,
        _write_srt,
        compute_metrics,
        parse_srt,
    )


_PHANTOM_PHRASES = {
    "ご視聴ありがとうございました",
    "ありがとうございました",
    "おはようございます",
    "おやすみなさい",
    "さようなら",
    "おわり",
}


def normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text).lower()
    return "".join(
        character
        for character in normalized
        if (
            "\u3040" <= character <= "\u30ff"
            or "\u3400" <= character <= "\u9fff"
            or character.isalnum()
        )
    )


def text_similarity(left: str, right: str) -> float:
    left_normalized = normalize_text(left)
    right_normalized = normalize_text(right)
    if not left_normalized or not right_normalized:
        return 0.0
    if left_normalized == right_normalized:
        return 1.0
    if min(len(left_normalized), len(right_normalized)) >= 3 and (
        left_normalized in right_normalized
        or right_normalized in left_normalized
    ):
        return 1.0
    return SequenceMatcher(None, left_normalized, right_normalized).ratio()


def is_suspicious(cue: Cue) -> bool:
    normalized = normalize_text(cue.text)
    if not normalized:
        return True
    duration = cue.end - cue.start
    if duration >= 10 and normalized in _PHANTOM_PHRASES:
        return True
    if duration >= 20 and len(normalized) / duration < 0.5:
        return True
    if len(normalized) >= 12 and len(set(normalized)) / len(normalized) <= 0.25:
        return True
    return False


def _nearby_sequences(
    cue: Cue,
    others: list[Cue],
    *,
    margin_seconds: float,
    max_sequence_cues: int = 4,
) -> list[str]:
    nearby = [
        other
        for other in others
        if other.end >= cue.start - margin_seconds
        and other.start <= cue.end + margin_seconds
        and not is_suspicious(other)
    ]
    sequences: list[str] = []
    for start_index in range(len(nearby)):
        text_parts: list[str] = []
        for end_index in range(
            start_index, min(len(nearby), start_index + max_sequence_cues)
        ):
            text_parts.append(nearby[end_index].text)
            sequences.append("".join(text_parts))
    return sequences


def consensus_cues(
    grid_a: list[Cue],
    grid_b: list[Cue],
    *,
    margin_seconds: float = 5.0,
    minimum_similarity: float = 0.72,
) -> list[Cue]:
    confirmed: list[Cue] = []
    for cue in grid_a:
        if is_suspicious(cue):
            continue
        sequences = _nearby_sequences(
            cue,
            grid_b,
            margin_seconds=margin_seconds,
        )
        best_similarity = max(
            (text_similarity(cue.text, text) for text in sequences),
            default=0.0,
        )
        if best_similarity >= minimum_similarity:
            confirmed.append(cue)
    return confirmed


def merge_with_base(base: list[Cue], confirmed: list[Cue]) -> list[Cue]:
    merged = [cue for cue in base if not is_suspicious(cue)]
    for candidate in confirmed:
        duplicate = any(
            existing.end >= candidate.start - 2
            and existing.start <= candidate.end + 2
            and text_similarity(existing.text, candidate.text) >= 0.72
            for existing in merged
        )
        if not duplicate:
            merged.append(candidate)
    merged.sort(key=lambda cue: (cue.start, cue.end, cue.text))
    return [
        Cue(index=index, start=cue.start, end=cue.end, text=cue.text)
        for index, cue in enumerate(merged, start=1)
    ]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--grid-a", type=Path, required=True)
    parser.add_argument("--grid-b", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--margin-seconds", type=float, default=5.0)
    parser.add_argument("--minimum-similarity", type=float, default=0.72)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    base = parse_srt(args.base)
    grid_a = parse_srt(args.grid_a)
    grid_b = parse_srt(args.grid_b)
    confirmed = consensus_cues(
        grid_a,
        grid_b,
        margin_seconds=args.margin_seconds,
        minimum_similarity=args.minimum_similarity,
    )
    merged = merge_with_base(base, confirmed)
    _write_srt(merged, args.output)
    report = {
        "base_path": str(args.base),
        "grid_a_path": str(args.grid_a),
        "grid_b_path": str(args.grid_b),
        "output_path": str(args.output),
        "base_cue_count": len(base),
        "grid_a_cue_count": len(grid_a),
        "grid_b_cue_count": len(grid_b),
        "confirmed_candidate_count": len(confirmed),
        "merged_cue_count": len(merged),
        "base_metrics": asdict(
            compute_metrics(base, window_start=0, window_end=args.duration)
        ),
        "merged_metrics": asdict(
            compute_metrics(merged, window_start=0, window_end=args.duration)
        ),
        "settings": {
            "margin_seconds": args.margin_seconds,
            "minimum_similarity": args.minimum_similarity,
        },
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
