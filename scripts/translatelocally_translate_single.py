from __future__ import annotations

import argparse
import sys
from pathlib import Path

from translate_srt_translatelocally import translate_srt


def english_output_name(input_path: Path) -> str:
    if input_path.name.endswith(".Japanese.srt"):
        return input_path.name.replace(".Japanese.srt", ".English.srt")
    return f"{input_path.stem}.English.srt"


def parse_langs(value: str) -> list[str]:
    return [lang.strip().lower() for lang in value.split(",") if lang.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Worker-compatible wrapper for TranslateLocally SRT translation."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--langs", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--model", default=None)
    parser.add_argument("--translate-locally-path", default=None)
    parser.add_argument("--batch-lines", type=int, default=50)
    parser.add_argument("--batch-chars", type=int, default=4000)
    parser.add_argument("--timeout-seconds", type=float, default=120)
    parser.add_argument("--retries", type=int, default=1)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    langs = parse_langs(args.langs)
    unsupported = [lang for lang in langs if lang != "en"]
    if unsupported:
        raise SystemExit(
            "TranslateLocally wrapper only supports English output; "
            f"unsupported langs: {', '.join(unsupported)}"
        )
    if not langs:
        raise SystemExit("--langs must include en")

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / english_output_name(input_path)
    translate_srt(
        input_path,
        output_path,
        translate_locally_path=args.translate_locally_path,
        model=args.model,
        batch_lines=args.batch_lines,
        batch_chars=args.batch_chars,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
    )
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
