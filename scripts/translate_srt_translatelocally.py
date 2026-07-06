#!/usr/bin/env python3
"""Translate Japanese SRT files with TranslateLocally.

This script preserves SRT cue numbers and timing lines, sends only subtitle text
to TranslateLocally, and writes a translated SRT with the original structure.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


DEFAULT_MODEL = "ja-en-tiny"
TIMING_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2},\d{3}\s+-->\s+\d{2}:\d{2}:\d{2},\d{3}(?:\s+.*)?$"
)
COMMON_TRANSLATELOCALLY_PATHS = (
    "/Applications/translateLocally.app/Contents/MacOS/translateLocally",
    r"C:\Program Files\translateLocally\translateLocally.exe",
    r"C:\Program Files (x86)\translateLocally\translateLocally.exe",
)


@dataclass(frozen=True)
class TextLine:
    index: int
    text: str


def decode_srt(path: Path) -> tuple[str, bool]:
    data = path.read_bytes()
    has_bom = data.startswith(b"\xef\xbb\xbf")
    try:
        return data.decode("utf-8-sig"), has_bom
    except UnicodeDecodeError as exc:
        raise SystemExit(f"{path} must be UTF-8 or UTF-8-BOM: {exc}") from exc


def detect_line_ending(text: str) -> str:
    if "\r\n" in text:
        return "\r\n"
    if "\r" in text:
        return "\r"
    return "\n"


def extract_text_lines(lines: list[str]) -> list[TextLine]:
    text_lines: list[TextLine] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.isdigit():
            continue
        if TIMING_RE.match(stripped):
            continue
        text_lines.append(TextLine(index=index, text=line))
    return text_lines


def find_translate_locally(explicit_path: str | None) -> str:
    candidates: list[str] = []
    if explicit_path:
        candidates.append(explicit_path)
    env_path = os.environ.get("TRANSLATELOCALLY_PATH")
    if env_path:
        candidates.append(env_path)
    path_executable = shutil.which("translateLocally")
    if path_executable:
        candidates.append(path_executable)
    candidates.extend(COMMON_TRANSLATELOCALLY_PATHS)

    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    raise SystemExit(
        "Could not find TranslateLocally. Pass --tl or set TRANSLATELOCALLY_PATH."
    )


def translate_lines(
    translate_locally: str,
    model: str,
    text_lines: list[TextLine],
    extra_tl_args: list[str],
) -> list[str]:
    if not text_lines:
        return []
    payload = "\n".join(line.text for line in text_lines) + "\n"
    command = [translate_locally, *extra_tl_args, "--model", model]
    completed = subprocess.run(
        command,
        input=payload,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr)
        raise SystemExit(completed.returncode)

    translations = completed.stdout.splitlines()
    if len(translations) != len(text_lines):
        raise SystemExit(
            "TranslateLocally returned "
            f"{len(translations)} lines for {len(text_lines)} input text lines"
        )
    return translations


def translate_srt(
    input_path: Path,
    output_path: Path,
    *,
    translate_locally: str,
    model: str,
    extra_tl_args: list[str],
) -> None:
    source_text, has_bom = decode_srt(input_path)
    line_ending = detect_line_ending(source_text)
    normalized = source_text.replace("\r\n", "\n").replace("\r", "\n")
    trailing_newline = normalized.endswith("\n")
    lines = normalized.splitlines()
    text_lines = extract_text_lines(lines)
    translations = translate_lines(translate_locally, model, text_lines, extra_tl_args)

    output_lines = list(lines)
    for text_line, translation in zip(text_lines, translations):
        output_lines[text_line.index] = translation.strip()

    rendered = "\n".join(output_lines)
    if trailing_newline:
        rendered += "\n"
    rendered = rendered.replace("\n", line_ending)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data = rendered.encode("utf-8")
    output_path.write_bytes((b"\xef\xbb\xbf" + data) if has_bom else data)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Translate a Japanese SRT to English with local TranslateLocally."
    )
    parser.add_argument("input_srt", type=Path)
    parser.add_argument("output_srt", type=Path)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--tl", help="Path to translateLocally executable")
    parser.add_argument(
        "--tl-arg",
        action="append",
        default=[],
        help="Extra argument passed to TranslateLocally before --model. May be repeated.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    translate_locally = find_translate_locally(args.tl)
    translate_srt(
        args.input_srt,
        args.output_srt,
        translate_locally=translate_locally,
        model=args.model,
        extra_tl_args=args.tl_arg,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
