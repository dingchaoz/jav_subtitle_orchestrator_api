from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory


DEFAULT_MODEL = "ja-en-tiny"
TIMING_MARKER = "-->"
DEFAULT_BATCH_LINES = 50
DEFAULT_BATCH_CHARS = 4000
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_RETRIES = 1
REPLACEMENT_CHARACTER = "\ufffd"
TRANSLATELOCALLY_CANDIDATES = [
    r"C:\Users\dingc\AppData\Local\Programs\TranslateLocally\translateLocally.exe",
    r"C:\Program Files\translateLocally\translateLocally.exe",
    r"C:\Program Files\TranslateLocally\translateLocally.exe",
]


@dataclass(frozen=True)
class SanitizedTranslationInput:
    lines: tuple[str, ...]
    replacement_character_count: int
    sanitized_line_count: int


def detect_newline(text: str) -> str:
    return "\r\n" if "\r\n" in text else "\n"


def find_translate_locally(explicit_path: str | None = None) -> str:
    if explicit_path:
        if Path(explicit_path).is_file():
            return str(Path(explicit_path))
        raise FileNotFoundError(
            f"{explicit_path} was not found. Set TRANSLATELOCALLY_PATH to the full executable path."
        )

    candidates = [
        os.environ.get("TRANSLATELOCALLY_PATH"),
        shutil.which("translateLocally"),
        shutil.which("translateLocally.exe"),
        *TRANSLATELOCALLY_CANDIDATES,
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return str(Path(candidate))
    raise FileNotFoundError(
        "translateLocally.exe was not found. Set TRANSLATELOCALLY_PATH to the full executable path."
    )


def ensure_model_available(translate_locally: str, model: str) -> None:
    completed = subprocess.run(
        [translate_locally, "-l"],
        text=True,
        capture_output=True,
        check=False,
    )
    output = f"{completed.stdout}\n{completed.stderr}"
    if completed.returncode != 0:
        raise RuntimeError(
            f"translateLocally -l failed with exit code {completed.returncode}: {output.strip()}"
        )
    if model not in output:
        raise RuntimeError(
            f"TranslateLocally model {model!r} is not installed. "
            "Install Japanese-English tiny before running translation."
        )


def is_translatable_line(line: str, block_line_index: int) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if block_line_index == 0 and stripped.isdigit():
        return False
    if TIMING_MARKER in stripped:
        return False
    if stripped.upper().startswith(("WEBVTT", "NOTE", "STYLE", "REGION")):
        return False
    return True


def collect_text_line_indexes(lines: list[str]) -> list[int]:
    indexes: list[int] = []
    block_line_index = 0
    for index, line in enumerate(lines):
        if not line.strip():
            block_line_index = 0
            continue
        if is_translatable_line(line, block_line_index):
            indexes.append(index)
        block_line_index += 1
    return indexes


def sanitize_translation_input(lines: list[str]) -> SanitizedTranslationInput:
    sanitized: list[str] = []
    replacement_count = 0
    sanitized_line_count = 0
    for line_number, line in enumerate(lines, start=1):
        line_replacement_count = line.count(REPLACEMENT_CHARACTER)
        cleaned = line.replace(REPLACEMENT_CHARACTER, "")
        if line.strip() and not cleaned.strip():
            raise ValueError(
                "translation_input_corrupt: "
                f"line {line_number} empty after removing replacement characters"
            )
        if line_replacement_count:
            sanitized_line_count += 1
            replacement_count += line_replacement_count
        sanitized.append(cleaned)
    return SanitizedTranslationInput(
        lines=tuple(sanitized),
        replacement_character_count=replacement_count,
        sanitized_line_count=sanitized_line_count,
    )


def run_translate_locally(
    lines: list[str],
    *,
    translate_locally: str,
    model: str,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[str]:
    if not lines:
        return []

    payload = "\n".join(lines) + "\n"
    try:
        completed = subprocess.run(
            [translate_locally, "-m", model],
            input=payload,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"translateLocally timed out after {timeout_seconds:g} seconds"
        ) from exc
    if completed.returncode == 0:
        translated = completed.stdout.splitlines()
        if len(translated) != len(lines):
            raise RuntimeError(
                "translateLocally changed the number of translated text lines: "
                f"input={len(lines)} output={len(translated)}"
            )
        return translated

    # Older/non-sandboxed builds may require explicit files. The macOS app build
    # succeeds through stdin/stdout, while its sandbox rejects temporary file paths.
    with TemporaryDirectory(prefix="translatelocally-") as temp_dir:
        temp_path = Path(temp_dir)
        input_path = temp_path / "input.ja.txt"
        output_path = temp_path / "output.en.txt"
        input_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        try:
            completed = subprocess.run(
                [
                    translate_locally,
                    "-m",
                    model,
                    "-i",
                    str(input_path),
                    "-o",
                    str(output_path),
                ],
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                check=False,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"translateLocally timed out after {timeout_seconds:g} seconds"
            ) from exc
        if completed.returncode != 0:
            output = (completed.stderr or completed.stdout).strip()
            raise RuntimeError(
                f"translateLocally failed with exit code {completed.returncode}: {output}"
            )
        if not output_path.exists():
            raise FileNotFoundError(f"translateLocally did not create {output_path}")

        translated = read_translate_locally_output(output_path).splitlines()

    if len(translated) != len(lines):
        raise RuntimeError(
            "translateLocally changed the number of translated text lines: "
            f"input={len(lines)} output={len(translated)}"
        )
    return translated


def iter_translation_batches(
    lines: list[str],
    *,
    max_lines: int = DEFAULT_BATCH_LINES,
    max_chars: int = DEFAULT_BATCH_CHARS,
):
    if max_lines < 1 or max_chars < 1:
        raise ValueError("batch limits must be positive")
    batch: list[str] = []
    character_count = 0
    for line_number, line in enumerate(lines, start=1):
        if len(line) > max_chars:
            raise ValueError(
                f"translation text line {line_number} exceeds {max_chars} characters"
            )
        if batch and (len(batch) >= max_lines or character_count + len(line) > max_chars):
            yield batch
            batch = []
            character_count = 0
        batch.append(line)
        character_count += len(line)
    if batch:
        yield batch


def _append_batch_log(path: Path | None, payload: dict[str, object]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")


def run_translate_locally_batched(
    lines: list[str],
    *,
    translate_locally: str,
    model: str,
    batch_lines: int = DEFAULT_BATCH_LINES,
    batch_chars: int = DEFAULT_BATCH_CHARS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
    batch_log_path: Path | None = None,
) -> list[str]:
    if retries < 0:
        raise ValueError("retries cannot be negative")
    translated: list[str] = []
    batches = iter_translation_batches(
        lines,
        max_lines=batch_lines,
        max_chars=batch_chars,
    )
    for batch_number, batch in enumerate(batches, start=1):
        started = time.monotonic()
        result: list[str] | None = None
        error: Exception | None = None
        attempts = 0
        for attempt in range(retries + 1):
            attempts = attempt + 1
            try:
                result = run_translate_locally(
                    batch,
                    translate_locally=translate_locally,
                    model=model,
                    timeout_seconds=timeout_seconds,
                )
                error = None
                break
            except Exception as exc:
                error = exc
        duration = time.monotonic() - started
        _append_batch_log(
            batch_log_path,
            {
                "attempt_count": attempts,
                "batch_number": batch_number,
                "duration_seconds": round(duration, 3),
                "input_character_count": sum(len(line) for line in batch),
                "input_line_count": len(batch),
                "output_line_count": len(result) if result is not None else 0,
                "return_code": 0 if result is not None else None,
            },
        )
        if error is not None:
            raise error
        assert result is not None
        translated.extend(result)
    return translated


def read_translate_locally_output(path: Path) -> str:
    data = path.read_bytes()
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def translate_srt(
    input_srt: Path,
    output_srt: Path,
    *,
    translate_locally_path: str | None = None,
    model: str | None = None,
    batch_lines: int = DEFAULT_BATCH_LINES,
    batch_chars: int = DEFAULT_BATCH_CHARS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
    batch_log_path: Path | None = None,
) -> None:
    source = input_srt.read_bytes().decode("utf-8-sig")
    newline = detect_newline(source)
    lines = source.splitlines()
    text_indexes = collect_text_line_indexes(lines)
    source_text = [lines[index] for index in text_indexes]
    sanitized_input = sanitize_translation_input(source_text)
    effective_batch_log_path = batch_log_path or (
        Path(os.environ["TRANSLATE_BATCH_LOG_PATH"])
        if os.environ.get("TRANSLATE_BATCH_LOG_PATH")
        else None
    )
    _append_batch_log(
        effective_batch_log_path,
        {
            "event": "input_sanitization",
            "input_replacement_character_count": (
                sanitized_input.replacement_character_count
            ),
            "sanitized_input_line_count": sanitized_input.sanitized_line_count,
        },
    )

    translate_locally = find_translate_locally(translate_locally_path)
    selected_model = model or os.environ.get("TRANSLATELOCALLY_MODEL") or DEFAULT_MODEL
    ensure_model_available(translate_locally, selected_model)
    translated_text = run_translate_locally_batched(
        list(sanitized_input.lines),
        translate_locally=translate_locally,
        model=selected_model,
        batch_lines=batch_lines,
        batch_chars=batch_chars,
        timeout_seconds=timeout_seconds,
        retries=retries,
        batch_log_path=effective_batch_log_path,
    )

    output_lines = list(lines)
    for index, translated in zip(text_indexes, translated_text):
        output_lines[index] = translated

    rendered = newline.join(output_lines)
    if source.endswith(("\n", "\r\n")):
        rendered += newline
    output_srt.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output_srt.with_name(
        f".{output_srt.name}.{uuid.uuid4().hex}.tmp"
    )
    try:
        temporary_output.write_bytes(rendered.encode("utf-8"))
        os.replace(temporary_output, output_srt)
    finally:
        temporary_output.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Translate a Japanese SRT to English with local TranslateLocally."
    )
    parser.add_argument("positional_input", nargs="?")
    parser.add_argument("positional_output", nargs="?")
    parser.add_argument("--input", dest="input_path")
    parser.add_argument("--output", dest="output_path")
    parser.add_argument("--model", default=None)
    parser.add_argument("--translate-locally-path", default=None)
    parser.add_argument("--batch-lines", type=int, default=DEFAULT_BATCH_LINES)
    parser.add_argument("--batch-chars", type=int, default=DEFAULT_BATCH_CHARS)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--batch-log", default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_value = args.input_path or args.positional_input
    output_value = args.output_path or args.positional_output
    if not input_value or not output_value:
        raise SystemExit("input and output SRT paths are required")

    translate_srt(
        Path(input_value),
        Path(output_value),
        translate_locally_path=args.translate_locally_path,
        model=args.model,
        batch_lines=args.batch_lines,
        batch_chars=args.batch_chars,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        batch_log_path=Path(args.batch_log) if args.batch_log else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
