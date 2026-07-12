from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the legacy Codex batch translator once.")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--langs", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    batch_script = os.environ.get("CODEX_TRANSLATE_SCRIPT_PATH")
    if not batch_script:
        raise SystemExit("CODEX_TRANSLATE_SCRIPT_PATH must be set")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    python_executable = os.environ.get("CODEX_TRANSLATE_PYTHON_EXECUTABLE", sys.executable)
    provider = os.environ.get("CODEX_TRANSLATION_PROVIDER", "codex")
    command = [
        python_executable,
        batch_script,
        "--source-root",
        str(args.input.parent),
        "--output-root",
        str(args.output_dir),
        "--targets",
        args.input.name,
        "--provider",
        provider,
        "--resume",
    ]
    optional = (
        ("CODEX_TRANSLATION_WORKERS", "--workers"),
        ("CODEX_TRANSLATION_BATCH_WORKERS", "--batch-workers"),
        ("CODEX_TRANSLATION_ANTHROPIC_MODELS", "--anthropic-models"),
        ("CODEX_TRANSLATION_ANTHROPIC_RECHECK_MINUTES", "--anthropic-recheck-minutes"),
        ("CODEX_BIN_PATH", "--codex-bin"),
    )
    for env_name, option in optional:
        if value := os.environ.get(env_name):
            command.extend((option, value))

    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        return completed.returncode

    expected_name = args.input.name.replace(".Japanese.srt", ".English.srt")
    candidates = list(args.output_dir.rglob(expected_name))
    if not candidates:
        raise SystemExit(f"Codex batch translator did not create {expected_name}")
    destination = args.output_dir / expected_name
    if candidates[0] != destination:
        shutil.copyfile(candidates[0], destination)
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
