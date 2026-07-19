from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from .errors import SiteAudioError, UnsupportedSiteURL
from .urls import default_output_path, detect_provider


DEFAULT_PROFILE_DIR = Path.home() / ".jav-subtitle-orchestrator" / "site-audio-chrome"


@dataclass(frozen=True)
class PipelineOptions:
    profile_dir: Path
    browser_timeout: float
    headless: bool


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download a supported movie page as WAV")
    parser.add_argument("url")
    parser.add_argument("-o", "--output", type=Path)
    parser.add_argument("--profile-dir", type=Path, default=DEFAULT_PROFILE_DIR)
    parser.add_argument("--browser-timeout", type=float, default=120.0)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser


def _default_pipeline_factory(options: PipelineOptions):
    from .pipeline import SiteAudioPipeline

    return SiteAudioPipeline(options)


def main(
    argv: Sequence[str] | None = None,
    *,
    pipeline_factory: Callable[[PipelineOptions], object] = _default_pipeline_factory,
) -> int:
    args = build_parser().parse_args(argv)
    try:
        detect_provider(args.url)
        output = args.output or default_output_path(args.url)
        if output.exists() and not args.overwrite:
            raise UnsupportedSiteURL(f"output already exists: {output}")
        options = PipelineOptions(args.profile_dir, args.browser_timeout, args.headless)
        pipeline = pipeline_factory(options)
        result = pipeline.download(args.url, output, overwrite=args.overwrite)
    except SiteAudioError as exc:
        print(str(exc), file=sys.stderr)
        return exc.exit_code
    print(Path(result).resolve())
    return 0

