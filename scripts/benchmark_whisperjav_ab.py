#!/usr/bin/env python3
"""A/B benchmark existing Japanese SRTs against WhisperJAV sidecar outputs."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable


TIMING_SEPARATOR = "-->"
VERY_LONG_TEXT_CHARS = 90
REPEATED_TEXT_MIN_CHARS = 8
REPEATED_TEXT_UNIQUE_RATIO = 0.2


@dataclass(frozen=True)
class Cue:
    index: int
    start: float
    end: float
    text: str


@dataclass(frozen=True)
class SrtMetrics:
    path: str
    exists: bool
    file_size_bytes: int
    cue_count: int
    total_span_seconds: float
    cues_per_minute: float
    average_cue_duration_seconds: float
    average_chars_per_cue: float
    overlap_count: int
    regression_count: int
    empty_cue_count: int
    repeated_text_count: int
    very_long_cue_count: int
    first_cues: list[dict]
    middle_cues: list[dict]
    last_cues: list[dict]


def timestamp_to_seconds(value: str) -> float:
    hours, minutes, rest = value.strip().split(":")
    seconds, millis = rest.split(",")
    return (
        int(hours) * 3600
        + int(minutes) * 60
        + int(seconds)
        + int(millis) / 1000
    )


def parse_srt(path: Path) -> list[Cue]:
    if not path.exists():
        return []

    content = path.read_text(encoding="utf-8-sig", errors="replace")
    blocks = content.replace("\r\n", "\n").replace("\r", "\n").strip().split("\n\n")
    cues: list[Cue] = []
    fallback_index = 1
    for block in blocks:
        lines = [line.strip() for line in block.splitlines()]
        if len(lines) < 2:
            continue

        timing_pos = next((i for i, line in enumerate(lines) if TIMING_SEPARATOR in line), None)
        if timing_pos is None:
            continue

        index = fallback_index
        if timing_pos > 0:
            try:
                index = int(lines[timing_pos - 1])
            except ValueError:
                pass

        start_raw, end_raw = lines[timing_pos].split(TIMING_SEPARATOR, 1)
        try:
            start = timestamp_to_seconds(start_raw)
            end = timestamp_to_seconds(end_raw)
        except (ValueError, IndexError):
            continue

        text = "\n".join(lines[timing_pos + 1 :]).strip()
        cues.append(Cue(index=index, start=start, end=end, text=text))
        fallback_index += 1

    return cues


def _cue_to_dict(cue: Cue) -> dict:
    return {
        "index": cue.index,
        "start": round(cue.start, 3),
        "end": round(cue.end, 3),
        "text": cue.text,
    }


def _middle_slice(cues: list[Cue], size: int) -> list[Cue]:
    if len(cues) <= size:
        return cues
    midpoint = len(cues) // 2
    start = max(0, midpoint - size // 2)
    return cues[start : start + size]


def looks_repeated(text: str) -> bool:
    compact = "".join(text.split())
    if len(compact) < REPEATED_TEXT_MIN_CHARS:
        return False
    return len(set(compact)) / len(compact) < REPEATED_TEXT_UNIQUE_RATIO


def total_span_seconds(cues: list[Cue]) -> float:
    if not cues:
        return 0.0
    return max(0.0, max(cue.end for cue in cues) - min(cue.start for cue in cues))


def compute_srt_metrics(path: Path, sample_size: int = 20) -> SrtMetrics:
    cues = parse_srt(path)
    overlap_count = 0
    regression_count = 0
    for previous, current in zip(cues, cues[1:]):
        if current.start < previous.start:
            regression_count += 1
        elif current.start < previous.end:
            overlap_count += 1

    durations = [max(0.0, cue.end - cue.start) for cue in cues]
    span = total_span_seconds(cues)
    text_lengths = [len(cue.text) for cue in cues]
    return SrtMetrics(
        path=str(path),
        exists=path.exists(),
        file_size_bytes=path.stat().st_size if path.exists() else 0,
        cue_count=len(cues),
        total_span_seconds=round(span, 3),
        cues_per_minute=round(len(cues) / (span / 60), 3) if span > 0 else 0.0,
        average_cue_duration_seconds=round(sum(durations) / len(durations), 3)
        if durations
        else 0.0,
        average_chars_per_cue=round(sum(text_lengths) / len(text_lengths), 3)
        if text_lengths
        else 0.0,
        overlap_count=overlap_count,
        regression_count=regression_count,
        empty_cue_count=sum(1 for cue in cues if not cue.text.strip()),
        repeated_text_count=sum(1 for cue in cues if looks_repeated(cue.text)),
        very_long_cue_count=sum(1 for cue in cues if len(cue.text) > VERY_LONG_TEXT_CHARS),
        first_cues=[_cue_to_dict(cue) for cue in cues[:sample_size]],
        middle_cues=[_cue_to_dict(cue) for cue in _middle_slice(cues, sample_size)],
        last_cues=[_cue_to_dict(cue) for cue in cues[-sample_size:]],
    )


def compare_metrics(candidate: dict, baseline: dict) -> dict:
    return {
        "cue_count_delta": candidate["cue_count"] - baseline["cue_count"],
        "cue_count_ratio": round(candidate["cue_count"] / baseline["cue_count"], 3)
        if baseline["cue_count"]
        else None,
        "cues_per_minute_delta": round(
            candidate["cues_per_minute"] - baseline["cues_per_minute"], 3
        ),
        "overlap_delta": candidate["overlap_count"] - baseline["overlap_count"],
        "regression_delta": candidate["regression_count"] - baseline["regression_count"],
        "repeated_text_delta": candidate["repeated_text_count"] - baseline["repeated_text_count"],
        "very_long_cue_delta": candidate["very_long_cue_count"] - baseline["very_long_cue_count"],
    }


def normalize_for_cer(text: str) -> str:
    remove_chars = set("。、！？「」『』（）()…・〜～.,!?\"' \n\r\t")
    return "".join(char for char in text if char not in remove_chars)


def levenshtein_distance(left: str, right: str) -> int:
    if len(left) < len(right):
        return levenshtein_distance(right, left)
    if not right:
        return len(left)
    previous = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, start=1):
        current = [left_index]
        for right_index, right_char in enumerate(right, start=1):
            current.append(
                min(
                    previous[right_index] + 1,
                    current[right_index - 1] + 1,
                    previous[right_index - 1] + (left_char != right_char),
                )
            )
        previous = current
    return previous[-1]


def timing_iou(left: Cue, right: Cue) -> float:
    intersection = max(0.0, min(left.end, right.end) - max(left.start, right.start))
    union = max(left.end, right.end) - min(left.start, right.start)
    return intersection / union if union > 0 else 0.0


def compare_to_reference(candidate_path: Path, reference_path: Path) -> dict | None:
    if not reference_path.exists() or not candidate_path.exists():
        return None
    candidate_cues = parse_srt(candidate_path)
    reference_cues = parse_srt(reference_path)
    candidate_text = normalize_for_cer("".join(cue.text for cue in candidate_cues))
    reference_text = normalize_for_cer("".join(cue.text for cue in reference_cues))
    cer = None
    if reference_text:
        cer = round(levenshtein_distance(candidate_text, reference_text) / len(reference_text), 4)
    matched = list(zip(candidate_cues, reference_cues))
    mean_iou = round(sum(timing_iou(left, right) for left, right in matched) / len(matched), 4) if matched else None
    return {
        "reference_srt": str(reference_path),
        "character_error_rate": cer,
        "mean_timing_iou_by_ordinal": mean_iou,
        "matched_cue_count": len(matched),
    }


def find_whisperjav(explicit_path: str | None = None) -> str:
    candidates = [explicit_path] if explicit_path else []
    candidates.extend(["whisperjav", "whisperjav.exe"])
    for candidate in candidates:
        if not candidate:
            continue
        resolved = shutil.which(candidate) or candidate
        if Path(resolved).exists() or shutil.which(resolved):
            return resolved
    raise FileNotFoundError(
        "WhisperJAV CLI was not found. Install it first, for example: "
        "pip install \"whisperjav[cli] @ git+https://github.com/meizhong986/WhisperJAV.git\" "
        "or pass --whisperjav-bin FULL_PATH_TO_whisperjav."
    )


def discover_whisperjav_srt(output_dir: Path) -> Path:
    candidates = sorted(
        output_dir.rglob("*.srt"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"WhisperJAV did not produce an SRT under {output_dir}")
    return candidates[0]


def run_whisperjav_pipeline(
    *,
    audio_path: Path,
    output_srt: Path,
    pipeline: str,
    whisperjav_bin: str,
    timeout_seconds: int,
    keep_temp: bool,
    reference_srt: Path | None,
    log_dir: Path | None = None,
    device: str = "auto",
    compute_type: str = "auto",
    verbosity: str = "summary",
    model: str | None = None,
) -> dict:
    started = time.perf_counter()
    temp_manager = None
    if keep_temp:
        temp_root_path = Path(tempfile.mkdtemp(prefix=f"whisperjav-{pipeline}-"))
    else:
        temp_manager = TemporaryDirectory(prefix=f"whisperjav-{pipeline}-")
        temp_root_path = Path(temp_manager.__enter__())
    try:
        output_dir = temp_root_path / "out"
        temp_dir = temp_root_path / "tmp"
        output_dir.mkdir()
        temp_dir.mkdir()
        log_file = None
        stats_file = None
        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"{output_srt.stem}.log"
            stats_file = log_dir / f"{output_srt.stem}.stats.json"

        command = [
            whisperjav_bin,
            str(audio_path),
            "--mode",
            pipeline,
        ]
        if model:
            command.extend(["--model", model])
        command.extend([
            "--language",
            "japanese",
            "--subs-language",
            "native",
            "--output-dir",
            str(output_dir),
            "--temp-dir",
            str(temp_dir),
            "--output-format",
            "srt",
            "--device",
            device,
            "--compute-type",
            compute_type,
            "--verbosity",
            verbosity,
            "--accept-cpu-mode",
        ])
        if log_file:
            command.extend(["--log-file", str(log_file)])
        if stats_file:
            command.extend(["--stats-file", str(stats_file)])
        completed = subprocess.run(
            command,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        elapsed = round(time.perf_counter() - started, 3)
        if completed.returncode != 0:
            return {
                "status": "failed",
                "elapsed_seconds": elapsed,
                "command": command,
                "temp_dir": str(temp_root_path) if keep_temp else None,
                "log_file": str(log_file) if log_file else None,
                "stats_file": str(stats_file) if stats_file else None,
                "error": (completed.stderr or completed.stdout).strip(),
            }

        produced = discover_whisperjav_srt(output_dir)
        output_srt.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(produced, output_srt)
        metrics = asdict(compute_srt_metrics(output_srt))
        return {
            "status": "ok",
            "elapsed_seconds": elapsed,
            "command": command,
            "temp_dir": str(temp_root_path) if keep_temp else None,
            "log_file": str(log_file) if log_file else None,
            "stats_file": str(stats_file) if stats_file else None,
            "output_srt": str(output_srt),
            "metrics": metrics,
            "reference_comparison": compare_to_reference(output_srt, reference_srt)
            if reference_srt
            else None,
        }
    finally:
        if temp_manager is not None:
            temp_manager.__exit__(None, None, None)


def movie_number_from_job_dir(job_dir: Path) -> str:
    return job_dir.name


def benchmark_job(
    job_dir: Path,
    *,
    pipelines: Iterable[str],
    whisperjav_bin: str | None,
    timeout_seconds: int,
    keep_temp: bool = False,
    reference_srt: Path | None = None,
    log_dir: Path | None = None,
    device: str = "auto",
    compute_type: str = "auto",
    verbosity: str = "summary",
    model: str | None = None,
) -> dict:
    movie_number = movie_number_from_job_dir(job_dir)
    audio_path = job_dir / "audio.wav"
    baseline_japanese = job_dir / f"{movie_number}.Japanese.srt"
    baseline_english = job_dir / f"{movie_number}.English.srt"

    result = {
        "movie_number": movie_number,
        "job_dir": str(job_dir),
        "audio_path": str(audio_path),
        "baseline": {
            "japanese": asdict(compute_srt_metrics(baseline_japanese)),
            "english": asdict(compute_srt_metrics(baseline_english)),
        },
        "reference_srt": str(reference_srt) if reference_srt else None,
        "pipelines": {},
    }
    result["baseline"]["reference_comparison"] = compare_to_reference(
        baseline_japanese,
        reference_srt,
    ) if reference_srt else None

    if not audio_path.exists():
        result["error"] = f"audio.wav not found: {audio_path}"
        return result

    try:
        resolved_whisperjav = find_whisperjav(whisperjav_bin)
    except FileNotFoundError as exc:
        result["error"] = str(exc)
        return result

    for pipeline in pipelines:
        output_srt = job_dir / f"{movie_number}.Japanese.whisperjav-{pipeline}.srt"
        try:
            result["pipelines"][pipeline] = run_whisperjav_pipeline(
                audio_path=audio_path,
                output_srt=output_srt,
                pipeline=pipeline,
                whisperjav_bin=resolved_whisperjav,
                timeout_seconds=timeout_seconds,
                keep_temp=keep_temp,
                reference_srt=reference_srt,
                log_dir=log_dir / movie_number if log_dir else None,
                device=device,
                compute_type=compute_type,
                verbosity=verbosity,
                model=model,
            )
            pipeline_metrics = result["pipelines"][pipeline].get("metrics")
            if pipeline_metrics:
                result["pipelines"][pipeline]["baseline_comparison"] = compare_metrics(
                    pipeline_metrics,
                    result["baseline"]["japanese"],
                )
        except subprocess.TimeoutExpired:
            result["pipelines"][pipeline] = {
                "status": "failed",
                "error": f"WhisperJAV pipeline '{pipeline}' timed out after {timeout_seconds}s",
            }
        except Exception as exc:
            result["pipelines"][pipeline] = {
                "status": "failed",
                "error": str(exc),
            }

    return result


def write_reports(results: list[dict], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_json = output_dir / "benchmark-report.json"
    report_md = output_dir / "benchmark-summary.md"
    report_json.write_text(
        json.dumps({"jobs": results}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report_md.write_text(render_markdown_summary(results), encoding="utf-8")
    return report_json, report_md


def render_markdown_summary(results: list[dict]) -> str:
    lines = [
        "# WhisperJAV A/B Benchmark Summary",
        "",
        "| Movie | Variant | Status | Seconds | Cues | Avg Dur | Overlaps | Regressions | Repeated | Long |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for job in results:
        movie = job["movie_number"]
        baseline = job["baseline"]["japanese"]
        lines.append(_summary_row(movie, "baseline", "ok" if baseline["exists"] else "missing", None, baseline))
        for pipeline, pipeline_result in job.get("pipelines", {}).items():
            metrics = pipeline_result.get("metrics")
            lines.append(
                _summary_row(
                    movie,
                    f"whisperjav-{pipeline}",
                    pipeline_result.get("status", "unknown"),
                    pipeline_result.get("elapsed_seconds"),
                    metrics,
                )
            )
            comparison = pipeline_result.get("baseline_comparison")
            if comparison:
                lines.append(
                    f"| {movie} | {pipeline} vs baseline | delta |  | "
                    f"{comparison['cue_count_delta']} |  | "
                    f"{comparison['overlap_delta']} | {comparison['regression_delta']} | "
                    f"{comparison['repeated_text_delta']} | {comparison['very_long_cue_delta']} |"
                )
            reference = pipeline_result.get("reference_comparison")
            if reference:
                lines.append(
                    f"| {movie} | {pipeline} reference | CER/IoU |  | "
                    f"{reference['matched_cue_count']} | {reference['mean_timing_iou_by_ordinal']} | "
                    f" |  | {reference['character_error_rate']} |  |"
                )
        if job.get("error"):
            lines.append(f"| {movie} | setup | failed |  |  |  |  |  |  |  |")
            lines.append("")
            lines.append(f"> {job['error']}")
            lines.append("")
    lines.append("")
    return "\n".join(lines)


def _summary_row(
    movie: str,
    variant: str,
    status: str,
    elapsed: float | None,
    metrics: dict | None,
) -> str:
    if not metrics:
        return f"| {movie} | {variant} | {status} | {elapsed or ''} |  |  |  |  |  |  |"
    return (
        f"| {movie} | {variant} | {status} | {elapsed or ''} | "
        f"{metrics['cue_count']} | {metrics['average_cue_duration_seconds']} | "
        f"{metrics['overlap_count']} | {metrics['regression_count']} | "
        f"{metrics['repeated_text_count']} | {metrics['very_long_cue_count']} |"
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run sidecar A/B transcription benchmarks for completed jobs."
    )
    parser.add_argument("job_dirs", nargs="+", type=Path)
    parser.add_argument("--pipelines", default="faster,balanced")
    parser.add_argument("--whisperjav-bin", default=None)
    parser.add_argument("--model", default=None, help="Optional WhisperJAV model override.")
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument(
        "--device",
        choices=["auto", "cuda", "cpu"],
        default="auto",
        help="Device forwarded to WhisperJAV. Use cuda for GPU-only benchmark validation.",
    )
    parser.add_argument(
        "--compute-type",
        choices=["auto", "float16", "float32", "int8", "int8_float16", "int8_float32"],
        default="auto",
        help="Compute type forwarded to WhisperJAV.",
    )
    parser.add_argument(
        "--verbosity",
        choices=["quiet", "summary", "normal", "verbose"],
        default="summary",
        help="WhisperJAV verbosity forwarded to each pipeline.",
    )
    parser.add_argument(
        "--reference-srt",
        type=Path,
        default=None,
        help="Optional trusted reference SRT. Only useful with one job_dir.",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=Path("benchmark-results") / "whisperjav-ab",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    pipelines = [pipeline.strip() for pipeline in args.pipelines.split(",") if pipeline.strip()]
    results = [
        benchmark_job(
            job_dir,
            pipelines=pipelines,
            whisperjav_bin=args.whisperjav_bin,
            timeout_seconds=args.timeout_seconds,
            keep_temp=args.keep_temp,
            reference_srt=args.reference_srt,
            log_dir=args.report_dir / "logs",
            device=args.device,
            compute_type=args.compute_type,
            verbosity=args.verbosity,
            model=args.model,
        )
        for job_dir in args.job_dirs
    ]
    args.report_dir.mkdir(parents=True, exist_ok=True)
    report_json, report_md = write_reports(results, args.report_dir)
    print(f"Wrote {report_json}")
    print(f"Wrote {report_md}")
    return 1 if any(job.get("error") for job in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
