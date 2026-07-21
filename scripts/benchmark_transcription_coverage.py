"""Run reproducible faster-whisper coverage experiments and score their SRT output."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any


@dataclass(frozen=True)
class Cue:
    index: int
    start: float
    end: float
    text: str


@dataclass(frozen=True)
class CoverageMetrics:
    window_start: float
    window_end: float
    cue_count: int
    covered_seconds: float
    coverage_ratio: float
    max_gap_seconds: float
    japanese_char_count: int
    japanese_chars_per_minute: float
    long_cue_over_10_count: int
    long_cue_over_20_count: int


@dataclass(frozen=True)
class BenchmarkConfig:
    model: str
    device: str = "cuda"
    compute_type: str = "float16"
    chunk_seconds: int = 900
    beam_size: int = 5
    patience: float = 1.0
    temperature: float = 0.0
    repetition_penalty: float = 1.0
    no_repeat_ngram_size: int = 0
    no_speech_threshold: float = 0.6
    log_prob_threshold: float = -1.0
    condition_on_previous_text: bool = False
    vad_mode: str = "default"
    vad_threshold: float = 0.5
    vad_min_speech_ms: int = 0
    vad_min_silence_ms: int = 2000
    vad_speech_pad_ms: int = 400
    initial_prompt: str | None = None
    hotwords: str | None = None
    word_timestamps: bool = False

    def __post_init__(self) -> None:
        if self.vad_mode not in {"off", "default", "custom"}:
            raise ValueError("vad_mode must be one of: off, default, custom")
        if self.chunk_seconds <= 0:
            raise ValueError("chunk_seconds must be positive")

    @property
    def vad_filter(self) -> bool:
        return self.vad_mode != "off"

    @property
    def vad_parameters(self) -> dict[str, int | float] | None:
        if self.vad_mode != "custom":
            return None
        return {
            "threshold": self.vad_threshold,
            "min_speech_duration_ms": self.vad_min_speech_ms,
            "min_silence_duration_ms": self.vad_min_silence_ms,
            "speech_pad_ms": self.vad_speech_pad_ms,
        }


def _timestamp_seconds(value: str) -> float:
    hours, minutes, remainder = value.replace(".", ",").split(":")
    seconds, millis = remainder.split(",")
    return (
        int(hours) * 3600
        + int(minutes) * 60
        + int(seconds)
        + int(millis) / 1000
    )


def parse_srt(path: Path) -> list[Cue]:
    text = path.read_text(encoding="utf-8-sig").replace("\r\n", "\n").strip()
    if not text:
        return []
    cues: list[Cue] = []
    for block in text.split("\n\n"):
        lines = [line.strip() for line in block.splitlines()]
        if len(lines) < 3 or " --> " not in lines[1]:
            raise ValueError(f"invalid SRT block: {block!r}")
        start_text, end_text = lines[1].split(" --> ", maxsplit=1)
        cues.append(
            Cue(
                index=int(lines[0]),
                start=_timestamp_seconds(start_text),
                end=_timestamp_seconds(end_text),
                text="\n".join(line for line in lines[2:] if line),
            )
        )
    return cues


def compute_metrics(
    cues: list[Cue],
    *,
    window_start: float,
    window_end: float,
) -> CoverageMetrics:
    if window_end <= window_start:
        raise ValueError("window_end must be greater than window_start")
    selected = [
        cue for cue in cues if cue.end > window_start and cue.start < window_end
    ]
    intervals = sorted(
        (max(cue.start, window_start), min(cue.end, window_end))
        for cue in selected
    )
    merged: list[list[float]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    covered = sum(end - start for start, end in merged)
    cursor = window_start
    max_gap = 0.0
    for start, end in merged:
        max_gap = max(max_gap, start - cursor)
        cursor = max(cursor, end)
    max_gap = max(max_gap, window_end - cursor)
    window_duration = window_end - window_start
    japanese_chars = sum(
        1
        for cue in selected
        for char in cue.text
        if "\u3040" <= char <= "\u30ff" or "\u3400" <= char <= "\u9fff"
    )
    return CoverageMetrics(
        window_start=window_start,
        window_end=window_end,
        cue_count=len(selected),
        covered_seconds=covered,
        coverage_ratio=covered / window_duration,
        max_gap_seconds=max_gap,
        japanese_char_count=japanese_chars,
        japanese_chars_per_minute=japanese_chars / (window_duration / 60),
        long_cue_over_10_count=sum(cue.end - cue.start > 10 for cue in selected),
        long_cue_over_20_count=sum(cue.end - cue.start > 20 for cue in selected),
    )


def _srt_timestamp(seconds: float) -> str:
    millis = round(seconds * 1000)
    hours, remainder = divmod(millis, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    secs, millis = divmod(remainder, 1000)
    return f"{hours:02}:{minutes:02}:{secs:02},{millis:03}"


def _write_srt(cues: list[Cue], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for index, cue in enumerate(cues, start=1):
            handle.write(f"{index}\n")
            handle.write(
                f"{_srt_timestamp(cue.start)} --> {_srt_timestamp(cue.end)}\n"
            )
            handle.write(cue.text.strip() + "\n\n")


def _probe_duration(path: Path) -> float:
    completed = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        text=True,
        capture_output=True,
        check=True,
    )
    return float(completed.stdout.strip())


def _extract_chunk(source: Path, output: Path, start: float, duration: float) -> None:
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{start:.3f}",
            "-t",
            f"{duration:.3f}",
            "-i",
            str(source),
            "-ac",
            "1",
            "-ar",
            "16000",
            str(output),
        ],
        check=True,
    )


def run_benchmark(
    audio_path: Path,
    output_dir: Path,
    config: BenchmarkConfig,
    *,
    timestamp_offset: float = 0.0,
    force: bool = False,
) -> tuple[Path, Path]:
    srt_path = output_dir / "transcript.srt"
    manifest_path = output_dir / "manifest.json"
    if not force and (srt_path.exists() or manifest_path.exists()):
        raise FileExistsError(f"benchmark output already exists: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    import faster_whisper
    from faster_whisper import WhisperModel

    model = WhisperModel(
        config.model,
        device=config.device,
        compute_type=config.compute_type,
    )
    duration = _probe_duration(audio_path)
    cues: list[Cue] = []
    telemetry: list[dict[str, Any]] = []
    started = time.perf_counter()
    with TemporaryDirectory(prefix="coverage-benchmark-") as temp_name:
        temp_dir = Path(temp_name)
        chunk_start = 0.0
        chunk_index = 0
        while chunk_start < duration:
            chunk_duration = min(config.chunk_seconds, duration - chunk_start)
            chunk_path = temp_dir / f"chunk-{chunk_index:04d}.wav"
            _extract_chunk(audio_path, chunk_path, chunk_start, chunk_duration)
            raw_segments, info = model.transcribe(
                str(chunk_path),
                language="ja",
                beam_size=config.beam_size,
                patience=config.patience,
                temperature=config.temperature,
                repetition_penalty=config.repetition_penalty,
                no_repeat_ngram_size=config.no_repeat_ngram_size,
                no_speech_threshold=config.no_speech_threshold,
                log_prob_threshold=config.log_prob_threshold,
                condition_on_previous_text=config.condition_on_previous_text,
                vad_filter=config.vad_filter,
                vad_parameters=config.vad_parameters,
                initial_prompt=config.initial_prompt,
                hotwords=config.hotwords,
                word_timestamps=config.word_timestamps,
            )
            for segment in raw_segments:
                text = segment.text.strip()
                if not text:
                    continue
                start = timestamp_offset + chunk_start + segment.start
                end = timestamp_offset + chunk_start + segment.end
                cues.append(Cue(len(cues) + 1, start, end, text))
                telemetry.append(
                    {
                        "start": start,
                        "end": end,
                        "text": text,
                        "avg_logprob": segment.avg_logprob,
                        "no_speech_prob": segment.no_speech_prob,
                        "compression_ratio": segment.compression_ratio,
                        "chunk_index": chunk_index,
                    }
                )
            chunk_start += chunk_duration
            chunk_index += 1
    elapsed = time.perf_counter() - started
    cues.sort(key=lambda cue: (cue.start, cue.end))
    _write_srt(cues, srt_path)
    metrics = compute_metrics(
        cues,
        window_start=timestamp_offset,
        window_end=timestamp_offset + duration,
    )
    manifest = {
        "audio_path": str(audio_path),
        "audio_duration_seconds": duration,
        "timestamp_offset_seconds": timestamp_offset,
        "elapsed_seconds": elapsed,
        "faster_whisper_version": faster_whisper.__version__,
        "config": asdict(config),
        "metrics": asdict(metrics),
        "segments": telemetry,
        "srt_path": str(srt_path),
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return srt_path, manifest_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audio", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--compute-type", default="float16")
    parser.add_argument("--timestamp-offset", type=float, default=0.0)
    parser.add_argument("--chunk-seconds", type=int, default=900)
    parser.add_argument("--beam-size", type=int, default=5)
    parser.add_argument("--patience", type=float, default=1.0)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--repetition-penalty", type=float, default=1.0)
    parser.add_argument("--no-repeat-ngram-size", type=int, default=0)
    parser.add_argument("--no-speech-threshold", type=float, default=0.6)
    parser.add_argument("--log-prob-threshold", type=float, default=-1.0)
    parser.add_argument(
        "--condition-on-previous-text",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--vad-mode",
        choices=("off", "default", "custom"),
        default="default",
    )
    parser.add_argument("--vad-threshold", type=float, default=0.5)
    parser.add_argument("--vad-min-speech-ms", type=int, default=0)
    parser.add_argument("--vad-min-silence-ms", type=int, default=2000)
    parser.add_argument("--vad-speech-pad-ms", type=int, default=400)
    parser.add_argument("--initial-prompt")
    parser.add_argument("--hotwords")
    parser.add_argument("--word-timestamps", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    config = BenchmarkConfig(
        model=args.model,
        device=args.device,
        compute_type=args.compute_type,
        chunk_seconds=args.chunk_seconds,
        beam_size=args.beam_size,
        patience=args.patience,
        temperature=args.temperature,
        repetition_penalty=args.repetition_penalty,
        no_repeat_ngram_size=args.no_repeat_ngram_size,
        no_speech_threshold=args.no_speech_threshold,
        log_prob_threshold=args.log_prob_threshold,
        condition_on_previous_text=args.condition_on_previous_text,
        vad_mode=args.vad_mode,
        vad_threshold=args.vad_threshold,
        vad_min_speech_ms=args.vad_min_speech_ms,
        vad_min_silence_ms=args.vad_min_silence_ms,
        vad_speech_pad_ms=args.vad_speech_pad_ms,
        initial_prompt=args.initial_prompt,
        hotwords=args.hotwords,
        word_timestamps=args.word_timestamps,
    )
    srt_path, manifest_path = run_benchmark(
        args.audio,
        args.output_dir,
        config,
        timestamp_offset=args.timestamp_offset,
        force=args.force,
    )
    print(srt_path)
    print(manifest_path)


if __name__ == "__main__":
    main()
