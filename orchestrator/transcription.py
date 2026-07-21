import shutil
import subprocess
import sys
import unicodedata
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from math import floor
from pathlib import Path
from tempfile import TemporaryDirectory


@dataclass(frozen=True)
class Segment:
    start: float
    end: float
    text: str


@dataclass(frozen=True)
class TimeWindow:
    start: float
    end: float


@dataclass(frozen=True)
class TranscriptionReport:
    audio_duration_seconds: float | None
    primary_segment_count: int
    repair_window_count: int
    repair_grid_a_segment_count: int
    repair_grid_b_segment_count: int
    confirmed_segment_count: int
    final_segment_count: int
    final_max_gap_seconds: float | None

    def as_dict(self) -> dict[str, int | float | None]:
        return asdict(self)


PHANTOM_TRANSCRIPT_TEXTS = {
    "ご視聴ありがとうございました",
    "ありがとうございました",
    "おはようございます",
    "おやすみなさい",
    "さようなら",
    "おわり",
}


def normalize_transcript_text(text: str) -> str:
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


def transcript_text_similarity(left: str, right: str) -> float:
    left_normalized = normalize_transcript_text(left)
    right_normalized = normalize_transcript_text(right)
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


def is_definite_hallucination(segment: Segment) -> bool:
    normalized = normalize_transcript_text(segment.text)
    if not normalized:
        return True
    duration = segment.end - segment.start
    if duration >= 10 and normalized in PHANTOM_TRANSCRIPT_TEXTS:
        return True
    return len(normalized) >= 12 and len(set(normalized)) / len(normalized) <= 0.25


def is_repair_trigger(segment: Segment) -> bool:
    if is_definite_hallucination(segment):
        return True
    normalized = normalize_transcript_text(segment.text)
    duration = segment.end - segment.start
    return duration >= 20 and len(normalized) / duration < 0.5


def find_repair_windows(
    segments: list[Segment],
    *,
    duration: float,
    gap_seconds: float,
    padding_seconds: float,
) -> list[TimeWindow]:
    trusted = sorted(
        (segment for segment in segments if not is_repair_trigger(segment)),
        key=lambda segment: (segment.start, segment.end),
    )
    raw_windows: list[TimeWindow] = []
    cursor = 0.0
    for segment in trusted:
        start = max(0.0, min(duration, segment.start))
        end = max(start, min(duration, segment.end))
        if start - cursor >= gap_seconds:
            raw_windows.append(
                TimeWindow(
                    start=max(0.0, cursor - padding_seconds),
                    end=min(duration, start + padding_seconds),
                )
            )
        cursor = max(cursor, end)
    if duration - cursor >= gap_seconds:
        raw_windows.append(
            TimeWindow(
                start=max(0.0, cursor - padding_seconds),
                end=duration,
            )
        )

    merged: list[TimeWindow] = []
    for window in raw_windows:
        if not merged or window.start > merged[-1].end:
            merged.append(window)
        else:
            previous = merged[-1]
            merged[-1] = TimeWindow(previous.start, max(previous.end, window.end))
    return merged


def repair_grid_chunk_starts(
    *,
    window_start: float,
    window_end: float,
    duration: float,
    chunk_seconds: float,
    grid_offset_seconds: float,
) -> list[float]:
    if window_end <= window_start or duration <= 0 or chunk_seconds <= 0:
        return []
    first_index = floor((window_start - grid_offset_seconds) / chunk_seconds)
    start = grid_offset_seconds + first_index * chunk_seconds
    while start + chunk_seconds <= 0:
        start += chunk_seconds

    starts: list[float] = []
    while start < window_end and start < duration:
        if start >= 0 and start + chunk_seconds > window_start:
            starts.append(start)
        start += chunk_seconds
    return starts


def _nearby_text_sequences(
    segment: Segment,
    others: list[Segment],
    *,
    margin_seconds: float,
    max_sequence_segments: int = 4,
) -> list[str]:
    nearby = [
        other
        for other in others
        if other.end >= segment.start - margin_seconds
        and other.start <= segment.end + margin_seconds
        and not is_repair_trigger(other)
    ]
    sequences: list[str] = []
    for start_index in range(len(nearby)):
        parts: list[str] = []
        for end_index in range(
            start_index,
            min(len(nearby), start_index + max_sequence_segments),
        ):
            parts.append(nearby[end_index].text)
            sequences.append("".join(parts))
    return sequences


def confirm_repair_segments(
    grid_a: list[Segment],
    grid_b: list[Segment],
    *,
    margin_seconds: float = 5.0,
    minimum_similarity: float = 0.72,
) -> list[Segment]:
    confirmed: list[Segment] = []
    for segment in grid_a:
        if is_repair_trigger(segment):
            continue
        sequences = _nearby_text_sequences(
            segment,
            grid_b,
            margin_seconds=margin_seconds,
        )
        similarity = max(
            (
                transcript_text_similarity(segment.text, candidate)
                for candidate in sequences
            ),
            default=0.0,
        )
        if similarity >= minimum_similarity:
            confirmed.append(segment)
    return confirmed


def merge_transcript_segments(
    primary: list[Segment],
    confirmed: list[Segment],
) -> list[Segment]:
    merged = [
        segment for segment in primary if not is_definite_hallucination(segment)
    ]
    for candidate in confirmed:
        duplicate = any(
            existing.end >= candidate.start - 2
            and existing.start <= candidate.end + 2
            and transcript_text_similarity(existing.text, candidate.text) >= 0.72
            for existing in merged
        )
        if not duplicate:
            merged.append(candidate)
    return sorted(merged, key=lambda segment: (segment.start, segment.end, segment.text))


def _maximum_gap_seconds(segments: list[Segment], duration: float) -> float:
    cursor = 0.0
    maximum_gap = 0.0
    for segment in sorted(segments, key=lambda item: (item.start, item.end)):
        start = max(0.0, min(duration, segment.start))
        end = max(start, min(duration, segment.end))
        maximum_gap = max(maximum_gap, start - cursor)
        cursor = max(cursor, end)
    return max(maximum_gap, duration - cursor)


def srt_timestamp(seconds: float) -> str:
    millis = round(seconds * 1000)
    hours, remainder = divmod(millis, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    secs, millis = divmod(remainder, 1000)
    return f"{hours:02}:{minutes:02}:{secs:02},{millis:03}"


def write_srt(segments: list[Segment], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="\n") as handle:
        for index, segment in enumerate(segments, start=1):
            handle.write(f"{index}\n")
            handle.write(f"{srt_timestamp(segment.start)} --> {srt_timestamp(segment.end)}\n")
            handle.write(segment.text.strip() + "\n\n")
    tmp_path.replace(output_path)


class FasterWhisperTranscriber:
    def __init__(
        self,
        model_name: str,
        device: str,
        compute_type: str,
        chunk_seconds: int = 90,
        gap_repair_enabled: bool = True,
        repair_gap_seconds: float = 60,
        repair_chunk_seconds: float = 30,
        repair_offset_seconds: float = 15,
        repair_padding_seconds: float = 15,
        repair_minimum_similarity: float = 0.72,
    ) -> None:
        if chunk_seconds <= 0:
            raise ValueError("chunk_seconds must be positive")
        if repair_gap_seconds <= 0:
            raise ValueError("repair_gap_seconds must be positive")
        if repair_chunk_seconds <= 0:
            raise ValueError("repair_chunk_seconds must be positive")
        if not 0 <= repair_offset_seconds < repair_chunk_seconds:
            raise ValueError(
                "repair_offset_seconds must be at least zero and less than "
                "repair_chunk_seconds"
            )
        if repair_padding_seconds < 0:
            raise ValueError("repair_padding_seconds must be at least zero")
        if not 0 <= repair_minimum_similarity <= 1:
            raise ValueError("repair_minimum_similarity must be between zero and one")
        self.model_name = model_name
        self.device = device
        self.compute_type = compute_type
        self.chunk_seconds = chunk_seconds
        self.gap_repair_enabled = gap_repair_enabled
        self.repair_gap_seconds = repair_gap_seconds
        self.repair_chunk_seconds = repair_chunk_seconds
        self.repair_offset_seconds = repair_offset_seconds
        self.repair_padding_seconds = repair_padding_seconds
        self.repair_minimum_similarity = repair_minimum_similarity
        self._model = None

    def _load_model(self):
        if self._model is None:
            from faster_whisper import WhisperModel

            self._model = WhisperModel(
                self.model_name,
                device=self.device,
                compute_type=self.compute_type,
            )
        return self._model

    def transcribe_to_srt(
        self,
        audio_path: Path,
        output_path: Path,
    ) -> TranscriptionReport:
        model = self._load_model()
        duration = _probe_audio_duration(audio_path)
        if duration is None:
            primary = self._transcribe_one(model, audio_path, offset_seconds=0.0)
            final = merge_transcript_segments(primary, [])
            write_srt(final, output_path)
            return TranscriptionReport(
                audio_duration_seconds=None,
                primary_segment_count=len(primary),
                repair_window_count=0,
                repair_grid_a_segment_count=0,
                repair_grid_b_segment_count=0,
                confirmed_segment_count=0,
                final_segment_count=len(final),
                final_max_gap_seconds=None,
            )

        if duration > self.chunk_seconds:
            primary = self._transcribe_in_chunks(
                model,
                audio_path,
                duration,
                chunk_seconds=self.chunk_seconds,
            )
        else:
            primary = self._transcribe_one(model, audio_path, offset_seconds=0.0)

        repair_windows = (
            find_repair_windows(
                primary,
                duration=duration,
                gap_seconds=self.repair_gap_seconds,
                padding_seconds=self.repair_padding_seconds,
            )
            if self.gap_repair_enabled
            else []
        )
        grid_a: list[Segment] = []
        grid_b: list[Segment] = []
        confirmed: list[Segment] = []
        if repair_windows:
            grid_a = self._transcribe_repair_grid(
                model,
                audio_path,
                duration,
                repair_windows,
                grid_offset_seconds=0,
            )
            grid_b = self._transcribe_repair_grid(
                model,
                audio_path,
                duration,
                repair_windows,
                grid_offset_seconds=self.repair_offset_seconds,
            )
            confirmed = confirm_repair_segments(
                grid_a,
                grid_b,
                minimum_similarity=self.repair_minimum_similarity,
            )

        final = merge_transcript_segments(primary, confirmed)
        write_srt(final, output_path)
        return TranscriptionReport(
            audio_duration_seconds=duration,
            primary_segment_count=len(primary),
            repair_window_count=len(repair_windows),
            repair_grid_a_segment_count=len(grid_a),
            repair_grid_b_segment_count=len(grid_b),
            confirmed_segment_count=len(confirmed),
            final_segment_count=len(final),
            final_max_gap_seconds=_maximum_gap_seconds(final, duration),
        )

    def _transcribe_in_chunks(
        self,
        model,
        audio_path: Path,
        duration: float,
        *,
        chunk_seconds: float,
    ) -> list[Segment]:
        segments: list[Segment] = []
        with TemporaryDirectory(prefix="chunked-transcribe-") as temp_dir:
            temp_path = Path(temp_dir)
            start = 0.0
            chunk_index = 1
            while start < duration:
                chunk_duration = min(chunk_seconds, duration - start)
                chunk_path = temp_path / f"chunk-{chunk_index:04d}.wav"
                _extract_audio_chunk(audio_path, chunk_path, start, chunk_duration)
                segments.extend(self._transcribe_one(model, chunk_path, offset_seconds=start))
                start += chunk_duration
                chunk_index += 1
        return segments

    def _transcribe_repair_grid(
        self,
        model,
        audio_path: Path,
        duration: float,
        windows: list[TimeWindow],
        *,
        grid_offset_seconds: float,
    ) -> list[Segment]:
        chunk_starts = sorted(
            {
                start
                for window in windows
                for start in repair_grid_chunk_starts(
                    window_start=window.start,
                    window_end=window.end,
                    duration=duration,
                    chunk_seconds=self.repair_chunk_seconds,
                    grid_offset_seconds=grid_offset_seconds,
                )
            }
        )
        segments: list[Segment] = []
        with TemporaryDirectory(prefix="repair-transcribe-") as temp_dir:
            temp_path = Path(temp_dir)
            for chunk_index, start in enumerate(chunk_starts, start=1):
                chunk_duration = min(self.repair_chunk_seconds, duration - start)
                chunk_path = temp_path / f"repair-{chunk_index:04d}.wav"
                _extract_audio_chunk(
                    audio_path,
                    chunk_path,
                    start,
                    chunk_duration,
                )
                segments.extend(
                    self._transcribe_one(
                        model,
                        chunk_path,
                        offset_seconds=start,
                    )
                )
        return sorted(segments, key=lambda segment: (segment.start, segment.end))

    @staticmethod
    def _transcribe_one(model, audio_path: Path, offset_seconds: float) -> list[Segment]:
        raw_segments, _info = model.transcribe(
            str(audio_path),
            language="ja",
            beam_size=5,
            vad_filter=True,
            condition_on_previous_text=False,
        )
        return [
            Segment(
                start=segment.start + offset_seconds,
                end=segment.end + offset_seconds,
                text=segment.text,
            )
            for segment in raw_segments
        ]


def _probe_audio_duration(audio_path: Path) -> float | None:
    completed = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(audio_path),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        return None
    try:
        return float(completed.stdout.strip())
    except ValueError:
        return None


def _extract_audio_chunk(
    audio_path: Path,
    chunk_path: Path,
    start_seconds: float,
    duration_seconds: float,
) -> None:
    command = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{start_seconds:.3f}",
        "-t",
        f"{duration_seconds:.3f}",
        "-i",
        str(audio_path),
        "-ac",
        "1",
        "-ar",
        "16000",
        str(chunk_path),
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(
            "ffmpeg chunk extraction failed: "
            + (completed.stderr or completed.stdout).strip()
        )


class ExternalScriptTranscriber:
    def __init__(
        self,
        script_path: str,
        python_executable: str | None = None,
        model_name: str = "large-v3-turbo",
        device: str = "cuda",
    ) -> None:
        self.script_path = Path(script_path)
        self.python_executable = python_executable or sys.executable
        self.model_name = model_name
        self.device = device

    def transcribe_to_srt(self, audio_path: Path, output_path: Path) -> None:
        if not self.script_path.exists():
            raise FileNotFoundError(f"transcription script not found: {self.script_path}")
        if not audio_path.exists():
            raise FileNotFoundError(f"audio file not found: {audio_path}")

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with TemporaryDirectory(prefix="external-transcribe-") as temp_dir:
            temp_path = Path(temp_dir)
            staged_audio = temp_path / f"{output_path.stem}.wav"
            staged_srt = staged_audio.with_suffix(".srt")
            shutil.copy2(audio_path, staged_audio)

            command = [
                self.python_executable,
                str(self.script_path),
                str(temp_path),
                "--model",
                self.model_name,
                "--language",
                "ja",
                "--device",
                self.device,
            ]
            completed = subprocess.run(command, text=True, capture_output=True, check=False)
            if completed.returncode != 0:
                output = "\n".join(
                    line.strip()
                    for line in (completed.stdout, completed.stderr)
                    if line and line.strip()
                )
                raise RuntimeError(
                    f"transcription script {self.script_path} failed with "
                    f"exit code {completed.returncode}: {output}"
                )

            if not staged_srt.exists():
                raise FileNotFoundError(
                    f"transcription script did not produce {staged_srt}"
                )

            tmp_output = output_path.with_suffix(output_path.suffix + ".tmp")
            shutil.copy2(staged_srt, tmp_output)
            tmp_output.replace(output_path)
