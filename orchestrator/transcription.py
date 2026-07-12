import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory


@dataclass(frozen=True)
class Segment:
    start: float
    end: float
    text: str


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
        chunk_seconds: int = 900,
    ) -> None:
        self.model_name = model_name
        self.device = device
        self.compute_type = compute_type
        self.chunk_seconds = chunk_seconds
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

    def transcribe_to_srt(self, audio_path: Path, output_path: Path) -> None:
        model = self._load_model()
        duration = _probe_audio_duration(audio_path)
        if duration is not None and duration > self.chunk_seconds:
            segments = self._transcribe_in_chunks(model, audio_path, duration)
        else:
            segments = self._transcribe_one(model, audio_path, offset_seconds=0.0)
        write_srt(segments, output_path)

    def _transcribe_in_chunks(self, model, audio_path: Path, duration: float) -> list[Segment]:
        segments: list[Segment] = []
        with TemporaryDirectory(prefix="chunked-transcribe-") as temp_dir:
            temp_path = Path(temp_dir)
            start = 0.0
            chunk_index = 1
            while start < duration:
                chunk_duration = min(float(self.chunk_seconds), duration - start)
                chunk_path = temp_path / f"chunk-{chunk_index:04d}.wav"
                _extract_audio_chunk(audio_path, chunk_path, start, chunk_duration)
                segments.extend(self._transcribe_one(model, chunk_path, offset_seconds=start))
                start += chunk_duration
                chunk_index += 1
        return segments

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
