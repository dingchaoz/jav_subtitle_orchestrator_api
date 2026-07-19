from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Callable

from .errors import DownloadFailure, UnsupportedSiteURL
from .hls import validate_public_https_url
from .models import ResolvedStream


_SAFE_MEDIA_HEADERS = ("User-Agent", "Referer", "Origin", "Cookie")
_RETRYABLE_ERROR_RE = re.compile(
    r"(?:\b(?:401|403|404)\b|forbidden|signature[^\n]*expired|token[^\n]*expired)",
    re.IGNORECASE,
)
_URL_QUERY_RE = re.compile(r"(https://[^\s?]+)\?[^\s]+")
_SECRET_HEADER_RE = re.compile(r"(?im)^(Cookie|Authorization):[^\r\n]*")


def redact_sensitive_text(text: str) -> str:
    redacted = _URL_QUERY_RE.sub(r"\1?<redacted>", text)
    return _SECRET_HEADER_RE.sub(lambda match: f"{match.group(1)}: <redacted>", redacted)


class FFmpegAudioWriter:
    def __init__(
        self,
        *,
        ffmpeg: str = "ffmpeg",
        ffprobe: str = "ffprobe",
        run: Callable = subprocess.run,
        validate_url: Callable[[str], None] = validate_public_https_url,
    ) -> None:
        self.ffmpeg = ffmpeg
        self.ffprobe = ffprobe
        self.run = run
        self.validate_url = validate_url

    def write(
        self,
        stream: ResolvedStream,
        output_path: Path,
        *,
        overwrite: bool,
    ) -> Path:
        output_path = Path(output_path)
        if output_path.exists() and not overwrite:
            raise UnsupportedSiteURL(f"output already exists: {output_path}")
        header_blob = self._header_blob(stream)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        partial_path = output_path.with_name(f"{output_path.stem}.part{output_path.suffix}")
        partial_path.unlink(missing_ok=True)
        try:
            if stream.resource_urls:
                for url in (stream.manifest_url, *stream.resource_urls):
                    self.validate_url(url)
            try:
                completed = self.run(
                    self._ffmpeg_command(stream, partial_path, header_blob),
                    text=True,
                    capture_output=True,
                    check=False,
                )
            except OSError as exc:
                raise DownloadFailure("could not start ffmpeg") from exc
            if completed.returncode != 0:
                detail = redact_sensitive_text(completed.stderr or completed.stdout or "unknown error")
                raise DownloadFailure(
                    f"ffmpeg failed: {detail.strip()}",
                    retryable=bool(_RETRYABLE_ERROR_RE.search(completed.stderr or completed.stdout or "")),
                )
            self._validate_probe(partial_path, stream.expected_duration)
            partial_path.replace(output_path)
            return output_path
        except Exception:
            partial_path.unlink(missing_ok=True)
            raise

    def _header_blob(self, stream: ResolvedStream) -> str:
        lines: list[str] = []
        for name in _SAFE_MEDIA_HEADERS:
            value = stream.headers.get(name)
            if not value:
                continue
            if "\r" in value or "\n" in value:
                raise DownloadFailure("unsafe media header contains a newline")
            lines.append(f"{name}: {value}")
        return "\r\n".join(lines) + ("\r\n" if lines else "")

    def _ffmpeg_command(
        self,
        stream: ResolvedStream,
        partial_path: Path,
        header_blob: str,
    ) -> list[str | Path]:
        command: list[str | Path] = [
            self.ffmpeg,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-y",
            "-reconnect",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_delay_max",
            "10",
            "-protocol_whitelist",
            "https,tls,tcp,crypto",
        ]
        if header_blob:
            command.extend(["-headers", header_blob])
        command.extend(
            [
                "-i",
                stream.manifest_url,
                "-map",
                "0:a:0",
                "-vn",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "pcm_s16le",
                partial_path,
            ]
        )
        return command

    def _validate_probe(self, partial_path: Path, expected_duration: float) -> None:
        command = [
            self.ffprobe,
            "-v",
            "error",
            "-select_streams",
            "a:0",
            "-show_entries",
            "stream=codec_name,sample_rate,channels:format=duration",
            "-of",
            "json",
            partial_path,
        ]
        try:
            completed = self.run(command, text=True, capture_output=True, check=False)
        except OSError as exc:
            raise DownloadFailure("could not start ffprobe") from exc
        if completed.returncode != 0:
            detail = redact_sensitive_text(completed.stderr or "unknown error")
            raise DownloadFailure(f"ffprobe failed: {detail.strip()}")
        try:
            payload = json.loads(completed.stdout)
            audio = payload["streams"][0]
            duration = float(payload["format"]["duration"])
            codec = audio["codec_name"]
            sample_rate = int(audio["sample_rate"])
            channels = int(audio["channels"])
        except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DownloadFailure("ffprobe returned incomplete audio metadata") from exc
        if codec != "pcm_s16le" or sample_rate != 16000 or channels != 1 or duration <= 0:
            raise DownloadFailure("output is not mono 16 kHz PCM s16le WAV")
        tolerance = max(5.0, expected_duration * 0.01)
        if abs(duration - expected_duration) > tolerance:
            raise DownloadFailure(
                f"output duration {duration:.3f}s does not match expected {expected_duration:.3f}s",
                retryable=True,
            )
