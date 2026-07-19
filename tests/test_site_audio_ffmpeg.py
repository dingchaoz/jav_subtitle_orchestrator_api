import json
import subprocess

import pytest

from orchestrator.site_audio.errors import DownloadFailure
from orchestrator.site_audio.ffmpeg import FFmpegAudioWriter, redact_sensitive_text
from orchestrator.site_audio.models import Provider, ResolvedStream


def _stream(duration=60.0, headers=None, resource_urls=()):
    return ResolvedStream(
        provider=Provider.JABLE,
        page_url="https://jable.tv/videos/movie/",
        manifest_url="https://cdn.example/media.m3u8?token=secret",
        headers=headers
        or {
            "User-Agent": "Browser UA",
            "Referer": "https://jable.tv/videos/movie/",
            "Origin": "https://jable.tv",
            "Cookie": "cdn_session=secret",
            "Authorization": "must-not-be-forwarded",
        },
        expected_duration=duration,
        resource_urls=resource_urls,
    )


def test_writer_uses_restricted_ffmpeg_command_and_atomically_publishes_wav(tmp_path):
    output = tmp_path / "movie.wav"
    commands = []

    def run(command, **kwargs):
        commands.append((command, kwargs))
        if command[0] == "ffmpeg":
            partial = command[-1]
            partial.write_bytes(b"RIFF complete WAVE")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        payload = {
            "streams": [{"codec_name": "pcm_s16le", "sample_rate": "16000", "channels": 1}],
            "format": {"duration": "60.2"},
        }
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload), stderr="")

    result = FFmpegAudioWriter(run=run).write(_stream(), output, overwrite=False)

    assert result == output
    assert output.read_bytes() == b"RIFF complete WAVE"
    assert not (tmp_path / "movie.part.wav").exists()
    ffmpeg_command, kwargs = commands[0]
    assert kwargs == {"text": True, "capture_output": True, "check": False}
    assert ffmpeg_command[0] == "ffmpeg"
    assert ffmpeg_command[ffmpeg_command.index("-protocol_whitelist") + 1] == (
        "https,tls,tcp,crypto"
    )
    assert "http" not in ffmpeg_command[ffmpeg_command.index("-protocol_whitelist") + 1].split(",")
    assert "file" not in ffmpeg_command[ffmpeg_command.index("-protocol_whitelist") + 1].split(",")
    assert ffmpeg_command[ffmpeg_command.index("-map") + 1] == "0:a:0"
    assert ffmpeg_command[ffmpeg_command.index("-ar") + 1] == "16000"
    assert ffmpeg_command[ffmpeg_command.index("-ac") + 1] == "1"
    header_blob = ffmpeg_command[ffmpeg_command.index("-headers") + 1]
    assert "Cookie: cdn_session=secret" in header_blob
    assert "Authorization" not in header_blob
    assert isinstance(ffmpeg_command[-1], type(output))


def test_writer_removes_partial_file_when_ffmpeg_fails(tmp_path):
    output = tmp_path / "movie.wav"

    def run(command, **kwargs):
        partial = command[-1]
        partial.write_bytes(b"partial")
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr="Server returned 403 for https://cdn.example/a.m3u8?token=secret",
        )

    with pytest.raises(DownloadFailure) as caught:
        FFmpegAudioWriter(run=run).write(_stream(), output, overwrite=False)

    assert caught.value.retryable is True
    assert "secret" not in str(caught.value)
    assert not output.exists()
    assert not (tmp_path / "movie.part.wav").exists()


@pytest.mark.parametrize(
    "probe_payload",
    [
        {"streams": [{"codec_name": "aac", "sample_rate": "16000", "channels": 1}], "format": {"duration": "60"}},
        {"streams": [{"codec_name": "pcm_s16le", "sample_rate": "48000", "channels": 1}], "format": {"duration": "60"}},
        {"streams": [{"codec_name": "pcm_s16le", "sample_rate": "16000", "channels": 2}], "format": {"duration": "60"}},
        {"streams": [{"codec_name": "pcm_s16le", "sample_rate": "16000", "channels": 1}], "format": {"duration": "40"}},
    ],
)
def test_writer_rejects_wrong_format_or_incomplete_duration(tmp_path, probe_payload):
    output = tmp_path / "movie.wav"

    def run(command, **kwargs):
        if command[0] == "ffmpeg":
            command[-1].write_bytes(b"RIFF WAVE")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(probe_payload), stderr="")

    with pytest.raises(DownloadFailure):
        FFmpegAudioWriter(run=run).write(_stream(), output, overwrite=False)

    assert not output.exists()
    assert not (tmp_path / "movie.part.wav").exists()


def test_writer_rejects_header_injection_before_running_process(tmp_path):
    stream = _stream(headers={"Referer": "https://example.com/\r\nX-Evil: yes"})

    with pytest.raises(DownloadFailure, match="unsafe media header"):
        FFmpegAudioWriter(run=pytest.fail).write(stream, tmp_path / "movie.wav", overwrite=False)


def test_sensitive_text_redaction_hides_query_and_cookie_values():
    text = (
        "failed https://cdn.example/index.m3u8?token=secret&expires=9\n"
        "Cookie: session=private\nAuthorization: Bearer private"
    )

    redacted = redact_sensitive_text(text)

    assert "secret" not in redacted
    assert "private" not in redacted
    assert "https://cdn.example/index.m3u8?<redacted>" in redacted


def test_writer_maps_missing_ffmpeg_binary_to_typed_failure(tmp_path):
    def run(command, **kwargs):
        raise FileNotFoundError("ffmpeg not installed")

    with pytest.raises(DownloadFailure, match="could not start ffmpeg"):
        FFmpegAudioWriter(run=run).write(_stream(), tmp_path / "movie.wav", overwrite=False)


def test_writer_revalidates_manifest_and_resource_network_targets_before_ffmpeg(tmp_path):
    validated = []

    def run(command, **kwargs):
        if command[0] == "ffmpeg":
            command[-1].write_bytes(b"RIFF WAVE")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        payload = {
            "streams": [{"codec_name": "pcm_s16le", "sample_rate": "16000", "channels": 1}],
            "format": {"duration": "60"},
        }
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload), stderr="")

    stream = _stream(resource_urls=("https://cdn.example/key", "https://cdn.example/seg.ts"))
    FFmpegAudioWriter(run=run, validate_url=validated.append).write(
        stream,
        tmp_path / "movie.wav",
        overwrite=False,
    )

    assert validated == [stream.manifest_url, *stream.resource_urls]
