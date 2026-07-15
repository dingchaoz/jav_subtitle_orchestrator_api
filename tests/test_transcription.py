import sys
import textwrap
from pathlib import Path

import pytest

from orchestrator.transcription import ExternalScriptTranscriber, Segment, write_srt


def test_write_srt_formats_segments_with_japanese_text(tmp_path):
    output = tmp_path / "ktb-096.Japanese.srt"
    segments = [
        Segment(start=0.0, end=1.5, text="\u3053\u3093\u306b\u3061\u306f"),
        Segment(start=61.25, end=62.5, text="\u30c6\u30b9\u30c8\u3067\u3059"),
    ]

    write_srt(segments, output)

    assert output.read_text(encoding="utf-8") == (
        "1\n"
        "00:00:00,000 --> 00:00:01,500\n"
        "\u3053\u3093\u306b\u3061\u306f\n\n"
        "2\n"
        "00:01:01,250 --> 00:01:02,500\n"
        "\u30c6\u30b9\u30c8\u3067\u3059\n\n"
    )


def test_external_script_transcriber_uses_staged_copy_and_preserves_original_audio(tmp_path):
    audio_path = tmp_path / "audio.wav"
    audio_path.write_bytes(b"fake wav")
    output_path = tmp_path / "ktb-096.Japanese.srt"
    script_path = tmp_path / "batch_transcribe_enhanced.py"
    script_path.write_text(
        textwrap.dedent(
            """
            import pathlib
            import sys

            input_dir = pathlib.Path(sys.argv[1])
            wav_path = next(input_dir.glob("*.wav"))
            wav_path.with_suffix(".srt").write_text("generated subtitle\\n", encoding="utf-8")
            wav_path.unlink()
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    transcriber = ExternalScriptTranscriber(
        str(script_path),
        python_executable=sys.executable,
        model_name="large-v3-turbo",
        device="cuda",
    )

    transcriber.transcribe_to_srt(audio_path, output_path)

    assert audio_path.exists()
    assert output_path.read_text(encoding="utf-8") == "generated subtitle\n"


def test_external_script_transcriber_raises_when_script_fails(tmp_path):
    audio_path = tmp_path / "audio.wav"
    audio_path.write_bytes(b"fake wav")
    output_path = tmp_path / "ktb-096.Japanese.srt"
    script_path = tmp_path / "batch_transcribe_enhanced.py"
    script_path.write_text(
        "import sys\nprint('boom')\nsys.exit(3)\n",
        encoding="utf-8",
    )

    transcriber = ExternalScriptTranscriber(
        str(script_path),
        python_executable=sys.executable,
    )

    with pytest.raises(RuntimeError, match="exit code 3"):
        transcriber.transcribe_to_srt(audio_path, output_path)
