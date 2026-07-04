from pathlib import Path

from orchestrator.transcription import Segment, write_srt


def test_write_srt_formats_segments_with_japanese_text(tmp_path):
    output = tmp_path / "ktb-096.Japanese.srt"
    segments = [
        Segment(start=0.0, end=1.5, text="こんにちは"),
        Segment(start=61.25, end=62.5, text="テストです"),
    ]

    write_srt(segments, output)

    assert output.read_text(encoding="utf-8") == (
        "1\n"
        "00:00:00,000 --> 00:00:01,500\n"
        "こんにちは\n\n"
        "2\n"
        "00:01:01,250 --> 00:01:02,500\n"
        "テストです\n\n"
    )
