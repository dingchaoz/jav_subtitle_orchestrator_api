import sys
import textwrap

import pytest

from orchestrator.transcription import (
    ExternalScriptTranscriber,
    FasterWhisperTranscriber,
    Segment,
    confirm_repair_segments,
    find_repair_windows,
    is_definite_hallucination,
    is_repair_trigger,
    merge_transcript_segments,
    normalize_transcript_text,
    repair_grid_chunk_starts,
    transcript_text_similarity,
    write_srt,
)


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


def test_normalizes_and_compares_japanese_transcript_text():
    assert normalize_transcript_text(" マスク、取って。") == "マスク取って"
    assert (
        transcript_text_similarity(
            "マスク",
            "マスク取ってもらえませんか",
        )
        == 1.0
    )


def test_detects_definite_hallucinations_and_repair_triggers():
    template = Segment(100, 130, "ご視聴ありがとうございました")
    repeated = Segment(200, 228, "あ" * 100)
    sparse = Segment(300, 325, "我慢できないよ")
    dialogue = Segment(400, 404, "マスク取ってもらえませんか")

    assert is_definite_hallucination(template)
    assert is_definite_hallucination(repeated)
    assert not is_definite_hallucination(sparse)
    assert is_repair_trigger(sparse)
    assert not is_repair_trigger(dialogue)


def test_finds_padded_repair_windows_while_ignoring_suspicious_coverage():
    segments = [
        Segment(10, 20, "最初の台詞"),
        Segment(70, 100, "ご視聴ありがとうございました"),
        Segment(140, 145, "次の台詞"),
        Segment(180, 185, "短い台詞"),
        Segment(250, 255, "最後の台詞"),
    ]

    windows = find_repair_windows(
        segments,
        duration=300,
        gap_seconds=60,
        padding_seconds=15,
    )

    assert [(window.start, window.end) for window in windows] == [
        (5, 155),
        (170, 265),
    ]


def test_repair_grid_chunk_starts_use_global_alignment():
    assert repair_grid_chunk_starts(
        window_start=71,
        window_end=106,
        duration=200,
        chunk_seconds=30,
        grid_offset_seconds=0,
    ) == [60, 90]
    assert repair_grid_chunk_starts(
        window_start=71,
        window_end=106,
        duration=200,
        chunk_seconds=30,
        grid_offset_seconds=15,
    ) == [45, 75, 105]


def test_dual_grid_consensus_handles_different_cue_segmentation():
    grid_a = [
        Segment(1292.61, 1294.61, "顔が見たいんで"),
        Segment(1294.61, 1298.53, "マスク"),
        Segment(1298.53, 1300.53, "取ってもらえませんか"),
        Segment(1320, 1349.98, "ご視聴ありがとうございました"),
    ]
    grid_b = [
        Segment(1275.94, 1294.10, "あのー顔が見たいんで"),
        Segment(1294.10, 1299.91, "マスク取ってもらえませんか"),
    ]

    confirmed = confirm_repair_segments(grid_a, grid_b)

    assert [segment.text for segment in confirmed] == [
        "顔が見たいんで",
        "マスク",
        "取ってもらえませんか",
    ]


def test_merge_adds_confirmed_text_without_duplicates_or_definite_noise():
    primary = [
        Segment(100, 104, "既存の台詞"),
        Segment(200, 230, "おはようございます"),
        Segment(300, 325, "我慢できないよ"),
    ]
    confirmed = [
        Segment(100.2, 104.2, "既存の台詞"),
        Segment(205, 208, "新しい台詞"),
    ]

    merged = merge_transcript_segments(primary, confirmed)

    assert [segment.text for segment in merged] == [
        "既存の台詞",
        "新しい台詞",
        "我慢できないよ",
    ]


def test_faster_whisper_defaults_to_benchmarked_adaptive_settings():
    transcriber = FasterWhisperTranscriber(
        "large-v3-turbo",
        "cuda",
        "float16",
    )

    assert transcriber.chunk_seconds == 90
    assert transcriber.gap_repair_enabled is True
    assert transcriber.repair_gap_seconds == 60
    assert transcriber.repair_chunk_seconds == 30
    assert transcriber.repair_offset_seconds == 15
    assert transcriber.repair_padding_seconds == 15
    assert transcriber.repair_minimum_similarity == 0.72


def test_faster_whisper_rejects_invalid_repair_grid_settings():
    with pytest.raises(ValueError, match="repair_offset_seconds"):
        FasterWhisperTranscriber(
            "model",
            "cpu",
            "int8",
            repair_chunk_seconds=30,
            repair_offset_seconds=30,
        )


def test_faster_whisper_runs_targeted_dual_grid_repair(
    tmp_path,
    monkeypatch,
):
    audio_path = tmp_path / "audio.wav"
    output_path = tmp_path / "movie.Japanese.srt"
    audio_path.write_bytes(b"audio")
    transcriber = FasterWhisperTranscriber(
        "model",
        "cpu",
        "int8",
        repair_padding_seconds=0,
    )
    primary = [
        Segment(0, 10, "最初の台詞"),
        Segment(150, 160, "最後の台詞"),
    ]
    grid_a = [Segment(70, 74, "マスク取ってもらえませんか")]
    grid_b = [Segment(70.2, 74.2, "マスク、取ってもらえませんか")]
    repair_calls = []

    monkeypatch.setattr(
        "orchestrator.transcription._probe_audio_duration",
        lambda _path: 200.0,
    )
    monkeypatch.setattr(transcriber, "_load_model", lambda: object())
    monkeypatch.setattr(
        transcriber,
        "_transcribe_in_chunks",
        lambda _model, _path, _duration, *, chunk_seconds: primary,
    )

    def fake_repair_grid(
        _model,
        _path,
        _duration,
        windows,
        *,
        grid_offset_seconds,
    ):
        repair_calls.append((windows, grid_offset_seconds))
        return grid_a if grid_offset_seconds == 0 else grid_b

    monkeypatch.setattr(transcriber, "_transcribe_repair_grid", fake_repair_grid)

    report = transcriber.transcribe_to_srt(audio_path, output_path)

    assert [offset for _windows, offset in repair_calls] == [0, 15]
    assert [(window.start, window.end) for window in repair_calls[0][0]] == [
        (10, 150),
    ]
    assert "マスク取ってもらえませんか" in output_path.read_text(encoding="utf-8")
    assert report.primary_segment_count == 2
    assert report.repair_window_count == 1
    assert report.confirmed_segment_count == 1
    assert report.final_segment_count == 3


def test_faster_whisper_skips_repair_without_qualifying_gap(tmp_path, monkeypatch):
    audio_path = tmp_path / "audio.wav"
    output_path = tmp_path / "movie.Japanese.srt"
    audio_path.write_bytes(b"audio")
    transcriber = FasterWhisperTranscriber("model", "cpu", "int8")
    primary = [
        Segment(0, 10, "最初の台詞です"),
        Segment(35, 45, "次の台詞です"),
        Segment(75, 85, "続きの台詞です"),
        Segment(110, 120, "最後の台詞です"),
    ]

    monkeypatch.setattr(
        "orchestrator.transcription._probe_audio_duration",
        lambda _path: 120.0,
    )
    monkeypatch.setattr(transcriber, "_load_model", lambda: object())
    monkeypatch.setattr(
        transcriber,
        "_transcribe_in_chunks",
        lambda _model, _path, _duration, *, chunk_seconds: primary,
    )
    monkeypatch.setattr(
        transcriber,
        "_transcribe_repair_grid",
        lambda *_args, **_kwargs: pytest.fail("repair should not run"),
    )

    report = transcriber.transcribe_to_srt(audio_path, output_path)

    assert report.repair_window_count == 0
    assert report.confirmed_segment_count == 0


def test_faster_whisper_does_not_publish_partial_srt_when_repair_fails(
    tmp_path,
    monkeypatch,
):
    audio_path = tmp_path / "audio.wav"
    output_path = tmp_path / "movie.Japanese.srt"
    audio_path.write_bytes(b"audio")
    transcriber = FasterWhisperTranscriber(
        "model",
        "cpu",
        "int8",
        repair_padding_seconds=0,
    )

    monkeypatch.setattr(
        "orchestrator.transcription._probe_audio_duration",
        lambda _path: 200.0,
    )
    monkeypatch.setattr(transcriber, "_load_model", lambda: object())
    monkeypatch.setattr(
        transcriber,
        "_transcribe_in_chunks",
        lambda _model, _path, _duration, *, chunk_seconds: [
            Segment(0, 10, "最初"),
            Segment(150, 160, "最後"),
        ],
    )
    monkeypatch.setattr(
        transcriber,
        "_transcribe_repair_grid",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("repair failed")),
    )

    with pytest.raises(RuntimeError, match="repair failed"):
        transcriber.transcribe_to_srt(audio_path, output_path)

    assert not output_path.exists()


def test_faster_whisper_unknown_duration_uses_single_pass_fallback(
    tmp_path,
    monkeypatch,
):
    audio_path = tmp_path / "audio.wav"
    output_path = tmp_path / "movie.Japanese.srt"
    audio_path.write_bytes(b"audio")
    transcriber = FasterWhisperTranscriber("model", "cpu", "int8")

    monkeypatch.setattr(
        "orchestrator.transcription._probe_audio_duration",
        lambda _path: None,
    )
    monkeypatch.setattr(transcriber, "_load_model", lambda: object())
    monkeypatch.setattr(
        transcriber,
        "_transcribe_one",
        lambda _model, _path, offset_seconds: [
            Segment(offset_seconds, offset_seconds + 2, "台詞")
        ],
    )
    monkeypatch.setattr(
        transcriber,
        "_transcribe_in_chunks",
        lambda *_args, **_kwargs: pytest.fail("chunking should not run"),
    )

    report = transcriber.transcribe_to_srt(audio_path, output_path)

    assert report.audio_duration_seconds is None
    assert report.repair_window_count == 0
    assert "台詞" in output_path.read_text(encoding="utf-8")


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
