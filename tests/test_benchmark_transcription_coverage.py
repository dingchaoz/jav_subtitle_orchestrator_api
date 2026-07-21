from pathlib import Path

import pytest

from scripts.benchmark_transcription_coverage import (
    BenchmarkConfig,
    Cue,
    compute_metrics,
    parse_srt,
)


def test_parse_srt_and_compute_union_coverage(tmp_path: Path):
    srt = tmp_path / "sample.srt"
    srt.write_text(
        "1\n"
        "00:00:01,000 --> 00:00:04,000\n"
        "こんにちは\n\n"
        "2\n"
        "00:00:03,000 --> 00:00:05,000\n"
        "はい\n\n"
        "3\n"
        "00:00:10,000 --> 00:00:25,000\n"
        "長い字幕です\n",
        encoding="utf-8",
    )

    cues = parse_srt(srt)
    metrics = compute_metrics(cues, window_start=0.0, window_end=30.0)

    assert cues == [
        Cue(1, 1.0, 4.0, "こんにちは"),
        Cue(2, 3.0, 5.0, "はい"),
        Cue(3, 10.0, 25.0, "長い字幕です"),
    ]
    assert metrics.cue_count == 3
    assert metrics.covered_seconds == pytest.approx(19.0)
    assert metrics.coverage_ratio == pytest.approx(19 / 30)
    assert metrics.max_gap_seconds == pytest.approx(5.0)
    assert metrics.long_cue_over_10_count == 1
    assert metrics.long_cue_over_20_count == 0
    assert metrics.japanese_char_count == 13


def test_compute_metrics_clips_cues_to_requested_window():
    metrics = compute_metrics(
        [
            Cue(1, 0.0, 12.0, "前"),
            Cue(2, 18.0, 30.0, "後"),
        ],
        window_start=10.0,
        window_end=20.0,
    )

    assert metrics.covered_seconds == pytest.approx(4.0)
    assert metrics.max_gap_seconds == pytest.approx(6.0)
    assert metrics.cue_count == 2


def test_benchmark_config_builds_custom_vad_parameters():
    config = BenchmarkConfig(
        model="model",
        vad_mode="custom",
        vad_threshold=0.25,
        vad_min_speech_ms=100,
        vad_min_silence_ms=500,
        vad_speech_pad_ms=600,
    )

    assert config.vad_filter is True
    assert config.vad_parameters == {
        "threshold": 0.25,
        "min_speech_duration_ms": 100,
        "min_silence_duration_ms": 500,
        "speech_pad_ms": 600,
    }


def test_benchmark_config_disables_vad():
    config = BenchmarkConfig(model="model", vad_mode="off")

    assert config.vad_filter is False
    assert config.vad_parameters is None


def test_invalid_vad_mode_is_rejected():
    with pytest.raises(ValueError, match="vad_mode"):
        BenchmarkConfig(model="model", vad_mode="invalid")
