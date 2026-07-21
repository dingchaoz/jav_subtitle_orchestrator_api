import os
import sys
from pathlib import Path

from scripts.benchmark_whisperjav_ab import (
    benchmark_job,
    compare_to_reference,
    compute_srt_metrics,
    write_reports,
)


def make_fake_whisperjav(tmp_path: Path, *, fail_modes: set[str] | None = None) -> Path:
    fail_modes = fail_modes or set()
    fake_py = tmp_path / "fake_whisperjav.py"
    fake_py.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        f"fail_modes = {sorted(fail_modes)!r}\n"
        "args = sys.argv[1:]\n"
        "mode = args[args.index('--mode') + 1]\n"
        "if mode in fail_modes:\n"
        "    print(f'forced failure for {mode}', file=sys.stderr)\n"
        "    raise SystemExit(7)\n"
        "output_dir = Path(args[args.index('--output-dir') + 1])\n"
        "output_dir.mkdir(parents=True, exist_ok=True)\n"
        "content = '1\\n00:00:00,000 --> 00:00:01,000\\nhello from ' + mode + '\\n\\n'\n"
        "(output_dir / 'audio.ja.whisperjav.srt').write_text(content, encoding='utf-8')\n",
        encoding="utf-8",
    )
    if os.name == "nt":
        fake_cmd = tmp_path / "fake_whisperjav.cmd"
        fake_cmd.write_text(f'@"{sys.executable}" "{fake_py}" %*\n', encoding="utf-8")
        return fake_cmd

    fake_sh = tmp_path / "fake_whisperjav"
    fake_sh.write_text(f'#!/usr/bin/env sh\nexec "{sys.executable}" "{fake_py}" "$@"\n')
    fake_sh.chmod(0o755)
    return fake_sh


def write_job(tmp_path: Path, movie_number: str = "ebwh-335") -> Path:
    job_dir = tmp_path / movie_number
    job_dir.mkdir()
    (job_dir / "audio.wav").write_bytes(b"fake wav")
    (job_dir / f"{movie_number}.Japanese.srt").write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nbaseline ja\n\n",
        encoding="utf-8",
    )
    (job_dir / f"{movie_number}.English.srt").write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nbaseline en\n\n",
        encoding="utf-8",
    )
    return job_dir


def test_srt_metrics_detects_timing_and_text_quality_issues(tmp_path):
    srt = tmp_path / "sample.srt"
    srt.write_text(
        "1\n00:00:00,000 --> 00:00:02,000\nnormal\n\n"
        "2\n00:00:01,000 --> 00:00:03,000\noverlap\n\n"
        "3\n00:00:00,500 --> 00:00:01,000\nregression\n\n"
        "4\n00:00:04,000 --> 00:00:05,000\n \n\n"
        "5\n00:00:05,000 --> 00:00:06,000\naaaaaaaaaa\n\n"
        f"6\n00:00:06,000 --> 00:00:07,000\n{'x' * 91}\n\n",
        encoding="utf-8",
    )

    metrics = compute_srt_metrics(srt, sample_size=2)

    assert metrics.cue_count == 6
    assert metrics.total_span_seconds == 7.0
    assert metrics.cues_per_minute == 51.429
    assert metrics.average_cue_duration_seconds == 1.25
    assert metrics.average_chars_per_cue == 20.667
    assert metrics.overlap_count == 1
    assert metrics.regression_count == 1
    assert metrics.empty_cue_count == 1
    assert metrics.repeated_text_count == 2
    assert metrics.very_long_cue_count == 1
    assert len(metrics.first_cues) == 2
    assert len(metrics.middle_cues) == 2
    assert len(metrics.last_cues) == 2


def test_benchmark_job_reports_missing_audio(tmp_path):
    job_dir = tmp_path / "missing-audio"
    job_dir.mkdir()

    result = benchmark_job(
        job_dir,
        pipelines=["faster"],
        whisperjav_bin=None,
        timeout_seconds=5,
    )

    assert "audio.wav not found" in result["error"]
    assert result["pipelines"] == {}


def test_benchmark_job_reports_missing_whisperjav_without_touching_pipeline(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", "")
    job_dir = write_job(tmp_path)

    result = benchmark_job(
        job_dir,
        pipelines=["faster"],
        whisperjav_bin=str(tmp_path / "missing-whisperjav.exe"),
        timeout_seconds=5,
    )

    assert "WhisperJAV CLI was not found" in result["error"]
    assert result["baseline"]["japanese"]["cue_count"] == 1
    assert result["pipelines"] == {}


def test_benchmark_job_continues_when_one_pipeline_fails(tmp_path):
    job_dir = write_job(tmp_path)
    fake_whisperjav = make_fake_whisperjav(tmp_path, fail_modes={"balanced"})

    result = benchmark_job(
        job_dir,
        pipelines=["faster", "balanced"],
        whisperjav_bin=str(fake_whisperjav),
        timeout_seconds=5,
    )

    assert result["pipelines"]["faster"]["status"] == "ok"
    assert result["pipelines"]["balanced"]["status"] == "failed"
    assert result["pipelines"]["faster"]["baseline_comparison"]["cue_count_delta"] == 0
    output = job_dir / "ebwh-335.Japanese.whisperjav-faster.srt"
    assert output.exists()
    assert "hello from faster" in output.read_text(encoding="utf-8")


def test_write_reports_includes_baseline_and_whisperjav_outputs(tmp_path):
    job_dir = write_job(tmp_path)
    fake_whisperjav = make_fake_whisperjav(tmp_path)
    result = benchmark_job(
        job_dir,
        pipelines=["faster"],
        whisperjav_bin=str(fake_whisperjav),
        timeout_seconds=5,
    )

    report_json, report_md = write_reports([result], tmp_path / "reports")

    assert report_json.exists()
    summary = report_md.read_text(encoding="utf-8")
    assert "baseline" in summary
    assert "whisperjav-faster" in summary
    assert "ebwh-335" in summary


def test_benchmark_job_can_keep_temp_dir_for_debugging(tmp_path):
    job_dir = write_job(tmp_path)
    fake_whisperjav = make_fake_whisperjav(tmp_path)

    result = benchmark_job(
        job_dir,
        pipelines=["faster"],
        whisperjav_bin=str(fake_whisperjav),
        timeout_seconds=5,
        keep_temp=True,
    )

    temp_dir = Path(result["pipelines"]["faster"]["temp_dir"])
    assert temp_dir.exists()
    assert (temp_dir / "out" / "audio.ja.whisperjav.srt").exists()


def test_reference_comparison_reports_cer_and_timing_iou(tmp_path):
    candidate = tmp_path / "candidate.srt"
    reference = tmp_path / "reference.srt"
    candidate.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nこんにちは\n\n",
        encoding="utf-8",
    )
    reference.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nこんにちは\n\n",
        encoding="utf-8",
    )

    comparison = compare_to_reference(candidate, reference)

    assert comparison is not None
    assert comparison["character_error_rate"] == 0
    assert comparison["mean_timing_iou_by_ordinal"] == 1
    assert comparison["matched_cue_count"] == 1
