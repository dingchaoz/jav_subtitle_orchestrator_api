import pytest

from orchestrator.site_audio.cli import build_parser, main
from orchestrator.site_audio.errors import SourceUnavailable


def test_parser_exposes_documented_download_arguments(tmp_path):
    profile = tmp_path / "profile"
    output = tmp_path / "movie.wav"

    args = build_parser().parse_args(
        [
            "https://jable.tv/videos/abf-367/",
            "--output",
            str(output),
            "--profile-dir",
            str(profile),
            "--browser-timeout",
            "45",
            "--headless",
            "--overwrite",
        ]
    )

    assert args.url.endswith("/abf-367/")
    assert args.output == output
    assert args.profile_dir == profile
    assert args.browser_timeout == 45
    assert args.headless is True
    assert args.overwrite is True


def test_main_prints_absolute_output_after_success(tmp_path, capsys):
    output = tmp_path / "audio.wav"
    calls = []

    class FakePipeline:
        def download(self, url, output_path, *, overwrite):
            calls.append((url, output_path, overwrite))
            return output_path

    result = main(
        ["https://jable.tv/videos/abf-367/", "-o", str(output)],
        pipeline_factory=lambda options: FakePipeline(),
    )

    assert result == 0
    assert calls == [("https://jable.tv/videos/abf-367/", output, False)]
    assert capsys.readouterr().out.strip() == str(output.resolve())


def test_main_maps_pipeline_errors_to_documented_exit_code(tmp_path, capsys):
    class FailingPipeline:
        def download(self, url, output_path, *, overwrite):
            raise SourceUnavailable("streaming service is unavailable")

    result = main(
        ["https://www.bestjavporn.com/video/unavailable/", "-o", str(tmp_path / "x.wav")],
        pipeline_factory=lambda options: FailingPipeline(),
    )

    assert result == 3
    assert "streaming service is unavailable" in capsys.readouterr().err


def test_main_refuses_existing_output_without_overwrite(tmp_path, capsys):
    output = tmp_path / "existing.wav"
    output.write_bytes(b"already here")

    result = main(
        ["https://jable.tv/videos/abf-367/", "-o", str(output)],
        pipeline_factory=lambda options: pytest.fail("pipeline must not be constructed"),
    )

    assert result == 2
    assert "already exists" in capsys.readouterr().err
