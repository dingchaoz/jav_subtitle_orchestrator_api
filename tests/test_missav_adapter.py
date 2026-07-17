import json
import subprocess
from pathlib import Path

import pytest

from orchestrator.missav_adapter import (
    DownloadDeferredError,
    MissAVAdapter,
    SourceNoAudioError,
)


def _fake_pipeline_root(tmp_path: Path) -> Path:
    root = tmp_path / "MissAV-Pipeline"
    new_release = root / "new-release"
    new_release.mkdir(parents=True)
    pipeline_python = root / ".venv" / "bin" / "python"
    pipeline_python.parent.mkdir(parents=True)
    pipeline_python.write_text("# fake python\n", encoding="utf-8")
    (new_release / "unified_download.py").write_text("# fake\n", encoding="utf-8")
    (new_release / "batch_audio_downloader.py").write_text("# fake\n", encoding="utf-8")
    return root


def test_download_metadata_writes_matching_movie_record(monkeypatch, tmp_path):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    output_path = tmp_path / "jobs" / "ktb-096" / "metadata.json"

    def fake_run(command, **kwargs):
        assert command[0] == str(pipeline_root / ".venv" / "bin" / "python")
        assert command[1] == str(pipeline_root / "new-release" / "unified_download.py")
        assert kwargs["cwd"] == pipeline_root
        catalog_path.write_text(
            json.dumps(
                {
                    "movies": [
                        {"number": "abc-001", "title": "Wrong movie"},
                        {"number": "KTB-096", "title": "Requested movie", "link": "https://missav/ws"},
                    ]
                }
            )
            + "\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    MissAVAdapter(pipeline_root).download_metadata("ktb-096", output_path)

    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "number": "KTB-096",
        "title": "Requested movie",
        "link": "https://missav/ws",
    }


def test_adapter_uses_explicit_python_executable(monkeypatch, tmp_path):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    output_path = tmp_path / "jobs" / "ktb-096" / "metadata.json"
    explicit_python = tmp_path / "python-for-missav"
    explicit_python.write_text("# explicit python\n", encoding="utf-8")

    def fake_run(command, **kwargs):
        assert command[0] == str(explicit_python)
        catalog_path.write_text(
            json.dumps({"movies": [{"number": "ktb-096", "title": "Requested movie"}]}) + "\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    MissAVAdapter(pipeline_root, python_executable=explicit_python).download_metadata(
        "ktb-096",
        output_path,
    )

    assert json.loads(output_path.read_text(encoding="utf-8"))["number"] == "ktb-096"


def test_download_metadata_uses_existing_catalog_without_refresh(monkeypatch, tmp_path):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    output_path = tmp_path / "jobs" / "ktb-096" / "metadata.json"
    catalog_path.write_text(
        json.dumps({"movies": [{"number": "ktb-096", "title": "Already cached"}]}) + "\n",
        encoding="utf-8",
    )

    def fail_if_run(command, **kwargs):
        raise AssertionError("cached metadata should not refresh MissAV catalog")

    monkeypatch.setattr(subprocess, "run", fail_if_run)

    MissAVAdapter(pipeline_root).download_metadata("ktb-096", output_path)

    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "number": "ktb-096",
        "title": "Already cached",
    }


def test_download_metadata_raises_when_requested_movie_missing(monkeypatch, tmp_path):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    output_path = tmp_path / "jobs" / "ktb-096" / "metadata.json"

    def fake_run(command, **kwargs):
        assert command[0] == str(pipeline_root / ".venv" / "bin" / "python")
        catalog_path.write_text(
            json.dumps({"movies": [{"number": "abc-001", "title": "Wrong movie"}]}) + "\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(FileNotFoundError, match="ktb-096"):
        MissAVAdapter(pipeline_root).download_metadata("ktb-096", output_path)

    assert not output_path.exists()


def test_download_audio_queues_requested_movie_and_moves_produced_audio(
    monkeypatch,
    tmp_path,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    output_path = tmp_path / "jobs" / "ktb-096" / "audio.wav"
    metadata_path = output_path.parent / "metadata.json"
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text(
        json.dumps(
            {
                "number": "ktb-096",
                "title": "Requested movie",
                "link": "https://missav/ws/ktb-096",
                "duration": "1:00:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_run(command, **kwargs):
        assert command[0] == str(pipeline_root / ".venv" / "bin" / "python")
        assert command[1] == str(pipeline_root / "new-release" / "batch_audio_downloader.py")
        assert kwargs["cwd"] != pipeline_root
        json_file = Path(command[command.index("--json-file") + 1])
        queue_file = Path(command[command.index("--queue-file") + 1])
        output_dir = Path(command[command.index("--output-dir") + 1])
        log_file = Path(command[command.index("--log-file") + 1])
        assert json_file.is_absolute()
        assert queue_file.is_absolute()
        assert log_file.is_absolute()
        assert output_dir.is_absolute()
        assert output_dir == output_path.parent
        assert command[command.index("--max-downloads") + 1] == "1"
        assert "--only-pending" in command
        assert "--direct-audio" in command

        catalog = json.loads(json_file.read_text(encoding="utf-8"))
        queue = json.loads(queue_file.read_text(encoding="utf-8"))
        assert [movie["number"] for movie in catalog["movies"]] == ["ktb-096"]
        assert [movie["number"] for movie in queue["pending"]] == ["ktb-096"]

        produced = output_dir / "audio" / "ktb-096.wav"
        produced.parent.mkdir(parents=True)
        produced.write_bytes(b"RIFFrequestedWAVE")
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    MissAVAdapter(pipeline_root).download_audio("ktb-096", output_path)

    assert output_path.read_bytes() == b"RIFFrequestedWAVE"
    assert not (output_path.parent / "audio" / "ktb-096.wav").exists()


def test_download_audio_classifies_low_disk_pause_as_deferred(monkeypatch, tmp_path):
    pipeline_root = _fake_pipeline_root(tmp_path)
    output_path = tmp_path / "jobs" / "ktb-096" / "audio.wav"

    def fake_run(command, **kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="Free space below threshold. Pausing before next download...\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(DownloadDeferredError, match="low disk space"):
        MissAVAdapter(pipeline_root).download_audio("ktb-096", output_path)


def test_download_audio_cleans_stale_tmp_and_prioritizes_low_disk_pause(
    monkeypatch,
    tmp_path,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    output_path = tmp_path / "jobs" / "ktb-096" / "audio.wav"
    output_path.parent.mkdir(parents=True)
    output_path.write_bytes(b"canonical audio")
    staging_path = output_path.with_suffix(".wav.tmp")
    staging_path.write_bytes(b"stale partial audio")

    def fake_run(command, **kwargs):
        assert not staging_path.exists()
        staging_path.write_bytes(b"fresh partial audio")
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="Free space below threshold. Pausing before next download...\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(DownloadDeferredError, match="low disk space"):
        MissAVAdapter(pipeline_root).download_audio("ktb-096", output_path)

    assert output_path.read_bytes() == b"canonical audio"


def test_download_audio_classifies_retryable_pipeline_failure_as_deferred(
    monkeypatch,
    tmp_path,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    output_path = tmp_path / "jobs" / "ktb-096" / "audio.wav"

    def fake_run(command, **kwargs):
        log_file = Path(command[command.index("--log-file") + 1])
        log_file.write_text(
            json.dumps(
                {
                    "failed": {
                        "ktb-096": {
                            "error": "page_http_403",
                            "attempts": 1,
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="failed\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(DownloadDeferredError, match="page_http_403"):
        MissAVAdapter(pipeline_root).download_audio("ktb-096", output_path)


def test_download_audio_preserves_nonretryable_pipeline_failure_detail(
    monkeypatch,
    tmp_path,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    output_path = tmp_path / "jobs" / "ktb-096" / "audio.wav"

    def fake_run(command, **kwargs):
        log_file = Path(command[command.index("--log-file") + 1])
        log_file.write_text(
            json.dumps(
                {
                    "failed": {
                        "ktb-096": {
                            "error": "stream URL unavailable",
                            "attempts": 1,
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="failed\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(FileNotFoundError, match="stream URL unavailable"):
        MissAVAdapter(pipeline_root).download_audio("ktb-096", output_path)


def test_download_audio_cleans_stale_candidate_and_prioritizes_no_audio_log(
    monkeypatch,
    tmp_path,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    catalog_path.write_text(json.dumps({"movies": []}) + "\n", encoding="utf-8")
    output_path = tmp_path / "jobs" / "mfyd-123" / "audio.wav"
    staging_path = output_path.parent / "audio" / "mfyd-123.wav"
    staging_path.parent.mkdir(parents=True)
    staging_path.write_bytes(b"stale candidate audio")

    def fake_run(command, **kwargs):
        assert not staging_path.exists()
        staging_path.write_bytes(b"fresh partial audio")
        log_file = Path(command[command.index("--log-file") + 1])
        log_file.write_text(
            json.dumps(
                {"failed": {"mfyd-123": {"error": "source_no_audio: mfyd-123"}}}
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(SourceNoAudioError, match="source_no_audio"):
        MissAVAdapter(pipeline_root).download_audio("mfyd-123", output_path)

    assert not output_path.exists()


def test_download_audio_cleans_stale_fallback_candidate_before_attempt(
    monkeypatch,
    tmp_path,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    catalog_path.write_text(
        json.dumps({"movies": [{"number": "mfyd-123-uncensored-leak"}]}) + "\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "jobs" / "mfyd-123" / "audio.wav"
    fallback_staging_path = (
        output_path.parent / "audio" / "mfyd-123-uncensored-leak.wav"
    )
    fallback_staging_path.parent.mkdir(parents=True)
    fallback_staging_path.write_bytes(b"stale fallback audio")
    attempted = []

    def fake_run(command, **kwargs):
        queue_file = Path(command[command.index("--queue-file") + 1])
        log_file = Path(command[command.index("--log-file") + 1])
        queued_number = json.loads(queue_file.read_text(encoding="utf-8"))["pending"][0][
            "number"
        ]
        attempted.append(queued_number)
        if queued_number == "mfyd-123-uncensored-leak":
            assert not fallback_staging_path.exists()
            fallback_staging_path.write_bytes(b"fresh fallback partial")
        log_file.write_text(
            json.dumps(
                {
                    "failed": {
                        "mfyd-123": {
                            "error": f"source_no_audio: {queued_number}",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(SourceNoAudioError, match="source_no_audio"):
        MissAVAdapter(pipeline_root).download_audio("mfyd-123", output_path)

    assert attempted == ["mfyd-123", "mfyd-123-uncensored-leak"]
    assert not output_path.exists()


def test_download_audio_rejects_empty_produced_audio(monkeypatch, tmp_path):
    pipeline_root = _fake_pipeline_root(tmp_path)
    output_path = tmp_path / "jobs" / "ktb-096" / "audio.wav"

    def fake_run(command, **kwargs):
        output_dir = Path(command[command.index("--output-dir") + 1])
        produced = output_dir / "audio" / "ktb-096.wav"
        produced.parent.mkdir(parents=True)
        produced.write_bytes(b"")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(FileNotFoundError, match="Downloaded audio for ktb-096 not found"):
        MissAVAdapter(pipeline_root).download_audio("ktb-096", output_path)

    assert not output_path.exists()


def test_download_audio_falls_back_to_catalog_variant_when_primary_has_no_audio(
    monkeypatch,
    tmp_path,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    catalog_path.write_text(
        json.dumps(
            {
                "movies": [
                    {
                        "number": "mfyd-123-uncensored-leak",
                        "title": "Alternate with audio",
                        "link": "https://missav.example/mfyd-123-uncensored-leak",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "jobs" / "mfyd-123" / "audio.wav"
    attempted = []

    def fake_run(command, **kwargs):
        queue_file = Path(command[command.index("--queue-file") + 1])
        log_file = Path(command[command.index("--log-file") + 1])
        output_dir = Path(command[command.index("--output-dir") + 1])
        queued_number = json.loads(queue_file.read_text(encoding="utf-8"))["pending"][0][
            "number"
        ]
        attempted.append(queued_number)
        if queued_number == "mfyd-123":
            log_file.write_text(
                json.dumps(
                    {
                        "failed": {
                            "mfyd-123": {
                                "error": "source_no_audio: mfyd-123",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
        else:
            produced = output_dir / "audio" / "mfyd-123-uncensored-leak.wav"
            produced.parent.mkdir(parents=True, exist_ok=True)
            produced.write_bytes(b"alternate audio bytes")
        return subprocess.CompletedProcess(command, 0, stdout="pipeline output", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    MissAVAdapter(pipeline_root).download_audio("mfyd-123", output_path)

    assert attempted == ["mfyd-123", "mfyd-123-uncensored-leak"]
    assert output_path.read_bytes() == b"alternate audio bytes"
    assert not (output_path.parent / "audio" / "mfyd-123-uncensored-leak.wav").exists()
    job_log = (output_path.parent / "logs" / "mac-download.log").read_text(
        encoding="utf-8"
    )
    assert "source_no_audio mfyd-123" in job_log
    assert "source_fallback mfyd-123 -> mfyd-123-uncensored-leak" in job_log
    assert "source_fallback_success mfyd-123 -> mfyd-123-uncensored-leak" in job_log
    assert "pipeline output" not in job_log
    assert "https://" not in job_log


def test_download_audio_never_uses_unrelated_catalog_variant(monkeypatch, tmp_path):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    catalog_path.write_text(
        json.dumps(
            {
                "movies": [
                    {
                        "number": "other-999-uncensored-leak",
                        "title": "Unrelated movie",
                    }
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "jobs" / "mfyd-123" / "audio.wav"
    attempted = []

    def fake_run(command, **kwargs):
        queue_file = Path(command[command.index("--queue-file") + 1])
        log_file = Path(command[command.index("--log-file") + 1])
        queued_number = json.loads(queue_file.read_text(encoding="utf-8"))["pending"][0][
            "number"
        ]
        attempted.append(queued_number)
        log_file.write_text(
            json.dumps(
                {
                    "failed": {
                        "mfyd-123": {
                            "error": "source_no_audio: mfyd-123",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(SourceNoAudioError) as exc_info:
        MissAVAdapter(pipeline_root).download_audio("mfyd-123", output_path)

    assert attempted == ["mfyd-123"]
    assert "source_no_audio" in str(exc_info.value)
    assert "mfyd-123" in str(exc_info.value)
    assert "other-999-uncensored-leak" not in str(exc_info.value)


def test_download_audio_tries_same_base_variants_in_suffix_order(monkeypatch, tmp_path):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    catalog_path.write_text(
        json.dumps(
            {
                "movies": [
                    {"number": "mfyd-123-english-subtitle"},
                    {"number": "mfyd-123-uncensored"},
                    {"number": "mfyd-123-uncensored-leak"},
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "jobs" / "mfyd-123" / "audio.wav"
    attempted = []

    def fake_run(command, **kwargs):
        queue_file = Path(command[command.index("--queue-file") + 1])
        log_file = Path(command[command.index("--log-file") + 1])
        queued_number = json.loads(queue_file.read_text(encoding="utf-8"))["pending"][0][
            "number"
        ]
        attempted.append(queued_number)
        log_file.write_text(
            json.dumps(
                {
                    "failed": {
                        "mfyd-123": {
                            "error": f"source_no_audio: {queued_number}",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(SourceNoAudioError) as exc_info:
        MissAVAdapter(pipeline_root).download_audio("mfyd-123", output_path)

    expected_attempts = [
        "mfyd-123",
        "mfyd-123-uncensored-leak",
        "mfyd-123-uncensored",
        "mfyd-123-english-subtitle",
    ]
    assert attempted == expected_attempts
    for movie_number in expected_attempts:
        assert movie_number in str(exc_info.value)


def test_download_audio_tries_unsuffixed_same_base_catalog_entry_last(
    monkeypatch,
    tmp_path,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    catalog_path.write_text(
        json.dumps(
            {
                "movies": [
                    {"number": "mfyd-123"},
                    {"number": "mfyd-123-english-subtitle"},
                    {"number": "mfyd-123-uncensored-leak"},
                ]
            }
        )
        + "\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "jobs" / "mfyd-123-uncensored" / "audio.wav"
    attempted = []

    def fake_run(command, **kwargs):
        queue_file = Path(command[command.index("--queue-file") + 1])
        log_file = Path(command[command.index("--log-file") + 1])
        output_dir = Path(command[command.index("--output-dir") + 1])
        queued_number = json.loads(queue_file.read_text(encoding="utf-8"))["pending"][0][
            "number"
        ]
        attempted.append(queued_number)
        if queued_number == "mfyd-123":
            produced = output_dir / "audio" / "mfyd-123.wav"
            produced.parent.mkdir(parents=True, exist_ok=True)
            produced.write_bytes(b"unsuffixed alternate")
        else:
            log_file.write_text(
                json.dumps(
                    {
                        "failed": {
                            "mfyd-123": {
                                "error": f"source_no_audio: {queued_number}",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    MissAVAdapter(pipeline_root).download_audio("mfyd-123-uncensored", output_path)

    assert attempted == [
        "mfyd-123-uncensored",
        "mfyd-123-uncensored-leak",
        "mfyd-123-english-subtitle",
        "mfyd-123",
    ]
    assert output_path.read_bytes() == b"unsuffixed alternate"


def test_download_audio_preserves_non_no_audio_error_from_fallback(
    monkeypatch,
    tmp_path,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    catalog_path.write_text(
        json.dumps({"movies": [{"number": "mfyd-123-uncensored-leak"}]}) + "\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "jobs" / "mfyd-123" / "audio.wav"
    attempted = []

    def fake_run(command, **kwargs):
        queue_file = Path(command[command.index("--queue-file") + 1])
        log_file = Path(command[command.index("--log-file") + 1])
        queued_number = json.loads(queue_file.read_text(encoding="utf-8"))["pending"][0][
            "number"
        ]
        attempted.append(queued_number)
        detail = (
            "source_no_audio: mfyd-123"
            if queued_number == "mfyd-123"
            else "page_http_403"
        )
        log_file.write_text(
            json.dumps({"failed": {"mfyd-123": {"error": detail}}}),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(DownloadDeferredError, match="page_http_403"):
        MissAVAdapter(pipeline_root).download_audio("mfyd-123", output_path)

    assert attempted == ["mfyd-123", "mfyd-123-uncensored-leak"]


@pytest.mark.parametrize(
    "detail",
    [
        "Output file does not contain any stream",
        "Stream map '0:a' matches no streams",
        "Input does not contain any audio stream",
    ],
)
def test_download_audio_classifies_ffmpeg_no_audio_details(
    monkeypatch,
    tmp_path,
    detail,
):
    pipeline_root = _fake_pipeline_root(tmp_path)
    catalog_path = pipeline_root / "new-release" / "release_movies_complete.json"
    catalog_path.write_text(json.dumps({"movies": []}) + "\n", encoding="utf-8")
    output_path = tmp_path / "jobs" / "mfyd-123" / "audio.wav"

    def fake_run(command, **kwargs):
        log_file = Path(command[command.index("--log-file") + 1])
        log_file.write_text(
            json.dumps({"failed": {"mfyd-123": {"error": detail}}}),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(SourceNoAudioError, match="source_no_audio"):
        MissAVAdapter(pipeline_root).download_audio("mfyd-123", output_path)
