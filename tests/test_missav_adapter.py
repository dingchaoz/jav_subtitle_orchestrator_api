import json
import subprocess
from pathlib import Path

import pytest

from orchestrator.missav_adapter import DownloadDeferredError, MissAVAdapter


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
