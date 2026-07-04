from pathlib import Path

from orchestrator.config import MacSettings, WindowsSettings


def test_mac_settings_defaults_match_design_spec(monkeypatch, tmp_path):
    db_path = tmp_path / "jobs.sqlite3"
    monkeypatch.setenv("ORCHESTRATOR_DB_PATH", str(db_path))
    monkeypatch.setenv("MISSAV_PIPELINE_ROOT", "/Users/ytt/Documents/startup/MissAV-Pipeline")
    monkeypatch.setenv("JOBS_ROOT_MAC", "/Users/ytt/MissAVJobs")
    monkeypatch.setenv("JOBS_ROOT_WINDOWS", "M:\\")

    settings = MacSettings()

    assert settings.host == "0.0.0.0"
    assert settings.port == 8000
    assert settings.db_path == db_path
    assert settings.missav_pipeline_root == Path("/Users/ytt/Documents/startup/MissAV-Pipeline")
    assert settings.jobs_root_mac == Path("/Users/ytt/MissAVJobs")
    assert settings.jobs_root_windows == "M:\\"
    assert settings.mac_download_concurrency == 1
    assert settings.worker_lease_seconds == 1800
    assert settings.max_download_attempts == 3
    assert settings.max_worker_attempts == 3


def test_windows_settings_defaults_match_design_spec(monkeypatch):
    monkeypatch.setenv("MAC_API_BASE_URL", "http://192.168.1.25:8000")
    monkeypatch.setenv("WORKER_ID", "windows-gpu-1")
    monkeypatch.setenv("WINDOWS_JOBS_ROOT", "M:\\")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv(
        "TRANSLATE_SCRIPT_PATH",
        "C:\\Users\\ytt\\Documents\\startup\\E2E-download-subtitle-generation-translation-scripts\\scripts\\subtitle_translate.py",
    )

    settings = WindowsSettings()

    assert settings.mac_api_base_url == "http://192.168.1.25:8000"
    assert settings.worker_id == "windows-gpu-1"
    assert settings.windows_jobs_root == "M:\\"
    assert settings.whisper_model == "large-v3-turbo"
    assert settings.whisper_device == "cuda"
    assert settings.whisper_compute_type == "float16"
    assert settings.openai_api_key == "test-key"
    assert settings.poll_interval_seconds == 10
    assert settings.heartbeat_interval_seconds == 60
