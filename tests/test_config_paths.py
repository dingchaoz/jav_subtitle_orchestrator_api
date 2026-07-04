from pathlib import Path

from orchestrator.config import MacSettings, WindowsSettings
from orchestrator.paths import build_job_paths, normalize_movie_number


MAC_ENV_ALIASES = (
    "ORCHESTRATOR_HOST",
    "ORCHESTRATOR_PORT",
    "ORCHESTRATOR_DB_PATH",
    "MISSAV_PIPELINE_ROOT",
    "JOBS_ROOT_MAC",
    "JOBS_ROOT_WINDOWS",
    "MAC_DOWNLOAD_CONCURRENCY",
    "WORKER_LEASE_SECONDS",
    "MAX_DOWNLOAD_ATTEMPTS",
    "MAX_WORKER_ATTEMPTS",
)

WINDOWS_ENV_ALIASES = (
    "MAC_API_BASE_URL",
    "WORKER_ID",
    "WINDOWS_JOBS_ROOT",
    "WHISPER_MODEL",
    "WHISPER_DEVICE",
    "WHISPER_COMPUTE_TYPE",
    "OPENAI_API_KEY",
    "TRANSLATE_SCRIPT_PATH",
    "POLL_INTERVAL_SECONDS",
    "HEARTBEAT_INTERVAL_SECONDS",
)


def clear_env_aliases(monkeypatch, aliases):
    for alias in aliases:
        monkeypatch.delenv(alias, raising=False)


def test_mac_settings_defaults_match_design_spec(monkeypatch, tmp_path):
    clear_env_aliases(monkeypatch, MAC_ENV_ALIASES)
    db_path = tmp_path / "jobs.sqlite3"
    monkeypatch.setenv("ORCHESTRATOR_DB_PATH", str(db_path))
    monkeypatch.setenv("MISSAV_PIPELINE_ROOT", "/Users/ytt/Documents/startup/MissAV-Pipeline")
    monkeypatch.setenv("JOBS_ROOT_MAC", "/Users/ytt/MissAVJobs")
    monkeypatch.setenv("JOBS_ROOT_WINDOWS", "M:\\")

    settings = MacSettings(_env_file=None)

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
    clear_env_aliases(monkeypatch, WINDOWS_ENV_ALIASES)
    monkeypatch.setenv("MAC_API_BASE_URL", "http://192.168.1.25:8000")
    monkeypatch.setenv("WORKER_ID", "windows-gpu-1")
    monkeypatch.setenv("WINDOWS_JOBS_ROOT", "M:\\")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv(
        "TRANSLATE_SCRIPT_PATH",
        "C:\\Users\\ytt\\Documents\\startup\\E2E-download-subtitle-generation-translation-scripts\\scripts\\subtitle_translate.py",
    )

    settings = WindowsSettings(_env_file=None)

    assert settings.mac_api_base_url == "http://192.168.1.25:8000"
    assert settings.worker_id == "windows-gpu-1"
    assert settings.windows_jobs_root == "M:\\"
    assert settings.whisper_model == "large-v3-turbo"
    assert settings.whisper_device == "cuda"
    assert settings.whisper_compute_type == "float16"
    assert settings.openai_api_key == "test-key"
    assert settings.poll_interval_seconds == 10
    assert settings.heartbeat_interval_seconds == 60


def test_mac_settings_do_not_load_env_from_ambient_cwd(monkeypatch, tmp_path):
    clear_env_aliases(monkeypatch, MAC_ENV_ALIASES)
    (tmp_path / ".env").write_text(
        "ORCHESTRATOR_HOST=127.0.0.1\nORCHESTRATOR_PORT=9999\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = MacSettings()

    assert settings.host == "0.0.0.0"
    assert settings.port == 8000


def test_normalize_movie_number_lowercases_and_keeps_dash():
    assert normalize_movie_number(" KTB-096 ") == "ktb-096"


def test_normalize_movie_number_accepts_id_without_dash():
    assert normalize_movie_number("KTB096") == "ktb-096"


def test_normalize_movie_number_rejects_invalid_ids():
    assert normalize_movie_number("bad id") is None
    assert normalize_movie_number("") is None


def test_build_job_paths_maps_mac_root_to_windows_root(tmp_path):
    paths = build_job_paths("ktb-096", tmp_path, "M:\\")

    assert paths.job_dir_mac == tmp_path / "ktb-096"
    assert paths.job_dir_windows == "M:\\ktb-096"
    assert paths.metadata_path_mac == tmp_path / "ktb-096" / "metadata.json"
    assert paths.audio_path_mac == tmp_path / "ktb-096" / "audio.wav"
    assert paths.audio_path_windows == "M:\\ktb-096\\audio.wav"
    assert paths.japanese_srt_path_windows == "M:\\ktb-096\\ktb-096.Japanese.srt"
    assert paths.english_srt_path_windows == "M:\\ktb-096\\ktb-096.English.srt"
