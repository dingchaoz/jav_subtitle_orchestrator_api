import inspect
import os
import subprocess
import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from orchestrator.__main__ import (
    build_catalog_sync_client,
    _build_windows_transcriber,
    build_supabase_publisher,
    run_mac_translation_worker,
    run_mac_translation_worker_once,
)
from orchestrator.config import MacSettings, WindowsSettings
from orchestrator.movie_catalog import SupabaseMovieCatalogEnsurer
from orchestrator.paths import build_job_paths, normalize_movie_number
from orchestrator.transcription import ExternalScriptTranscriber, FasterWhisperTranscriber


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
    "MAC_TRANSLATION_PUBLISH_ENABLED",
    "MAX_PUBLISH_ATTEMPTS",
    "MAC_PUBLISH_RETRY_SECONDS",
    "SUPABASE_PUBLISH_VERIFY_TIMEOUT_SECONDS",
    "JAVSUBTITLE_API_BASE",
    "JAVSUBTITLE_ADMIN_API_TOKEN",
    "CATALOG_SYNC_RETRY_SECONDS",
    "CATALOG_SYNC_MAX_RETRY_SECONDS",
    "MAX_CATALOG_SYNC_ATTEMPTS",
    "CALLBACK_CLIENTS_JSON",
    "CALLBACK_TIMEOUT_SECONDS",
)

WINDOWS_ENV_ALIASES = (
    "MAC_API_BASE_URL",
    "WORKER_ID",
    "WINDOWS_JOBS_ROOT",
    "WHISPER_MODEL",
    "WHISPER_DEVICE",
    "WHISPER_COMPUTE_TYPE",
    "WHISPER_CHUNK_SECONDS",
    "WHISPER_GAP_REPAIR_ENABLED",
    "WHISPER_REPAIR_GAP_SECONDS",
    "WHISPER_REPAIR_CHUNK_SECONDS",
    "WHISPER_REPAIR_OFFSET_SECONDS",
    "WHISPER_REPAIR_PADDING_SECONDS",
    "WHISPER_REPAIR_MIN_SIMILARITY",
    "TRANSCRIBE_SCRIPT_PATH",
    "TRANSCRIBE_PYTHON_EXECUTABLE",
    "OPENAI_API_KEY",
    "TRANSLATE_SCRIPT_PATH",
    "TRANSLATELOCALLY_PATH",
    "TRANSLATELOCALLY_MODEL",
    "CODEX_TRANSLATE_SCRIPT_PATH",
    "CODEX_TRANSLATE_PYTHON_EXECUTABLE",
    "CODEX_BIN_PATH",
    "CODEX_TRANSLATION_PROVIDER",
    "CODEX_TRANSLATION_TARGETS",
    "CODEX_TRANSLATION_WORKERS",
    "CODEX_TRANSLATION_BATCH_WORKERS",
    "CODEX_TRANSLATION_ANTHROPIC_MODELS",
    "CODEX_TRANSLATION_ANTHROPIC_RECHECK_MINUTES",
    "POLL_INTERVAL_SECONDS",
    "HEARTBEAT_INTERVAL_SECONDS",
)


def clear_env_aliases(monkeypatch, aliases):
    for alias in aliases:
        monkeypatch.delenv(alias, raising=False)


def test_module_cli_prints_help():
    result = subprocess.run(
        [sys.executable, "-m", "orchestrator", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "api" in result.stdout
    assert "mac-worker" in result.stdout
    assert "windows-worker" in result.stdout


def test_windows_runtime_env_exports_openai_api_key(monkeypatch):
    from orchestrator.__main__ import _export_windows_runtime_env

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    _export_windows_runtime_env(
        SimpleNamespace(
            openai_api_key="loaded-key",
            translatelocally_path=None,
            translatelocally_model=None,
            codex_translate_script_path=None,
            codex_translate_python_executable=None,
            codex_bin_path=None,
            codex_translation_provider=None,
            codex_translation_targets=None,
            codex_translation_workers=None,
            codex_translation_batch_workers=None,
            codex_translation_anthropic_models=None,
            codex_translation_anthropic_recheck_minutes=None,
        )
    )

    assert os.environ["OPENAI_API_KEY"] == "loaded-key"


def test_windows_runtime_env_skips_openai_api_key_when_missing(monkeypatch):
    from orchestrator.__main__ import _export_windows_runtime_env

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    _export_windows_runtime_env(
        SimpleNamespace(
            openai_api_key=None,
            translatelocally_path=None,
            translatelocally_model=None,
            codex_translate_script_path=None,
            codex_translate_python_executable=None,
            codex_bin_path=None,
            codex_translation_provider=None,
            codex_translation_targets=None,
            codex_translation_workers=None,
            codex_translation_batch_workers=None,
            codex_translation_anthropic_models=None,
            codex_translation_anthropic_recheck_minutes=None,
        )
    )

    assert "OPENAI_API_KEY" not in os.environ


def test_windows_runtime_env_exports_codex_translation_settings(monkeypatch):
    from orchestrator.__main__ import _export_windows_runtime_env

    aliases = (
        "CODEX_TRANSLATE_SCRIPT_PATH",
        "CODEX_TRANSLATE_PYTHON_EXECUTABLE",
        "CODEX_BIN_PATH",
        "CODEX_TRANSLATION_PROVIDER",
        "CODEX_TRANSLATION_TARGETS",
        "CODEX_TRANSLATION_WORKERS",
        "CODEX_TRANSLATION_BATCH_WORKERS",
        "CODEX_TRANSLATION_ANTHROPIC_MODELS",
        "CODEX_TRANSLATION_ANTHROPIC_RECHECK_MINUTES",
    )
    clear_env_aliases(monkeypatch, aliases)

    _export_windows_runtime_env(
        SimpleNamespace(
            openai_api_key=None,
            translatelocally_path=None,
            translatelocally_model=None,
            codex_translate_script_path="C:\\scripts\\translate_srts.py",
            codex_translate_python_executable="C:\\Python312\\python.exe",
            codex_bin_path="C:\\tools\\codex.exe",
            codex_translation_provider="codex",
            codex_translation_targets="en",
            codex_translation_workers="1",
            codex_translation_batch_workers="5",
            codex_translation_anthropic_models="haiku",
            codex_translation_anthropic_recheck_minutes="30",
        )
    )

    assert os.environ["CODEX_TRANSLATE_SCRIPT_PATH"] == "C:\\scripts\\translate_srts.py"
    assert os.environ["CODEX_TRANSLATE_PYTHON_EXECUTABLE"] == "C:\\Python312\\python.exe"
    assert os.environ["CODEX_BIN_PATH"] == "C:\\tools\\codex.exe"
    assert os.environ["CODEX_TRANSLATION_PROVIDER"] == "codex"
    assert os.environ["CODEX_TRANSLATION_TARGETS"] == "en"
    assert os.environ["CODEX_TRANSLATION_WORKERS"] == "1"
    assert os.environ["CODEX_TRANSLATION_BATCH_WORKERS"] == "5"
    assert os.environ["CODEX_TRANSLATION_ANTHROPIC_MODELS"] == "haiku"
    assert os.environ["CODEX_TRANSLATION_ANTHROPIC_RECHECK_MINUTES"] == "30"


def test_windows_runtime_env_exports_translatelocally_settings(monkeypatch):
    from orchestrator.__main__ import _export_windows_runtime_env

    aliases = ("TRANSLATELOCALLY_PATH", "TRANSLATELOCALLY_MODEL")
    clear_env_aliases(monkeypatch, aliases)

    _export_windows_runtime_env(
        SimpleNamespace(
            openai_api_key=None,
            translatelocally_path="C:\\tools\\translateLocally.exe",
            translatelocally_model="ja-en-tiny",
            codex_translate_script_path=None,
            codex_translate_python_executable=None,
            codex_bin_path=None,
            codex_translation_provider=None,
            codex_translation_targets=None,
            codex_translation_workers=None,
            codex_translation_batch_workers=None,
            codex_translation_anthropic_models=None,
            codex_translation_anthropic_recheck_minutes=None,
        )
    )

    assert os.environ["TRANSLATELOCALLY_PATH"] == "C:\\tools\\translateLocally.exe"
    assert os.environ["TRANSLATELOCALLY_MODEL"] == "ja-en-tiny"


def test_windows_extra_installs_translation_script_dependencies():
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    windows_deps = pyproject["project"]["optional-dependencies"]["windows"]

    assert "faster-whisper>=1.0.3" in windows_deps
    assert "openai>=1.0.0" in windows_deps
    assert "tqdm>=4.66.0" in windows_deps


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
    assert settings.mac_translation_publish_enabled is False
    assert settings.max_publish_attempts == 10
    assert settings.mac_publish_retry_seconds == 30
    assert settings.supabase_publish_verify_timeout_seconds == 90
    assert settings.javsubtitle_api_base is None
    assert settings.javsubtitle_admin_api_token is None
    assert settings.catalog_sync_retry_seconds == 30
    assert settings.catalog_sync_max_retry_seconds == 900
    assert settings.max_catalog_sync_attempts == 10
    assert settings.callback_clients == {}
    assert settings.callback_timeout_seconds == 10


def test_mac_callback_settings_parse_json_without_exposing_secrets(monkeypatch):
    clear_env_aliases(monkeypatch, MAC_ENV_ALIASES)
    monkeypatch.setenv(
        "CALLBACK_CLIENTS_JSON",
        '{"machine-a.access":{"url":"https://client.example/ready",'
        '"secret":"hmac-secret"}}',
    )
    monkeypatch.setenv("CALLBACK_TIMEOUT_SECONDS", "7")

    settings = MacSettings(_env_file=None)

    assert settings.callback_clients["machine-a.access"].url == (
        "https://client.example/ready"
    )
    assert settings.callback_clients["machine-a.access"].secret == "hmac-secret"
    assert settings.callback_timeout_seconds == 7


def test_mac_catalog_sync_settings_load_overrides(monkeypatch):
    clear_env_aliases(monkeypatch, MAC_ENV_ALIASES)
    monkeypatch.setenv("JAVSUBTITLE_API_BASE", "https://javsubtitle.example")
    monkeypatch.setenv("JAVSUBTITLE_ADMIN_API_TOKEN", "test-admin-token")
    monkeypatch.setenv("CATALOG_SYNC_RETRY_SECONDS", "45")
    monkeypatch.setenv("CATALOG_SYNC_MAX_RETRY_SECONDS", "180")
    monkeypatch.setenv("MAX_CATALOG_SYNC_ATTEMPTS", "12")

    settings = MacSettings(_env_file=None)

    assert settings.javsubtitle_api_base == "https://javsubtitle.example"
    assert settings.javsubtitle_admin_api_token == "test-admin-token"
    assert settings.catalog_sync_retry_seconds == 45
    assert settings.catalog_sync_max_retry_seconds == 180
    assert settings.max_catalog_sync_attempts == 12


@pytest.mark.parametrize(
    ("alias", "value"),
    [
        ("MAX_PUBLISH_ATTEMPTS", "0"),
        ("MAC_PUBLISH_RETRY_SECONDS", "0"),
        ("MAC_PUBLISH_RETRY_SECONDS", "3601"),
        ("CATALOG_SYNC_RETRY_SECONDS", "0"),
        ("CATALOG_SYNC_RETRY_SECONDS", "3601"),
        ("CATALOG_SYNC_MAX_RETRY_SECONDS", "0"),
        ("MAX_CATALOG_SYNC_ATTEMPTS", "0"),
    ],
)
def test_mac_publication_retry_settings_are_bounded(monkeypatch, alias, value):
    clear_env_aliases(monkeypatch, MAC_ENV_ALIASES)
    monkeypatch.setenv(alias, value)

    with pytest.raises(ValueError):
        MacSettings(_env_file=None)


def test_supabase_publisher_factory_requires_credentials_when_enabled():
    disabled = SimpleNamespace(mac_translation_publish_enabled=False)
    assert build_supabase_publisher(disabled) is None

    enabled = SimpleNamespace(
        mac_translation_publish_enabled=True,
        supabase_url=None,
        supabase_service_role_key=None,
        supabase_subtitle_bucket="subtitles",
        supabase_publish_verify_timeout_seconds=90,
    )
    with pytest.raises(RuntimeError, match="publication is enabled"):
        build_supabase_publisher(enabled)


def test_catalog_sync_factory_is_disabled_with_local_publication():
    settings = SimpleNamespace(mac_translation_publish_enabled=False)

    assert build_catalog_sync_client(settings) is None


@pytest.mark.parametrize("missing", ["javsubtitle_api_base", "javsubtitle_admin_api_token"])
def test_catalog_sync_factory_is_optional_when_supabase_publication_is_enabled(missing):
    values = {
        "mac_translation_publish_enabled": True,
        "javsubtitle_api_base": "https://javsubtitle.example",
        "javsubtitle_admin_api_token": "admin-token",
    }
    values[missing] = None

    assert build_catalog_sync_client(SimpleNamespace(**values)) is None


def test_supabase_publisher_factory_shares_session_with_catalog_ensurer():
    settings = SimpleNamespace(
        mac_translation_publish_enabled=True,
        supabase_url="https://example.supabase.co",
        supabase_service_role_key="service-role-key",
        supabase_subtitle_bucket="subtitles",
        supabase_publish_verify_timeout_seconds=90,
    )

    publisher = build_supabase_publisher(settings)

    assert isinstance(publisher.catalog_ensurer, SupabaseMovieCatalogEnsurer)
    assert publisher.catalog_ensurer.session is publisher.session
    assert publisher.catalog_ensurer.url == "https://example.supabase.co"
    assert publisher.catalog_ensurer.service_key == "service-role-key"
    source = inspect.getsource(build_supabase_publisher)
    assert "SupabaseMovieCatalogEnsurer(" in source
    assert "catalog_ensurer=catalog_ensurer" in source


@pytest.mark.parametrize(
    "entrypoint",
    [run_mac_translation_worker, run_mac_translation_worker_once],
)
def test_mac_translation_entrypoints_wire_publication_retry_settings(entrypoint):
    source = inspect.getsource(entrypoint)

    assert "max_publish_attempts=settings.max_publish_attempts" in source
    assert "publish_retry_seconds=settings.mac_publish_retry_seconds" in source
    assert "catalog_sync_client=catalog_sync_client" in source
    assert "max_catalog_sync_attempts=settings.max_catalog_sync_attempts" in source
    assert "catalog_sync_retry_seconds=settings.catalog_sync_retry_seconds" in source


def test_windows_settings_defaults_match_design_spec(monkeypatch):
    clear_env_aliases(monkeypatch, WINDOWS_ENV_ALIASES)
    monkeypatch.setenv("MAC_API_BASE_URL", "http://192.168.1.25:8000")
    monkeypatch.setenv("WORKER_ID", "windows-gpu-1")
    monkeypatch.setenv("WINDOWS_JOBS_ROOT", "M:\\")
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
    assert settings.whisper_chunk_seconds == 90
    assert settings.whisper_gap_repair_enabled is True
    assert settings.whisper_repair_gap_seconds == 60
    assert settings.whisper_repair_chunk_seconds == 30
    assert settings.whisper_repair_offset_seconds == 15
    assert settings.whisper_repair_padding_seconds == 15
    assert settings.whisper_repair_min_similarity == 0.72
    assert settings.transcribe_script_path is None
    assert settings.transcribe_python_executable is None
    assert settings.openai_api_key is None
    assert settings.translatelocally_path is None
    assert settings.translatelocally_model is None
    assert settings.codex_translate_script_path is None
    assert settings.codex_translate_python_executable is None
    assert settings.codex_bin_path is None
    assert settings.codex_translation_provider is None
    assert settings.codex_translation_targets is None
    assert settings.codex_translation_workers is None
    assert settings.codex_translation_batch_workers is None
    assert settings.codex_translation_anthropic_models is None
    assert settings.codex_translation_anthropic_recheck_minutes is None
    assert settings.poll_interval_seconds == 10
    assert settings.heartbeat_interval_seconds == 60


def test_windows_settings_reject_invalid_repair_grid_offset(monkeypatch):
    clear_env_aliases(monkeypatch, WINDOWS_ENV_ALIASES)
    monkeypatch.setenv("MAC_API_BASE_URL", "http://192.168.1.25:8000")
    monkeypatch.setenv("WHISPER_REPAIR_CHUNK_SECONDS", "30")
    monkeypatch.setenv("WHISPER_REPAIR_OFFSET_SECONDS", "30")

    with pytest.raises(ValidationError, match="WHISPER_REPAIR_OFFSET_SECONDS"):
        WindowsSettings(_env_file=None)


def test_build_windows_transcriber_prefers_external_script_when_configured():
    settings = SimpleNamespace(
        transcribe_script_path="C:\\transcribe.py",
        transcribe_python_executable="C:\\Python312\\python.exe",
        whisper_model="large-v3-turbo",
        whisper_device="cuda",
        whisper_compute_type="float16",
    )

    transcriber = _build_windows_transcriber(settings)

    assert isinstance(transcriber, ExternalScriptTranscriber)
    assert transcriber.script_path == Path("C:/transcribe.py")
    assert transcriber.python_executable == "C:\\Python312\\python.exe"


def test_build_windows_transcriber_falls_back_to_internal_whisper():
    settings = SimpleNamespace(
        transcribe_script_path=None,
        transcribe_python_executable=None,
        whisper_model="large-v3-turbo",
        whisper_device="cuda",
        whisper_compute_type="float16",
        whisper_chunk_seconds=75,
        whisper_gap_repair_enabled=True,
        whisper_repair_gap_seconds=55,
        whisper_repair_chunk_seconds=25,
        whisper_repair_offset_seconds=12.5,
        whisper_repair_padding_seconds=10,
        whisper_repair_min_similarity=0.8,
    )

    transcriber = _build_windows_transcriber(settings)

    assert isinstance(transcriber, FasterWhisperTranscriber)
    assert transcriber.chunk_seconds == 75
    assert transcriber.gap_repair_enabled is True
    assert transcriber.repair_gap_seconds == 55
    assert transcriber.repair_chunk_seconds == 25
    assert transcriber.repair_offset_seconds == 12.5
    assert transcriber.repair_padding_seconds == 10
    assert transcriber.repair_minimum_similarity == 0.8


def test_mac_settings_do_not_load_env_from_ambient_cwd(monkeypatch, tmp_path):
    clear_env_aliases(monkeypatch, MAC_ENV_ALIASES)
    (tmp_path / ".env").write_text(
        "ORCHESTRATOR_HOST=127.0.0.1\nORCHESTRATOR_PORT=9999\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    settings = MacSettings(_env_file=None)

    assert settings.host == "0.0.0.0"
    assert settings.port == 8000


def test_normalize_movie_number_lowercases_and_keeps_dash():
    assert normalize_movie_number(" KTB-096 ") == "ktb-096"


def test_normalize_movie_number_accepts_id_without_dash():
    assert normalize_movie_number("KTB096") == "ktb-096"


@pytest.mark.parametrize("movie_number", ["ABC7", "abc-7", "abc-007"])
def test_normalize_movie_number_canonicalizes_numeric_padding(movie_number):
    assert normalize_movie_number(movie_number) == "abc-007"


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
