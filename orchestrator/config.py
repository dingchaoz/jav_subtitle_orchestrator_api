import json
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAC_ENV_FILE = PROJECT_ROOT / ".env"
WINDOWS_ENV_FILE = PROJECT_ROOT / ".env.windows"


class CallbackClientSettings(BaseModel):
    url: str
    secret: str = Field(min_length=1)

    @field_validator("url")
    @classmethod
    def validate_https_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError("callback URL must use HTTPS")
        return value


class MacSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=MAC_ENV_FILE, extra="ignore")

    host: str = Field(default="0.0.0.0", alias="ORCHESTRATOR_HOST")
    port: int = Field(default=8000, alias="ORCHESTRATOR_PORT")
    db_path: Path = Field(
        default=Path("/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/data/jobs.sqlite3"),
        alias="ORCHESTRATOR_DB_PATH",
    )
    missav_pipeline_root: Path = Field(
        default=Path("/Users/ytt/Documents/startup/MissAV-Pipeline"),
        alias="MISSAV_PIPELINE_ROOT",
    )
    jobs_root_mac: Path = Field(default=Path("/Users/ytt/MissAVJobs"), alias="JOBS_ROOT_MAC")
    jobs_root_windows: str = Field(default="M:\\", alias="JOBS_ROOT_WINDOWS")
    mac_download_concurrency: int = Field(default=1, alias="MAC_DOWNLOAD_CONCURRENCY")
    mac_download_worker_id: str = Field(
        default="mac-downloader-1",
        alias="MAC_DOWNLOAD_WORKER_ID",
    )
    worker_lease_seconds: int = Field(default=1800, alias="WORKER_LEASE_SECONDS")
    max_download_attempts: int = Field(default=3, alias="MAX_DOWNLOAD_ATTEMPTS")
    max_worker_attempts: int = Field(default=3, alias="MAX_WORKER_ATTEMPTS")
    mac_translate_script_path: str = Field(
        default=str(PROJECT_ROOT / "scripts" / "translatelocally_translate_single.py"),
        alias="MAC_TRANSLATE_SCRIPT_PATH",
    )
    translatelocally_path: str | None = Field(default=None, alias="TRANSLATELOCALLY_PATH")
    translatelocally_model: str = Field(default="ja-en-tiny", alias="TRANSLATELOCALLY_MODEL")
    mac_translation_worker_id: str = Field(
        default="mac-translation-1",
        alias="MAC_TRANSLATION_WORKER_ID",
    )
    mac_translation_lease_seconds: int = Field(
        default=1800,
        alias="MAC_TRANSLATION_LEASE_SECONDS",
    )
    max_translation_attempts: int = Field(default=3, alias="MAX_TRANSLATION_ATTEMPTS")
    max_publish_attempts: int = Field(
        default=10,
        ge=1,
        alias="MAX_PUBLISH_ATTEMPTS",
    )
    mac_publish_retry_seconds: int = Field(
        default=30,
        ge=1,
        le=3600,
        alias="MAC_PUBLISH_RETRY_SECONDS",
    )
    mac_translation_poll_interval_seconds: int = Field(
        default=10,
        alias="MAC_TRANSLATION_POLL_INTERVAL_SECONDS",
    )
    translation_quality_failure_limit: int = Field(
        default=3,
        ge=1,
        alias="TRANSLATION_QUALITY_FAILURE_LIMIT",
    )
    mac_translation_publish_enabled: bool = Field(
        default=False,
        alias="MAC_TRANSLATION_PUBLISH_ENABLED",
    )
    supabase_publish_verify_timeout_seconds: int = Field(
        default=90,
        ge=60,
        le=300,
        alias="SUPABASE_PUBLISH_VERIFY_TIMEOUT_SECONDS",
    )
    supabase_url: str | None = Field(default=None, alias="SUPABASE_URL")
    supabase_service_role_key: str | None = Field(
        default=None,
        alias="SUPABASE_SERVICE_ROLE_KEY",
    )
    cloudflare_account_id: str | None = Field(
        default=None,
        alias="CLOUDFLARE_ACCOUNT_ID",
    )
    cloudflare_d1_api_token: str | None = Field(
        default=None,
        alias="CLOUDFLARE_D1_API_TOKEN",
    )
    cloudflare_d1_database_id: str = Field(
        default="401de37d-51fc-44b1-aacc-6ccff9d74f52",
        alias="CLOUDFLARE_D1_DATABASE_ID",
    )
    requested_subtitle_import_timeout_seconds: int = Field(
        default=30,
        ge=1,
        le=120,
        alias="REQUESTED_SUBTITLE_IMPORT_TIMEOUT_SECONDS",
    )
    javsubtitle_api_base: str | None = Field(
        default=None,
        alias="JAVSUBTITLE_API_BASE",
    )
    javsubtitle_admin_api_token: str | None = Field(
        default=None,
        alias="JAVSUBTITLE_ADMIN_API_TOKEN",
    )
    catalog_sync_retry_seconds: int = Field(
        default=30,
        ge=1,
        le=3600,
        alias="CATALOG_SYNC_RETRY_SECONDS",
    )
    catalog_sync_max_retry_seconds: int = Field(
        default=900,
        ge=1,
        alias="CATALOG_SYNC_MAX_RETRY_SECONDS",
    )
    max_catalog_sync_attempts: int = Field(
        default=10,
        ge=1,
        alias="MAX_CATALOG_SYNC_ATTEMPTS",
    )
    callback_clients: dict[str, CallbackClientSettings] = Field(
        default_factory=dict,
        alias="CALLBACK_CLIENTS_JSON",
    )
    callback_timeout_seconds: int = Field(
        default=10,
        ge=1,
        le=60,
        alias="CALLBACK_TIMEOUT_SECONDS",
    )
    supabase_subtitle_bucket: str = Field(
        default="subtitles",
        alias="SUPABASE_SUBTITLE_BUCKET",
    )
    local_audit_timeout_seconds: int = Field(
        default=30,
        ge=1,
        le=120,
        alias="LOCAL_AUDIT_TIMEOUT_SECONDS",
    )
    subtitle_audit_visibility_enabled: bool = Field(
        default=False,
        alias="SUBTITLE_AUDIT_VISIBILITY_ENABLED",
    )
    subtitle_audit_timeout_seconds: int = Field(
        default=30,
        ge=1,
        le=120,
        alias="SUBTITLE_AUDIT_TIMEOUT_SECONDS",
    )

    @field_validator("callback_clients", mode="before")
    @classmethod
    def parse_callback_clients_json(cls, value):
        if value in (None, ""):
            return {}
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (TypeError, ValueError):
                raise ValueError("callback clients JSON is invalid") from None
        if not isinstance(value, dict) or any(
            not isinstance(key, str) or not key or len(key) > 256
            for key in value
        ):
            raise ValueError("callback clients JSON is invalid")
        return value


class WindowsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=WINDOWS_ENV_FILE, extra="ignore")

    mac_api_base_url: str = Field(alias="MAC_API_BASE_URL")
    worker_id: str = Field(default="windows-gpu-1", alias="WORKER_ID")
    windows_jobs_root: str = Field(default="M:\\", alias="WINDOWS_JOBS_ROOT")
    whisper_model: str = Field(default="large-v3-turbo", alias="WHISPER_MODEL")
    whisper_device: str = Field(default="cuda", alias="WHISPER_DEVICE")
    whisper_compute_type: str = Field(default="float16", alias="WHISPER_COMPUTE_TYPE")
    transcribe_script_path: str | None = Field(default=None, alias="TRANSCRIBE_SCRIPT_PATH")
    transcribe_python_executable: str | None = Field(
        default=None,
        alias="TRANSCRIBE_PYTHON_EXECUTABLE",
    )
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    translate_script_path: str | None = Field(default=None, alias="TRANSLATE_SCRIPT_PATH")
    translatelocally_path: str | None = Field(default=None, alias="TRANSLATELOCALLY_PATH")
    translatelocally_model: str | None = Field(default=None, alias="TRANSLATELOCALLY_MODEL")
    codex_translate_script_path: str | None = Field(
        default=None,
        alias="CODEX_TRANSLATE_SCRIPT_PATH",
    )
    codex_translate_python_executable: str | None = Field(
        default=None,
        alias="CODEX_TRANSLATE_PYTHON_EXECUTABLE",
    )
    codex_bin_path: str | None = Field(default=None, alias="CODEX_BIN_PATH")
    codex_translation_provider: str | None = Field(
        default=None,
        alias="CODEX_TRANSLATION_PROVIDER",
    )
    codex_translation_targets: str | None = Field(
        default=None,
        alias="CODEX_TRANSLATION_TARGETS",
    )
    codex_translation_workers: str | None = Field(
        default=None,
        alias="CODEX_TRANSLATION_WORKERS",
    )
    codex_translation_batch_workers: str | None = Field(
        default=None,
        alias="CODEX_TRANSLATION_BATCH_WORKERS",
    )
    codex_translation_anthropic_models: str | None = Field(
        default=None,
        alias="CODEX_TRANSLATION_ANTHROPIC_MODELS",
    )
    codex_translation_anthropic_recheck_minutes: str | None = Field(
        default=None,
        alias="CODEX_TRANSLATION_ANTHROPIC_RECHECK_MINUTES",
    )
    poll_interval_seconds: int = Field(default=10, alias="POLL_INTERVAL_SECONDS")
    heartbeat_interval_seconds: int = Field(default=60, alias="HEARTBEAT_INTERVAL_SECONDS")
