import json
from pathlib import Path

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAC_ENV_FILE = PROJECT_ROOT / ".env"
WINDOWS_ENV_FILE = PROJECT_ROOT / ".env.windows"


class CallbackClientSettings(BaseModel):
    url: str
    secret: str


class MacSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=MAC_ENV_FILE, extra="ignore")

    host: str = Field(default="0.0.0.0", alias="ORCHESTRATOR_HOST")
    port: int = Field(default=8010, alias="ORCHESTRATOR_PORT")
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
    worker_lease_seconds: int = Field(default=1800, alias="WORKER_LEASE_SECONDS")
    max_download_attempts: int = Field(default=3, alias="MAX_DOWNLOAD_ATTEMPTS")
    max_worker_attempts: int = Field(default=3, alias="MAX_WORKER_ATTEMPTS")
    publish_to_supabase: bool = Field(default=False, alias="PUBLISH_TO_SUPABASE")
    supabase_url: str | None = Field(default=None, alias="SUPABASE_URL")
    supabase_service_role_key: str | None = Field(
        default=None,
        alias="SUPABASE_SERVICE_ROLE_KEY",
    )
    supabase_storage_bucket: str = Field(default="subtitles", alias="SUPABASE_STORAGE_BUCKET")
    javsubtitle_api_base: str = Field(
        default="https://javsubtitle.com",
        alias="JAVSUBTITLE_API_BASE",
    )
    javsubtitle_admin_api_token: str | None = Field(
        default=None,
        alias="JAVSUBTITLE_ADMIN_API_TOKEN",
    )
    cloudflare_account_id: str | None = Field(default=None, alias="CLOUDFLARE_ACCOUNT_ID")
    cloudflare_api_token: str | None = Field(default=None, alias="CLOUDFLARE_API_TOKEN")
    cloudflare_d1_api_token: str | None = Field(default=None, alias="CLOUDFLARE_D1_API_TOKEN")
    cloudflare_d1_database_id: str = Field(
        default="401de37d-51fc-44b1-aacc-6ccff9d74f52",
        alias="CLOUDFLARE_D1_DATABASE_ID",
    )
    javsubtitle_post_sync_enabled: bool = Field(
        default=False,
        alias="JAVSUBTITLE_POST_SYNC_ENABLED",
    )
    callback_clients: dict[str, CallbackClientSettings] = Field(
        default_factory=dict,
        alias="CALLBACK_CLIENTS_JSON",
    )
    callback_timeout_seconds: int = Field(default=10, alias="CALLBACK_TIMEOUT_SECONDS")

    @field_validator("callback_clients", mode="before")
    @classmethod
    def parse_callback_clients_json(cls, value):
        if value in (None, ""):
            return {}
        if isinstance(value, dict):
            return value
        if not isinstance(value, str):
            raise ValueError("CALLBACK_CLIENTS_JSON must be a JSON object")
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("CALLBACK_CLIENTS_JSON must be valid JSON") from exc
        if not isinstance(parsed, dict):
            raise ValueError("CALLBACK_CLIENTS_JSON must be a JSON object")
        return parsed


class WindowsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=WINDOWS_ENV_FILE, extra="ignore")

    mac_api_base_url: str = Field(alias="MAC_API_BASE_URL")
    worker_id: str = Field(default="windows-gpu-1", alias="WORKER_ID")
    windows_jobs_root: str = Field(default="M:\\", alias="WINDOWS_JOBS_ROOT")
    whisper_model: str = Field(default="large-v3-turbo", alias="WHISPER_MODEL")
    whisper_device: str = Field(default="cuda", alias="WHISPER_DEVICE")
    whisper_compute_type: str = Field(default="float16", alias="WHISPER_COMPUTE_TYPE")
    openai_api_key: str = Field(alias="OPENAI_API_KEY")
    translate_script_path: str = Field(alias="TRANSLATE_SCRIPT_PATH")
    poll_interval_seconds: int = Field(default=10, alias="POLL_INTERVAL_SECONDS")
    heartbeat_interval_seconds: int = Field(default=60, alias="HEARTBEAT_INTERVAL_SECONDS")
    delete_audio_after_transcription: bool = Field(
        default=True,
        alias="DELETE_AUDIO_AFTER_TRANSCRIPTION",
    )
