from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MacSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

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
    worker_lease_seconds: int = Field(default=1800, alias="WORKER_LEASE_SECONDS")
    max_download_attempts: int = Field(default=3, alias="MAX_DOWNLOAD_ATTEMPTS")
    max_worker_attempts: int = Field(default=3, alias="MAX_WORKER_ATTEMPTS")


class WindowsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

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
