from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class JobStatus(StrEnum):
    QUEUED = "queued"
    DOWNLOADING_METADATA = "downloading_metadata"
    DOWNLOADING_AUDIO = "downloading_audio"
    AUDIO_READY = "audio_ready"
    TRANSCRIPTION_CLAIMED = "transcription_claimed"
    TRANSCRIBING = "transcribing"
    TRANSCRIPTION_DONE = "transcription_done"
    TRANSLATING = "translating"
    ENGLISH_SRT_READY = "english_srt_ready"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobPaths(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    job_dir_mac: Path
    job_dir_windows: str
    metadata_path_mac: Path
    audio_path_mac: Path
    audio_path_windows: str
    japanese_srt_path_mac: Path
    japanese_srt_path_windows: str
    english_srt_path_mac: Path
    english_srt_path_windows: str


class SubmitJobRequest(BaseModel):
    movie_number: str
    priority: int = 100
    force: bool = False


class SubmitBatchRequest(BaseModel):
    movie_numbers: list[str]
    priority: int = 100
    force: bool = False


class WorkerHeartbeatRequest(BaseModel):
    worker_id: str
    stage: JobStatus


class WorkerCompleteRequest(BaseModel):
    worker_id: str
    japanese_srt_path_windows: str
    english_srt_path_windows: str


class WorkerFailedRequest(BaseModel):
    worker_id: str
    stage: JobStatus
    error: str = Field(min_length=1)


class JobResponse(BaseModel):
    id: str
    movie_number: str
    status: JobStatus
    job_dir_mac: str
    job_dir_windows: str
    error: str | None = None


class BatchJobResponse(BaseModel):
    created: list[JobResponse]
    existing: list[JobResponse]
    invalid: list[str]


class WorkerJobResponse(BaseModel):
    id: str
    movie_number: str
    audio_path_windows: str
    japanese_srt_path_windows: str
    english_srt_path_windows: str


class WorkerNextJobResponse(BaseModel):
    job: WorkerJobResponse | None


class DashboardJobSummary(BaseModel):
    id: str
    movie_number: str
    status: JobStatus
    priority: int
    updated_at: str
    claimed_by: str | None = None
    error: str | None = None


class DashboardStateResponse(BaseModel):
    api: dict[str, str | bool]
    activity: dict[str, dict[str, str | None]]
    counts: dict[str, int]
    latest_jobs: list[DashboardJobSummary]
    active_errors: list[DashboardJobSummary]


class JobDetailResponse(BaseModel):
    id: str
    movie_number: str
    normalized_movie_number: str
    status: JobStatus
    priority: int
    attempt_count: int
    worker_attempt_count: int
    claimed_by: str | None = None
    lease_expires_at: str | None = None
    created_at: str
    updated_at: str
    error: str | None = None
    job_dir_mac: str
    job_dir_windows: str
    metadata_path_mac: str | None = None
    audio_path_mac: str | None = None
    audio_path_windows: str | None = None
    japanese_srt_path_mac: str | None = None
    japanese_srt_path_windows: str | None = None
    english_srt_path_mac: str | None = None
    english_srt_path_windows: str | None = None


class JobLogSummary(BaseModel):
    name: str
    size_bytes: int
    available: bool


class JobLogsResponse(BaseModel):
    job_id: str
    logs: list[JobLogSummary]


class JobLogTailResponse(BaseModel):
    job_id: str
    log_name: str
    tail: int
    lines: list[str]
