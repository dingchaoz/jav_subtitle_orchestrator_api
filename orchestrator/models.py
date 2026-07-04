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
