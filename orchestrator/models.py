from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


MAX_AUDIT_OFFSET = 1_000_000


class AuditStatus(StrEnum):
    PASS = "pass"
    WARNING = "warning"
    REVIEW = "review"
    BAD = "bad"
    INVALID = "invalid"
    MISSING = "missing"


class ReasonCode(StrEnum):
    STORAGE_OBJECT_MISSING = "STORAGE_OBJECT_MISSING"
    EMPTY_FILE = "EMPTY_FILE"
    NO_VALID_CUES = "NO_VALID_CUES"
    INVALID_TIMELINE = "INVALID_TIMELINE"
    SEVERE_MOJIBAKE = "SEVERE_MOJIBAKE"
    KNOWN_BAD_TRANSLATION = "KNOWN_BAD_TRANSLATION"
    CUE_COUNT_MISMATCH = "CUE_COUNT_MISMATCH"
    DOMINANT_TEXT_COLLAPSE = "DOMINANT_TEXT_COLLAPSE"
    LOW_DIVERSITY_COLLAPSE = "LOW_DIVERSITY_COLLAPSE"
    SEVERE_COVERAGE_GAP = "SEVERE_COVERAGE_GAP"
    COVERAGE_REVIEW = "COVERAGE_REVIEW"
    EXPECTED_DURATION_UNKNOWN = "EXPECTED_DURATION_UNKNOWN"
    VERY_FEW_CUES = "VERY_FEW_CUES"
    EARLY_LAST_CUE = "EARLY_LAST_CUE"
    LANGUAGE_SCRIPT_MISMATCH = "LANGUAGE_SCRIPT_MISMATCH"
    NON_UTF8_ENCODING = "NON_UTF8_ENCODING"
    SPARSE_TEXT = "SPARSE_TEXT"
    FILE_SIZE_OUTLIER = "FILE_SIZE_OUTLIER"


class JobStatus(StrEnum):
    QUEUED = "queued"
    DOWNLOADING_METADATA = "downloading_metadata"
    DOWNLOADING_AUDIO = "downloading_audio"
    AUDIO_READY = "audio_ready"
    TRANSCRIPTION_CLAIMED = "transcription_claimed"
    TRANSCRIBING = "transcribing"
    TRANSCRIPTION_DONE = "transcription_done"
    TRANSLATING = "translating"
    PUBLISH_PENDING = "publish_pending"
    PUBLISHING = "publishing"
    CATALOG_SYNC_PENDING = "catalog_sync_pending"
    CATALOG_SYNCING = "catalog_syncing"
    ENGLISH_SRT_READY = "english_srt_ready"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ArtifactStatus(StrEnum):
    READY = "ready"


class CatalogSyncStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobWarning(BaseModel):
    code: str
    message: str


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


class ImportRequestedSubtitlesRequest(BaseModel):
    min_count: int = Field(default=1, ge=1)
    limit: int = Field(default=500, ge=1, le=500)
    priority: int = 100
    force: bool = False


class WorkerHeartbeatRequest(BaseModel):
    worker_id: str
    stage: JobStatus


class WorkerCompleteRequest(BaseModel):
    worker_id: str
    japanese_srt_path_windows: str
    english_srt_path_windows: str


class WorkerTranscriptionCompleteRequest(BaseModel):
    worker_id: str
    japanese_srt_path_windows: str


class WorkerFailedRequest(BaseModel):
    worker_id: str
    stage: JobStatus
    error: str = Field(min_length=1)
    permanent: bool = False


class JobResponse(BaseModel):
    id: str
    movie_number: str
    status: JobStatus
    job_dir_mac: str
    job_dir_windows: str
    error: str | None = None
    ready: bool = False
    artifact_status: ArtifactStatus | None = None
    catalog_sync_status: CatalogSyncStatus | None = None
    warnings: list[JobWarning] = Field(default_factory=list)


class BatchJobResponse(BaseModel):
    created: list[JobResponse]
    existing: list[JobResponse]
    invalid: list[str]


class RequestedSubtitle(BaseModel):
    code: str
    movie_id: str | None = None
    request_count: int
    last_requested_at: str | None = None


class RequestedSubtitleImportResponse(BaseModel):
    requested: list[RequestedSubtitle]
    imported: list[RequestedSubtitle]
    skipped_available: list[RequestedSubtitle]
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


class WorkerHealthSummary(BaseModel):
    worker_id: str
    role: str
    state: str
    status: str
    last_seen_at: str
    last_poll_at: str | None = None
    last_ip: str | None = None
    current_job_id: str | None = None
    current_movie_number: str | None = None
    stage: str | None = None
    updated_at: str
    last_error: str | None = None


class HistoricalRepairDashboardCounts(BaseModel):
    total: int = 0
    planned: int = 0
    pending: int = 0
    running: int = 0
    retry_wait: int = 0
    paused: int = 0
    succeeded: int = 0
    permanent_failed: int = 0
    unknown: int = 0


class HistoricalRepairDashboardCurrent(BaseModel):
    batch_id: str
    repair_id: str
    job_id: str
    movie_number: str
    stage: str
    state: str
    reason_code: str | None = None
    updated_at: str


class HistoricalRepairDashboardProgress(BaseModel):
    counts: HistoricalRepairDashboardCounts = Field(
        default_factory=HistoricalRepairDashboardCounts
    )
    current: HistoricalRepairDashboardCurrent | None = None
    lane_paused: bool = False
    reason_code: str | None = None
    consecutive_quality_failures: int = 0
    updated_at: str | None = None


class DashboardStateResponse(BaseModel):
    api: dict[str, str | bool]
    activity: dict[str, dict[str, str | None]]
    counts: dict[str, int]
    historical_repairs: HistoricalRepairDashboardProgress = Field(
        default_factory=HistoricalRepairDashboardProgress
    )
    workers: list[WorkerHealthSummary] = Field(default_factory=list)
    latest_jobs: list[DashboardJobSummary]
    active_errors: list[DashboardJobSummary]


class JobBrowserItem(BaseModel):
    id: str
    movie_number: str
    status: JobStatus
    priority: int
    created_at: str
    updated_at: str
    claimed_by: str | None = None
    error: str | None = None


class JobBrowserResponse(BaseModel):
    items: list[JobBrowserItem]
    total: int
    page: int
    page_size: int
    pages: int
    view: str


class JobDetailResponse(BaseModel):
    id: str
    movie_number: str
    normalized_movie_number: str
    status: JobStatus
    priority: int
    attempt_count: int
    worker_attempt_count: int
    translation_attempt_count: int
    publish_attempt_count: int
    next_publish_attempt_at: str | None = None
    artifact_status: ArtifactStatus | None = None
    catalog_sync_status: CatalogSyncStatus | None = None
    catalog_sync_warning_code: str | None = None
    catalog_sync_warning_message: str | None = None
    catalog_sync_attempt_count: int = 0
    next_catalog_sync_attempt_at: str | None = None
    catalog_sync_last_http_status: int | None = None
    catalog_sync_last_response_json: str | None = None
    catalog_sync_last_attempt_at: str | None = None
    catalog_movie_uuid: str | None = None
    metadata_status: str | None = None
    metadata_source: str | None = None
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


class SubtitleAuditSummaryResponse(BaseModel):
    status_counts: dict[AuditStatus, int]
    reason_counts: dict[ReasonCode, int]
    total_audited: int = Field(ge=0)
    catalog_total: int = Field(ge=0)
    progress_ratio: float = Field(ge=0, le=1)
    latest_scanned_at: datetime | None = None


class SubtitleAuditItem(BaseModel):
    id: int
    subtitle_id: UUID
    movie_id: UUID
    canonical_code: str
    language: str
    file_path: str
    audit_version: str
    status: AuditStatus
    score: int = Field(ge=0, le=100)
    reason_codes: list[ReasonCode]
    metrics: dict[str, int | float | bool | str | None]
    expected_duration_seconds: float | None = None
    duration_source: str | None = None
    duration_confidence: Literal["unknown", "low", "medium", "high"]
    scanned_at: datetime


class SubtitleAuditPageResponse(BaseModel):
    items: list[SubtitleAuditItem]
    total: int = Field(ge=0)
    page: int = Field(ge=1, le=MAX_AUDIT_OFFSET + 1)
    page_size: int = Field(ge=1, le=100)
    pages: int = Field(ge=1)
    accessible_pages: int = Field(ge=1)
