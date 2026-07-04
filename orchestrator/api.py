from collections.abc import Callable
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from orchestrator.models import (
    BatchJobResponse,
    JobResponse,
    JobStatus,
    SubmitBatchRequest,
    SubmitJobRequest,
    WorkerCompleteRequest,
    WorkerFailedRequest,
    WorkerHeartbeatRequest,
    WorkerJobResponse,
    WorkerNextJobResponse,
)
from orchestrator.store import JobRecord, JobStore


def job_response(job: JobRecord) -> JobResponse:
    return JobResponse(
        id=job.id,
        movie_number=job.normalized_movie_number,
        status=job.status,
        job_dir_mac=job.job_dir_mac,
        job_dir_windows=job.job_dir_windows,
        error=job.error,
    )


def worker_job_response(job: JobRecord) -> WorkerJobResponse:
    job_dir_windows = job.job_dir_windows.rstrip("\\/")
    return WorkerJobResponse(
        id=job.id,
        movie_number=job.normalized_movie_number,
        audio_path_windows=job.audio_path_windows or f"{job_dir_windows}\\audio.wav",
        japanese_srt_path_windows=(
            job.japanese_srt_path_windows
            or f"{job_dir_windows}\\{job.normalized_movie_number}.Japanese.srt"
        ),
        english_srt_path_windows=(
            job.english_srt_path_windows
            or f"{job_dir_windows}\\{job.normalized_movie_number}.English.srt"
        ),
    )


def create_app(
    store: JobStore,
    *,
    worker_lease_seconds: int = 1800,
    max_worker_attempts: int = 3,
    final_file_exists: Callable[[str], bool] | None = None,
) -> FastAPI:
    app = FastAPI(title="JAV Subtitle Orchestrator")
    final_file_exists = final_file_exists or (lambda path: Path(path).exists())

    @app.post("/jobs", response_model=JobResponse)
    def submit_job(request: SubmitJobRequest) -> JobResponse:
        result = store.submit_job(request.movie_number, request.priority, request.force)
        if result.kind == "invalid":
            raise HTTPException(status_code=422, detail="invalid movie_number")
        if result.kind == "conflict":
            raise HTTPException(
                status_code=409,
                detail=job_response(result.job).model_dump(mode="json"),
            )
        return job_response(result.job)

    @app.post("/jobs/batch", response_model=BatchJobResponse)
    def submit_batch(request: SubmitBatchRequest) -> BatchJobResponse:
        result = store.submit_batch(request.movie_numbers, request.priority, request.force)
        return BatchJobResponse(
            created=[job_response(item.job) for item in result.created],
            existing=[job_response(item.job) for item in result.existing if item.job is not None],
            invalid=[item.movie_number for item in result.invalid],
        )

    @app.get("/jobs", response_model=list[JobResponse])
    def list_jobs(status: JobStatus | None = Query(default=None)) -> list[JobResponse]:
        return [job_response(job) for job in store.list_jobs(status)]

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    def get_job(job_id: str) -> JobResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job_response(job)

    @app.get("/worker/next-job", response_model=WorkerNextJobResponse)
    def next_job(worker_id: str) -> WorkerNextJobResponse:
        job = store.claim_next_worker_job(worker_id, worker_lease_seconds)
        if job is None:
            return WorkerNextJobResponse(job=None)
        return WorkerNextJobResponse(job=worker_job_response(job))

    @app.post("/worker/jobs/{job_id}/heartbeat", response_model=JobResponse)
    def heartbeat(job_id: str, request: WorkerHeartbeatRequest) -> JobResponse:
        job = store.heartbeat(job_id, request.worker_id, request.stage, worker_lease_seconds)
        return job_response(job)

    @app.post("/worker/jobs/{job_id}/complete", response_model=JobResponse)
    def complete(job_id: str, request: WorkerCompleteRequest) -> JobResponse:
        job = store.complete_worker_job(
            job_id,
            request.worker_id,
            request.japanese_srt_path_windows,
            request.english_srt_path_windows,
            final_file_exists,
        )
        return job_response(job)

    @app.post("/worker/jobs/{job_id}/failed", response_model=JobResponse)
    def failed(job_id: str, request: WorkerFailedRequest) -> JobResponse:
        job = store.fail_worker_job(
            job_id,
            request.worker_id,
            request.stage,
            request.error,
            max_worker_attempts,
        )
        return job_response(job)

    return app
