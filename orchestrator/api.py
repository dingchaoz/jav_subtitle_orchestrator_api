from collections.abc import Callable
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from orchestrator.dashboard import (
    build_dashboard_state,
    build_job_browser,
    build_job_detail,
    dashboard_html,
    list_job_logs,
    read_job_log_tail,
)

from orchestrator.models import (
    BatchJobResponse,
    DashboardStateResponse,
    JobBrowserResponse,
    JobDetailResponse,
    JobLogTailResponse,
    JobLogsResponse,
    JobResponse,
    JobStatus,
    SubmitBatchRequest,
    SubmitJobRequest,
    WorkerCompleteRequest,
    WorkerFailedRequest,
    WorkerHeartbeatRequest,
    WorkerJobResponse,
    WorkerNextJobResponse,
    WorkerTranscriptionCompleteRequest,
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


def worker_mutation_http_error(
    exc: KeyError | PermissionError | FileNotFoundError,
) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail="job not found")
    if isinstance(exc, PermissionError):
        return HTTPException(status_code=409, detail="job is not claimed by worker")
    return HTTPException(status_code=409, detail=f"final file not found: {exc}")


def create_app(
    store: JobStore,
    *,
    worker_lease_seconds: int = 1800,
    max_worker_attempts: int = 3,
    final_file_exists: Callable[[str], bool] | None = None,
) -> FastAPI:
    app = FastAPI(title="JAV Subtitle Orchestrator")
    final_file_exists = final_file_exists or (lambda path: Path(path).exists())

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard_page() -> str:
        return dashboard_html()

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

    @app.get("/dashboard/state", response_model=DashboardStateResponse)
    def dashboard_state() -> DashboardStateResponse:
        return build_dashboard_state(store)

    @app.get("/jobs/browser", response_model=JobBrowserResponse)
    def jobs_browser(
        view: str = "active",
        q: str = "",
        page: int = 1,
        page_size: int = 50,
    ) -> JobBrowserResponse:
        return build_job_browser(
            store, view=view, q=q, page=page, page_size=page_size
        )

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    def get_job(job_id: str) -> JobResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job_response(job)

    @app.get("/jobs/{job_id}/detail", response_model=JobDetailResponse)
    def job_detail(job_id: str) -> JobDetailResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return build_job_detail(job)

    @app.get("/jobs/{job_id}/logs", response_model=JobLogsResponse)
    def job_logs(job_id: str) -> JobLogsResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return list_job_logs(job)

    @app.get(
        "/jobs/{job_id}/logs/{log_name}", response_model=JobLogTailResponse
    )
    def job_log_tail(
        job_id: str, log_name: str, tail: int = 200
    ) -> JobLogTailResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        try:
            return read_job_log_tail(job, log_name, tail)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="log not found") from exc

    @app.get("/worker/next-job", response_model=WorkerNextJobResponse)
    def next_job(worker_id: str) -> WorkerNextJobResponse:
        store.recover_expired_worker_leases(max_worker_attempts)
        job = store.claim_next_worker_job(worker_id, worker_lease_seconds)
        if job is None:
            store.record_worker_idle(
                worker_id,
                role="windows_transcriber",
                stage="polling",
            )
            return WorkerNextJobResponse(job=None)
        store.record_worker_processing(
            worker_id,
            role="windows_transcriber",
            job=job,
            stage=JobStatus.TRANSCRIPTION_CLAIMED.value,
        )
        return WorkerNextJobResponse(job=worker_job_response(job))

    @app.post("/worker/jobs/{job_id}/heartbeat", response_model=JobResponse)
    def heartbeat(job_id: str, request: WorkerHeartbeatRequest) -> JobResponse:
        if store.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        try:
            job = store.heartbeat(job_id, request.worker_id, request.stage, worker_lease_seconds)
        except (KeyError, PermissionError, FileNotFoundError) as exc:
            raise worker_mutation_http_error(exc) from exc
        store.record_worker_processing(
            request.worker_id,
            role="windows_transcriber",
            job=job,
            stage=request.stage.value,
        )
        return job_response(job)

    @app.post("/worker/jobs/{job_id}/complete", response_model=JobResponse)
    def complete(job_id: str, request: WorkerCompleteRequest) -> JobResponse:
        raise HTTPException(
            status_code=409,
            detail=(
                "Windows translation completion is disabled; "
                "use transcription-complete and let the Mac translation worker publish English"
            ),
        )

    @app.post(
        "/worker/jobs/{job_id}/transcription-complete",
        response_model=JobResponse,
    )
    def transcription_complete(
        job_id: str,
        request: WorkerTranscriptionCompleteRequest,
    ) -> JobResponse:
        try:
            job = store.complete_worker_transcription(
                job_id,
                request.worker_id,
                request.japanese_srt_path_windows,
                final_file_exists,
            )
        except (KeyError, PermissionError, FileNotFoundError) as exc:
            raise worker_mutation_http_error(exc) from exc
        store.record_worker_idle(
            request.worker_id,
            role="windows_transcriber",
            stage=JobStatus.TRANSCRIPTION_DONE.value,
        )
        return job_response(job)

    @app.post("/worker/jobs/{job_id}/failed", response_model=JobResponse)
    def failed(job_id: str, request: WorkerFailedRequest) -> JobResponse:
        try:
            job = store.fail_worker_job(
                job_id,
                request.worker_id,
                request.stage,
                request.error,
                max_worker_attempts,
                permanent=request.permanent,
            )
        except (KeyError, PermissionError, FileNotFoundError) as exc:
            raise worker_mutation_http_error(exc) from exc
        store.record_worker_idle(
            request.worker_id,
            role="windows_transcriber",
            stage=job.status.value,
            last_error=request.error,
        )
        return job_response(job)

    return app
