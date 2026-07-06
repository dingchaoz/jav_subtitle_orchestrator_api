from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from orchestrator.callbacks import CallbackClient, CallbackNotifier, CallbackSender
from orchestrator.dashboard import (
    build_dashboard_state,
    build_job_detail,
    dashboard_html,
    list_job_logs,
    read_job_log_tail,
)
from orchestrator.models import (
    BatchJobResponse,
    DashboardStateResponse,
    ImportRequestedSubtitlesRequest,
    JobDetailResponse,
    JobLogTailResponse,
    JobLogsResponse,
    JobResponse,
    JobStatus,
    RequestedSubtitleImportResponse,
    SubmitBatchRequest,
    SubmitJobRequest,
    WorkerCompleteRequest,
    WorkerFailedRequest,
    WorkerHeartbeatRequest,
    WorkerJobResponse,
    WorkerNextJobResponse,
)
from orchestrator.subtitle_request_importer import RequestedSubtitleImportSelection
from orchestrator.store import JobRecord, JobStore


class EnglishAiPublisher(Protocol):
    def publish_english_ai(self, movie_code: str, srt_path: Path) -> object:
        ...


class RequestedSubtitleImporterProtocol(Protocol):
    def fetch_requested_subtitles(
        self,
        *,
        min_count: int,
        limit: int,
    ) -> RequestedSubtitleImportSelection:
        ...


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
    publisher: EnglishAiPublisher | None = None,
    requested_subtitle_importer: RequestedSubtitleImporterProtocol | None = None,
    callback_clients: dict[str, CallbackClient] | None = None,
    callback_sender: CallbackSender | None = None,
    callback_timeout_seconds: int = 10,
) -> FastAPI:
    app = FastAPI(title="JAV Subtitle Orchestrator")
    final_file_exists = final_file_exists or (lambda path: Path(path).exists())
    callback_clients = callback_clients or {}
    callback_notifier = CallbackNotifier(
        store,
        callback_clients,
        sender=callback_sender,
        timeout_seconds=callback_timeout_seconds,
    )

    def callback_client_key_from_request(request: Request) -> str | None:
        client_id = request.headers.get("cf-access-client-id")
        if client_id and client_id in callback_clients:
            return client_id
        return None

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard_page() -> str:
        return dashboard_html()

    @app.post("/jobs", response_model=JobResponse)
    def submit_job(request: Request, payload: SubmitJobRequest) -> JobResponse:
        result = store.submit_job(
            payload.movie_number,
            payload.priority,
            payload.force,
            callback_client_key=callback_client_key_from_request(request),
        )
        if result.kind == "invalid":
            raise HTTPException(status_code=422, detail="invalid movie_number")
        if result.kind == "conflict":
            raise HTTPException(
                status_code=409,
                detail=job_response(result.job).model_dump(mode="json"),
            )
        return job_response(result.job)

    @app.post("/jobs/batch", response_model=BatchJobResponse)
    def submit_batch(request: Request, payload: SubmitBatchRequest) -> BatchJobResponse:
        result = store.submit_batch(
            payload.movie_numbers,
            payload.priority,
            payload.force,
            callback_client_key=callback_client_key_from_request(request),
        )
        return BatchJobResponse(
            created=[job_response(item.job) for item in result.created],
            existing=[job_response(item.job) for item in result.existing if item.job is not None],
            invalid=[item.movie_number for item in result.invalid],
        )

    @app.post("/jobs/import-subtitle-requests", response_model=RequestedSubtitleImportResponse)
    def import_subtitle_requests(
        http_request: Request,
        payload: ImportRequestedSubtitlesRequest | None = None,
    ) -> RequestedSubtitleImportResponse:
        if requested_subtitle_importer is None:
            raise HTTPException(
                status_code=503,
                detail="requested subtitle importer is not configured",
            )
        import_request = payload or ImportRequestedSubtitlesRequest()
        try:
            selection = requested_subtitle_importer.fetch_requested_subtitles(
                min_count=import_request.min_count,
                limit=import_request.limit,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        result = store.submit_batch(
            [item.code for item in selection.imported],
            priority=import_request.priority,
            force=import_request.force,
            callback_client_key=callback_client_key_from_request(http_request),
        )
        return RequestedSubtitleImportResponse(
            requested=selection.requested,
            imported=selection.imported,
            skipped_available=selection.skipped_available,
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

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    def get_job(job_id: str) -> JobResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job_response(job)

    @app.get("/jobs/{job_id}/detail", response_model=JobDetailResponse)
    def get_job_detail(job_id: str) -> JobDetailResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return build_job_detail(job, store.get_latest_callback_event(job_id))

    @app.get("/jobs/{job_id}/logs", response_model=JobLogsResponse)
    def get_job_logs(job_id: str) -> JobLogsResponse:
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return list_job_logs(job)

    @app.get("/jobs/{job_id}/logs/{log_name}", response_model=JobLogTailResponse)
    def get_job_log_tail(
        job_id: str, log_name: str, tail: int = Query(default=200)
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
            return WorkerNextJobResponse(job=None)
        return WorkerNextJobResponse(job=worker_job_response(job))

    @app.post("/worker/jobs/{job_id}/heartbeat", response_model=JobResponse)
    def heartbeat(job_id: str, request: WorkerHeartbeatRequest) -> JobResponse:
        if store.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        try:
            job = store.heartbeat(job_id, request.worker_id, request.stage, worker_lease_seconds)
        except (KeyError, PermissionError, FileNotFoundError) as exc:
            raise worker_mutation_http_error(exc) from exc
        return job_response(job)

    @app.post("/worker/jobs/{job_id}/complete", response_model=JobResponse)
    def complete(job_id: str, request: WorkerCompleteRequest) -> JobResponse:
        try:
            job = store.complete_worker_job(
                job_id,
                request.worker_id,
                request.japanese_srt_path_windows,
                request.english_srt_path_windows,
                final_file_exists,
            )
        except (KeyError, PermissionError, FileNotFoundError) as exc:
            raise worker_mutation_http_error(exc) from exc
        if publisher is not None and job.english_srt_path_mac:
            publisher.publish_english_ai(
                job.normalized_movie_number,
                Path(job.english_srt_path_mac),
            )
            callback_notifier.notify_subtitle_ready(job)
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
            )
        except (KeyError, PermissionError, FileNotFoundError) as exc:
            raise worker_mutation_http_error(exc) from exc
        return job_response(job)

    return app
