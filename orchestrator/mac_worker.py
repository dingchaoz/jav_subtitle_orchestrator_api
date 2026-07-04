import time
from pathlib import Path

from orchestrator.job_logs import append_job_log
from orchestrator.job_snapshot import write_job_snapshot
from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobRecord, JobStore


class MacDownloadWorker:
    def __init__(self, store: JobStore, adapter, max_download_attempts: int) -> None:
        self.store = store
        self.adapter = adapter
        self.max_download_attempts = max_download_attempts

    def process_one(self) -> bool:
        job = self.store.claim_next_download_job()
        if job is None:
            return False
        try:
            self._process_job(job)
        except Exception as exc:
            self._record_failure(job, str(exc))
        return True

    def _process_job(self, job: JobRecord) -> None:
        paths = build_job_paths(
            job.normalized_movie_number,
            self.store.jobs_root_mac,
            self.store.jobs_root_windows,
        )
        Path(paths.job_dir_mac).mkdir(parents=True, exist_ok=True)
        write_job_snapshot(job)

        append_job_log(
            paths.job_dir_mac,
            "mac-download.log",
            f"downloading_metadata {job.normalized_movie_number}",
        )
        self.adapter.download_metadata(job.normalized_movie_number, paths.metadata_path_mac)
        updated = self.store.update_download_status(
            job.id,
            JobStatus.DOWNLOADING_AUDIO,
            metadata_path_mac=str(paths.metadata_path_mac),
        )
        write_job_snapshot(updated)

        append_job_log(
            paths.job_dir_mac,
            "mac-download.log",
            f"downloading_audio {job.normalized_movie_number}",
        )
        self.adapter.download_audio(job.normalized_movie_number, paths.audio_path_mac)
        updated = self.store.update_download_status(
            job.id,
            JobStatus.AUDIO_READY,
            metadata_path_mac=str(paths.metadata_path_mac),
            audio_path_mac=str(paths.audio_path_mac),
            audio_path_windows=paths.audio_path_windows,
        )
        write_job_snapshot(updated)
        append_job_log(
            paths.job_dir_mac,
            "mac-download.log",
            f"audio_ready {job.normalized_movie_number}",
        )

    def _record_failure(self, job: JobRecord, error: str) -> None:
        next_attempts = job.attempt_count + 1
        next_status = (
            JobStatus.FAILED
            if next_attempts >= self.max_download_attempts
            else JobStatus.QUEUED
        )
        updated = self.store.record_download_failure(
            job.id,
            next_status,
            next_attempts,
            error,
        )
        write_job_snapshot(updated)


def run_forever(worker: MacDownloadWorker, poll_interval_seconds: int = 10) -> None:
    while True:
        worker.process_one()
        time.sleep(poll_interval_seconds)
