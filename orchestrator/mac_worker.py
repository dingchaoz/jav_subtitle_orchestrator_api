import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path

from orchestrator.job_logs import append_job_log
from orchestrator.job_snapshot import write_job_snapshot
from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobRecord, JobStore
from orchestrator.subtitle_quality import QualityReport, validate_translation_quality


def _append_job_log_safely(job_dir: Path, filename: str, message: str) -> None:
    try:
        append_job_log(job_dir, filename, message)
    except Exception:
        return


class MacDownloadWorker:
    def __init__(
        self,
        store: JobStore,
        adapter,
        max_download_attempts: int,
        worker_id: str = "mac-downloader-1",
    ) -> None:
        self.store = store
        self.adapter = adapter
        self.max_download_attempts = max_download_attempts
        self.worker_id = worker_id

    def _record_idle(self, *, error: str | None = None) -> None:
        try:
            self.store.record_worker_idle(
                self.worker_id,
                role="mac_downloader",
                stage="polling" if error is None else "error",
                last_error=error,
            )
        except Exception:
            return

    def process_one(self) -> bool:
        self.store.recover_interrupted_downloads(self.max_download_attempts)
        job = self.store.claim_next_download_job()
        if job is None:
            self._record_idle()
            return False
        try:
            self.store.record_worker_processing(
                self.worker_id,
                role="mac_downloader",
                job=job,
                stage=job.status.value,
            )
        except Exception:
            pass
        error: str | None = None
        try:
            self._process_job(job)
        except Exception as exc:
            error = str(exc)
            self._record_failure(job, error)
        finally:
            self._record_idle(error=error)
        return True

    def _process_job(self, job: JobRecord) -> None:
        paths = build_job_paths(
            job.normalized_movie_number,
            self.store.jobs_root_mac,
            self.store.jobs_root_windows,
        )
        Path(paths.job_dir_mac).mkdir(parents=True, exist_ok=True)
        write_job_snapshot(job)

        _append_job_log_safely(
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

        _append_job_log_safely(
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
        _append_job_log_safely(
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


class MacTranslationQualityError(RuntimeError):
    pass


class MacTranslationUnhealthyError(RuntimeError):
    pass


class MacTranslationWorker:
    def __init__(
        self,
        store: JobStore,
        translator,
        max_translation_attempts: int,
        worker_id: str,
        lease_seconds: int,
        quality_failure_limit: int = 3,
    ) -> None:
        self.store = store
        self.translator = translator
        self.max_translation_attempts = max_translation_attempts
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.quality_failure_limit = quality_failure_limit
        self.consecutive_quality_failures = 0

    def _record_idle(self, *, error: str | None = None) -> None:
        try:
            self.store.record_worker_idle(
                self.worker_id,
                role="mac_translator",
                stage="polling" if error is None else "error",
                last_error=error,
            )
        except Exception:
            return

    def process_one(self) -> bool:
        if self.consecutive_quality_failures >= self.quality_failure_limit:
            raise MacTranslationUnhealthyError(
                "Mac translation worker stopped after "
                f"{self.consecutive_quality_failures} consecutive quality failures"
            )
        self.store.recover_expired_translation_leases(self.max_translation_attempts)
        job = self.store.claim_next_translation_job(self.worker_id, self.lease_seconds)
        if job is None:
            self._record_idle()
            return False
        try:
            self.store.record_worker_processing(
                self.worker_id,
                role="mac_translator",
                job=job,
                stage=JobStatus.TRANSLATING.value,
            )
        except Exception:
            pass
        error: str | None = None
        try:
            self._process_job(job)
        except Exception as exc:
            error = str(exc)
            if isinstance(exc, MacTranslationQualityError):
                self.consecutive_quality_failures += 1
            updated = self.store.fail_mac_translation(
                job.id,
                self.worker_id,
                str(exc),
                self.max_translation_attempts,
                permanent=isinstance(exc, MacTranslationQualityError),
            )
            write_job_snapshot(updated)
            _append_job_log_safely(
                Path(job.job_dir_mac),
                "mac-translation.log",
                f"failed {job.id}: {exc}",
            )
        finally:
            self._record_idle(error=error)
        return True

    def _process_job(self, job: JobRecord) -> None:
        paths = build_job_paths(
            job.normalized_movie_number,
            self.store.jobs_root_mac,
            self.store.jobs_root_windows,
        )
        if paths.english_srt_path_mac.exists():
            self._quarantine(paths.english_srt_path_mac, "stale")
        _append_job_log_safely(
            paths.job_dir_mac,
            "mac-translation.log",
            f"translating {job.id}",
        )
        self.translator.translate_to_english(
            paths.japanese_srt_path_mac,
            paths.english_srt_path_mac,
        )
        report = validate_translation_quality(
            paths.japanese_srt_path_mac,
            paths.english_srt_path_mac,
        )
        self._write_quality_log(paths.job_dir_mac, report)
        if not report.passed:
            self._quarantine(paths.english_srt_path_mac, "quality")
            raise MacTranslationQualityError(
                "quality_gate_failed:" + ",".join(report.reason_codes)
            )
        updated = self.store.complete_mac_translation(
            job.id,
            self.worker_id,
            lambda path: Path(path).exists(),
        )
        self.consecutive_quality_failures = 0
        write_job_snapshot(updated)
        _append_job_log_safely(
            paths.job_dir_mac,
            "mac-translation.log",
            f"english_srt_ready {job.id}",
        )

    def _write_quality_log(self, job_dir: Path, report: QualityReport) -> None:
        payload = report.as_dict()
        dominant = str(payload.pop("dominant_normalized_text", ""))
        payload["dominant_normalized_text_sha256"] = (
            hashlib.sha256(dominant.encode("utf-8")).hexdigest() if dominant else None
        )
        _append_job_log_safely(
            job_dir,
            "quality.log",
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
        )

    def _quarantine(self, english_srt: Path, reason: str) -> Path:
        rejected_dir = english_srt.parent / "rejected"
        rejected_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        rejected = rejected_dir / (
            f"{english_srt.stem}.rejected-{reason}-{timestamp}.srt"
        )
        english_srt.replace(rejected)
        return rejected

def run_forever(worker: MacDownloadWorker, poll_interval_seconds: int = 10) -> None:
    while True:
        worker.process_one()
        time.sleep(poll_interval_seconds)


def run_translation_forever(
    worker: MacTranslationWorker,
    poll_interval_seconds: int = 10,
) -> None:
    while True:
        worker.process_one()
        time.sleep(poll_interval_seconds)
