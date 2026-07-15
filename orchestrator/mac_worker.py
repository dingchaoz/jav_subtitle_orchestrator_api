import hashlib
import json
import os
import stat
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from orchestrator.audio_lock import exclusive_audio_job_lock
from orchestrator.catalog_sync import CatalogSyncError
from orchestrator.job_logs import append_job_log
from orchestrator.job_snapshot import write_job_snapshot
from orchestrator.job_files_lock import exclusive_job_files_lock
from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import (
    HISTORICAL_TRANSLATION_ORIGIN,
    NORMAL_TRANSLATION_ORIGIN,
    CatalogLeaseLostError,
    HistoricalRepairActivationError,
    HistoricalRepairState,
    JobRecord,
    JobStore,
    StageLeaseLostError,
)
from orchestrator.subtitle_quality import (
    QualityReport,
    SubtitleQualityGateError,
    validate_translation_quality,
)


def _append_job_log_safely(job_dir: Path, filename: str, message: str) -> None:
    try:
        append_job_log(job_dir, filename, message)
    except Exception:
        return


def _write_job_snapshot_safely(job: JobRecord) -> None:
    try:
        write_job_snapshot(job)
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
        _write_job_snapshot_safely(job)

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
        _write_job_snapshot_safely(updated)

        _append_job_log_safely(
            paths.job_dir_mac,
            "mac-download.log",
            f"downloading_audio {job.normalized_movie_number}",
        )
        with exclusive_audio_job_lock(
            self.store.jobs_root_mac,
            job.normalized_movie_number,
            blocking=True,
        ):
            self.adapter.download_audio(
                job.normalized_movie_number,
                paths.audio_path_mac,
            )
            updated = self.store.update_download_status(
                job.id,
                JobStatus.AUDIO_READY,
                metadata_path_mac=str(paths.metadata_path_mac),
                audio_path_mac=str(paths.audio_path_mac),
                audio_path_windows=paths.audio_path_windows,
            )
        _write_job_snapshot_safely(updated)
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
        _write_job_snapshot_safely(updated)


class MacTranslationQualityError(RuntimeError):
    pass


class MacPublicationQualityError(RuntimeError):
    pass


class MacTranslationUnhealthyError(RuntimeError):
    pass


@dataclass(frozen=True)
class HistoricalQualityQuarantine:
    reason_code: str
    candidate_sha256: str
    rejected_basename: str
    stage: str


class MacTranslationWorker:
    def __init__(
        self,
        store: JobStore,
        translator,
        max_translation_attempts: int,
        worker_id: str,
        lease_seconds: int,
        quality_failure_limit: int = 3,
        publisher=None,
        max_publish_attempts: int = 10,
        publish_retry_seconds: int = 30,
        catalog_sync_client=None,
        max_catalog_sync_attempts: int = 10,
        catalog_sync_retry_seconds: int = 30,
        catalog_sync_max_retry_seconds: int = 900,
    ) -> None:
        self.store = store
        self.translator = translator
        self.max_translation_attempts = max_translation_attempts
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.quality_failure_limit = quality_failure_limit
        self.publisher = publisher
        self.max_publish_attempts = max_publish_attempts
        self.publish_retry_seconds = publish_retry_seconds
        self.catalog_sync_client = catalog_sync_client
        self.publication_pipeline_configured = bool(
            publisher is not None
            and catalog_sync_client is not None
            and getattr(
                catalog_sync_client,
                "public_visibility_verification_enabled",
                False,
            )
        )
        self.max_catalog_sync_attempts = max_catalog_sync_attempts
        self.catalog_sync_retry_seconds = catalog_sync_retry_seconds
        self.catalog_sync_max_retry_seconds = catalog_sync_max_retry_seconds
        self.consecutive_quality_failures = 0
        self.historical_quality_failures = (
            self.store.historical_lane_state().consecutive_quality_failures
        )

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

    def _recover_historical_marker_before_generic_leases(self) -> bool:
        job = self.store.find_recoverable_historical_marker_job()
        if job is None:
            return False
        repair = self.store.get_historical_repair(job.id)
        if repair is None or repair.state is not HistoricalRepairState.RUNNING:
            return False
        paths = build_job_paths(
            job.normalized_movie_number,
            self.store.jobs_root_mac,
            self.store.jobs_root_windows,
        )
        try:
            with exclusive_job_files_lock(
                self.store.jobs_root_mac,
                job.normalized_movie_number,
                blocking=True,
            ) as files_lock:
                try:
                    marker = self._find_historical_quality_quarantine_locked(
                        files_lock,
                        repair,
                        paths,
                        stages={"translation", "publication", "publisher"},
                    )
                except (OSError, RuntimeError) as marker_error:
                    try:
                        self._require_historical_preservation_locked(
                            files_lock, repair, paths
                        )
                    except (OSError, RuntimeError):
                        reason_code = "preservation_hash_changed"
                        outcome = self.store.complete_historical_marker_recovery(
                            job.id,
                            reason_code=reason_code,
                            candidate_sha256=None,
                            quality_failure_limit=self.quality_failure_limit,
                        )
                        marker = None
                    else:
                        raise marker_error
                else:
                    outcome = None
                if marker is None:
                    if outcome is None:
                        return False
                else:
                    try:
                        self._require_historical_preservation_locked(
                            files_lock, repair, paths
                        )
                    except (OSError, RuntimeError):
                        reason_code = "preservation_hash_changed"
                        candidate_sha256 = marker.candidate_sha256
                    else:
                        reason_code = marker.reason_code
                        candidate_sha256 = marker.candidate_sha256
                    outcome = self.store.complete_historical_marker_recovery(
                        job.id,
                        reason_code=reason_code,
                        candidate_sha256=candidate_sha256,
                        quality_failure_limit=self.quality_failure_limit,
                    )
        except (OSError, RuntimeError):
            self.store.pause_historical_lane("quarantine_failed")
            self._record_idle(error="quarantine_failed")
            return True
        if outcome is None:
            return True
        self.historical_quality_failures = (
            outcome.consecutive_quality_failures
        )
        _write_job_snapshot_safely(outcome.job)
        _append_job_log_safely(
            paths.job_dir_mac,
            "mac-translation.log",
            f"historical_marker_recovered {job.id} reason_code={reason_code}",
        )
        return True

    def process_one(self) -> bool:
        if self._recover_historical_marker_before_generic_leases():
            return True
        if self.consecutive_quality_failures >= self.quality_failure_limit:
            raise MacTranslationUnhealthyError(
                "Mac translation worker stopped after "
                f"{self.consecutive_quality_failures} consecutive quality failures"
            )
        self.store.reconcile_orphaned_historical_repairs(
            max_translation_attempts=self.max_translation_attempts,
            max_publish_attempts=self.max_publish_attempts,
            max_catalog_sync_attempts=self.max_catalog_sync_attempts,
            quality_failure_limit=self.quality_failure_limit,
        )
        self.store.recover_expired_translation_leases(self.max_translation_attempts)
        self.store.recover_expired_publication_leases(
            self.max_publish_attempts,
            self.publish_retry_seconds,
        )
        self.store.recover_expired_catalog_sync_leases(
            self.max_catalog_sync_attempts,
            self.catalog_sync_retry_seconds,
            self.catalog_sync_max_retry_seconds,
        )
        if (
            self.store.has_due_historical_repair()
            and not self.publication_pipeline_configured
        ):
            lane = self.store.historical_lane_state()
            if (
                not lane.paused
                or lane.reason_code != "publication_configuration_missing"
            ):
                self.store.pause_historical_lane(
                    "publication_configuration_missing"
                )
        lane = self.store.historical_lane_state()
        ready_historical = self.store.find_historical_ready_to_finalize()
        if (
            ready_historical is not None
            and self.publication_pipeline_configured
            and not lane.paused
        ):
            self._finalize_ready_historical(ready_historical)
            return True
        if self.publication_pipeline_configured:
            inflight = self.store.claim_inflight_historical_stage(
                self.worker_id,
                self.lease_seconds,
            )
            if inflight is not None:
                return self._process_claimed_stage(inflight)
            normal_stage = self.store.claim_normal_catalog_or_publication(
                self.worker_id,
                self.lease_seconds,
            )
            if normal_stage is not None:
                return self._process_claimed_stage(normal_stage)
        job = self.store.claim_next_translation_job(
            self.worker_id,
            self.lease_seconds,
            origin=NORMAL_TRANSLATION_ORIGIN,
        )
        if job is not None:
            return self._process_claimed_translation(job)
        lane = self.store.historical_lane_state()
        if not lane.paused and not self.store.has_claimable_normal_work():
            if not self.publication_pipeline_configured:
                error = "publication_configuration_missing"
                self._record_idle(error=error)
                return False
            try:
                job = self.store.claim_next_historical_repair(
                    self.worker_id,
                    self.lease_seconds,
                )
            except HistoricalRepairActivationError as exc:
                self._record_idle(error=f"historical_repair: {exc}")
                return True
            if job is not None:
                return self._process_claimed_translation(job)
        if (
            lane.paused
            and lane.reason_code == "publication_configuration_missing"
        ):
            self._record_idle(error="publication_configuration_missing")
            return False
        self._record_idle()
        return False

    def _finalize_ready_historical(self, job: JobRecord) -> None:
        repair = self.store.get_historical_repair(job.id)
        if repair is None:
            raise RuntimeError("historical_repair_missing")
        try:
            self.store.validate_historical_preservation(repair)
        except (OSError, RuntimeError):
            self.store.mark_historical_permanent_failure(
                job.id,
                "preservation_hash_changed",
            )
            return
        self.store.mark_historical_success(
            job.id,
            job.published_content_sha256,
        )

    def _process_claimed_stage(self, job: JobRecord) -> bool:
        if job.status is JobStatus.PUBLISHING:
            return self._process_claimed_publication(job)
        if (
            job.status is JobStatus.ENGLISH_SRT_READY
            and job.artifact_status == "ready"
            and job.catalog_sync_status == "pending"
            and job.catalog_lease_token is not None
        ):
            return self._process_claimed_catalog_sync(job)
        raise RuntimeError("claimed Mac stage is invalid")

    def process_job_id(self, job_id: str) -> bool:
        if self.consecutive_quality_failures >= self.quality_failure_limit:
            raise MacTranslationUnhealthyError(
                "Mac translation worker stopped after consecutive quality failures"
            )
        current = self.store.get_job(job_id)
        if current is None:
            raise RuntimeError(f"exact job {job_id} does not exist")
        if current.status is JobStatus.TRANSCRIPTION_DONE:
            job = self.store.claim_translation_job(
                job_id,
                self.worker_id,
                self.lease_seconds,
            )
            if job is None:
                raise RuntimeError(
                    f"exact job {job_id} is not claimable for translation"
                )
            self._process_claimed_translation(job)
            if not self.publication_pipeline_configured:
                return True
            current = self.store.get_job(job_id)
            if current is None or current.status is not JobStatus.PUBLISH_PENDING:
                return True
        elif current.status not in {
            JobStatus.PUBLISH_PENDING,
            JobStatus.ENGLISH_SRT_READY,
        }:
            raise RuntimeError(
                f"exact job {job_id} is not claimable from stage "
                f"{current.status.value}"
            )

        if not self.publication_pipeline_configured:
            raise RuntimeError(
                f"exact job {job_id} requires a publisher from stage publish_pending"
            )
        if current.status is JobStatus.PUBLISH_PENDING:
            publication = self.store.claim_publication_job(
                self.worker_id,
                self.lease_seconds,
                job_id=job_id,
                origin=current.translation_origin,
            )
            if publication is None:
                raise RuntimeError(
                    f"exact job {job_id} is not claimable for publication"
                )
            self._process_claimed_publication(publication)
            current = self.store.get_job(job_id)
            if (
                current is None
                or current.status is not JobStatus.ENGLISH_SRT_READY
                or current.catalog_sync_status != "pending"
            ):
                return True
        catalog_job = self.store.claim_catalog_sync_job(
            self.worker_id,
            self.lease_seconds,
            job_id=job_id,
            origin=current.translation_origin,
        )
        if catalog_job is None:
            raise RuntimeError(f"exact job {job_id} is not claimable for catalog sync")
        return self._process_claimed_catalog_sync(catalog_job)

    def _process_claimed_translation(self, job: JobRecord) -> bool:
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
            if job.translation_origin == HISTORICAL_TRANSLATION_ORIGIN:
                self._process_historical_translation(job)
            else:
                self._process_translation(job)
        except Exception as exc:
            if getattr(exc, "historical_quality_quarantine_complete", False):
                raise
            error = str(exc)
            if isinstance(exc, StageLeaseLostError):
                _append_job_log_safely(
                    Path(job.job_dir_mac),
                    "mac-translation.log",
                    f"translation_lease_lost {job.id}",
                )
                return True
            historical = job.translation_origin == HISTORICAL_TRANSLATION_ORIGIN
            if isinstance(exc, MacTranslationQualityError) and not historical:
                self.consecutive_quality_failures += 1
            if historical:
                try:
                    updated = self._fail_historical_translation(job, exc)
                except StageLeaseLostError:
                    error = "translation_lease_lost"
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"translation_lease_lost {job.id}",
                    )
                    return True
                safe_log_error = (
                    str(exc)
                    if isinstance(exc, MacTranslationQualityError)
                    else "translation_failed"
                )
                error = safe_log_error
            else:
                try:
                    updated = self.store.fail_mac_translation(
                        job.id,
                        self.worker_id,
                        str(exc),
                        self.max_translation_attempts,
                        permanent=isinstance(exc, MacTranslationQualityError),
                        lease_token=job.stage_lease_token,
                    )
                except StageLeaseLostError:
                    error = "translation_lease_lost"
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"translation_lease_lost {job.id}",
                    )
                    return True
            _write_job_snapshot_safely(updated)
            _append_job_log_safely(
                Path(job.job_dir_mac),
                "mac-translation.log",
                f"failed {job.id}: "
                f"{safe_log_error if historical else str(exc)}",
            )
        finally:
            self._record_idle(error=error)
        return True

    def _process_historical_translation(self, job: JobRecord) -> None:
        repair = self.store.get_historical_repair(job.id)
        if repair is None or repair.state is not HistoricalRepairState.RUNNING:
            raise RuntimeError("historical_repair_not_running")
        paths = build_job_paths(
            job.normalized_movie_number,
            self.store.jobs_root_mac,
            self.store.jobs_root_windows,
        )
        with exclusive_job_files_lock(
            self.store.jobs_root_mac,
            job.normalized_movie_number,
            blocking=True,
        ) as files_lock:
            try:
                recovered_quality = (
                    self._find_historical_quality_quarantine_locked(
                        files_lock,
                        repair,
                        paths,
                        stages={"translation"},
                    )
                )
            except Exception:
                outcome = self.store.fail_historical_translation_quarantine(
                    job.id,
                    self.worker_id,
                    lease_token=job.stage_lease_token,
                    retry_seconds=self.publish_retry_seconds,
                )
                _write_job_snapshot_safely(outcome.job)
                _append_job_log_safely(
                    paths.job_dir_mac,
                    "mac-translation.log",
                    f"failed {job.id}: quarantine_failed",
                )
                return
            if recovered_quality is not None:
                try:
                    outcome = self.store.fail_historical_translation_permanent(
                        job.id,
                        self.worker_id,
                        lease_token=job.stage_lease_token,
                        reason_code=recovered_quality.reason_code,
                        english_sha256=recovered_quality.candidate_sha256,
                        quality_failure_limit=self.quality_failure_limit,
                    )
                except Exception as exc:
                    exc.historical_quality_quarantine_complete = True
                    raise
                self.historical_quality_failures = (
                    outcome.consecutive_quality_failures
                )
                _write_job_snapshot_safely(outcome.job)
                _append_job_log_safely(
                    paths.job_dir_mac,
                    "mac-translation.log",
                    f"failed {job.id}: {recovered_quality.reason_code}",
                )
                return
            quarantine = self.store.historical_source_quarantine_path(repair)
            preserved_name = self.store.find_historical_source_quarantine_name(
                repair, files_lock.job_fd
            )
            try:
                current_hash = self._sha256_file_at(
                    files_lock.job_fd,
                    paths.english_srt_path_mac.name,
                )
            except FileNotFoundError:
                current_hash = None
            if current_hash is not None:
                if current_hash == repair.source_english_sha256:
                    desired_name = quarantine.name
                elif preserved_name is not None:
                    desired_name = (
                        f"{paths.english_srt_path_mac.stem}."
                        f"rejected-interrupted-{repair.id}-"
                        f"{current_hash[:12]}.srt"
                    )
                else:
                    raise RuntimeError("historical_source_english_changed")
                self._historical_quarantine_locked(
                    files_lock,
                    paths.english_srt_path_mac.name,
                    desired_name,
                    expected_sha256=current_hash,
                )
            elif preserved_name is None:
                raise RuntimeError("historical_source_english_missing")
            _append_job_log_safely(
                paths.job_dir_mac,
                "mac-translation.log",
                f"translating_historical {job.id}",
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
                reason_code = "quality_gate_failed:" + ",".join(
                    report.reason_codes
                )
                try:
                    candidate_sha256 = self._sha256_file_at(
                        files_lock.job_fd,
                        paths.english_srt_path_mac.name,
                    )
                except Exception:
                    outcome = (
                        self.store.fail_historical_translation_quarantine(
                            job.id,
                            self.worker_id,
                            lease_token=job.stage_lease_token,
                            retry_seconds=self.publish_retry_seconds,
                        )
                    )
                    _write_job_snapshot_safely(outcome.job)
                    _append_job_log_safely(
                        paths.job_dir_mac,
                        "mac-translation.log",
                        f"failed {job.id}: quarantine_failed",
                    )
                    return
                try:
                    self._require_historical_preservation_locked(
                        files_lock, repair, paths
                    )
                except (OSError, RuntimeError):
                    try:
                        self._historical_quarantine_locked(
                            files_lock,
                            paths.english_srt_path_mac.name,
                            f"{paths.english_srt_path_mac.stem}."
                            f"rejected-quality-{repair.id}-"
                            f"{candidate_sha256[:12]}.srt",
                            expected_sha256=candidate_sha256,
                            quality_marker={
                                "job_id": repair.job_id,
                                "repair_id": repair.id,
                                "stage": "translation",
                                "reason_code": reason_code,
                            },
                        )
                    except Exception:
                        pass
                    outcome = self.store.fail_historical_translation_permanent(
                        job.id,
                        self.worker_id,
                        lease_token=job.stage_lease_token,
                        reason_code="preservation_hash_changed",
                        english_sha256=candidate_sha256,
                    )
                    _write_job_snapshot_safely(outcome.job)
                    _append_job_log_safely(
                        paths.job_dir_mac,
                        "mac-translation.log",
                        f"failed {job.id}: preservation_hash_changed",
                    )
                    return
                try:
                    self._historical_quarantine_locked(
                        files_lock,
                        paths.english_srt_path_mac.name,
                        f"{paths.english_srt_path_mac.stem}."
                        f"rejected-quality-{repair.id}-"
                        f"{candidate_sha256[:12]}.srt",
                        expected_sha256=candidate_sha256,
                        quality_marker={
                            "job_id": repair.job_id,
                            "repair_id": repair.id,
                            "stage": "translation",
                            "reason_code": reason_code,
                        },
                    )
                except Exception:
                    outcome = (
                        self.store.fail_historical_translation_quarantine(
                            job.id,
                            self.worker_id,
                            lease_token=job.stage_lease_token,
                            retry_seconds=self.publish_retry_seconds,
                        )
                    )
                    _write_job_snapshot_safely(outcome.job)
                    _append_job_log_safely(
                        paths.job_dir_mac,
                        "mac-translation.log",
                        f"failed {job.id}: quarantine_failed",
                    )
                    return
                try:
                    outcome = self.store.fail_historical_translation_permanent(
                        job.id,
                        self.worker_id,
                        lease_token=job.stage_lease_token,
                        reason_code=reason_code,
                        english_sha256=candidate_sha256,
                        quality_failure_limit=self.quality_failure_limit,
                    )
                except Exception as exc:
                    exc.historical_quality_quarantine_complete = True
                    raise
                self.historical_quality_failures = (
                    outcome.consecutive_quality_failures
                )
                _write_job_snapshot_safely(outcome.job)
                _append_job_log_safely(
                    paths.job_dir_mac,
                    "mac-translation.log",
                    f"failed {job.id}: {reason_code}",
                )
                return
            self._require_historical_preservation_locked(
                files_lock, repair, paths
            )
            updated = self.store.complete_mac_translation_quality(
                job.id,
                self.worker_id,
                lambda path: Path(path).exists(),
                lease_token=job.stage_lease_token,
            )
        _write_job_snapshot_safely(updated)
        _append_job_log_safely(
            paths.job_dir_mac,
            "mac-translation.log",
            f"publish_pending {job.id}",
        )

    @staticmethod
    def _sha256_file_at(directory_fd: int, basename: str) -> str:
        if not basename or basename in {".", ".."} or "/" in basename:
            raise RuntimeError("historical_quarantine_path_invalid")
        fd = os.open(
            basename,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=directory_fd,
        )
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode):
                raise RuntimeError("historical_quarantine_source_invalid")
            digest = hashlib.sha256()
            while chunk := os.read(fd, 1024 * 1024):
                digest.update(chunk)
            after = os.fstat(fd)
            entry = os.stat(basename, dir_fd=directory_fd, follow_symlinks=False)
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ) or (before.st_dev, before.st_ino) != (entry.st_dev, entry.st_ino):
                raise RuntimeError("historical_quarantine_source_changed")
            return digest.hexdigest()
        finally:
            os.close(fd)

    @staticmethod
    def _read_small_regular_file_at(
        directory_fd: int,
        basename: str,
        *,
        max_bytes: int = 8192,
    ) -> bytes:
        if not basename or basename in {".", ".."} or "/" in basename:
            raise RuntimeError("historical_quality_marker_invalid")
        fd = os.open(
            basename,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=directory_fd,
        )
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size > max_bytes:
                raise RuntimeError("historical_quality_marker_invalid")
            chunks: list[bytes] = []
            size = 0
            while chunk := os.read(fd, min(4096, max_bytes + 1 - size)):
                chunks.append(chunk)
                size += len(chunk)
                if size > max_bytes:
                    raise RuntimeError("historical_quality_marker_invalid")
            after = os.fstat(fd)
            current = os.stat(
                basename,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            snapshot = (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            )
            if snapshot != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ) or (before.st_dev, before.st_ino) != (
                current.st_dev,
                current.st_ino,
            ):
                raise RuntimeError("historical_quality_marker_changed")
            return b"".join(chunks)
        finally:
            os.close(fd)

    @staticmethod
    def _quality_marker_basename(repair_id: str, candidate_sha256: str) -> str:
        if (
            not repair_id
            or repair_id in {".", ".."}
            or "/" in repair_id
            or len(candidate_sha256) != 64
            or any(char not in "0123456789abcdef" for char in candidate_sha256)
        ):
            raise RuntimeError("historical_quality_marker_invalid")
        return f".quality-rejected-{repair_id}-{candidate_sha256}.json"

    @staticmethod
    def _validate_quality_reason(reason_code: object) -> str:
        if not isinstance(reason_code, str) or not reason_code.startswith(
            "quality_gate_failed:"
        ):
            raise RuntimeError("historical_quality_marker_invalid")
        codes = reason_code.removeprefix("quality_gate_failed:").split(",")
        if not codes or any(
            not code
            or any(
                char not in "abcdefghijklmnopqrstuvwxyz0123456789_"
                for char in code
            )
            for code in codes
        ):
            raise RuntimeError("historical_quality_marker_invalid")
        return reason_code

    def _write_quality_marker_at(
        self,
        rejected_fd: int,
        marker_basename: str,
        payload: dict[str, object],
    ) -> None:
        encoded = json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        try:
            marker_fd = os.open(
                marker_basename,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=rejected_fd,
            )
        except FileExistsError:
            if self._read_small_regular_file_at(
                rejected_fd, marker_basename
            ) != encoded:
                raise RuntimeError("historical_quality_marker_conflict")
            return
        try:
            written = 0
            while written < len(encoded):
                count = os.write(marker_fd, encoded[written:])
                if count <= 0:
                    raise OSError("historical_quality_marker_write_failed")
                written += count
            os.fsync(marker_fd)
        finally:
            os.close(marker_fd)

    def _parse_quality_marker_at(
        self,
        rejected_fd: int,
        marker_basename: str,
        repair,
    ) -> HistoricalQualityQuarantine:
        try:
            payload = json.loads(
                self._read_small_regular_file_at(
                    rejected_fd, marker_basename
                ).decode("ascii")
            )
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RuntimeError("historical_quality_marker_invalid") from None
        if not isinstance(payload, dict) or set(payload) != {
            "version",
            "job_id",
            "repair_id",
            "stage",
            "reason_code",
            "candidate_sha256",
            "rejected_basename",
        }:
            raise RuntimeError("historical_quality_marker_invalid")
        candidate_sha256 = payload["candidate_sha256"]
        rejected_basename = payload["rejected_basename"]
        stage = payload["stage"]
        reason_code = self._validate_quality_reason(payload["reason_code"])
        if (
            payload["version"] != 1
            or payload["job_id"] != repair.job_id
            or payload["repair_id"] != repair.id
            or stage not in {"translation", "publication", "publisher"}
            or not isinstance(candidate_sha256, str)
            or not isinstance(rejected_basename, str)
            or not rejected_basename
            or rejected_basename in {".", ".."}
            or "/" in rejected_basename
            or marker_basename
            != self._quality_marker_basename(repair.id, candidate_sha256)
        ):
            raise RuntimeError("historical_quality_marker_invalid")
        if (
            self._sha256_file_at(rejected_fd, rejected_basename)
            != candidate_sha256
        ):
            raise RuntimeError("historical_quality_marker_hash_mismatch")
        return HistoricalQualityQuarantine(
            reason_code=reason_code,
            candidate_sha256=candidate_sha256,
            rejected_basename=rejected_basename,
            stage=stage,
        )

    def _find_historical_quality_quarantine_locked(
        self,
        files_lock,
        repair,
        paths,
        *,
        stages: set[str],
    ) -> HistoricalQualityQuarantine | None:
        try:
            rejected_fd = os.open(
                "rejected",
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=files_lock.job_fd,
            )
        except FileNotFoundError:
            return None
        try:
            prefix = f".quality-rejected-{repair.id}-"
            markers = sorted(
                name
                for name in os.listdir(rejected_fd)
                if name.startswith(prefix) and name.endswith(".json")
            )
            for marker_basename in markers:
                marker = self._parse_quality_marker_at(
                    rejected_fd, marker_basename, repair
                )
                if marker.stage not in stages:
                    raise RuntimeError(
                        "historical_quality_marker_stage_mismatch"
                    )
                try:
                    canonical_hash = self._sha256_file_at(
                        files_lock.job_fd,
                        paths.english_srt_path_mac.name,
                    )
                except FileNotFoundError:
                    canonical_hash = None
                if canonical_hash is not None:
                    if canonical_hash != marker.candidate_sha256:
                        raise RuntimeError(
                            "historical_quality_canonical_changed"
                        )
                    self._historical_quarantine_locked(
                        files_lock,
                        paths.english_srt_path_mac.name,
                        marker.rejected_basename,
                        expected_sha256=marker.candidate_sha256,
                        quality_marker={
                            "job_id": repair.job_id,
                            "repair_id": repair.id,
                            "stage": marker.stage,
                            "reason_code": marker.reason_code,
                        },
                    )
                return marker
        finally:
            os.close(rejected_fd)
        return None

    def _historical_quarantine_locked(
        self,
        files_lock,
        source_name: str,
        desired_name: str,
        *,
        expected_sha256: str,
        quality_marker: dict[str, str] | None = None,
    ) -> Path:
        if not desired_name or desired_name in {".", ".."} or "/" in desired_name:
            raise RuntimeError("historical_quarantine_path_invalid")
        files_lock.require_bound()
        source_hash = self._sha256_file_at(files_lock.job_fd, source_name)
        if source_hash != expected_sha256:
            raise RuntimeError("historical_quarantine_source_changed")
        try:
            os.mkdir("rejected", mode=0o755, dir_fd=files_lock.job_fd)
        except FileExistsError:
            pass
        else:
            os.fsync(files_lock.job_fd)
        rejected_fd = os.open(
            "rejected",
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=files_lock.job_fd,
        )
        try:
            chosen = desired_name
            collision_index = 0
            while True:
                try:
                    os.link(
                        source_name,
                        chosen,
                        src_dir_fd=files_lock.job_fd,
                        dst_dir_fd=rejected_fd,
                        follow_symlinks=False,
                    )
                    break
                except FileExistsError:
                    try:
                        existing_hash = self._sha256_file_at(rejected_fd, chosen)
                    except (OSError, RuntimeError):
                        existing_hash = None
                    if existing_hash == source_hash:
                        break
                    collision_index += 1
                    stem = Path(desired_name).stem
                    chosen = f"{stem}.collision-{source_hash[:12]}-{collision_index}.srt"
            os.fsync(rejected_fd)
            if quality_marker is not None:
                reason_code = self._validate_quality_reason(
                    quality_marker.get("reason_code")
                )
                stage = quality_marker.get("stage")
                repair_id = quality_marker.get("repair_id")
                job_id = quality_marker.get("job_id")
                if (
                    stage not in {"translation", "publication", "publisher"}
                    or not isinstance(repair_id, str)
                    or not isinstance(job_id, str)
                    or not job_id
                ):
                    raise RuntimeError("historical_quality_marker_invalid")
                marker_basename = self._quality_marker_basename(
                    repair_id, source_hash
                )
                self._write_quality_marker_at(
                    rejected_fd,
                    marker_basename,
                    {
                        "version": 1,
                        "job_id": job_id,
                        "repair_id": repair_id,
                        "stage": stage,
                        "reason_code": reason_code,
                        "candidate_sha256": source_hash,
                        "rejected_basename": chosen,
                    },
                )
                os.fsync(rejected_fd)
            if self._sha256_file_at(files_lock.job_fd, source_name) != source_hash:
                raise RuntimeError("historical_quarantine_source_changed")
            if self._sha256_file_at(rejected_fd, chosen) != source_hash:
                raise RuntimeError("historical_quarantine_destination_changed")
            os.unlink(source_name, dir_fd=files_lock.job_fd)
            os.fsync(files_lock.job_fd)
            files_lock.require_bound()
            return Path(files_lock.jobs_root) / files_lock.movie_code / "rejected" / chosen
        finally:
            os.close(rejected_fd)

    def _require_historical_preservation_locked(
        self, files_lock, repair, paths
    ) -> None:
        files_lock.require_bound()
        if (
            self.store._sha256_regular_file_at(
                files_lock.job_fd, paths.japanese_srt_path_mac.name
            )
            != repair.japanese_sha256
            or self.store._sha256_regular_file_at(
                files_lock.job_fd, paths.audio_path_mac.name
            )
            != repair.audio_sha256
        ):
            raise RuntimeError("preservation_hash_changed")
        files_lock.require_bound()

    def _quarantine_unlocked(self, english_srt: Path, reason: str) -> Path | None:
        if not english_srt.exists():
            return None
        rejected_dir = english_srt.parent / "rejected"
        rejected_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        rejected = rejected_dir / (
            f"{english_srt.stem}.rejected-{reason}-{timestamp}.srt"
        )
        english_srt.replace(rejected)
        return rejected

    def _fail_historical_translation(self, job: JobRecord, exc: Exception):
        repair = self.store.get_historical_repair(job.id)
        if repair is None:
            raise RuntimeError("historical_repair_missing")
        paths = build_job_paths(
            job.normalized_movie_number,
            self.store.jobs_root_mac,
            self.store.jobs_root_windows,
        )
        reason_code = (
            str(exc)
            if isinstance(exc, MacTranslationQualityError)
            else "translation_failed"
        )
        try:
            with exclusive_job_files_lock(
                self.store.jobs_root_mac,
                job.normalized_movie_number,
                blocking=True,
            ) as files_lock:
                self._require_historical_preservation_locked(
                    files_lock, repair, paths
                )
        except Exception:
            reason_code = "preservation_hash_changed"
            outcome = self.store.fail_historical_translation_permanent(
                job.id,
                self.worker_id,
                lease_token=job.stage_lease_token,
                reason_code=reason_code,
            )
            return outcome.job
        if isinstance(exc, MacTranslationQualityError):
            outcome = self.store.fail_historical_translation_permanent(
                job.id,
                self.worker_id,
                lease_token=job.stage_lease_token,
                reason_code=reason_code,
                english_sha256=getattr(exc, "candidate_sha256", None),
                quality_failure_limit=self.quality_failure_limit,
            )
            self.historical_quality_failures = (
                outcome.consecutive_quality_failures
            )
            return outcome.job
        return self.store.mark_historical_retry(
            job.id,
            reason_code,
            self.publish_retry_seconds,
            max_attempts=self.max_translation_attempts,
            worker_id=self.worker_id,
            lease_token=job.stage_lease_token,
        )

    def _process_translation(self, job: JobRecord) -> None:
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
        if not self.publication_pipeline_configured:
            updated = self.store.complete_mac_translation(
                job.id,
                self.worker_id,
                lambda path: Path(path).exists(),
                lease_token=job.stage_lease_token,
            )
            self.consecutive_quality_failures = 0
            ready_message = f"english_srt_ready {job.id}"
        else:
            updated = self.store.complete_mac_translation_quality(
                job.id,
                self.worker_id,
                lambda path: Path(path).exists(),
                lease_token=job.stage_lease_token,
            )
            ready_message = f"publish_pending {job.id}"
        _write_job_snapshot_safely(updated)
        _append_job_log_safely(
            paths.job_dir_mac,
            "mac-translation.log",
            ready_message,
        )

    def _process_claimed_publication(self, job: JobRecord) -> bool:
        try:
            self.store.record_worker_processing(
                self.worker_id,
                role="mac_translator",
                job=job,
                stage=JobStatus.PUBLISHING.value,
            )
        except Exception:
            pass
        error: str | None = None
        historical = job.translation_origin == HISTORICAL_TRANSLATION_ORIGIN
        try:
            if historical:
                repair = self.store.get_historical_repair(job.id)
                if repair is None:
                    raise RuntimeError("historical_repair_missing")
                paths = build_job_paths(
                    job.normalized_movie_number,
                    self.store.jobs_root_mac,
                    self.store.jobs_root_windows,
                )
                recovered_quality = None
                recovered_outcome = None
                try:
                    with exclusive_job_files_lock(
                        self.store.jobs_root_mac,
                        job.normalized_movie_number,
                        blocking=True,
                    ) as files_lock:
                        recovered_quality = (
                            self._find_historical_quality_quarantine_locked(
                                files_lock,
                                repair,
                                paths,
                                stages={"publication", "publisher"},
                            )
                        )
                        if recovered_quality is not None:
                            recovered_outcome = (
                                self.store.fail_historical_publication(
                                    job.id,
                                    self.worker_id,
                                    recovered_quality.reason_code,
                                    lease_token=job.stage_lease_token,
                                    max_publish_attempts=(
                                        self.max_publish_attempts
                                    ),
                                    retry_seconds=self.publish_retry_seconds,
                                    permanent=True,
                                    quality_failure_limit=(
                                        self.quality_failure_limit
                                    ),
                                )
                            )
                except Exception as recovery_exc:
                    if recovered_quality is not None:
                        if isinstance(recovery_exc, StageLeaseLostError):
                            error = "publishing: publication_lease_lost"
                            _append_job_log_safely(
                                Path(job.job_dir_mac),
                                "mac-translation.log",
                                f"publication_lease_lost {job.id}",
                            )
                            return True
                        recovery_exc.historical_quality_quarantine_complete = (
                            True
                        )
                        raise
                    outcome = (
                        self.store.fail_historical_publication_quarantine(
                            job.id,
                            self.worker_id,
                            lease_token=job.stage_lease_token,
                            retry_seconds=self.publish_retry_seconds,
                        )
                    )
                    _write_job_snapshot_safely(outcome.job)
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"publication_failed {job.id} quarantine_failed",
                    )
                    return True
                if recovered_outcome is not None:
                    self.historical_quality_failures = (
                        recovered_outcome.consecutive_quality_failures
                    )
                    _write_job_snapshot_safely(recovered_outcome.job)
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"publication_failed {job.id}",
                    )
                    return True
            self._process_publication(job)
        except Exception as exc:
            if getattr(exc, "historical_quality_quarantine_complete", False):
                raise
            error = "publishing: publication failed"
            if isinstance(exc, StageLeaseLostError):
                error = "publishing: publication_lease_lost"
                _append_job_log_safely(
                    Path(job.job_dir_mac),
                    "mac-translation.log",
                    f"publication_lease_lost {job.id}",
                )
                return True
            publisher_quality_failure = isinstance(exc, SubtitleQualityGateError)
            quality_failure = publisher_quality_failure or isinstance(
                exc, MacPublicationQualityError
            )
            preservation_failure = str(exc) == "preservation_hash_changed"
            repair = None
            paths = None
            if historical:
                repair = self.store.get_historical_repair(job.id)
                if repair is None:
                    raise RuntimeError("historical_repair_missing")
                paths = build_job_paths(
                    job.normalized_movie_number,
                    self.store.jobs_root_mac,
                    self.store.jobs_root_windows,
                )
                try:
                    with exclusive_job_files_lock(
                        self.store.jobs_root_mac,
                        job.normalized_movie_number,
                        blocking=True,
                    ) as files_lock:
                        self._require_historical_preservation_locked(
                            files_lock, repair, paths
                        )
                except (OSError, RuntimeError):
                    preservation_failure = True
            permanent = quality_failure or preservation_failure
            safe_error = (
                "preservation_hash_changed"
                if preservation_failure
                else str(exc)
                if permanent
                else "publication_failed"
            )
            quality_reason = str(exc) if quality_failure else None
            if permanent and not historical:
                self.consecutive_quality_failures += 1
            if publisher_quality_failure and not historical:
                paths = build_job_paths(
                    job.normalized_movie_number,
                    self.store.jobs_root_mac,
                    self.store.jobs_root_windows,
                )
                self._quarantine(paths.english_srt_path_mac, "quality")
            historical_quality_outcome = None
            if historical and quality_failure:
                assert repair is not None and paths is not None
                quarantine_stage = (
                    "publisher" if publisher_quality_failure else "publication"
                )
                quarantine_completed = False
                try:
                    with exclusive_job_files_lock(
                        self.store.jobs_root_mac,
                        job.normalized_movie_number,
                        blocking=True,
                    ) as files_lock:
                        candidate_hash = self._sha256_file_at(
                            files_lock.job_fd,
                            paths.english_srt_path_mac.name,
                        )
                        self._historical_quarantine_locked(
                            files_lock,
                            paths.english_srt_path_mac.name,
                            f"{paths.english_srt_path_mac.stem}."
                            f"rejected-quality-{quarantine_stage}-{repair.id}-"
                            f"{candidate_hash[:12]}.srt",
                            expected_sha256=candidate_hash,
                            quality_marker={
                                "job_id": repair.job_id,
                                "repair_id": repair.id,
                                "stage": quarantine_stage,
                                "reason_code": quality_reason,
                            },
                        )
                        quarantine_completed = True
                        historical_quality_outcome = (
                            self.store.fail_historical_publication(
                                job.id,
                                self.worker_id,
                                safe_error,
                                lease_token=job.stage_lease_token,
                                max_publish_attempts=self.max_publish_attempts,
                                retry_seconds=self.publish_retry_seconds,
                                permanent=True,
                                quality_failure_limit=(
                                    None
                                    if preservation_failure
                                    else self.quality_failure_limit
                                ),
                            )
                        )
                except Exception as quarantine_exc:
                    if quarantine_completed:
                        if isinstance(quarantine_exc, StageLeaseLostError):
                            error = "publishing: publication_lease_lost"
                            _append_job_log_safely(
                                Path(job.job_dir_mac),
                                "mac-translation.log",
                                f"publication_lease_lost {job.id}",
                            )
                            return True
                        quarantine_exc.historical_quality_quarantine_complete = (
                            True
                        )
                        raise
                    if preservation_failure:
                        outcome = self.store.fail_historical_publication(
                            job.id,
                            self.worker_id,
                            "preservation_hash_changed",
                            lease_token=job.stage_lease_token,
                            max_publish_attempts=self.max_publish_attempts,
                            retry_seconds=self.publish_retry_seconds,
                            permanent=True,
                        )
                        _write_job_snapshot_safely(outcome.job)
                        _append_job_log_safely(
                            Path(job.job_dir_mac),
                            "mac-translation.log",
                            f"publication_failed {job.id} "
                            "preservation_hash_changed",
                        )
                        return True
                    try:
                        outcome = (
                            self.store.fail_historical_publication_quarantine(
                                job.id,
                                self.worker_id,
                                lease_token=job.stage_lease_token,
                                retry_seconds=self.publish_retry_seconds,
                            )
                        )
                    except StageLeaseLostError:
                        error = "publishing: publication_lease_lost"
                        _append_job_log_safely(
                            Path(job.job_dir_mac),
                            "mac-translation.log",
                            f"publication_lease_lost {job.id}",
                        )
                        return True
                    _write_job_snapshot_safely(outcome.job)
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"publication_failed {job.id} quarantine_failed",
                    )
                    error = "publishing: quarantine_failed"
                    return True
            try:
                if historical_quality_outcome is not None:
                    outcome = historical_quality_outcome
                    updated = outcome.job
                    self.historical_quality_failures = (
                        outcome.consecutive_quality_failures
                    )
                elif historical:
                    outcome = self.store.fail_historical_publication(
                        job.id,
                        self.worker_id,
                        safe_error,
                        lease_token=job.stage_lease_token,
                        max_publish_attempts=self.max_publish_attempts,
                        retry_seconds=self.publish_retry_seconds,
                        permanent=permanent,
                        quality_failure_limit=(
                            self.quality_failure_limit
                            if quality_failure and not preservation_failure
                            else None
                        ),
                    )
                    updated = outcome.job
                    self.historical_quality_failures = (
                        outcome.consecutive_quality_failures
                    )
                else:
                    updated = self.store.fail_publication(
                        job.id,
                        self.worker_id,
                        safe_error,
                        max_publish_attempts=self.max_publish_attempts,
                        retry_seconds=self.publish_retry_seconds,
                        permanent=permanent,
                        lease_token=job.stage_lease_token,
                    )
            except StageLeaseLostError:
                error = "publishing: publication_lease_lost"
                _append_job_log_safely(
                    Path(job.job_dir_mac),
                    "mac-translation.log",
                    f"publication_lease_lost {job.id}",
                )
                return True
            _write_job_snapshot_safely(updated)
            _append_job_log_safely(
                Path(job.job_dir_mac),
                "mac-translation.log",
                f"publication_failed {job.id}",
            )
        finally:
            self._record_idle(error=error)
        return True

    def _process_publication(self, job: JobRecord) -> None:
        paths = build_job_paths(
            job.normalized_movie_number,
            self.store.jobs_root_mac,
            self.store.jobs_root_windows,
        )
        if job.translation_origin == HISTORICAL_TRANSLATION_ORIGIN:
            repair = self.store.get_historical_repair(job.id)
            if repair is None:
                raise RuntimeError("historical_repair_missing")
            with exclusive_job_files_lock(
                self.store.jobs_root_mac,
                job.normalized_movie_number,
                blocking=True,
            ) as files_lock:
                self._require_historical_preservation_locked(
                    files_lock, repair, paths
                )
                report = validate_translation_quality(
                    paths.japanese_srt_path_mac,
                    paths.english_srt_path_mac,
                )
                self._write_quality_log(paths.job_dir_mac, report)
                if not report.passed:
                    raise MacPublicationQualityError(
                        "quality_gate_failed:" + ",".join(report.reason_codes)
                    )
        else:
            report = validate_translation_quality(
                paths.japanese_srt_path_mac,
                paths.english_srt_path_mac,
            )
            self._write_quality_log(paths.job_dir_mac, report)
            if not report.passed:
                self._quarantine(paths.english_srt_path_mac, "quality")
                raise MacPublicationQualityError(
                    "quality_gate_failed:" + ",".join(report.reason_codes)
                )
        published = self.publisher.publish_english_ai(
            job.normalized_movie_number,
            paths.english_srt_path_mac,
            paths.metadata_path_mac,
        )
        if published.verified is not True:
            raise RuntimeError("Supabase publication was not verified")
        updated = self.store.complete_supabase_publication(
            job.id,
            self.worker_id,
            movie_uuid=published.movie_uuid,
            metadata_status=published.metadata_status,
            metadata_source=published.metadata_source,
            subtitle_id=published.subtitle_id,
            storage_path=published.storage_path,
            content_sha256=published.content_sha256,
            file_size=published.file_size,
            lease_token=job.stage_lease_token,
        )
        if job.translation_origin == NORMAL_TRANSLATION_ORIGIN:
            self.consecutive_quality_failures = 0
        else:
            self.historical_quality_failures = 0
            self.store.mark_historical_success(
                job.id,
                updated.published_content_sha256,
            )
        _write_job_snapshot_safely(updated)
        _append_job_log_safely(
            paths.job_dir_mac,
            "mac-translation.log",
            f"publish_verified {job.id} sha256={published.content_sha256} "
            f"size={published.file_size}",
        )

    def _process_claimed_catalog_sync(self, job: JobRecord) -> bool:
        try:
            self.store.record_worker_processing(
                self.worker_id,
                role="mac_translator",
                job=job,
                stage=JobStatus.CATALOG_SYNCING.value,
            )
        except Exception:
            pass
        error: str | None = None
        try:
            try:
                result = self.catalog_sync_client.sync(
                    job.normalized_movie_number,
                    expected_subtitle_id=job.published_subtitle_id,
                    expected_content_sha256=job.published_content_sha256,
                )
            except Exception as exc:
                reason_code = (
                    exc.reason_code if isinstance(exc, CatalogSyncError) else "catalog_sync_failed"
                )
                retryable = (
                    exc.retryable if isinstance(exc, CatalogSyncError) else True
                )
                http_status = (
                    exc.http_status if isinstance(exc, CatalogSyncError) else None
                )
                response_json = (
                    exc.response_json if isinstance(exc, CatalogSyncError) else None
                )
                error = f"catalog_sync: {reason_code}"
                try:
                    updated = self.store.fail_catalog_sync(
                        job.id,
                        self.worker_id,
                        reason_code,
                        lease_token=job.catalog_lease_token,
                        max_catalog_sync_attempts=self.max_catalog_sync_attempts,
                        retry_seconds=self.catalog_sync_retry_seconds,
                        max_retry_seconds=self.catalog_sync_max_retry_seconds,
                        retryable=retryable,
                        http_status=http_status,
                        response_json=response_json,
                    )
                except CatalogLeaseLostError:
                    error = "catalog_sync: catalog_lease_lost"
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"catalog_lease_lost {job.id}",
                    )
                else:
                    _write_job_snapshot_safely(updated)
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"catalog_sync_failed {job.id} reason_code={reason_code}",
                    )
            else:
                try:
                    updated = self.store.complete_catalog_sync(
                        job.id,
                        self.worker_id,
                        lease_token=job.catalog_lease_token,
                        canonical_code=result.canonical_code,
                        d1_rows_updated=result.d1_rows_updated,
                        subtitle_count=result.subtitle_count,
                        kv_keys_deleted=result.kv_keys_deleted,
                        http_status=result.diagnostic.http_status,
                        response_json=result.diagnostic.response_json,
                    )
                except CatalogLeaseLostError:
                    error = "catalog_sync: catalog_lease_lost"
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"catalog_lease_lost {job.id}",
                    )
                else:
                    _write_job_snapshot_safely(updated)
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"catalog_sync_verified {job.id} "
                        f"code={result.canonical_code} "
                        f"d1_rows={result.d1_rows_updated} "
                        f"subtitle_count={result.subtitle_count}",
                    )
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"english_srt_ready {job.id}",
                    )
        finally:
            self._record_idle(error=error)
        return True

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

    def _quarantine(self, english_srt: Path, reason: str) -> Path | None:
        with exclusive_job_files_lock(
            english_srt.parent.parent,
            english_srt.parent.name,
            blocking=True,
        ):
            if not english_srt.exists():
                return None
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
