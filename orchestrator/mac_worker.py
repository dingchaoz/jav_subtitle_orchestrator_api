import hashlib
import json
import time
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
    ) -> None:
        if (publisher is None) != (catalog_sync_client is None):
            raise ValueError(
                "publisher and catalog sync client must be configured together"
            )
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
        self.max_catalog_sync_attempts = max_catalog_sync_attempts
        self.catalog_sync_retry_seconds = catalog_sync_retry_seconds
        self.consecutive_quality_failures = 0
        self.historical_quality_failures = 0

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
        self.store.recover_expired_publication_leases(
            self.max_publish_attempts,
            self.publish_retry_seconds,
        )
        self.store.recover_expired_catalog_sync_leases(
            self.max_catalog_sync_attempts,
            self.catalog_sync_retry_seconds,
        )
        ready_historical = self.store.find_historical_ready_to_finalize()
        if ready_historical is not None:
            self._finalize_ready_historical(ready_historical)
            return True
        if self.publisher is not None:
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
        if job.status is JobStatus.CATALOG_SYNCING:
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
            if self.publisher is None:
                return True
            current = self.store.get_job(job_id)
            if current is None or current.status is not JobStatus.PUBLISH_PENDING:
                return True
        elif current.status not in {
            JobStatus.PUBLISH_PENDING,
            JobStatus.CATALOG_SYNC_PENDING,
        }:
            raise RuntimeError(
                f"exact job {job_id} is not claimable from stage "
                f"{current.status.value}"
            )

        if self.publisher is None:
            raise RuntimeError(
                f"exact job {job_id} requires a publisher from stage publish_pending"
            )
        if current.status is JobStatus.PUBLISH_PENDING:
            publication = self.store.claim_publication_job(
                self.worker_id,
                self.lease_seconds,
                job_id=job_id,
            )
            if publication is None:
                raise RuntimeError(
                    f"exact job {job_id} is not claimable for publication"
                )
            self._process_claimed_publication(publication)
            current = self.store.get_job(job_id)
            if current is None or current.status is not JobStatus.CATALOG_SYNC_PENDING:
                return True
        catalog_job = self.store.claim_catalog_sync_job(
            self.worker_id,
            self.lease_seconds,
            job_id=job_id,
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
        ):
            quarantine = self.store.historical_source_quarantine_path(repair)
            if quarantine.exists():
                if (
                    self.store._sha256_regular_file(quarantine)
                    != repair.source_english_sha256
                ):
                    raise RuntimeError("historical_quarantine_collision")
                if paths.english_srt_path_mac.exists():
                    self._quarantine_unlocked(
                        paths.english_srt_path_mac,
                        "interrupted",
                    )
            elif paths.english_srt_path_mac.exists():
                if (
                    self.store._sha256_regular_file(paths.english_srt_path_mac)
                    != repair.source_english_sha256
                ):
                    raise RuntimeError("historical_source_english_changed")
                quarantine.parent.mkdir(parents=True, exist_ok=True)
                paths.english_srt_path_mac.replace(quarantine)
            elif (
                not quarantine.exists()
                or self.store._sha256_regular_file(quarantine)
                != repair.source_english_sha256
            ):
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
                candidate_sha256 = None
                if paths.english_srt_path_mac.exists():
                    candidate_sha256 = self.store._sha256_regular_file(
                        paths.english_srt_path_mac
                    )
                    self._quarantine_unlocked(
                        paths.english_srt_path_mac,
                        "quality",
                    )
                quality_error = MacTranslationQualityError(
                    "quality_gate_failed:" + ",".join(report.reason_codes)
                )
                quality_error.candidate_sha256 = candidate_sha256
                raise quality_error
            self._require_historical_preservation_locked(repair, paths)
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

    def _require_historical_preservation_locked(self, repair, paths) -> None:
        if (
            self.store._sha256_regular_file(paths.japanese_srt_path_mac)
            != repair.japanese_sha256
            or self.store._sha256_regular_file(paths.audio_path_mac)
            != repair.audio_sha256
        ):
            raise RuntimeError("preservation_hash_changed")

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
            ):
                self._require_historical_preservation_locked(repair, paths)
        except Exception:
            reason_code = "preservation_hash_changed"
            return self.store.mark_historical_permanent_failure(
                job.id,
                reason_code,
                worker_id=self.worker_id,
                lease_token=job.stage_lease_token,
            )
        if isinstance(exc, MacTranslationQualityError):
            self.historical_quality_failures += 1
            updated = self.store.mark_historical_permanent_failure(
                job.id,
                reason_code,
                getattr(exc, "candidate_sha256", None),
                worker_id=self.worker_id,
                lease_token=job.stage_lease_token,
            )
            if self.historical_quality_failures >= self.quality_failure_limit:
                self.store.pause_historical_lane("quality_failure_limit")
            return updated
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
        if self.publisher is None:
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
        try:
            self._process_publication(job)
        except Exception as exc:
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
            permanent = publisher_quality_failure or isinstance(
                exc, MacPublicationQualityError
            )
            historical = job.translation_origin == HISTORICAL_TRANSLATION_ORIGIN
            safe_error = str(exc) if permanent else "publication_failed"
            if permanent and not historical:
                self.consecutive_quality_failures += 1
            if publisher_quality_failure:
                paths = build_job_paths(
                    job.normalized_movie_number,
                    self.store.jobs_root_mac,
                    self.store.jobs_root_windows,
                )
                self._quarantine(paths.english_srt_path_mac, "quality")
            try:
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
            if historical and updated.status is JobStatus.FAILED:
                reason_code = (
                    safe_error
                    if permanent
                    else "publication_attempts_exhausted"
                )
                updated = self._finish_historical_failure(job, reason_code)
                if permanent:
                    self.historical_quality_failures += 1
                    if self.historical_quality_failures >= self.quality_failure_limit:
                        self.store.pause_historical_lane("quality_failure_limit")
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
            ):
                self._require_historical_preservation_locked(repair, paths)
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
                result = self.catalog_sync_client.sync(job.normalized_movie_number)
            except Exception as exc:
                reason_code = (
                    exc.reason_code
                    if isinstance(exc, CatalogSyncError)
                    else "catalog_sync_failed"
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
                    )
                except CatalogLeaseLostError:
                    error = "catalog_sync: catalog_lease_lost"
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"catalog_lease_lost {job.id}",
                    )
                else:
                    if (
                        job.translation_origin == HISTORICAL_TRANSLATION_ORIGIN
                        and updated.status is JobStatus.FAILED
                    ):
                        updated = self._finish_historical_failure(
                            job,
                            reason_code,
                        )
                    _write_job_snapshot_safely(updated)
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"catalog_sync_failed {job.id} reason_code={reason_code}",
                    )
            else:
                historical = (
                    job.translation_origin == HISTORICAL_TRANSLATION_ORIGIN
                )
                if historical:
                    repair = self.store.get_historical_repair(job.id)
                    paths = build_job_paths(
                        job.normalized_movie_number,
                        self.store.jobs_root_mac,
                        self.store.jobs_root_windows,
                    )
                    try:
                        if repair is None:
                            raise RuntimeError("historical_repair_missing")
                        with exclusive_job_files_lock(
                            self.store.jobs_root_mac,
                            job.normalized_movie_number,
                            blocking=True,
                        ):
                            self._require_historical_preservation_locked(
                                repair, paths
                            )
                    except Exception:
                        error = "catalog_sync: preservation_hash_changed"
                        updated = self.store.mark_historical_permanent_failure(
                            job.id,
                            "preservation_hash_changed",
                        )
                        _write_job_snapshot_safely(self.store.get_job(job.id))
                        _append_job_log_safely(
                            Path(job.job_dir_mac),
                            "mac-translation.log",
                            f"historical_failed {job.id} "
                            "reason_code=preservation_hash_changed",
                        )
                        return True
                try:
                    updated = self.store.complete_catalog_sync(
                        job.id,
                        self.worker_id,
                        lease_token=job.catalog_lease_token,
                        canonical_code=result.canonical_code,
                        d1_rows_updated=result.d1_rows_updated,
                        subtitle_count=result.subtitle_count,
                        kv_keys_deleted=result.kv_keys_deleted,
                    )
                except CatalogLeaseLostError:
                    error = "catalog_sync: catalog_lease_lost"
                    _append_job_log_safely(
                        Path(job.job_dir_mac),
                        "mac-translation.log",
                        f"catalog_lease_lost {job.id}",
                    )
                else:
                    if historical:
                        repair = self.store.get_historical_repair(job.id)
                        if (
                            repair is not None
                            and repair.state is HistoricalRepairState.RUNNING
                        ):
                            self.store.mark_historical_success(
                                job.id,
                                job.published_content_sha256,
                            )
                        self.historical_quality_failures = 0
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

    def _finish_historical_failure(
        self,
        job: JobRecord,
        reason_code: str,
    ):
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
            ):
                self._require_historical_preservation_locked(repair, paths)
        except Exception:
            reason_code = "preservation_hash_changed"
        self.store.mark_historical_permanent_failure(job.id, reason_code)
        refreshed = self.store.get_job(job.id)
        assert refreshed is not None
        return refreshed

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
