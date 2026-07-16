from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path

from orchestrator.job_logs import append_job_log
from orchestrator.models import JobStatus
from orchestrator.store import (
    NORMAL_TRANSLATION_ORIGIN,
    JobRecord,
    validate_verified_supabase_receipt,
)


@dataclass(frozen=True)
class AudioCleanupResult:
    outcome: str
    bytes_deleted: int = 0


def _log(job_dir: Path, message: str) -> None:
    try:
        append_job_log(job_dir, "audio-cleanup.log", message)
    except OSError:
        return


def delete_published_job_audio(
    job: JobRecord,
    jobs_root_mac: Path,
) -> AudioCleanupResult:
    try:
        if (
            job.status is not JobStatus.ENGLISH_SRT_READY
            or job.translation_origin != NORMAL_TRANSLATION_ORIGIN
            or job.artifact_status != "ready"
        ):
            raise ValueError("job is not a ready normal artifact")
        validate_verified_supabase_receipt(
            movie_code=job.normalized_movie_number,
            movie_uuid=job.catalog_movie_uuid,
            metadata_status=job.metadata_status,
            metadata_source=job.metadata_source,
            subtitle_id=job.published_subtitle_id,
            storage_path=job.published_storage_path,
            content_sha256=job.published_content_sha256,
            file_size=job.published_file_size,
        )
    except ValueError:
        _log(Path(job.job_dir_mac), "ineligible unpublished job; audio retained")
        return AudioCleanupResult("ineligible")

    root = Path(jobs_root_mac)
    expected_job_dir = root / job.normalized_movie_number
    expected_audio = expected_job_dir / "audio.wav"
    if (
        Path(job.job_dir_mac) != expected_job_dir
        or root.is_symlink()
        or expected_job_dir.is_symlink()
    ):
        _log(expected_job_dir, "unsafe path validation failed; audio retained")
        return AudioCleanupResult("unsafe")
    if job.audio_path_mac is not None and Path(job.audio_path_mac) != expected_audio:
        _log(expected_job_dir, "unsafe recorded audio path; audio retained")
        return AudioCleanupResult("unsafe")

    try:
        directory_fd = os.open(
            expected_job_dir,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
    except FileNotFoundError:
        _log(expected_job_dir, "missing audio.wav; cleanup already complete")
        return AudioCleanupResult("missing")
    except OSError as exc:
        _log(expected_job_dir, f"error opening job directory; audio retained: {exc}")
        return AudioCleanupResult("error")

    try:
        try:
            audio_stat = os.stat("audio.wav", dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            _log(expected_job_dir, "missing audio.wav; cleanup already complete")
            return AudioCleanupResult("missing")
        if not stat.S_ISREG(audio_stat.st_mode) or audio_stat.st_nlink != 1:
            _log(expected_job_dir, "unsafe audio.wav identity; audio retained")
            return AudioCleanupResult("unsafe")
        try:
            os.unlink("audio.wav", dir_fd=directory_fd)
        except OSError as exc:
            _log(expected_job_dir, f"error deleting audio.wav; audio retained: {exc}")
            return AudioCleanupResult("error")
    finally:
        os.close(directory_fd)

    _log(expected_job_dir, f"deleted audio.wav bytes={audio_stat.st_size}")
    return AudioCleanupResult("deleted", bytes_deleted=audio_stat.st_size)
