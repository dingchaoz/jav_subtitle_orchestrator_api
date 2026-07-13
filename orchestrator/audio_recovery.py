from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
import struct
import wave
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from orchestrator.movie_code import canonical_movie_code
from orchestrator.models import JobPaths, JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobRecord, JobStore


_LOWERCASE_HEX = frozenset("0123456789abcdef")


class AudioRecoveryError(RuntimeError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class AudioRecoveryReceipt:
    job_id: str
    movie_code: str
    status: JobStatus
    final_path: Path
    sha256: str
    size_bytes: int
    duration_seconds: float
    reused_final: bool


@dataclass(frozen=True)
class _ValidatedPcmWav:
    sha256: str
    size_bytes: int
    duration_seconds: float
    device: int
    inode: int


def _same_snapshot(before: os.stat_result, after: os.stat_result) -> bool:
    return (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
        before.st_mode,
        before.st_nlink,
    ) == (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
        after.st_mode,
        after.st_nlink,
    )


def _valid_expected_sha256(value: str) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in _LOWERCASE_HEX for character in value)
    )


def validate_pcm_wav(
    path: Path,
    *,
    expected_sha256: str,
    _while_open: Callable[[_ValidatedPcmWav, int, os.stat_result], None]
    | None = None,
) -> _ValidatedPcmWav:
    """Validate one immutable, canonical PCM WAV snapshot without exposing content."""
    if not _valid_expected_sha256(expected_sha256):
        raise AudioRecoveryError("invalid_expected_sha256")

    try:
        path_stat = path.lstat()
        if not stat.S_ISREG(path_stat.st_mode):
            raise AudioRecoveryError("audio_not_regular")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except AudioRecoveryError:
        raise
    except OSError:
        raise AudioRecoveryError("audio_unavailable") from None

    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise AudioRecoveryError("audio_not_regular")
        if not _same_snapshot(path_stat, before):
            raise AudioRecoveryError("audio_snapshot_changed")
        if before.st_size <= 0:
            raise AudioRecoveryError("invalid_pcm_wav")

        digest = hashlib.sha256()
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise AudioRecoveryError("audio_snapshot_changed")
            digest.update(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise AudioRecoveryError("audio_snapshot_changed")
        sha256 = digest.hexdigest()
        hashed = os.fstat(descriptor)
        try:
            hashed_path = path.lstat()
        except OSError:
            raise AudioRecoveryError("audio_snapshot_changed") from None
        if (
            not _same_snapshot(before, hashed)
            or not _same_snapshot(hashed, hashed_path)
        ):
            raise AudioRecoveryError("audio_snapshot_changed")
        if sha256 != expected_sha256:
            raise AudioRecoveryError("audio_sha256_mismatch")

        riff_header = os.pread(descriptor, 12, 0)
        if (
            len(riff_header) != 12
            or riff_header[:4] != b"RIFF"
            or riff_header[8:] != b"WAVE"
            or struct.unpack("<I", riff_header[4:8])[0] + 8 != before.st_size
        ):
            raise AudioRecoveryError("invalid_pcm_wav")

        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            with os.fdopen(os.dup(descriptor), "rb") as wav_file:
                with wave.open(wav_file, "rb") as reader:
                    channels = reader.getnchannels()
                    sample_width = reader.getsampwidth()
                    frame_rate = reader.getframerate()
                    frame_count = reader.getnframes()
                    compression = reader.getcomptype()
                    data_chunk_size = reader._data_chunk.chunksize
                    expected_frame_bytes = frame_count * channels * sample_width
                    if (
                        compression != "NONE"
                        or frame_rate != 16_000
                        or channels != 1
                        or sample_width != 2
                        or frame_count <= 0
                        or data_chunk_size != expected_frame_bytes
                    ):
                        raise AudioRecoveryError("invalid_pcm_wav")
                    remaining_frames = frame_count
                    frame_bytes_read = 0
                    while remaining_frames:
                        requested_frames = min(4_096, remaining_frames)
                        frame_chunk = reader.readframes(requested_frames)
                        requested_bytes = requested_frames * channels * sample_width
                        if len(frame_chunk) != requested_bytes:
                            raise AudioRecoveryError("invalid_pcm_wav")
                        frame_bytes_read += len(frame_chunk)
                        remaining_frames -= requested_frames
                    if reader.readframes(1):
                        raise AudioRecoveryError("invalid_pcm_wav")
        except (EOFError, MemoryError, OSError, wave.Error):
            raise AudioRecoveryError("invalid_pcm_wav") from None

        if frame_bytes_read != expected_frame_bytes:
            raise AudioRecoveryError("invalid_pcm_wav")
        duration_seconds = frame_count / frame_rate
        if duration_seconds <= 0:
            raise AudioRecoveryError("invalid_pcm_wav")

        after = os.fstat(descriptor)
        try:
            current_path_stat = path.lstat()
        except OSError:
            raise AudioRecoveryError("audio_snapshot_changed") from None
        if (
            not _same_snapshot(before, after)
            or not _same_snapshot(after, current_path_stat)
        ):
            raise AudioRecoveryError("audio_snapshot_changed")
        validated = _ValidatedPcmWav(
            sha256=sha256,
            size_bytes=before.st_size,
            duration_seconds=duration_seconds,
            device=before.st_dev,
            inode=before.st_ino,
        )
        if _while_open is not None:
            _while_open(validated, descriptor, after)
        return validated
    except AudioRecoveryError:
        raise
    except (AttributeError, MemoryError, OSError, RuntimeError, struct.error, wave.Error):
        raise AudioRecoveryError("audio_unavailable") from None
    finally:
        os.close(descriptor)


def _require_exact_directory(path: Path, expected_resolved: Path) -> None:
    try:
        path_stat = path.lstat()
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError):
        raise AudioRecoveryError("job_path_mismatch") from None
    if (
        stat.S_ISLNK(path_stat.st_mode)
        or not stat.S_ISDIR(path_stat.st_mode)
        or resolved != expected_resolved
    ):
        raise AudioRecoveryError("job_path_mismatch")


def _require_path_matches_validation(
    path: Path,
    validated: _ValidatedPcmWav,
) -> None:
    try:
        current = path.lstat()
    except OSError:
        raise AudioRecoveryError("audio_snapshot_changed") from None
    if (
        not stat.S_ISREG(current.st_mode)
        or (current.st_dev, current.st_ino)
        != (validated.device, validated.inode)
    ):
        raise AudioRecoveryError("audio_snapshot_changed")


def _require_open_final_snapshot(
    final_path: Path,
    descriptor: int,
    validated_snapshot: os.stat_result,
) -> None:
    directory_descriptor: int | None = None
    try:
        directory_descriptor = os.open(
            final_path.parent,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
        opened_directory = os.fstat(directory_descriptor)
        path_directory = final_path.parent.lstat()
        if (
            stat.S_ISLNK(path_directory.st_mode)
            or not stat.S_ISDIR(path_directory.st_mode)
            or not stat.S_ISDIR(opened_directory.st_mode)
            or (path_directory.st_dev, path_directory.st_ino)
            != (opened_directory.st_dev, opened_directory.st_ino)
        ):
            raise AudioRecoveryError("audio_snapshot_changed")
        basename_snapshot = os.stat(
            final_path.name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        opened_snapshot = os.fstat(descriptor)
        current_path_directory = final_path.parent.lstat()
        if (
            not stat.S_ISREG(basename_snapshot.st_mode)
            or not _same_snapshot(validated_snapshot, opened_snapshot)
            or not _same_snapshot(opened_snapshot, basename_snapshot)
            or (current_path_directory.st_dev, current_path_directory.st_ino)
            != (opened_directory.st_dev, opened_directory.st_ino)
        ):
            raise AudioRecoveryError("audio_snapshot_changed")
    except AudioRecoveryError:
        raise
    except (OSError, RuntimeError):
        raise AudioRecoveryError("audio_snapshot_changed") from None
    finally:
        if directory_descriptor is not None:
            os.close(directory_descriptor)


def _require_exact_job(
    store: JobStore,
    *,
    job_id: str,
    movie_code: str,
) -> tuple[JobRecord, JobPaths]:
    job = store.get_job(job_id)
    if job is None:
        raise AudioRecoveryError("job_not_found")
    if job.normalized_movie_number != movie_code:
        raise AudioRecoveryError("job_movie_mismatch")
    if job.status is not JobStatus.DOWNLOADING_AUDIO:
        raise AudioRecoveryError("job_status_mismatch")
    if job.claimed_by is not None or job.lease_expires_at is not None:
        raise AudioRecoveryError("job_is_claimed")

    paths = build_job_paths(
        movie_code,
        store.jobs_root_mac,
        store.jobs_root_windows,
    )
    exact_database_paths = (
        (job.job_dir_mac, str(paths.job_dir_mac)),
        (job.job_dir_windows, paths.job_dir_windows),
        (job.metadata_path_mac, str(paths.metadata_path_mac)),
        (job.audio_path_mac, str(paths.audio_path_mac)),
        (job.audio_path_windows, paths.audio_path_windows),
        (job.japanese_srt_path_mac, str(paths.japanese_srt_path_mac)),
        (job.japanese_srt_path_windows, paths.japanese_srt_path_windows),
        (job.english_srt_path_mac, str(paths.english_srt_path_mac)),
        (job.english_srt_path_windows, paths.english_srt_path_windows),
    )
    if any(actual is not None and actual != expected for actual, expected in exact_database_paths):
        raise AudioRecoveryError("job_path_mismatch")

    try:
        root_stat = store.jobs_root_mac.lstat()
        resolved_root = store.jobs_root_mac.resolve(strict=True)
    except (OSError, RuntimeError):
        raise AudioRecoveryError("job_path_mismatch") from None
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise AudioRecoveryError("job_path_mismatch")
    if paths.job_dir_mac.parent != store.jobs_root_mac:
        raise AudioRecoveryError("job_path_mismatch")
    _require_exact_directory(
        paths.job_dir_mac,
        resolved_root / movie_code,
    )
    return job, paths


def recover_interrupted_audio(
    store: JobStore,
    *,
    job_id: str,
    movie: str,
    expected_sha256: str,
) -> AudioRecoveryReceipt:
    if not _valid_expected_sha256(expected_sha256):
        raise AudioRecoveryError("invalid_expected_sha256")
    try:
        movie_code = canonical_movie_code(movie)
    except (AttributeError, TypeError, ValueError):
        raise AudioRecoveryError("invalid_movie") from None

    try:
        _job, paths = _require_exact_job(
            store,
            job_id=job_id,
            movie_code=movie_code,
        )
    except sqlite3.Error:
        raise AudioRecoveryError("audio_recovery_store_error") from None
    final_path = paths.audio_path_mac
    staged_directory = paths.job_dir_mac / "audio"
    staged_path = staged_directory / f"{movie_code}.wav"

    try:
        final_exists = final_path.lstat() is not None
    except FileNotFoundError:
        final_exists = False
    except OSError:
        raise AudioRecoveryError("audio_unavailable") from None

    reused_final = final_exists
    moved_staged = False
    if not final_exists:
        try:
            expected_staged_directory = paths.job_dir_mac.resolve(strict=True) / "audio"
        except (OSError, RuntimeError):
            raise AudioRecoveryError("job_path_mismatch") from None
        _require_exact_directory(staged_directory, expected_staged_directory)
        validated = validate_pcm_wav(
            staged_path,
            expected_sha256=expected_sha256,
        )
        _require_path_matches_validation(staged_path, validated)
        try:
            os.replace(staged_path, final_path)
        except OSError:
            raise AudioRecoveryError("audio_move_failed") from None
        moved_staged = True

    final_validation_complete = False
    finalized: JobRecord | None = None

    def finalize_while_snapshot_open(
        _validated: _ValidatedPcmWav,
        descriptor: int,
        validated_snapshot: os.stat_result,
    ) -> None:
        nonlocal final_validation_complete, finalized
        final_validation_complete = True
        finalized = store.finalize_interrupted_audio(
            job_id,
            expected_movie_code=movie_code,
            expected_job_dir_mac=str(paths.job_dir_mac),
            expected_job_dir_windows=paths.job_dir_windows,
            expected_audio_path_mac=str(paths.audio_path_mac),
            expected_audio_path_windows=paths.audio_path_windows,
            audio_snapshot_check=lambda: _require_open_final_snapshot(
                final_path,
                descriptor,
                validated_snapshot,
            ),
        )

    try:
        validated = validate_pcm_wav(
            final_path,
            expected_sha256=expected_sha256,
            _while_open=finalize_while_snapshot_open,
        )
    except AudioRecoveryError:
        if not final_validation_complete:
            if moved_staged:
                try:
                    if not staged_path.exists() and final_path.exists():
                        os.replace(final_path, staged_path)
                except OSError:
                    pass
            raise
        raise AudioRecoveryError("audio_recovery_state_changed") from None
    except (KeyError, RuntimeError, sqlite3.Error):
        raise AudioRecoveryError("audio_recovery_state_changed") from None
    assert finalized is not None
    return AudioRecoveryReceipt(
        job_id=finalized.id,
        movie_code=movie_code,
        status=finalized.status,
        final_path=final_path,
        sha256=validated.sha256,
        size_bytes=validated.size_bytes,
        duration_seconds=validated.duration_seconds,
        reused_final=reused_final,
    )
