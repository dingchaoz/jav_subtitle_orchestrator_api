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

from orchestrator.audio_lock import (
    AudioJobLock,
    AudioJobLockBusy,
    AudioJobLockError,
    exclusive_audio_job_lock,
)
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


def _parse_riff_chunks(descriptor: int, riff_boundary: int) -> int:
    offset = 12
    format_chunks = 0
    data_chunks = 0
    data_chunk_size = 0
    while offset < riff_boundary:
        if riff_boundary - offset < 8:
            raise AudioRecoveryError("invalid_pcm_wav")
        chunk_header = os.pread(descriptor, 8, offset)
        if len(chunk_header) != 8:
            raise AudioRecoveryError("invalid_pcm_wav")
        chunk_id = chunk_header[:4]
        chunk_size = struct.unpack("<I", chunk_header[4:])[0]
        payload_start = offset + 8
        if chunk_size > riff_boundary - payload_start:
            raise AudioRecoveryError("invalid_pcm_wav")
        payload_end = payload_start + chunk_size
        padded_end = payload_end + (chunk_size & 1)
        if padded_end > riff_boundary:
            raise AudioRecoveryError("invalid_pcm_wav")
        if chunk_size & 1 and len(os.pread(descriptor, 1, payload_end)) != 1:
            raise AudioRecoveryError("invalid_pcm_wav")
        if chunk_id == b"fmt ":
            format_chunks += 1
        elif chunk_id == b"data":
            data_chunks += 1
            data_chunk_size = chunk_size
        offset = padded_end
    if (
        offset != riff_boundary
        or format_chunks != 1
        or data_chunks != 1
    ):
        raise AudioRecoveryError("invalid_pcm_wav")
    return data_chunk_size


def validate_pcm_wav(
    path: Path,
    *,
    expected_sha256: str,
    _while_open: Callable[[_ValidatedPcmWav, int, os.stat_result], None]
    | None = None,
    _directory_fd: int | None = None,
    _basename: str | None = None,
) -> _ValidatedPcmWav:
    """Validate one immutable, canonical PCM WAV snapshot without exposing content."""
    if not _valid_expected_sha256(expected_sha256):
        raise AudioRecoveryError("invalid_expected_sha256")

    basename = _basename or path.name

    def stat_entry() -> os.stat_result:
        if _directory_fd is None:
            return path.lstat()
        return os.stat(
            basename,
            dir_fd=_directory_fd,
            follow_symlinks=False,
        )

    try:
        path_stat = stat_entry()
        if not stat.S_ISREG(path_stat.st_mode):
            raise AudioRecoveryError("audio_not_regular")
        if _directory_fd is None:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        else:
            descriptor = os.open(
                basename,
                os.O_RDONLY | os.O_NOFOLLOW,
                dir_fd=_directory_fd,
            )
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
            hashed_path = stat_entry()
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
        riff_boundary = struct.unpack("<I", riff_header[4:8])[0] + 8
        data_chunk_size = _parse_riff_chunks(descriptor, riff_boundary)

        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            with os.fdopen(os.dup(descriptor), "rb") as wav_file:
                with wave.open(wav_file, "rb") as reader:
                    channels = reader.getnchannels()
                    sample_width = reader.getsampwidth()
                    frame_rate = reader.getframerate()
                    frame_count = reader.getnframes()
                    compression = reader.getcomptype()
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
            current_path_stat = stat_entry()
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


def _require_open_final_snapshot(
    directory_fd: int,
    basename: str,
    descriptor: int,
    validated_snapshot: os.stat_result,
    directory_binding_check: Callable[[], None],
) -> None:
    try:
        directory_binding_check()
        basename_snapshot = os.stat(
            basename,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        opened_snapshot = os.fstat(descriptor)
        if (
            not stat.S_ISREG(basename_snapshot.st_mode)
            or not _same_snapshot(validated_snapshot, opened_snapshot)
            or not _same_snapshot(opened_snapshot, basename_snapshot)
        ):
            raise AudioRecoveryError("audio_snapshot_changed")
        directory_binding_check()
    except AudioRecoveryError:
        raise
    except (OSError, RuntimeError):
        raise AudioRecoveryError("audio_snapshot_changed") from None


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


def _entry_exists(directory_fd: int, basename: str) -> bool:
    try:
        os.stat(basename, dir_fd=directory_fd, follow_symlinks=False)
        return True
    except FileNotFoundError:
        return False
    except OSError:
        raise AudioRecoveryError("audio_unavailable") from None


def _recover_interrupted_audio_with_directories(
    store: JobStore,
    *,
    job_id: str,
    movie_code: str,
    expected_sha256: str,
    paths: JobPaths,
    audio_lock: AudioJobLock,
    audio_directory_fd: int,
) -> AudioRecoveryReceipt:
    final_path = paths.audio_path_mac
    final_basename = "audio.wav"
    staged_basename = f"{movie_code}.wav"
    staged_path = paths.job_dir_mac / "audio" / staged_basename
    final_exists = _entry_exists(audio_lock.job_fd, final_basename)

    reused_final = final_exists
    moved_staged = False
    if not final_exists:
        validated = validate_pcm_wav(
            staged_path,
            expected_sha256=expected_sha256,
            _directory_fd=audio_directory_fd,
            _basename=staged_basename,
        )
        staged_snapshot = os.stat(
            staged_basename,
            dir_fd=audio_directory_fd,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(staged_snapshot.st_mode)
            or (staged_snapshot.st_dev, staged_snapshot.st_ino)
            != (validated.device, validated.inode)
        ):
            raise AudioRecoveryError("audio_snapshot_changed")
        audio_lock.require_bound()
        try:
            os.replace(
                staged_basename,
                final_basename,
                src_dir_fd=audio_directory_fd,
                dst_dir_fd=audio_lock.job_fd,
            )
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
                audio_lock.job_fd,
                final_basename,
                descriptor,
                validated_snapshot,
                audio_lock.require_bound,
            ),
        )

    try:
        validated = validate_pcm_wav(
            final_path,
            expected_sha256=expected_sha256,
            _while_open=finalize_while_snapshot_open,
            _directory_fd=audio_lock.job_fd,
            _basename=final_basename,
        )
    except AudioRecoveryError:
        if not final_validation_complete:
            if moved_staged:
                try:
                    if (
                        not _entry_exists(audio_directory_fd, staged_basename)
                        and _entry_exists(audio_lock.job_fd, final_basename)
                    ):
                        os.replace(
                            final_basename,
                            staged_basename,
                            src_dir_fd=audio_lock.job_fd,
                            dst_dir_fd=audio_directory_fd,
                        )
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


def _recover_interrupted_audio_locked(
    store: JobStore,
    *,
    job_id: str,
    movie_code: str,
    expected_sha256: str,
    paths: JobPaths,
    audio_lock: AudioJobLock,
) -> AudioRecoveryReceipt:
    audio_directory_fd: int | None = None
    try:
        audio_lock.require_bound()
        audio_entry = os.stat(
            "audio",
            dir_fd=audio_lock.job_fd,
            follow_symlinks=False,
        )
        audio_directory_fd = os.open(
            "audio",
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=audio_lock.job_fd,
        )
        opened_audio_directory = os.fstat(audio_directory_fd)
        if (
            not stat.S_ISDIR(audio_entry.st_mode)
            or not stat.S_ISDIR(opened_audio_directory.st_mode)
            or (audio_entry.st_dev, audio_entry.st_ino)
            != (opened_audio_directory.st_dev, opened_audio_directory.st_ino)
        ):
            raise AudioJobLockError("audio_lock_path_mismatch")
        return _recover_interrupted_audio_with_directories(
            store,
            job_id=job_id,
            movie_code=movie_code,
            expected_sha256=expected_sha256,
            paths=paths,
            audio_lock=audio_lock,
            audio_directory_fd=audio_directory_fd,
        )
    except AudioJobLockError:
        raise
    except OSError:
        raise AudioJobLockError("audio_lock_path_mismatch") from None
    finally:
        if audio_directory_fd is not None:
            os.close(audio_directory_fd)


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

    try:
        with exclusive_audio_job_lock(
            store.jobs_root_mac,
            movie_code,
            blocking=False,
        ) as audio_lock:
            return _recover_interrupted_audio_locked(
                store,
                job_id=job_id,
                movie_code=movie_code,
                expected_sha256=expected_sha256,
                paths=paths,
                audio_lock=audio_lock,
            )
    except AudioJobLockBusy:
        raise AudioRecoveryError("audio_recovery_busy") from None
    except AudioJobLockError:
        raise AudioRecoveryError("job_path_mismatch") from None
