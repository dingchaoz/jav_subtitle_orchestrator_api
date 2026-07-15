"""Orchestrator-wide advisory locks for writes to one job's audio state.

Every orchestrator component that writes ``audio.wav`` or marks it ready must
hold the jobs-root shared lock and this job-exclusive lock. The order is root,
job, then database, matching historical snapshot coordination. The locks
coordinate cooperating orchestrator processes;
arbitrary local processes that deliberately ignore OS advisory locks are out
of scope.  Held directory descriptors still make path substitution fail safe.
"""

from __future__ import annotations

import fcntl
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


class AudioJobLockError(RuntimeError):
    pass


class AudioJobLockBusy(AudioJobLockError):
    pass


def _same_inode(first: os.stat_result, second: os.stat_result) -> bool:
    return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)


@dataclass(frozen=True)
class AudioJobLock:
    jobs_root: Path
    movie_code: str
    root_fd: int
    job_fd: int

    def require_bound(self) -> None:
        try:
            root_path = self.jobs_root.lstat()
            root_opened = os.fstat(self.root_fd)
            job_entry = os.stat(
                self.movie_code,
                dir_fd=self.root_fd,
                follow_symlinks=False,
            )
            job_opened = os.fstat(self.job_fd)
            job_path = (self.jobs_root / self.movie_code).lstat()
        except OSError:
            raise AudioJobLockError("audio_lock_path_mismatch") from None
        if (
            stat.S_ISLNK(root_path.st_mode)
            or not stat.S_ISDIR(root_path.st_mode)
            or not stat.S_ISDIR(root_opened.st_mode)
            or not stat.S_ISDIR(job_entry.st_mode)
            or not stat.S_ISDIR(job_opened.st_mode)
            or stat.S_ISLNK(job_path.st_mode)
            or not stat.S_ISDIR(job_path.st_mode)
            or not _same_inode(root_path, root_opened)
            or not _same_inode(job_entry, job_opened)
            or not _same_inode(job_path, job_opened)
        ):
            raise AudioJobLockError("audio_lock_path_mismatch")


@contextmanager
def exclusive_audio_job_lock(
    jobs_root: Path,
    movie_code: str,
    *,
    blocking: bool,
) -> Iterator[AudioJobLock]:
    root_fd: int | None = None
    job_fd: int | None = None
    root_locked = False
    locked = False

    def close_opened_descriptors() -> None:
        if job_fd is not None:
            if locked:
                fcntl.flock(job_fd, fcntl.LOCK_UN)
            os.close(job_fd)
        if root_fd is not None:
            if root_locked:
                fcntl.flock(root_fd, fcntl.LOCK_UN)
            os.close(root_fd)

    try:
        root_fd = os.open(
            jobs_root,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
        root_operation = fcntl.LOCK_SH
        if not blocking:
            root_operation |= fcntl.LOCK_NB
        try:
            fcntl.flock(root_fd, root_operation)
        except BlockingIOError:
            raise AudioJobLockBusy("audio_recovery_busy") from None
        root_locked = True
        job_fd = os.open(
            movie_code,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=root_fd,
        )
        held = AudioJobLock(jobs_root, movie_code, root_fd, job_fd)
        held.require_bound()
        operation = fcntl.LOCK_EX
        if not blocking:
            operation |= fcntl.LOCK_NB
        try:
            fcntl.flock(job_fd, operation)
        except BlockingIOError:
            raise AudioJobLockBusy("audio_recovery_busy") from None
        locked = True
        held.require_bound()
    except AudioJobLockError:
        close_opened_descriptors()
        raise
    except OSError:
        close_opened_descriptors()
        raise AudioJobLockError("audio_lock_path_mismatch") from None

    try:
        yield held
    finally:
        close_opened_descriptors()
