"""Cooperative lock for stable snapshots and writes in one Mac job directory.

The lock is an advisory ``flock`` on the job-directory inode.  Historical
snapshot readers hold it shared; orchestrator writers that replace Japanese or
English subtitle paths hold it exclusive.  ``audio_lock`` already takes an
exclusive flock on the same inode, so callers must not nest these two locks.

Directory/file descriptors and final path validation make uncoordinated path
replacement fail safe when it happens before validation.  Advisory locks cannot
prevent a deliberately non-cooperating external process from writing afterward.
"""

from __future__ import annotations

import fcntl
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


class JobFilesLockError(RuntimeError):
    pass


class JobFilesLockBusy(JobFilesLockError):
    pass


def _same_inode(first: os.stat_result, second: os.stat_result) -> bool:
    return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)


@dataclass(frozen=True, slots=True)
class JobFilesLock:
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
        except OSError:
            raise JobFilesLockError("job_files_lock_path_mismatch") from None
        if (
            stat.S_ISLNK(root_path.st_mode)
            or not stat.S_ISDIR(root_path.st_mode)
            or not stat.S_ISDIR(root_opened.st_mode)
            or not stat.S_ISDIR(job_entry.st_mode)
            or not stat.S_ISDIR(job_opened.st_mode)
            or not _same_inode(root_path, root_opened)
            or not _same_inode(job_entry, job_opened)
        ):
            raise JobFilesLockError("job_files_lock_path_mismatch")


@contextmanager
def _job_files_lock(
    jobs_root: Path,
    movie_code: str,
    *,
    exclusive: bool,
    blocking: bool,
) -> Iterator[JobFilesLock]:
    root_fd: int | None = None
    job_fd: int | None = None
    locked = False
    try:
        root_fd = os.open(
            jobs_root,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
        job_fd = os.open(
            movie_code,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=root_fd,
        )
        held = JobFilesLock(Path(jobs_root), movie_code, root_fd, job_fd)
        held.require_bound()
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not blocking:
            operation |= fcntl.LOCK_NB
        try:
            fcntl.flock(job_fd, operation)
        except BlockingIOError:
            raise JobFilesLockBusy("job_files_lock_busy") from None
        locked = True
        held.require_bound()
    except JobFilesLockError:
        if job_fd is not None:
            if locked:
                fcntl.flock(job_fd, fcntl.LOCK_UN)
            os.close(job_fd)
        if root_fd is not None:
            os.close(root_fd)
        raise
    except OSError:
        if job_fd is not None:
            if locked:
                fcntl.flock(job_fd, fcntl.LOCK_UN)
            os.close(job_fd)
        if root_fd is not None:
            os.close(root_fd)
        raise JobFilesLockError("job_files_lock_path_mismatch") from None

    try:
        yield held
    finally:
        if job_fd is not None:
            if locked:
                fcntl.flock(job_fd, fcntl.LOCK_UN)
            os.close(job_fd)
        if root_fd is not None:
            os.close(root_fd)


def shared_job_files_lock(
    jobs_root: Path,
    movie_code: str,
    *,
    blocking: bool,
) -> Iterator[JobFilesLock]:
    return _job_files_lock(
        jobs_root,
        movie_code,
        exclusive=False,
        blocking=blocking,
    )


def exclusive_job_files_lock(
    jobs_root: Path,
    movie_code: str,
    *,
    blocking: bool,
) -> Iterator[JobFilesLock]:
    return _job_files_lock(
        jobs_root,
        movie_code,
        exclusive=True,
        blocking=blocking,
    )
