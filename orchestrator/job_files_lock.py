"""Mac-only cooperative root/job locks for orchestrator job-file writes.

Lock order is always jobs-root, then zero or more job directories, then SQLite.
Normal Mac writers hold the root shared and their job exclusive. Historical
planning/enqueue holds the root exclusive, which gives a consistent bounded-FD
scan of every allowlisted job. Arbitrary processes ignoring advisory locks are
outside this coordination contract and are handled fail-safe where possible.
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
class JobsRootLock:
    jobs_root: Path
    root_fd: int
    exclusive: bool

    def require_bound(self) -> None:
        try:
            path_stat = self.jobs_root.lstat()
            opened = os.fstat(self.root_fd)
        except OSError:
            raise JobFilesLockError("job_files_root_lock_path_mismatch") from None
        if (
            stat.S_ISLNK(path_stat.st_mode)
            or not stat.S_ISDIR(path_stat.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or not _same_inode(path_stat, opened)
        ):
            raise JobFilesLockError("job_files_root_lock_path_mismatch")


@dataclass(frozen=True, slots=True)
class JobFilesLock:
    root_lock: JobsRootLock
    movie_code: str
    job_fd: int

    @property
    def jobs_root(self) -> Path:
        return self.root_lock.jobs_root

    @property
    def root_fd(self) -> int:
        return self.root_lock.root_fd

    def require_bound(self) -> None:
        self.root_lock.require_bound()
        try:
            entry = os.stat(
                self.movie_code,
                dir_fd=self.root_fd,
                follow_symlinks=False,
            )
            opened = os.fstat(self.job_fd)
        except OSError:
            raise JobFilesLockError("job_files_lock_path_mismatch") from None
        if (
            not stat.S_ISDIR(entry.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or not _same_inode(entry, opened)
        ):
            raise JobFilesLockError("job_files_lock_path_mismatch")


@contextmanager
def _jobs_root_lock(
    jobs_root: Path,
    *,
    exclusive: bool,
    blocking: bool,
) -> Iterator[JobsRootLock]:
    root_fd: int | None = None
    locked = False
    try:
        root_fd = os.open(
            jobs_root,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
        held = JobsRootLock(Path(jobs_root), root_fd, exclusive)
        held.require_bound()
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not blocking:
            operation |= fcntl.LOCK_NB
        try:
            fcntl.flock(root_fd, operation)
        except BlockingIOError:
            raise JobFilesLockBusy("job_files_root_lock_busy") from None
        locked = True
        held.require_bound()
    except JobFilesLockError:
        if root_fd is not None:
            if locked:
                fcntl.flock(root_fd, fcntl.LOCK_UN)
            os.close(root_fd)
        raise
    except OSError:
        if root_fd is not None:
            if locked:
                fcntl.flock(root_fd, fcntl.LOCK_UN)
            os.close(root_fd)
        raise JobFilesLockError("job_files_root_lock_path_mismatch") from None
    try:
        yield held
    finally:
        if root_fd is not None:
            if locked:
                fcntl.flock(root_fd, fcntl.LOCK_UN)
            os.close(root_fd)


@contextmanager
def _job_files_lock_from_root(
    root_lock: JobsRootLock,
    movie_code: str,
    *,
    exclusive: bool,
    blocking: bool,
) -> Iterator[JobFilesLock]:
    job_fd: int | None = None
    locked = False
    try:
        root_lock.require_bound()
        job_fd = os.open(
            movie_code,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=root_lock.root_fd,
        )
        held = JobFilesLock(root_lock, movie_code, job_fd)
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
        raise
    except OSError:
        if job_fd is not None:
            if locked:
                fcntl.flock(job_fd, fcntl.LOCK_UN)
            os.close(job_fd)
        raise JobFilesLockError("job_files_lock_path_mismatch") from None
    try:
        yield held
    finally:
        if job_fd is not None:
            if locked:
                fcntl.flock(job_fd, fcntl.LOCK_UN)
            os.close(job_fd)


@contextmanager
def open_job_directory_from_root(
    root_lock: JobsRootLock,
    movie_code: str,
) -> Iterator[int]:
    job_fd: int | None = None
    try:
        root_lock.require_bound()
        job_fd = os.open(
            movie_code,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=root_lock.root_fd,
        )
        entry = os.stat(
            movie_code,
            dir_fd=root_lock.root_fd,
            follow_symlinks=False,
        )
        opened = os.fstat(job_fd)
        if not stat.S_ISDIR(entry.st_mode) or not _same_inode(entry, opened):
            raise JobFilesLockError("job_files_lock_path_mismatch")
        yield job_fd
    except OSError:
        raise JobFilesLockError("job_files_lock_path_mismatch") from None
    finally:
        if job_fd is not None:
            os.close(job_fd)


def shared_jobs_root_lock(
    jobs_root: Path,
    *,
    blocking: bool,
) -> Iterator[JobsRootLock]:
    return _jobs_root_lock(jobs_root, exclusive=False, blocking=blocking)


def exclusive_jobs_root_lock(
    jobs_root: Path,
    *,
    blocking: bool,
) -> Iterator[JobsRootLock]:
    return _jobs_root_lock(jobs_root, exclusive=True, blocking=blocking)


def shared_job_files_lock_from_root(
    root_lock: JobsRootLock,
    movie_code: str,
    *,
    blocking: bool,
) -> Iterator[JobFilesLock]:
    return _job_files_lock_from_root(
        root_lock,
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
    @contextmanager
    def locked() -> Iterator[JobFilesLock]:
        with shared_jobs_root_lock(jobs_root, blocking=blocking) as root_lock:
            with _job_files_lock_from_root(
                root_lock,
                movie_code,
                exclusive=True,
                blocking=blocking,
            ) as held:
                yield held

    return locked()
