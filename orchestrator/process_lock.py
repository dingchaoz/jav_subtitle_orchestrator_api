import fcntl
import os
from pathlib import Path
from typing import TextIO


class AlreadyRunningError(RuntimeError):
    pass


class SingleInstanceLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle: TextIO | None = None

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(
                self.handle.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
        except BlockingIOError as exc:
            handle = self.handle
            self.handle = None
            try:
                handle.close()
            except BaseException:
                pass
            raise AlreadyRunningError("worker_already_running") from exc
        except BaseException:
            handle = self.handle
            self.handle = None
            try:
                handle.close()
            except BaseException:
                pass
            raise
        try:
            self.handle.seek(0)
            self.handle.truncate()
            self.handle.write(f"{os.getpid()}\n")
            self.handle.flush()
        except BaseException:
            handle = self.handle
            self.handle = None
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except BaseException:
                pass
            try:
                handle.close()
            except BaseException:
                pass
            raise
        return self

    def release(self) -> None:
        handle = self.handle
        if handle is None:
            return
        # Detach before cleanup because a failed close leaves the FD state
        # indeterminate; a later release must not retry that same handle.
        self.handle = None
        cleanup_error: BaseException | None = None
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except BaseException as exc:
            cleanup_error = exc
        try:
            handle.close()
        except BaseException as exc:
            if cleanup_error is None:
                cleanup_error = exc
        if cleanup_error is not None:
            raise cleanup_error
