import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from orchestrator.job_logs import append_job_log


VARIANT_SUFFIXES = (
    "-uncensored-leak",
    "-uncensored",
    "-english-subtitle",
    "-chinese-subtitle",
    "-subtitle",
    "-leak",
)

QUEUE_MOVIE_FIELDS = ("number", "title", "link", "cover", "preview", "duration", "release_date")


class DownloadDeferredError(RuntimeError):
    pass


class SourceNoAudioError(RuntimeError):
    pass


def _append_job_log_safely(job_dir: Path, message: str) -> None:
    try:
        append_job_log(job_dir, "mac-download.log", message)
    except Exception:
        return


class MissAVAdapter:
    def __init__(
        self,
        missav_pipeline_root: Path,
        python_executable: Path | str | None = None,
    ) -> None:
        self.missav_pipeline_root = missav_pipeline_root
        self.python_executable = self._default_python_executable(python_executable)

    def download_metadata(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        catalog_path = self.missav_pipeline_root / "new-release" / "release_movies_complete.json"
        try:
            movie = self._find_movie_in_catalog(movie_number, catalog_path)
        except FileNotFoundError:
            movie = None
        if movie is not None:
            self._write_json(movie, output_path)
            return

        command = [
            str(self.python_executable),
            str(self.missav_pipeline_root / "new-release" / "unified_download.py"),
        ]
        completed = subprocess.run(
            command,
            cwd=self.missav_pipeline_root,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr or completed.stdout)
        movie = self._find_movie_in_catalog(movie_number, catalog_path)
        self._write_json(movie, output_path)

    def download_audio(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        movie = self._load_audio_movie(movie_number, output_path.parent / "metadata.json")
        queue_movie = self._queue_movie(movie, movie_number)

        try:
            produced_path = self._download_audio_candidate(queue_movie, output_path)
        except SourceNoAudioError:
            original_code = self._safe_log_movie_code(movie_number)
            _append_job_log_safely(output_path.parent, f"source_no_audio {original_code}")
        else:
            produced_path.replace(output_path)
            return

        attempted = [str(queue_movie["number"])]
        for fallback_movie in self._audio_fallback_candidates(movie_number):
            candidate_number = str(fallback_movie["number"])
            attempted.append(candidate_number)
            candidate_code = self._safe_log_movie_code(candidate_number)
            _append_job_log_safely(
                output_path.parent,
                f"source_fallback {original_code} -> {candidate_code}",
            )
            try:
                produced_path = self._download_audio_candidate(fallback_movie, output_path)
            except SourceNoAudioError:
                continue
            produced_path.replace(output_path)
            _append_job_log_safely(
                output_path.parent,
                f"source_fallback_success {original_code} -> {candidate_code}",
            )
            return

        attempted_codes = ", ".join(attempted)
        safe_attempted_codes = ", ".join(
            self._safe_log_movie_code(number) for number in attempted
        )
        _append_job_log_safely(
            output_path.parent,
            f"source_fallback_exhausted {original_code} attempted {safe_attempted_codes}",
        )
        raise SourceNoAudioError(
            f"source_no_audio: {movie_number}; attempted: {attempted_codes}"
        )

    def _download_audio_candidate(
        self,
        queue_movie: dict[str, Any],
        output_path: Path,
    ) -> Path:
        movie_number = str(queue_movie["number"])
        self._clean_audio_staging_candidates(movie_number, output_path)

        with tempfile.TemporaryDirectory(prefix="missav-adapter-") as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            catalog_path = temp_dir / "single_movie_catalog.json"
            queue_path = temp_dir / "single_movie_queue.json"
            log_path = temp_dir / "download_log.json"
            self._write_json({"movies": [queue_movie], "_metadata": {"movie_count": 0}}, catalog_path)
            self._write_json(
                {
                    "pending": [queue_movie],
                    "completed": {},
                    "failed": {},
                    "_metadata": {"last_updated": None},
                },
                queue_path,
            )

            command = [
                str(self.python_executable),
                str(self.missav_pipeline_root / "new-release" / "batch_audio_downloader.py"),
                "--json-file",
                str(catalog_path),
                "--queue-file",
                str(queue_path),
                "--output-dir",
                str(output_path.parent),
                "--log-file",
                str(log_path),
                "--max-downloads",
                "1",
                "--only-pending",
                "--direct-audio",
            ]
            completed = subprocess.run(
                command,
                cwd=temp_dir,
                text=True,
                capture_output=True,
                check=False,
            )
            detail = self._pipeline_failure_detail(log_path, movie_number)
            if completed.returncode != 0:
                if detail and self._is_source_no_audio_failure(detail):
                    raise SourceNoAudioError(f"source_no_audio: {movie_number}") from None
                raise RuntimeError(completed.stderr or completed.stdout)

            process_output = f"{completed.stdout}\n{completed.stderr}"
            if "pausing before next download" in process_output.lower():
                raise DownloadDeferredError("download deferred: low disk space") from None
            if detail and self._is_retryable_stream_failure(detail):
                raise DownloadDeferredError(
                    f"download deferred: upstream stream resolution: {detail}"
                ) from None
            if detail and self._is_source_no_audio_failure(detail):
                raise SourceNoAudioError(f"source_no_audio: {movie_number}") from None
            if detail:
                raise FileNotFoundError(
                    f"Downloaded audio for {movie_number} not found under "
                    f"{output_path.parent}: {detail}"
                ) from None
            return self._find_produced_audio(movie_number, output_path)

    def _audio_fallback_candidates(self, movie_number: str) -> list[dict[str, Any]]:
        catalog_path = self.missav_pipeline_root / "new-release" / "release_movies_complete.json"
        requested = movie_number.strip().lower()
        requested_base = self._base_movie_id(requested)
        matching_movies: dict[str, dict[str, Any]] = {}
        for movie in self._catalog_movies(catalog_path):
            normalized_number = str(movie.get("number", "")).strip().lower()
            if (
                normalized_number == requested
                or self._base_movie_id(normalized_number) != requested_base
            ):
                continue
            matching_movies.setdefault(normalized_number, movie)

        candidates = []
        for suffix in VARIANT_SUFFIXES:
            candidate_number = f"{requested_base}{suffix}"
            movie = matching_movies.get(candidate_number)
            if movie is not None:
                candidates.append(self._queue_movie(movie, candidate_number))
        base_movie = matching_movies.get(requested_base)
        if base_movie is not None:
            candidates.append(self._queue_movie(base_movie, requested_base))
        return candidates[: len(VARIANT_SUFFIXES)]

    def _pipeline_failure_detail(self, log_path: Path, movie_number: str) -> str | None:
        try:
            payload = json.loads(log_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return None
        failed = payload.get("failed") if isinstance(payload, dict) else None
        if not isinstance(failed, dict):
            return None
        record = failed.get(self._base_movie_id(movie_number)) or failed.get(movie_number)
        if not isinstance(record, dict):
            return None
        detail = record.get("last_error") or record.get("error")
        if not isinstance(detail, str) or not detail.strip():
            return None
        return detail.strip()[:1000]

    def _is_retryable_stream_failure(self, detail: str) -> bool:
        lowered = detail.lower()
        return any(
            token in lowered
            for token in (
                "page_http_400",
                "page_http_403",
                "page_cloudflare_challenge",
                "page_request_failed",
            )
        )

    def _is_source_no_audio_failure(self, detail: str) -> bool:
        lowered = detail.lower()
        return any(
            token in lowered
            for token in (
                "source_no_audio",
                "output file does not contain any stream",
                "matches no streams",
                "does not contain any audio stream",
            )
        )

    def _safe_log_movie_code(self, movie_number: str) -> str:
        normalized = movie_number.strip().lower()
        safe_code = re.sub(r"[^a-z0-9._-]", "_", normalized)
        return safe_code[:200] or "unknown"

    def _catalog_movies(self, catalog_path: Path) -> list[dict[str, Any]]:
        if not catalog_path.exists():
            raise FileNotFoundError(f"MissAV catalog not found: {catalog_path}")
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            movies = payload.get("movies", [])
        else:
            movies = payload
        if not isinstance(movies, list):
            raise RuntimeError(f"MissAV catalog has no movies list: {catalog_path}")
        return [movie for movie in movies if isinstance(movie, dict)]

    def _find_movie_in_catalog(self, movie_number: str, catalog_path: Path) -> dict[str, Any]:
        requested = movie_number.lower()
        for movie in self._catalog_movies(catalog_path):
            if str(movie.get("number", "")).lower() == requested:
                return movie
        raise FileNotFoundError(f"Movie {movie_number} not found in MissAV catalog: {catalog_path}")

    def _load_audio_movie(self, movie_number: str, metadata_path: Path) -> dict[str, Any]:
        if metadata_path.exists():
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                movie = dict(payload)
            else:
                movie = {}
        else:
            movie = {}
        movie["number"] = movie_number
        return movie

    def _queue_movie(self, movie: dict[str, Any], movie_number: str) -> dict[str, Any]:
        queued = {field: movie.get(field, "") for field in QUEUE_MOVIE_FIELDS}
        queued["number"] = movie_number
        return queued

    def _find_produced_audio(self, movie_number: str, output_path: Path) -> Path:
        job_dir = output_path.parent
        job_fd = self._open_staging_directory(job_dir, "job")
        audio_fd: int | None = None
        try:
            audio_fd = self._open_audio_staging_directory(job_fd, job_dir / "audio")
            for candidate in self._audio_staging_candidates(movie_number, output_path):
                parent_fd = job_fd if candidate.parent == job_dir else audio_fd
                if parent_fd is None:
                    continue
                if self._is_nonempty_regular_staging_file(parent_fd, candidate.name):
                    return candidate
        finally:
            if audio_fd is not None:
                os.close(audio_fd)
            os.close(job_fd)
        raise FileNotFoundError(
            f"Downloaded audio for {movie_number} not found under {output_path.parent}"
        )

    def _clean_audio_staging_candidates(
        self,
        movie_number: str,
        output_path: Path,
    ) -> None:
        job_dir = output_path.parent
        job_fd = self._open_staging_directory(job_dir, "job")
        audio_fd: int | None = None
        try:
            audio_fd = self._open_audio_staging_directory(job_fd, job_dir / "audio")
            for candidate in self._audio_staging_candidates(movie_number, output_path):
                parent_fd = job_fd if candidate.parent == job_dir else audio_fd
                if parent_fd is None:
                    continue
                try:
                    os.unlink(candidate.name, dir_fd=parent_fd)
                except FileNotFoundError:
                    continue
        finally:
            if audio_fd is not None:
                os.close(audio_fd)
            os.close(job_fd)

    def _open_staging_directory(self, directory: Path, label: str) -> int:
        descriptor: int | None = None
        try:
            path_snapshot = directory.lstat()
            descriptor = os.open(
                directory,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
            opened_snapshot = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(path_snapshot.st_mode)
                or not stat.S_ISDIR(opened_snapshot.st_mode)
                or (path_snapshot.st_dev, path_snapshot.st_ino)
                != (opened_snapshot.st_dev, opened_snapshot.st_ino)
            ):
                raise OSError("directory identity changed")
            return descriptor
        except OSError:
            if descriptor is not None:
                os.close(descriptor)
            raise RuntimeError(f"unsafe {label} staging directory: {directory}") from None

    def _open_audio_staging_directory(
        self,
        job_fd: int,
        audio_dir: Path,
    ) -> int | None:
        descriptor: int | None = None
        try:
            try:
                path_snapshot = os.stat(
                    "audio",
                    dir_fd=job_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return None
            descriptor = os.open(
                "audio",
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=job_fd,
            )
            opened_snapshot = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(path_snapshot.st_mode)
                or not stat.S_ISDIR(opened_snapshot.st_mode)
                or (path_snapshot.st_dev, path_snapshot.st_ino)
                != (opened_snapshot.st_dev, opened_snapshot.st_ino)
            ):
                raise OSError("directory identity changed")
            return descriptor
        except OSError:
            if descriptor is not None:
                os.close(descriptor)
            raise RuntimeError(f"unsafe audio staging directory: {audio_dir}") from None

    def _is_nonempty_regular_staging_file(self, directory_fd: int, name: str) -> bool:
        descriptor: int | None = None
        try:
            path_snapshot = os.stat(
                name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            if not stat.S_ISREG(path_snapshot.st_mode) or path_snapshot.st_size <= 0:
                return False
            descriptor = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW,
                dir_fd=directory_fd,
            )
            opened_snapshot = os.fstat(descriptor)
            current_snapshot = os.stat(
                name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            return (
                stat.S_ISREG(opened_snapshot.st_mode)
                and opened_snapshot.st_size > 0
                and (
                    path_snapshot.st_dev,
                    path_snapshot.st_ino,
                    path_snapshot.st_mode,
                    path_snapshot.st_size,
                )
                == (
                    opened_snapshot.st_dev,
                    opened_snapshot.st_ino,
                    opened_snapshot.st_mode,
                    opened_snapshot.st_size,
                )
                == (
                    current_snapshot.st_dev,
                    current_snapshot.st_ino,
                    current_snapshot.st_mode,
                    current_snapshot.st_size,
                )
            )
        except OSError:
            return False
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _audio_staging_candidates(self, movie_number: str, output_path: Path) -> list[Path]:
        audio_dir = output_path.parent / "audio"
        safe_number = re.sub(r"[^\w\-_.]", "_", movie_number)
        base_number = self._base_movie_id(movie_number)
        safe_base = re.sub(r"[^\w\-_.]", "_", base_number)
        candidates = [
            output_path.with_suffix(output_path.suffix + ".tmp"),
            audio_dir / f"{safe_number}.wav",
            audio_dir / f"{movie_number}.wav",
            audio_dir / f"{safe_base}.wav",
            audio_dir / f"{base_number}.wav",
        ]
        unique_candidates = []
        for candidate in candidates:
            if candidate == output_path or (
                candidate != output_path.with_suffix(output_path.suffix + ".tmp")
                and candidate.parent != audio_dir
            ):
                continue
            if candidate not in unique_candidates:
                unique_candidates.append(candidate)
        return unique_candidates

    def _base_movie_id(self, movie_number: str) -> str:
        base = movie_number.strip().lower()
        for suffix in VARIANT_SUFFIXES:
            if base.endswith(suffix):
                return base[: -len(suffix)]
        return base

    def _write_json(self, payload: Any, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    def _default_python_executable(self, python_executable: Path | str | None) -> Path:
        if python_executable is not None:
            return Path(python_executable)
        pipeline_python = self.missav_pipeline_root / ".venv" / "bin" / "python"
        if pipeline_python.exists():
            return pipeline_python
        return Path(sys.executable)
