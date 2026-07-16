import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


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
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr or completed.stdout)

        try:
            produced_path = self._find_produced_audio(movie_number, output_path)
        except FileNotFoundError:
            process_output = f"{completed.stdout}\n{completed.stderr}".lower()
            if "pausing before next download" in process_output:
                raise DownloadDeferredError(
                    "download deferred: low disk space"
                ) from None
            raise
        produced_path.replace(output_path)

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
        for candidate in candidates:
            if candidate.exists():
                return candidate
        raise FileNotFoundError(f"Downloaded audio for {movie_number} not found under {output_path.parent}")

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
