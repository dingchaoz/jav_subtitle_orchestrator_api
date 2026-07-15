import uuid
from pathlib import Path

from orchestrator.movie_code import canonical_movie_code
from orchestrator.models import JobPaths


def normalize_movie_number(raw: str) -> str | None:
    try:
        return canonical_movie_code(raw)
    except ValueError:
        return None


def new_job_id() -> str:
    return f"job_{uuid.uuid4().hex}"


def windows_join(root: str, *parts: str) -> str:
    clean_root = root.rstrip("\\/")
    return clean_root + "\\" + "\\".join(part.strip("\\/") for part in parts)


def build_job_paths(movie_number: str, jobs_root_mac: Path, jobs_root_windows: str) -> JobPaths:
    job_dir_mac = jobs_root_mac / movie_number
    job_dir_windows = windows_join(jobs_root_windows, movie_number)
    return JobPaths(
        job_dir_mac=job_dir_mac,
        job_dir_windows=job_dir_windows,
        metadata_path_mac=job_dir_mac / "metadata.json",
        audio_path_mac=job_dir_mac / "audio.wav",
        audio_path_windows=windows_join(job_dir_windows, "audio.wav"),
        japanese_srt_path_mac=job_dir_mac / f"{movie_number}.Japanese.srt",
        japanese_srt_path_windows=windows_join(job_dir_windows, f"{movie_number}.Japanese.srt"),
        english_srt_path_mac=job_dir_mac / f"{movie_number}.English.srt",
        english_srt_path_windows=windows_join(job_dir_windows, f"{movie_number}.English.srt"),
    )
