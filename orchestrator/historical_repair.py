from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths, normalize_movie_number
from orchestrator.store import JobRecord, JobStore
from orchestrator.subtitle_quality import validate_translation_quality


MAX_ALLOWLIST_ENTRIES = 10_000
ELIGIBLE_STATUSES = frozenset(
    {
        JobStatus.QUEUED,
        JobStatus.FAILED,
        JobStatus.ENGLISH_SRT_READY,
    }
)


@dataclass(frozen=True, slots=True)
class HistoricalRepairCandidate:
    job_id: str
    movie_number: str
    expected_status: JobStatus
    reason_codes: tuple[str, ...]
    japanese_path: str
    english_path: str
    audio_path: str
    quarantine_directory: str

    def to_safe_dict(self) -> dict[str, object]:
        return asdict(self)


def load_repair_allowlist(path: Path) -> frozenset[str]:
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("allowlist must be a regular file")
    movies: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, 1):
            if line_number > MAX_ALLOWLIST_ENTRIES:
                raise ValueError("allowlist exceeds 10000 entries")
            value = raw_line.strip()
            normalized = normalize_movie_number(value)
            if not value or normalized is None:
                raise ValueError(f"allowlist line {line_number} is invalid")
            if normalized in movies:
                raise ValueError(f"allowlist line {line_number} is duplicate")
            movies.add(normalized)
    if not movies:
        raise ValueError("allowlist must not be empty")
    return frozenset(movies)


def _nonempty_file(path: Path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _candidate_for_job(
    store: JobStore,
    job: JobRecord,
    allowlist: frozenset[str],
) -> HistoricalRepairCandidate | None:
    movie = job.normalized_movie_number
    if (
        movie not in allowlist
        or job.status not in ELIGIBLE_STATUSES
        or job.claimed_by is not None
    ):
        return None
    paths = build_job_paths(movie, store.jobs_root_mac, store.jobs_root_windows)
    if not all(
        _nonempty_file(path)
        for path in (
            paths.japanese_srt_path_mac,
            paths.english_srt_path_mac,
            paths.audio_path_mac,
        )
    ):
        return None
    report = validate_translation_quality(
        paths.japanese_srt_path_mac,
        paths.english_srt_path_mac,
    )
    if report.passed:
        return None
    return HistoricalRepairCandidate(
        job_id=job.id,
        movie_number=movie,
        expected_status=job.status,
        reason_codes=tuple(report.reason_codes),
        japanese_path=str(paths.japanese_srt_path_mac),
        english_path=str(paths.english_srt_path_mac),
        audio_path=str(paths.audio_path_mac),
        quarantine_directory=str(paths.job_dir_mac / "rejected"),
    )


def select_historical_repair_canary(
    store: JobStore,
    allowlist_path: Path,
    *,
    preferred_movie: str | None = None,
) -> HistoricalRepairCandidate | None:
    allowlist = load_repair_allowlist(allowlist_path)
    preferred: str | None = None
    if preferred_movie is not None:
        preferred = normalize_movie_number(preferred_movie)
        if preferred is None:
            raise ValueError("preferred movie is invalid")
    candidates = [
        candidate
        for job in store.list_jobs()
        if (candidate := _candidate_for_job(store, job, allowlist)) is not None
    ]
    candidates.sort(
        key=lambda candidate: (
            candidate.movie_number != preferred,
            candidate.movie_number,
            candidate.job_id,
        )
    )
    return candidates[0] if candidates else None


def prepare_historical_repair_canary(
    store: JobStore,
    allowlist_path: Path,
    *,
    movie: str,
    limit: int,
    confirm_job_id: str,
) -> JobRecord:
    if limit != 1:
        raise ValueError("limit must be exactly 1")
    normalized = normalize_movie_number(movie)
    if normalized is None:
        raise ValueError("movie is invalid")
    candidate = select_historical_repair_canary(
        store,
        allowlist_path,
        preferred_movie=normalized,
    )
    if candidate is None or candidate.movie_number != normalized:
        raise ValueError("movie is not an eligible allowlisted canary")
    if candidate.job_id != confirm_job_id:
        raise ValueError("confirmed job does not match selected canary")
    return store.prepare_historical_translation_repair(
        candidate.job_id,
        expected_status=candidate.expected_status,
    )
