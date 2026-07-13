from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import sqlite3

from orchestrator.paths import build_job_paths, normalize_movie_number
from orchestrator.store import JobStore
from orchestrator.subtitle_quality import validate_translation_quality


@dataclass(frozen=True)
class HistoricalRepairPlan:
    job_id: str
    movie_number: str
    reason_codes: tuple[str, ...]
    japanese_path: str
    english_path: str
    quarantine_path: str
    reset_stage: str = "translation_only"
    japanese_action: str = "preserve"
    english_action: str = "quarantine"
    would_requeue: bool = True
    would_overwrite_supabase: bool = True

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def plan_historical_repairs(
    store: JobStore,
    *,
    allowlist: set[str] | None,
    limit: int,
) -> list[HistoricalRepairPlan]:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    normalized_allowlist = (
        {
            normalized
            for movie in allowlist
            if (normalized := normalize_movie_number(movie)) is not None
        }
        if allowlist is not None
        else None
    )
    plans: list[HistoricalRepairPlan] = []
    database_uri = f"file:{store.db_path.resolve()}?mode=ro"
    with sqlite3.connect(database_uri, uri=True) as connection:
        rows = connection.execute(
            "SELECT id, normalized_movie_number FROM jobs "
            "ORDER BY priority ASC, created_at ASC"
        ).fetchall()

    for job_id, movie_number in rows:
        if normalized_allowlist is not None:
            comparison_movie = normalize_movie_number(movie_number)
            if comparison_movie not in normalized_allowlist:
                continue
        paths = build_job_paths(
            movie_number, store.jobs_root_mac, store.jobs_root_windows
        )
        if not paths.japanese_srt_path_mac.is_file():
            continue
        report = validate_translation_quality(
            paths.japanese_srt_path_mac,
            paths.english_srt_path_mac,
        )
        if report.passed:
            continue
        rejected_dir = paths.job_dir_mac / "rejected"
        quarantine_path = rejected_dir / (
            f"{paths.english_srt_path_mac.stem}.rejected-historical"
            f"{paths.english_srt_path_mac.suffix}"
        )
        plans.append(
            HistoricalRepairPlan(
                job_id=job_id,
                movie_number=movie_number,
                reason_codes=tuple(report.reason_codes),
                japanese_path=str(paths.japanese_srt_path_mac),
                english_path=str(paths.english_srt_path_mac),
                quarantine_path=str(quarantine_path),
            )
        )
        if len(plans) >= limit:
            break
    return plans


def render_repair_report(plans: list[HistoricalRepairPlan]) -> str:
    lines = [f"dry_run=true affected_count={len(plans)}"]
    for plan in plans:
        lines.append(
            f"job_id={plan.job_id} movie_number={plan.movie_number} "
            f"reset_stage={plan.reset_stage} preserve_japanese=true "
            f"quarantine_english={plan.quarantine_path} would_requeue=true "
            f"would_overwrite_supabase=true reasons={','.join(plan.reason_codes)}"
        )
    return "\n".join(lines)
