from __future__ import annotations

from dataclasses import dataclass
import sqlite3

from orchestrator.movie_catalog import load_publish_metadata
from orchestrator.movie_code import canonical_movie_code
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore
from orchestrator.subtitle_quality import validate_translation_quality
from orchestrator.supabase_publisher import build_ai_subtitle_storage_path


@dataclass(frozen=True)
class CatalogRepairPlan:
    job_id: str
    movie_code: str
    current_status: str
    japanese_srt: str
    english_srt: str
    metadata_path: str
    metadata_available: bool
    expected_metadata_source: str
    action: str
    storage_effect: str


def _nonempty_file(path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def plan_catalog_repairs(
    store: JobStore,
    *,
    allowlist: set[str] | None,
    limit: int,
) -> list[CatalogRepairPlan]:
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    canonical_allowlist = (
        {canonical_movie_code(movie) for movie in allowlist}
        if allowlist is not None
        else None
    )
    database_uri = f"file:{store.db_path.resolve()}?mode=ro"
    with sqlite3.connect(database_uri, uri=True) as connection:
        rows = connection.execute(
            "SELECT id, normalized_movie_number, status FROM jobs "
            "ORDER BY priority ASC, created_at ASC, "
            "normalized_movie_number ASC, id ASC"
        ).fetchall()

    plans: list[CatalogRepairPlan] = []
    for job_id, normalized_movie_number, status in rows:
        movie_code = canonical_movie_code(normalized_movie_number)
        if canonical_allowlist is not None and movie_code not in canonical_allowlist:
            continue
        paths = build_job_paths(
            normalized_movie_number,
            store.jobs_root_mac,
            store.jobs_root_windows,
        )
        if not all(
            _nonempty_file(path)
            for path in (
                paths.japanese_srt_path_mac,
                paths.english_srt_path_mac,
            )
        ):
            continue
        quality = validate_translation_quality(
            paths.japanese_srt_path_mac,
            paths.english_srt_path_mac,
        )
        if not quality.passed:
            continue
        metadata_available = bool(
            load_publish_metadata(paths.metadata_path_mac, movie_code)
        )
        storage_path = build_ai_subtitle_storage_path(movie_code)
        plans.append(
            CatalogRepairPlan(
                job_id=job_id,
                movie_code=movie_code,
                current_status=status,
                japanese_srt=str(paths.japanese_srt_path_mac),
                english_srt=str(paths.english_srt_path_mac),
                metadata_path=str(paths.metadata_path_mac),
                metadata_available=metadata_available,
                expected_metadata_source=(
                    "local" if metadata_available else "missav_or_placeholder"
                ),
                action="would_ensure_catalog_then_publish",
                storage_effect=(
                    f"would upsert/overwrite Storage path={storage_path}"
                ),
            )
        )
        if len(plans) >= limit:
            break
    return plans


def render_catalog_repair_report(plans: list[CatalogRepairPlan]) -> str:
    lines = [f"DRY RUN affected_count={len(plans)}"]
    for plan in plans:
        lines.append(
            f"job_id={plan.job_id} movie_code={plan.movie_code} "
            f"status={plan.current_status} "
            f"metadata_available={'yes' if plan.metadata_available else 'no'} "
            f"source={plan.expected_metadata_source} action={plan.action} "
            f"storage={plan.storage_effect}"
        )
    return "\n".join(lines)
