from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import sqlite3
import stat

from orchestrator.historical_repair import load_repair_allowlist
from orchestrator.movie_catalog import (
    METADATA_SOURCES,
    METADATA_STATUSES,
    load_publish_metadata,
)
from orchestrator.movie_code import canonical_movie_code
from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore
from orchestrator.subtitle_quality import validate_translation_quality
from orchestrator.supabase_publisher import build_ai_subtitle_storage_path


OPTIONAL_JOB_PROJECTIONS = {
    "publish_attempt_count": "0 AS publish_attempt_count",
    "next_publish_attempt_at": "NULL AS next_publish_attempt_at",
    "catalog_movie_uuid": "NULL AS catalog_movie_uuid",
    "metadata_status": "NULL AS metadata_status",
    "metadata_source": "NULL AS metadata_source",
}


@dataclass(frozen=True)
class CatalogRepairPlan:
    job_id: str
    movie_code: str
    current_status: str
    japanese_srt: str
    english_srt: str
    metadata_path: str | None
    metadata_available: bool
    expected_metadata_source: str
    action: str
    storage_effect: str


@dataclass(frozen=True, slots=True)
class CatalogPublicationCanaryReceipt:
    job_id: str
    movie_code: str
    prior_status: JobStatus
    new_status: JobStatus
    translation_attempt_count: int
    english_sha256: str
    quality_passed: bool
    english_cue_count: int
    english_unique_ratio: float
    known_bad_phrase_count: int


def _nonempty_file(path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _require_regular_nonempty_subtitle(path: Path, label: str) -> None:
    try:
        file_stat = path.lstat()
    except OSError as exc:
        raise ValueError(f"quality_gate_failed:{label}_srt_missing") from exc
    if not stat.S_ISREG(file_stat.st_mode):
        raise ValueError(f"quality_gate_failed:{label}_srt_not_regular")
    if file_stat.st_size <= 0:
        raise ValueError(f"quality_gate_failed:{label}_srt_empty")


def _require_direct_job_directory(job_directory: Path, jobs_root: Path) -> None:
    try:
        directory_stat = job_directory.lstat()
    except OSError as exc:
        raise ValueError("quality_gate_failed:job_directory_missing") from exc
    if stat.S_ISLNK(directory_stat.st_mode):
        raise ValueError("quality_gate_failed:job_directory_symlink")
    if not stat.S_ISDIR(directory_stat.st_mode):
        raise ValueError("quality_gate_failed:job_directory_not_directory")
    try:
        configured_root = jobs_root.resolve(strict=True)
        canonical_job_directory = job_directory.resolve(strict=True)
    except OSError as exc:
        raise ValueError("quality_gate_failed:job_directory_missing") from exc
    if canonical_job_directory.parent != configured_root:
        raise ValueError("quality_gate_failed:job_directory_not_direct_child")


def prepare_catalog_publication_canary(
    store: JobStore,
    allowlist_path: Path,
    *,
    movie: str,
    limit: int,
    confirm_job_id: str,
) -> CatalogPublicationCanaryReceipt:
    if limit != 1:
        raise ValueError("limit must be exactly 1")
    requested = canonical_movie_code(movie)
    allowlist = load_repair_allowlist(allowlist_path)
    if requested not in allowlist:
        raise ValueError("movie is not in the explicit allowlist")

    prior = store.get_job(confirm_job_id)
    if prior is None:
        raise ValueError("confirmed catalog publication job does not exist")
    if canonical_movie_code(prior.normalized_movie_number) != requested:
        raise ValueError("confirmed job does not match requested movie")
    if prior.status not in {JobStatus.FAILED, JobStatus.ENGLISH_SRT_READY}:
        raise ValueError("confirmed job status is not eligible")
    if prior.claimed_by is not None:
        raise ValueError("confirmed job is claimed")
    if (
        prior.status is JobStatus.ENGLISH_SRT_READY
        and prior.catalog_movie_uuid
        and prior.metadata_status in METADATA_STATUSES
        and prior.metadata_source in METADATA_SOURCES
    ):
        raise ValueError("verified publication is not eligible")

    paths = build_job_paths(
        prior.normalized_movie_number,
        store.jobs_root_mac,
        store.jobs_root_windows,
    )
    _require_direct_job_directory(paths.job_dir_mac, store.jobs_root_mac)
    if (
        paths.japanese_srt_path_mac.parent != paths.job_dir_mac
        or paths.english_srt_path_mac.parent != paths.job_dir_mac
    ):
        raise ValueError("quality_gate_failed:subtitle_not_direct_child")
    _require_regular_nonempty_subtitle(
        paths.japanese_srt_path_mac,
        "japanese",
    )
    _require_regular_nonempty_subtitle(
        paths.english_srt_path_mac,
        "english",
    )
    japanese_before = paths.japanese_srt_path_mac.read_bytes()
    english_before = paths.english_srt_path_mac.read_bytes()
    report = validate_translation_quality(
        paths.japanese_srt_path_mac,
        paths.english_srt_path_mac,
    )
    if not report.passed:
        reason_codes = ",".join(report.reason_codes) or "unknown"
        raise ValueError(f"quality_gate_failed:{reason_codes}")

    japanese_snapshot = paths.japanese_srt_path_mac.read_bytes()
    english_snapshot = paths.english_srt_path_mac.read_bytes()
    if japanese_snapshot != japanese_before or english_snapshot != english_before:
        raise ValueError(
            "quality_gate_failed:subtitle_changed_during_validation"
        )
    japanese_sha256 = hashlib.sha256(japanese_snapshot).hexdigest()
    english_sha256 = hashlib.sha256(english_snapshot).hexdigest()

    allowlist_after_validation = load_repair_allowlist(allowlist_path)
    if requested not in allowlist_after_validation:
        raise RuntimeError("allowlist changed before prepare")
    try:
        japanese_before_transition = paths.japanese_srt_path_mac.read_bytes()
        english_before_transition = paths.english_srt_path_mac.read_bytes()
    except OSError as exc:
        raise ValueError(
            "quality_gate_failed:subtitle_changed_after_validation"
        ) from exc
    if (
        japanese_before_transition != japanese_snapshot
        or english_before_transition != english_snapshot
    ):
        raise ValueError(
            "quality_gate_failed:subtitle_changed_after_validation"
        )
    prepared = store.prepare_catalog_publication_repair(
        prior.id,
        expected_status=prior.status,
        expected_movie=requested,
        expected_japanese_sha256=japanese_sha256,
        expected_english_sha256=english_sha256,
    )
    return CatalogPublicationCanaryReceipt(
        job_id=prepared.id,
        movie_code=requested,
        prior_status=prior.status,
        new_status=prepared.status,
        translation_attempt_count=prepared.translation_attempt_count,
        english_sha256=english_sha256,
        quality_passed=report.passed,
        english_cue_count=report.english_cue_count,
        english_unique_ratio=report.english_unique_ratio,
        known_bad_phrase_count=report.known_bad_phrase_count,
    )


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
        existing_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(jobs)")
        }
        optional_projection = [
            column
            if column in existing_columns
            else fallback
            for column, fallback in OPTIONAL_JOB_PROJECTIONS.items()
        ]
        select_projection = ", ".join(
            ["id", "normalized_movie_number", "status", *optional_projection]
        )
        rows = connection.execute(
            f"SELECT {select_projection} FROM jobs "
            "ORDER BY priority ASC, created_at ASC, "
            "normalized_movie_number ASC, id ASC"
        ).fetchall()

    plans: list[CatalogRepairPlan] = []
    for (
        job_id,
        normalized_movie_number,
        status,
        _publish_attempt_count,
        _next_publish_attempt_at,
        catalog_movie_uuid,
        metadata_status,
        metadata_source,
    ) in rows:
        movie_code = canonical_movie_code(normalized_movie_number)
        if canonical_allowlist is not None and movie_code not in canonical_allowlist:
            continue
        if (
            status == JobStatus.ENGLISH_SRT_READY.value
            and isinstance(catalog_movie_uuid, str)
            and bool(catalog_movie_uuid.strip())
            and metadata_status in METADATA_STATUSES
            and metadata_source in METADATA_SOURCES
        ):
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
                metadata_path=(
                    str(paths.metadata_path_mac)
                    if paths.metadata_path_mac.is_file()
                    else None
                ),
                metadata_available=metadata_available,
                expected_metadata_source=(
                    "public_or_missav_or_local"
                    if metadata_available
                    else "public_or_missav_or_placeholder"
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
            f"metadata_path={plan.metadata_path or '-'} "
            f"metadata_available={'yes' if plan.metadata_available else 'no'} "
            f"expected_source_candidates={plan.expected_metadata_source} "
            f"action={plan.action} "
            f"storage={plan.storage_effect}"
        )
    return "\n".join(lines)
