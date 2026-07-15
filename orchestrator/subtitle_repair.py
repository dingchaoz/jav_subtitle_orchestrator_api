from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any

from orchestrator.historical_repair import load_repair_allowlist
from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths, normalize_movie_number
from orchestrator.store import JobRecord, JobStore, utc_now_iso
from orchestrator.subtitle_quality import validate_translation_quality


TRANSLATION_ONLY_PLAN_VERSION = 1
TRANSLATION_ONLY_MAX_BATCH_LIMIT = 20


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


def _canonical_json_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _file_identity(path: Path) -> tuple[str, int]:
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("translation_only_plan_changed")
    payload = path.read_bytes()
    if not payload:
        raise ValueError("translation_only_plan_changed")
    return hashlib.sha256(payload).hexdigest(), len(payload)


@dataclass(frozen=True, slots=True)
class TranslationOnlyRepairItem:
    job_id: str
    movie_number: str
    expected_status: str
    expected_updated_at: str
    reason_codes: tuple[str, ...]
    japanese_path: str
    english_path: str
    japanese_sha256: str
    japanese_size: int
    english_sha256: str
    english_size: int
    prior_published_content_sha256: str | None

    def to_payload(self) -> dict[str, object]:
        payload = asdict(self)
        payload["reason_codes"] = list(self.reason_codes)
        return payload


@dataclass(frozen=True, slots=True)
class TranslationOnlyRepairBatchPlan:
    version: int
    allowlist_path: str
    allowlist_sha256: str
    allowlist_entry_count: int
    limit: int
    eligible_total: int
    ineligible: int
    blocked: int
    items: tuple[TranslationOnlyRepairItem, ...]
    plan_sha256: str

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "allowlist_path": self.allowlist_path,
            "allowlist_sha256": self.allowlist_sha256,
            "allowlist_entry_count": self.allowlist_entry_count,
            "limit": self.limit,
            "eligible_total": self.eligible_total,
            "ineligible": self.ineligible,
            "blocked": self.blocked,
            "items": [item.to_payload() for item in self.items],
        }

    def recalculate_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self._digest_payload())).hexdigest()

    def to_payload(self) -> dict[str, object]:
        return {**self._digest_payload(), "plan_sha256": self.plan_sha256}

    def to_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_payload())

    @classmethod
    def build(
        cls,
        *,
        allowlist_path: str,
        allowlist_sha256: str,
        allowlist_entry_count: int,
        limit: int,
        eligible_total: int,
        ineligible: int,
        blocked: int,
        items: tuple[TranslationOnlyRepairItem, ...],
    ) -> TranslationOnlyRepairBatchPlan:
        plan = cls(
            version=TRANSLATION_ONLY_PLAN_VERSION,
            allowlist_path=allowlist_path,
            allowlist_sha256=allowlist_sha256,
            allowlist_entry_count=allowlist_entry_count,
            limit=limit,
            eligible_total=eligible_total,
            ineligible=ineligible,
            blocked=blocked,
            items=items,
            plan_sha256="",
        )
        return replace(plan, plan_sha256=plan.recalculate_sha256())

    @classmethod
    def from_json_bytes(cls, snapshot: bytes) -> TranslationOnlyRepairBatchPlan:
        try:
            payload = json.loads(snapshot.decode("utf-8"))
            plan = _translation_only_plan_from_payload(payload)
        except (AttributeError, KeyError, TypeError, UnicodeError, ValueError):
            raise ValueError("translation_only_plan_invalid") from None
        return plan


def plan_translation_only_repair_batch(
    store: JobStore,
    allowlist_file: Path,
    *,
    limit: int,
) -> TranslationOnlyRepairBatchPlan:
    if limit < 1 or limit > TRANSLATION_ONLY_MAX_BATCH_LIMIT:
        raise ValueError("limit must be between 1 and 20")
    allowlist_file = Path(allowlist_file)
    allowlist = load_repair_allowlist(allowlist_file)
    allowlist_bytes = allowlist_file.read_bytes()
    allowlist_sha256 = hashlib.sha256(allowlist_bytes).hexdigest()
    absolute_allowlist_path = str(allowlist_file.resolve())

    eligible: list[TranslationOnlyRepairItem] = []
    ineligible = 0
    blocked = 0
    database_uri = f"file:{store.db_path.resolve()}?mode=ro"
    with sqlite3.connect(database_uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT * FROM jobs ORDER BY priority ASC, created_at ASC"
        ).fetchall()

    for row in rows:
        movie_number = row["normalized_movie_number"]
        normalized = normalize_movie_number(movie_number)
        if normalized not in allowlist:
            continue
        try:
            status = JobStatus(row["status"])
        except ValueError:
            blocked += 1
            continue
        if status is not JobStatus.ENGLISH_SRT_READY or row["claimed_by"] is not None:
            blocked += 1
            continue

        paths = build_job_paths(movie_number, store.jobs_root_mac, store.jobs_root_windows)
        try:
            japanese_sha256, japanese_size = _file_identity(paths.japanese_srt_path_mac)
            english_sha256, english_size = _file_identity(paths.english_srt_path_mac)
        except ValueError:
            blocked += 1
            continue

        report = validate_translation_quality(
            paths.japanese_srt_path_mac,
            paths.english_srt_path_mac,
        )
        if report.passed:
            ineligible += 1
            continue

        eligible.append(
            TranslationOnlyRepairItem(
                job_id=row["id"],
                movie_number=movie_number,
                expected_status=status.value,
                expected_updated_at=row["updated_at"],
                reason_codes=tuple(report.reason_codes),
                japanese_path=str(paths.japanese_srt_path_mac),
                english_path=str(paths.english_srt_path_mac),
                japanese_sha256=japanese_sha256,
                japanese_size=japanese_size,
                english_sha256=english_sha256,
                english_size=english_size,
                prior_published_content_sha256=row["published_content_sha256"],
            )
        )

    selected = tuple(eligible[:limit])
    return TranslationOnlyRepairBatchPlan.build(
        allowlist_path=absolute_allowlist_path,
        allowlist_sha256=allowlist_sha256,
        allowlist_entry_count=len(allowlist),
        limit=limit,
        eligible_total=len(eligible),
        ineligible=ineligible,
        blocked=blocked,
        items=selected,
    )


def render_translation_only_repair_batch_report(
    plan: TranslationOnlyRepairBatchPlan,
) -> str:
    lines = [
        f"planned=true selected={len(plan.items)} eligible_total={plan.eligible_total} "
        f"ineligible={plan.ineligible} blocked={plan.blocked} "
        f"plan_sha256={plan.plan_sha256}"
    ]
    for item in plan.items:
        lines.append(
            f"job_id={item.job_id} movie_number={item.movie_number} "
            f"reset_stage=translation_only preserve_japanese=true "
            f"preserve_rejected_english=true would_requeue=true "
            f"would_overwrite_supabase=true reasons={','.join(item.reason_codes)}"
        )
    return "\n".join(lines)


def write_translation_only_repair_plan(
    path: Path,
    plan: TranslationOnlyRepairBatchPlan,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plan.to_json_bytes())


def read_translation_only_repair_plan(path: Path) -> TranslationOnlyRepairBatchPlan:
    return TranslationOnlyRepairBatchPlan.from_json_bytes(Path(path).read_bytes())


def enqueue_translation_only_repair_batch(
    store: JobStore,
    plan: TranslationOnlyRepairBatchPlan,
    *,
    confirm_plan_sha256: str,
) -> list[JobRecord]:
    if confirm_plan_sha256 != plan.plan_sha256:
        raise ValueError("confirm_plan_sha256 mismatch")
    if plan.recalculate_sha256() != plan.plan_sha256:
        raise ValueError("translation_only_plan_invalid")
    if len(plan.items) > plan.limit or plan.limit > TRANSLATION_ONLY_MAX_BATCH_LIMIT:
        raise ValueError("translation_only_plan_invalid")

    now = utc_now_iso()
    updated: list[JobRecord] = []
    with store.connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        for item in plan.items:
            japanese_sha256, japanese_size = _file_identity(Path(item.japanese_path))
            english_sha256, english_size = _file_identity(Path(item.english_path))
            if (
                japanese_sha256 != item.japanese_sha256
                or japanese_size != item.japanese_size
                or english_sha256 != item.english_sha256
                or english_size != item.english_size
            ):
                raise ValueError("translation_only_plan_changed")
            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = ?, claimed_by = NULL, lease_expires_at = NULL,
                    stage_lease_token = NULL, catalog_lease_token = NULL,
                    translation_attempt_count = 0,
                    publish_attempt_count = 0, next_publish_attempt_at = NULL,
                    published_subtitle_id = NULL, published_storage_path = NULL,
                    published_content_sha256 = NULL, published_file_size = NULL,
                    catalog_sync_attempt_count = 0,
                    next_catalog_sync_attempt_at = NULL,
                    catalog_movie_uuid = NULL, metadata_status = NULL,
                    metadata_source = NULL, updated_at = ?, error = NULL
                WHERE id = ? AND normalized_movie_number = ? AND status = ?
                  AND updated_at = ? AND claimed_by IS NULL
                """,
                (
                    JobStatus.TRANSCRIPTION_DONE.value,
                    now,
                    item.job_id,
                    item.movie_number,
                    item.expected_status,
                    item.expected_updated_at,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("translation_only_plan_changed")
            refreshed = store.get_job(item.job_id, conn=connection)
            if refreshed is None:
                raise ValueError("translation_only_plan_changed")
            updated.append(refreshed)
    return updated


_PLAN_KEYS = frozenset(
    {
        "version",
        "allowlist_path",
        "allowlist_sha256",
        "allowlist_entry_count",
        "limit",
        "eligible_total",
        "ineligible",
        "blocked",
        "items",
        "plan_sha256",
    }
)
_ITEM_KEYS = frozenset(
    {
        "job_id",
        "movie_number",
        "expected_status",
        "expected_updated_at",
        "reason_codes",
        "japanese_path",
        "english_path",
        "japanese_sha256",
        "japanese_size",
        "english_sha256",
        "english_size",
        "prior_published_content_sha256",
    }
)


def _require_exact_keys(payload: object, keys: frozenset[str]) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != keys:
        raise ValueError("invalid payload")
    return payload


def _require_digest(value: object) -> str:
    if not isinstance(value, str) or len(value) != 64:
        raise ValueError("invalid digest")
    int(value, 16)
    return value


def _require_positive_int(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError("invalid integer")
    return value


def _require_nonnegative_int(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("invalid integer")
    return value


def _translation_only_item_from_payload(value: object) -> TranslationOnlyRepairItem:
    payload = _require_exact_keys(value, _ITEM_KEYS)
    reasons = payload["reason_codes"]
    if (
        not isinstance(payload["job_id"], str)
        or not payload["job_id"]
        or not isinstance(payload["movie_number"], str)
        or normalize_movie_number(payload["movie_number"]) != payload["movie_number"]
        or payload["expected_status"] != JobStatus.ENGLISH_SRT_READY.value
        or not isinstance(payload["expected_updated_at"], str)
        or not payload["expected_updated_at"]
        or not isinstance(reasons, list)
        or not reasons
        or any(not isinstance(reason, str) or not reason for reason in reasons)
        or not isinstance(payload["japanese_path"], str)
        or not isinstance(payload["english_path"], str)
    ):
        raise ValueError("invalid item")
    prior_hash = payload["prior_published_content_sha256"]
    if prior_hash is not None:
        prior_hash = _require_digest(prior_hash)
    return TranslationOnlyRepairItem(
        job_id=payload["job_id"],
        movie_number=payload["movie_number"],
        expected_status=payload["expected_status"],
        expected_updated_at=payload["expected_updated_at"],
        reason_codes=tuple(reasons),
        japanese_path=payload["japanese_path"],
        english_path=payload["english_path"],
        japanese_sha256=_require_digest(payload["japanese_sha256"]),
        japanese_size=_require_positive_int(payload["japanese_size"]),
        english_sha256=_require_digest(payload["english_sha256"]),
        english_size=_require_positive_int(payload["english_size"]),
        prior_published_content_sha256=prior_hash,
    )


def _translation_only_plan_from_payload(
    value: object,
) -> TranslationOnlyRepairBatchPlan:
    payload = _require_exact_keys(value, _PLAN_KEYS)
    items = tuple(_translation_only_item_from_payload(item) for item in payload["items"])
    version = payload["version"]
    limit = payload["limit"]
    if (
        version != TRANSLATION_ONLY_PLAN_VERSION
        or not isinstance(payload["allowlist_path"], str)
        or not Path(payload["allowlist_path"]).is_absolute()
        or not isinstance(payload["items"], list)
    ):
        raise ValueError("invalid plan")
    plan = TranslationOnlyRepairBatchPlan(
        version=version,
        allowlist_path=payload["allowlist_path"],
        allowlist_sha256=_require_digest(payload["allowlist_sha256"]),
        allowlist_entry_count=_require_positive_int(payload["allowlist_entry_count"]),
        limit=_require_positive_int(limit),
        eligible_total=_require_nonnegative_int(payload["eligible_total"]),
        ineligible=_require_nonnegative_int(payload["ineligible"]),
        blocked=_require_nonnegative_int(payload["blocked"]),
        items=items,
        plan_sha256=_require_digest(payload["plan_sha256"]),
    )
    if (
        plan.limit > TRANSLATION_ONLY_MAX_BATCH_LIMIT
        or len(plan.items) > plan.limit
        or len(plan.items) > plan.eligible_total
        or plan.recalculate_sha256() != plan.plan_sha256
    ):
        raise ValueError("invalid plan")
    return plan
