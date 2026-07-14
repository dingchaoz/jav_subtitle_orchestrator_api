from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import time
from typing import Callable

import requests

from orchestrator.models import JobStatus
from orchestrator.store import JobStore
from orchestrator.subtitle_repair import (
    enqueue_translation_only_repair_batch,
    plan_translation_only_repair_batch,
    write_translation_only_repair_plan,
)


TERMINAL_STATUSES = frozenset(
    {JobStatus.ENGLISH_SRT_READY, JobStatus.FAILED, JobStatus.CANCELLED}
)


@dataclass(frozen=True, slots=True)
class TranslationOnlySupervisorConfig:
    allowlist_file: Path
    work_dir: Path
    batch_size: int
    max_jobs: int
    execute: bool = False
    confirm_remaining_count: int | None = None
    verify_public_api: bool = False
    public_api_base_url: str = "https://javsubtitle.com"
    poll_interval_seconds: int = 30
    batch_timeout_seconds: int = 3600


@dataclass(frozen=True, slots=True)
class TranslationOnlySupervisorResult:
    action: str
    remaining_count: int
    enqueued_count: int
    completed_count: int
    failed_count: int
    batches: int
    plan_files: tuple[str, ...]
    receipt_file: str | None = None
    reason_code: str | None = None


def run_translation_only_supervisor(
    store: JobStore,
    config: TranslationOnlySupervisorConfig,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> TranslationOnlySupervisorResult:
    if config.batch_size < 1 or config.batch_size > 20:
        raise ValueError("batch_size must be between 1 and 20")
    if config.max_jobs < 1:
        raise ValueError("max_jobs must be at least 1")
    if config.poll_interval_seconds < 1:
        raise ValueError("poll_interval_seconds must be at least 1")
    if config.batch_timeout_seconds < config.poll_interval_seconds:
        raise ValueError("batch_timeout_seconds must be >= poll_interval_seconds")

    config.work_dir.mkdir(parents=True, exist_ok=True)
    first_limit = min(config.batch_size, config.max_jobs)
    first_plan = plan_translation_only_repair_batch(
        store,
        config.allowlist_file,
        limit=first_limit,
    )
    first_plan_file = config.work_dir / "batch-0001-plan.json"
    write_translation_only_repair_plan(first_plan_file, first_plan)
    remaining_count = first_plan.eligible_total
    if not config.execute:
        return TranslationOnlySupervisorResult(
            action="dry_run",
            remaining_count=remaining_count,
            enqueued_count=0,
            completed_count=0,
            failed_count=0,
            batches=0,
            plan_files=(str(first_plan_file),),
        )
    if config.confirm_remaining_count != remaining_count:
        raise ValueError("confirm_remaining_count mismatch")

    receipt_file = config.work_dir / "supervisor-receipts.jsonl"
    plan_files: list[str] = [str(first_plan_file)]
    enqueued_count = 0
    completed_count = 0
    failed_count = 0
    batch_index = 1
    plan = first_plan

    while enqueued_count < min(config.max_jobs, remaining_count):
        if not plan.items:
            break
        selected_count = len(plan.items)
        records = enqueue_translation_only_repair_batch(
            store,
            plan,
            confirm_plan_sha256=plan.plan_sha256,
        )
        job_ids = [record.id for record in records]
        enqueued_count += len(job_ids)
        wait_result = wait_for_translation_only_batch(
            store,
            job_ids,
            poll_interval_seconds=config.poll_interval_seconds,
            batch_timeout_seconds=config.batch_timeout_seconds,
            sleep_fn=sleep_fn,
        )
        if wait_result.failed:
            failed_count += len(wait_result.failed)
            _append_receipt(
                receipt_file,
                {
                    "batch": batch_index,
                    "plan_sha256": plan.plan_sha256,
                    "status": "failed",
                    "failed": wait_result.failed,
                },
            )
            return TranslationOnlySupervisorResult(
                action="stopped",
                remaining_count=remaining_count,
                enqueued_count=enqueued_count,
                completed_count=completed_count,
                failed_count=failed_count,
                batches=batch_index,
                plan_files=tuple(plan_files),
                receipt_file=str(receipt_file),
                reason_code="batch_failed",
            )
        verified = verify_translation_only_batch(
            store,
            job_ids,
            verify_public_api=config.verify_public_api,
            public_api_base_url=config.public_api_base_url,
        )
        completed_count += len(verified)
        _append_receipt(
            receipt_file,
            {
                "batch": batch_index,
                "plan_sha256": plan.plan_sha256,
                "status": "verified",
                "movies": sorted(verified),
            },
        )
        if selected_count < config.batch_size or enqueued_count >= config.max_jobs:
            break
        batch_index += 1
        next_limit = min(config.batch_size, config.max_jobs - enqueued_count)
        plan = plan_translation_only_repair_batch(
            store,
            config.allowlist_file,
            limit=next_limit,
        )
        plan_file = config.work_dir / f"batch-{batch_index:04d}-plan.json"
        write_translation_only_repair_plan(plan_file, plan)
        plan_files.append(str(plan_file))

    return TranslationOnlySupervisorResult(
        action="completed",
        remaining_count=remaining_count,
        enqueued_count=enqueued_count,
        completed_count=completed_count,
        failed_count=failed_count,
        batches=batch_index,
        plan_files=tuple(plan_files),
        receipt_file=str(receipt_file),
    )


@dataclass(frozen=True, slots=True)
class BatchWaitResult:
    ready: tuple[str, ...]
    failed: tuple[str, ...]


def wait_for_translation_only_batch(
    store: JobStore,
    job_ids: list[str],
    *,
    poll_interval_seconds: int,
    batch_timeout_seconds: int,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> BatchWaitResult:
    deadline = time.monotonic() + batch_timeout_seconds
    while True:
        ready: list[str] = []
        failed: list[str] = []
        nonterminal = 0
        for job_id in job_ids:
            job = store.get_job(job_id)
            if job is None:
                failed.append(job_id)
            elif job.status is JobStatus.ENGLISH_SRT_READY:
                ready.append(job_id)
            elif job.status in {JobStatus.FAILED, JobStatus.CANCELLED}:
                failed.append(job_id)
            else:
                nonterminal += 1
        if failed or nonterminal == 0:
            return BatchWaitResult(ready=tuple(ready), failed=tuple(failed))
        if time.monotonic() >= deadline:
            raise TimeoutError("translation_only_batch_timeout")
        sleep_fn(poll_interval_seconds)


def verify_translation_only_batch(
    store: JobStore,
    job_ids: list[str],
    *,
    verify_public_api: bool = False,
    public_api_base_url: str = "https://javsubtitle.com",
) -> dict[str, str]:
    verified: dict[str, str] = {}
    for job_id in job_ids:
        job = store.get_job(job_id)
        if job is None:
            raise ValueError("job_missing")
        if job.status is not JobStatus.ENGLISH_SRT_READY:
            raise ValueError(f"job_not_ready:{job.normalized_movie_number}")
        if (
            not job.published_subtitle_id
            or not job.published_storage_path
            or not job.published_content_sha256
            or not job.published_file_size
        ):
            raise ValueError(f"publication_missing:{job.normalized_movie_number}")
        quality = _latest_quality_log(Path(job.job_dir_mac) / "logs" / "quality.log")
        if quality.get("passed") is not True:
            raise ValueError(f"quality_not_passed:{job.normalized_movie_number}")
        if verify_public_api and not _public_api_has_english_ai(
            public_api_base_url,
            job.normalized_movie_number,
        ):
            raise ValueError(f"public_api_missing_english_ai:{job.normalized_movie_number}")
        verified[job.normalized_movie_number] = "verified"
    return verified


def _latest_quality_log(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise ValueError("quality_log_missing")
    latest: dict[str, object] | None = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            latest = payload
    if latest is None:
        raise ValueError("quality_log_empty")
    return latest


def _public_api_has_english_ai(base_url: str, movie_number: str) -> bool:
    response = requests.get(
        f"{base_url.rstrip('/')}/api/movie/{movie_number}",
        params={"cacheNonce": str(int(time.time()))},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    subtitles = payload.get("subtitles") if isinstance(payload, dict) else None
    if not isinstance(subtitles, list):
        return False
    labels = {
        str(row.get("label") or row.get("lang") or "")
        for row in subtitles
        if isinstance(row, dict)
    }
    return "English_AI" in labels


def _append_receipt(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, ensure_ascii=True) + "\n")
