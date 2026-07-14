from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore
from orchestrator.translation_only_supervisor import (
    TranslationOnlySupervisorConfig,
    run_translation_only_supervisor,
    verify_translation_only_batch,
)


def _write_pair(root: Path, movie: str, *, bad: bool) -> None:
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    japanese: list[str] = []
    english: list[str] = []
    for index in range(1, 26):
        japanese.append(
            f"{index}\n00:00:{index - 1:02d},000 --> 00:00:{index:02d},000\n日本語{index}\n"
        )
        line = "Cannot translate" if bad else f"Good translated sentence {index}."
        english.append(
            f"{index}\n00:00:{index - 1:02d},000 --> 00:00:{index:02d},000\n{line}\n"
        )
    paths.japanese_srt_path_mac.write_text("\n".join(japanese), encoding="utf-8")
    paths.english_srt_path_mac.write_text("\n".join(english), encoding="utf-8")


def _make_ready_job(store: JobStore, root: Path, movie: str, *, bad: bool = True):
    job = store.submit_job(movie, priority=100, force=False).job
    assert job is not None
    _write_pair(root, movie, bad=bad)
    paths = build_job_paths(movie, root, "M:\\")
    with store.connection() as connection:
        connection.execute(
            """
            UPDATE jobs
            SET status = ?, japanese_srt_path_mac = ?, japanese_srt_path_windows = ?,
                english_srt_path_mac = ?, english_srt_path_windows = ?,
                published_subtitle_id = ?, published_storage_path = ?,
                published_content_sha256 = ?, published_file_size = ?,
                catalog_movie_uuid = ?, metadata_status = ?, metadata_source = ?
            WHERE id = ?
            """,
            (
                JobStatus.ENGLISH_SRT_READY.value,
                str(paths.japanese_srt_path_mac),
                paths.japanese_srt_path_windows,
                str(paths.english_srt_path_mac),
                paths.english_srt_path_windows,
                "00000000-0000-0000-0000-000000000001",
                f"{movie}/{movie}/{movie}-English_AI.srt",
                "a" * 64,
                paths.english_srt_path_mac.stat().st_size,
                "00000000-0000-0000-0000-000000000002",
                "complete",
                "missav",
                job.id,
            ),
        )
    return store.get_job(job.id), paths


def test_supervisor_dry_run_does_not_enqueue(sqlite_path, mac_jobs_root, tmp_path):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _paths = _make_ready_job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n", encoding="utf-8")

    result = run_translation_only_supervisor(
        store,
        TranslationOnlySupervisorConfig(
            allowlist_file=allowlist,
            work_dir=tmp_path / "work",
            batch_size=1,
            max_jobs=1,
            execute=False,
        ),
    )

    assert result.action == "dry_run"
    assert result.remaining_count == 1
    assert result.enqueued_count == 0
    assert result.completed_count == 0
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert result.plan_files


def test_supervisor_execute_requires_exact_remaining_confirmation(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _paths = _make_ready_job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n", encoding="utf-8")

    with pytest.raises(ValueError, match="confirm_remaining_count"):
        run_translation_only_supervisor(
            store,
            TranslationOnlySupervisorConfig(
                allowlist_file=allowlist,
                work_dir=tmp_path / "work",
                batch_size=1,
                max_jobs=1,
                execute=True,
                confirm_remaining_count=2,
            ),
        )

    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY


def test_verify_translation_only_batch_checks_db_and_quality_log(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = _make_ready_job(store, mac_jobs_root, "abc-001", bad=False)
    quality_dir = paths.job_dir_mac / "logs"
    quality_dir.mkdir(parents=True, exist_ok=True)
    quality_dir.joinpath("quality.log").write_text(
        json.dumps({"passed": True, "reason_codes": []}) + "\n",
        encoding="utf-8",
    )

    verified = verify_translation_only_batch(store, [job.id])

    assert verified == {"abc-001": "verified"}


def test_verify_translation_only_batch_rejects_missing_publication(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = _make_ready_job(store, mac_jobs_root, "abc-001", bad=False)
    (paths.job_dir_mac / "logs").mkdir(parents=True, exist_ok=True)
    (paths.job_dir_mac / "logs" / "quality.log").write_text(
        json.dumps({"passed": True, "reason_codes": []}) + "\n",
        encoding="utf-8",
    )
    with sqlite3.connect(sqlite_path) as connection:
        connection.execute(
            "UPDATE jobs SET published_content_sha256 = NULL WHERE id = ?",
            (job.id,),
        )

    with pytest.raises(ValueError, match="publication_missing"):
        verify_translation_only_batch(store, [job.id])


def test_supervisor_cli_parses_execute_confirmation():
    from orchestrator.__main__ import build_parser

    args = build_parser().parse_args(
        [
            "run-translation-only-repair-supervisor",
            "--allowlist-file",
            "allowlist.txt",
            "--work-dir",
            "reports/run",
            "--batch-size",
            "20",
            "--max-jobs",
            "305",
            "--execute",
            "--confirm-remaining-count",
            "305",
            "--verify-public-api",
        ]
    )

    assert args.command == "run-translation-only-repair-supervisor"
    assert args.execute is True
    assert args.confirm_remaining_count == 305
    assert args.verify_public_api is True
