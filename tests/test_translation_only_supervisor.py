from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore
from orchestrator.translation_only_supervisor import (
    BatchWaitResult,
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


def _mark_ready(store: JobStore, job_id: str, root: Path, movie: str) -> None:
    paths = build_job_paths(movie, root, "M:\\")
    (paths.job_dir_mac / "logs").mkdir(parents=True, exist_ok=True)
    (paths.job_dir_mac / "logs" / "quality.log").write_text(
        json.dumps({"passed": True, "reason_codes": []}) + "\n",
        encoding="utf-8",
    )
    with store.connection() as connection:
        connection.execute(
            """
            UPDATE jobs
            SET status = ?, claimed_by = NULL, published_subtitle_id = ?,
                published_storage_path = ?, published_content_sha256 = ?,
                published_file_size = ?, error = NULL
            WHERE id = ?
            """,
            (
                JobStatus.ENGLISH_SRT_READY.value,
                f"subtitle-{movie}",
                f"{movie}/{movie}/{movie}-English_AI.srt",
                "b" * 64,
                paths.english_srt_path_mac.stat().st_size,
                job_id,
            ),
        )


def _mark_failed(store: JobStore, job_id: str) -> None:
    with store.connection() as connection:
        connection.execute(
            "UPDATE jobs SET status = ?, claimed_by = NULL, error = ? WHERE id = ?",
            (
                JobStatus.FAILED.value,
                "translating: quality_gate_failed:dominant_text_collapse",
                job_id,
            ),
        )


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


def test_supervisor_continues_after_isolated_job_failure(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch, capsys
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first, _ = _make_ready_job(store, mac_jobs_root, "abc-001")
    second, _ = _make_ready_job(store, mac_jobs_root, "abc-002")
    third, _ = _make_ready_job(store, mac_jobs_root, "abc-003")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\nabc-002\nabc-003\n", encoding="utf-8")

    def fake_wait(store_arg, job_ids, **_kwargs):
        assert store_arg is store
        ready: list[str] = []
        failed: list[str] = []
        for job_id in job_ids:
            job = store.get_job(job_id)
            assert job is not None
            if job_id == second.id:
                _mark_failed(store, job_id)
                failed.append(job_id)
            else:
                _mark_ready(store, job_id, mac_jobs_root, job.normalized_movie_number)
                ready.append(job_id)
        return BatchWaitResult(ready=tuple(ready), failed=tuple(failed))

    monkeypatch.setattr(
        "orchestrator.translation_only_supervisor.wait_for_translation_only_batch",
        fake_wait,
    )

    result = run_translation_only_supervisor(
        store,
        TranslationOnlySupervisorConfig(
            allowlist_file=allowlist,
            work_dir=tmp_path / "work",
            batch_size=2,
            max_jobs=3,
            execute=True,
            confirm_remaining_count=3,
        ),
    )

    assert result.action == "completed"
    assert result.enqueued_count == 3
    assert result.completed_count == 2
    assert result.failed_count == 1
    assert result.batches == 2
    assert store.get_job(first.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_job(second.id).status is JobStatus.FAILED
    assert store.get_job(third.id).status is JobStatus.ENGLISH_SRT_READY
    output = capsys.readouterr().out
    assert "batch=1" in output
    assert "failed=1" in output
    receipts = Path(result.receipt_file).read_text(encoding="utf-8").splitlines()
    assert len(receipts) == 2
    assert json.loads(receipts[0])["status"] == "verified_with_failures"
    assert json.loads(receipts[1])["status"] == "verified"


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
