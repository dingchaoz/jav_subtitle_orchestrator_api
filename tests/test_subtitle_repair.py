from pathlib import Path
import hashlib
import json

import pytest

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore
from orchestrator.subtitle_repair import (
    enqueue_translation_only_repair_batch,
    plan_historical_repairs,
    plan_translation_only_repair_batch,
    read_translation_only_repair_plan,
    render_repair_report,
    write_translation_only_repair_plan,
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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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
                "public",
                job.id,
            ),
        )
    return store.get_job(job.id), paths


def test_repair_plan_is_read_only_and_respects_allowlist_and_limit(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = store.submit_job("abc-001", priority=100, force=False).job
    second = store.submit_job("abc-002", priority=100, force=False).job
    store.submit_job("abc-003", priority=100, force=False)
    for movie in ("abc-001", "abc-002", "abc-003"):
        _write_pair(mac_jobs_root, movie, bad=True)

    plans = plan_historical_repairs(
        store, allowlist={"ABC-001", "abc-002"}, limit=1
    )

    assert [plan.job_id for plan in plans] == [first.id]
    assert plans[0].movie_number == "abc-001"
    assert plans[0].reset_stage == "translation_only"
    assert plans[0].japanese_action == "preserve"
    assert plans[0].english_action == "quarantine"
    assert plans[0].would_requeue is True
    assert plans[0].would_overwrite_supabase is True
    assert "force" not in plans[0].to_dict()
    assert store.get_job(first.id).status.value == "queued"
    assert store.get_job(second.id).status.value == "queued"


def test_repair_plan_excludes_good_translation(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("abc-001", priority=100, force=False)
    _write_pair(mac_jobs_root, "abc-001", bad=False)

    assert plan_historical_repairs(store, allowlist=None, limit=10) == []


def test_repair_plan_matches_legacy_unpadded_alias_without_changing_paths(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("abc-7", priority=100, force=False).job
    with store.connection() as connection:
        connection.execute(
            "UPDATE jobs SET normalized_movie_number = ? WHERE id = ?",
            ("abc-7", job.id),
        )
    _write_pair(mac_jobs_root, "abc-7", bad=True)

    plans = plan_historical_repairs(
        store,
        allowlist={"abc-007"},
        limit=10,
    )

    assert [plan.job_id for plan in plans] == [job.id]
    assert plans[0].movie_number == "abc-7"
    assert plans[0].japanese_path.endswith("/abc-7/abc-7.Japanese.srt")
    assert plans[0].english_path.endswith("/abc-7/abc-7.English.srt")


def test_repair_report_contains_actions_but_not_subtitle_text(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("abc-001", priority=100, force=False)
    _write_pair(mac_jobs_root, "abc-001", bad=True)

    report = render_repair_report(
        plan_historical_repairs(store, allowlist={"abc-001"}, limit=10)
    )

    assert report.startswith("dry_run=true affected_count=1")
    assert "reset_stage=translation_only" in report
    assert "preserve_japanese=true" in report
    assert "would_requeue=true" in report
    assert "would_overwrite_supabase=true" in report
    assert "Cannot translate" not in report


def test_repair_cli_is_dry_run_and_does_not_initialize_store(
    sqlite_path, mac_jobs_root, monkeypatch, capsys
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("abc-001", priority=100, force=False)
    _write_pair(mac_jobs_root, "abc-001", bad=True)

    class Settings:
        db_path = sqlite_path
        jobs_root_mac = mac_jobs_root
        jobs_root_windows = "M:\\"

    monkeypatch.setattr("orchestrator.config.MacSettings", Settings)
    monkeypatch.setattr(
        JobStore,
        "initialize",
        lambda self: (_ for _ in ()).throw(AssertionError("dry-run initialized store")),
    )
    from orchestrator.__main__ import run_plan_historical_repairs

    run_plan_historical_repairs(allowlist={"abc-001"}, limit=1)

    output = capsys.readouterr().out
    assert output.startswith("dry_run=true affected_count=1")
    assert "force=true" not in output.lower()


def test_translation_only_batch_plan_and_enqueue_preserve_files_and_reset_stage(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first, first_paths = _make_ready_job(store, mac_jobs_root, "abc-001")
    second, _second_paths = _make_ready_job(store, mac_jobs_root, "abc-002")
    _make_ready_job(store, mac_jobs_root, "abc-003", bad=False)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\nabc-002\nabc-003\n", encoding="utf-8")
    japanese_before = first_paths.japanese_srt_path_mac.read_bytes()
    english_before = first_paths.english_srt_path_mac.read_bytes()

    plan = plan_translation_only_repair_batch(store, allowlist, limit=2)

    assert [item.job_id for item in plan.items] == [first.id, second.id]
    assert plan.plan_sha256 == plan.recalculate_sha256()
    assert plan.items[0].japanese_sha256 == _sha256(first_paths.japanese_srt_path_mac)
    assert plan.items[0].english_sha256 == _sha256(first_paths.english_srt_path_mac)
    assert "force" not in plan.to_payload()

    plan_file = tmp_path / "translation-only-plan.json"
    write_translation_only_repair_plan(plan_file, plan)
    loaded = read_translation_only_repair_plan(plan_file)
    assert loaded == plan

    with pytest.raises(ValueError, match="confirm_plan_sha256"):
        enqueue_translation_only_repair_batch(
            store,
            loaded,
            confirm_plan_sha256="0" * 64,
        )

    reset = enqueue_translation_only_repair_batch(
        store,
        loaded,
        confirm_plan_sha256=plan.plan_sha256,
    )

    assert [job.id for job in reset] == [first.id, second.id]
    refreshed = store.get_job(first.id)
    assert refreshed.status is JobStatus.TRANSCRIPTION_DONE
    assert refreshed.claimed_by is None
    assert refreshed.translation_attempt_count == 0
    assert refreshed.publish_attempt_count == 0
    assert refreshed.catalog_sync_attempt_count == 0
    assert refreshed.published_subtitle_id is None
    assert refreshed.published_content_sha256 is None
    assert first_paths.japanese_srt_path_mac.read_bytes() == japanese_before
    assert first_paths.english_srt_path_mac.read_bytes() == english_before


def test_translation_only_batch_cli_requires_plan_confirmation(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch, capsys
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _paths = _make_ready_job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    output = tmp_path / "plan.json"
    allowlist.write_text("abc-001\n", encoding="utf-8")

    class Settings:
        db_path = sqlite_path
        jobs_root_mac = mac_jobs_root
        jobs_root_windows = "M:\\"

    monkeypatch.setattr("orchestrator.config.MacSettings", Settings)
    from orchestrator.__main__ import (
        build_parser,
        run_enqueue_translation_only_repair_batch,
        run_plan_translation_only_repair_batch,
    )

    parser = build_parser()
    planned_args = parser.parse_args(
        [
            "plan-translation-only-repair-batch",
            "--allowlist-file",
            str(allowlist),
            "--limit",
            "1",
            "--output",
            str(output),
        ]
    )
    assert planned_args.command == "plan-translation-only-repair-batch"
    assert not hasattr(planned_args, "force")

    run_plan_translation_only_repair_batch(
        allowlist_file=allowlist,
        limit=1,
        output=output,
    )
    planned_output = capsys.readouterr().out
    assert "planned=true" in planned_output
    assert "selected=1" in planned_output
    assert "Cannot translate" not in planned_output
    plan = read_translation_only_repair_plan(output)

    enqueued_args = parser.parse_args(
        [
            "enqueue-translation-only-repair-batch",
            "--plan-file",
            str(output),
            "--confirm-plan-sha256",
            plan.plan_sha256,
        ]
    )
    assert enqueued_args.command == "enqueue-translation-only-repair-batch"
    assert not hasattr(enqueued_args, "force")

    run_enqueue_translation_only_repair_batch(
        plan_file=output,
        confirm_plan_sha256=plan.plan_sha256,
    )

    enqueue_output = capsys.readouterr().out
    assert f"job_ids={job.id}" in enqueue_output
    assert store.get_job(job.id).status is JobStatus.TRANSCRIPTION_DONE
