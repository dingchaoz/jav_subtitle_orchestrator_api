from pathlib import Path

from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore
from orchestrator.subtitle_repair import plan_historical_repairs, render_repair_report


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
