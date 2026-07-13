from dataclasses import FrozenInstanceError
import json
from pathlib import Path

import pytest
import requests

from orchestrator.__main__ import build_parser
from orchestrator.catalog_repair import (
    plan_catalog_repairs,
    render_catalog_repair_report,
)
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore


def _write_subtitles(root: Path, movie: str, *, quality_passes: bool = True):
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    japanese = []
    english = []
    for index in range(1, 26):
        japanese.append(
            f"{index}\n00:00:{index - 1:02d},000 --> 00:00:{index:02d},000\n"
            f"日本語{index}\n"
        )
        translated = (
            f"Private translated sentence {index}."
            if quality_passes
            else "Cannot translate"
        )
        english.append(
            f"{index}\n00:00:{index - 1:02d},000 --> 00:00:{index:02d},000\n"
            f"{translated}\n"
        )
    paths.japanese_srt_path_mac.write_text("\n".join(japanese), encoding="utf-8")
    paths.english_srt_path_mac.write_text("\n".join(english), encoding="utf-8")
    return paths


def _store(sqlite_path: Path, mac_jobs_root: Path) -> JobStore:
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    return store


def test_catalog_repair_plan_is_deterministic_allowlisted_and_limited(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    later = store.submit_job("abc-002", priority=20, force=False).job
    first = store.submit_job("abc-001", priority=10, force=False).job
    store.submit_job("abc-003", priority=1, force=False)
    for movie in ("abc-001", "abc-002", "abc-003"):
        _write_subtitles(mac_jobs_root, movie)

    plans = plan_catalog_repairs(
        store,
        allowlist={"ABC1", "abc-002"},
        limit=1,
    )

    assert [plan.job_id for plan in plans] == [first.id]
    assert plans[0].movie_code == "abc-001"
    assert plans[0].current_status == "queued"
    assert plans[0].action == "would_ensure_catalog_then_publish"
    assert (
        plans[0].storage_effect
        == "would upsert/overwrite Storage path=abc/abc-001/abc-001-English_AI.srt"
    )
    assert later.id not in {plan.job_id for plan in plans}


@pytest.mark.parametrize("limit", [0, 1001])
def test_catalog_repair_plan_rejects_unsafe_limits(
    sqlite_path, mac_jobs_root, limit
):
    store = _store(sqlite_path, mac_jobs_root)

    with pytest.raises(ValueError, match="limit"):
        plan_catalog_repairs(store, allowlist=None, limit=limit)


def test_catalog_repair_excludes_missing_empty_and_bad_subtitles(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    store.submit_job("abc-001", priority=1, force=False)
    store.submit_job("abc-002", priority=2, force=False)
    store.submit_job("abc-003", priority=3, force=False)
    missing = build_job_paths("abc-001", mac_jobs_root, "M:\\")
    missing.job_dir_mac.mkdir(parents=True)
    missing.japanese_srt_path_mac.write_text("nonempty", encoding="utf-8")
    empty = build_job_paths("abc-002", mac_jobs_root, "M:\\")
    empty.job_dir_mac.mkdir(parents=True)
    empty.japanese_srt_path_mac.write_bytes(b"")
    empty.english_srt_path_mac.write_text("nonempty", encoding="utf-8")
    _write_subtitles(mac_jobs_root, "abc-003", quality_passes=False)

    assert plan_catalog_repairs(store, allowlist=None, limit=100) == []


def test_quality_pass_is_eligible_without_metadata(sqlite_path, mac_jobs_root):
    store = _store(sqlite_path, mac_jobs_root)
    job = store.submit_job("abc-004", priority=1, force=False).job
    paths = _write_subtitles(mac_jobs_root, "abc-004")

    plan = plan_catalog_repairs(store, allowlist=None, limit=100)[0]

    assert plan.job_id == job.id
    assert plan.japanese_srt == str(paths.japanese_srt_path_mac)
    assert plan.english_srt == str(paths.english_srt_path_mac)
    assert plan.metadata_path is None
    assert plan.metadata_available is False
    assert (
        plan.expected_metadata_source
        == "public_or_missav_or_placeholder"
    )
    report = render_catalog_repair_report([plan])
    assert "metadata_path=-" in report
    assert (
        "expected_source_candidates=public_or_missav_or_placeholder"
        in report
    )
    assert str(paths.metadata_path_mac) not in report
    with pytest.raises(FrozenInstanceError):
        plan.action = "mutate"


def test_valid_local_metadata_sets_expected_source(sqlite_path, mac_jobs_root):
    store = _store(sqlite_path, mac_jobs_root)
    store.submit_job("abc-005", priority=1, force=False)
    paths = _write_subtitles(mac_jobs_root, "abc-005")
    paths.metadata_path_mac.write_text(
        json.dumps({"number": "ABC5", "title": "Secret Movie Title"}),
        encoding="utf-8",
    )

    plan = plan_catalog_repairs(store, allowlist=None, limit=100)[0]

    assert plan.metadata_path == str(paths.metadata_path_mac)
    assert plan.metadata_available is True
    assert plan.expected_metadata_source == "public_or_missav_or_local"


def test_catalog_repair_excludes_verified_ready_publication_but_keeps_legacy_ready(
    sqlite_path, mac_jobs_root
):
    store = _store(sqlite_path, mac_jobs_root)
    verified = store.submit_job("abc-009", priority=1, force=False).job
    legacy = store.submit_job("abc-010", priority=2, force=False).job
    for movie in ("abc-009", "abc-010"):
        _write_subtitles(mac_jobs_root, movie)
    with store.connection() as connection:
        connection.execute(
            """
            UPDATE jobs
            SET status = 'english_srt_ready', catalog_movie_uuid = ?,
                metadata_status = 'complete', metadata_source = 'public'
            WHERE id = ?
            """,
            ("00000000-0000-0000-0000-000000000009", verified.id),
        )
        connection.execute(
            "UPDATE jobs SET status = 'english_srt_ready' WHERE id = ?",
            (legacy.id,),
        )

    plans = plan_catalog_repairs(store, allowlist=None, limit=100)

    assert [plan.job_id for plan in plans] == [legacy.id]


def test_catalog_repair_is_network_database_and_filesystem_read_only(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = _store(sqlite_path, mac_jobs_root)
    store.submit_job("abc-006", priority=1, force=False)
    paths = _write_subtitles(mac_jobs_root, "abc-006")
    paths.metadata_path_mac.write_text(
        json.dumps({"number": "abc-006", "title": "Secret Movie Title"}),
        encoding="utf-8",
    )
    before_database = sqlite_path.read_bytes()
    before_files = {
        path: path.read_bytes()
        for path in (
            paths.japanese_srt_path_mac,
            paths.english_srt_path_mac,
            paths.metadata_path_mac,
        )
    }
    calls = []

    def unexpected_request(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("dry run performed HTTP")

    monkeypatch.setattr(requests.Session, "request", unexpected_request)

    plans = plan_catalog_repairs(store, allowlist=None, limit=100)

    assert len(plans) == 1
    assert calls == []
    assert sqlite_path.read_bytes() == before_database
    assert {path: path.read_bytes() for path in before_files} == before_files


def test_catalog_repair_report_is_safe_and_complete(sqlite_path, mac_jobs_root):
    store = _store(sqlite_path, mac_jobs_root)
    store.submit_job("abc-007", priority=1, force=False)
    paths = _write_subtitles(mac_jobs_root, "abc-007")
    paths.metadata_path_mac.write_text(
        json.dumps({"number": "abc-007", "title": "Secret Movie Title"}),
        encoding="utf-8",
    )

    report = render_catalog_repair_report(
        plan_catalog_repairs(store, allowlist=None, limit=100)
    )

    assert report.startswith("DRY RUN affected_count=1")
    for label in (
        "job_id=",
        "movie_code=abc-007",
        "status=queued",
        "expected_source_candidates=public_or_missav_or_local",
        "action=would_ensure_catalog_then_publish",
        "storage=would upsert/overwrite Storage",
    ):
        assert label in report
    assert "force=True" not in report
    assert "Private translated sentence" not in report
    assert "Secret Movie Title" not in report
    assert "service_role" not in report.lower()


def test_catalog_repair_cli_parses_optional_comma_allowlist_and_safe_limit():
    parser = build_parser()

    explicit = parser.parse_args(
        ["plan-catalog-repairs", "--allowlist", "ABC1,abc-002", "--limit", "5"]
    )
    defaulted = parser.parse_args(["plan-catalog-repairs"])

    assert explicit.command == "plan-catalog-repairs"
    assert explicit.allowlist == "ABC1,abc-002"
    assert explicit.limit == 5
    assert defaulted.allowlist is None
    assert defaulted.limit == 100
    for parsed in (explicit, defaulted):
        for forbidden in ("force", "delete", "upload", "overwrite"):
            assert not hasattr(parsed, forbidden)


def test_catalog_repair_allowlist_ignores_empty_tokens_and_canonicalizes():
    from orchestrator.__main__ import _parse_allowlist

    assert _parse_allowlist(" ABC1,abc-002,, ") == {"abc-001", "abc-002"}
    assert _parse_allowlist(",,") is None
    assert _parse_allowlist(None) is None


def test_catalog_repair_cli_runner_only_prints(
    sqlite_path, mac_jobs_root, monkeypatch, capsys
):
    store = _store(sqlite_path, mac_jobs_root)
    store.submit_job("abc-008", priority=1, force=False)
    _write_subtitles(mac_jobs_root, "abc-008")

    class Settings:
        db_path = sqlite_path
        jobs_root_mac = mac_jobs_root
        jobs_root_windows = "M:\\"

    monkeypatch.setattr("orchestrator.config.MacSettings", Settings)
    monkeypatch.setattr(
        JobStore,
        "initialize",
        lambda self: (_ for _ in ()).throw(AssertionError("dry run initialized store")),
    )
    from orchestrator.__main__ import run_plan_catalog_repairs

    run_plan_catalog_repairs(allowlist={"abc-008"}, limit=1)

    output = capsys.readouterr().out
    assert output.startswith("DRY RUN affected_count=1")
    assert "force=True" not in output
