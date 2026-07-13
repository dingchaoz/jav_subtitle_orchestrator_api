from dataclasses import FrozenInstanceError
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest
import requests

import orchestrator.catalog_repair as catalog_repair
from orchestrator.__main__ import build_parser
from orchestrator.catalog_repair import (
    CatalogPublicationCanaryReceipt,
    plan_catalog_repairs,
    prepare_catalog_publication_canary,
    render_catalog_repair_report,
)
from orchestrator.models import JobStatus
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


def _write_canary_allowlist(path: Path, *movies: str) -> Path:
    path.write_text("".join(f"{movie}\n" for movie in movies), encoding="utf-8")
    return path


def _prepare_canary_candidate(
    store: JobStore,
    root: Path,
    movie: str,
    *,
    quality_passes: bool = True,
    status: JobStatus = JobStatus.FAILED,
    claimed_by: str | None = None,
    catalog_movie_uuid: str | None = None,
    metadata_status: str | None = None,
    metadata_source: str | None = None,
):
    job = store.submit_job(movie, priority=100, force=False).job
    paths = _write_subtitles(root, movie, quality_passes=quality_passes)
    paths.audio_path_mac.write_bytes(b"canary-audio")
    rejected = paths.job_dir_mac / "rejected"
    rejected.mkdir()
    (rejected / "existing.srt").write_bytes(b"existing-rejected")
    with store.connection() as connection:
        connection.execute(
            "UPDATE jobs SET status = ?, claimed_by = ?, lease_expires_at = ?, "
            "translation_attempt_count = 3, publish_attempt_count = 4, "
            "next_publish_attempt_at = ?, catalog_movie_uuid = ?, "
            "metadata_status = ?, metadata_source = ?, error = ? WHERE id = ?",
            (
                status.value,
                claimed_by,
                "2026-07-13T12:00:00+00:00" if claimed_by else None,
                "2026-07-13T13:00:00+00:00",
                catalog_movie_uuid,
                metadata_status,
                metadata_source,
                "stale publication error",
                job.id,
            ),
        )
    return job, paths


def _canary_files_snapshot(paths) -> dict[str, object]:
    rejected = paths.job_dir_mac / "rejected"
    return {
        "audio": paths.audio_path_mac.read_bytes(),
        "japanese": (
            paths.japanese_srt_path_mac.read_bytes()
            if paths.japanese_srt_path_mac.exists()
            else None
        ),
        "english": (
            paths.english_srt_path_mac.read_bytes()
            if paths.english_srt_path_mac.exists()
            else None
        ),
        "japanese_symlink": paths.japanese_srt_path_mac.is_symlink(),
        "english_symlink": paths.english_srt_path_mac.is_symlink(),
        "rejected": {
            path.name: path.read_bytes()
            for path in sorted(rejected.iterdir())
        },
    }


def _assert_canary_failure_is_atomic(store, job_id, paths, row_before, files_before):
    assert store.get_job(job_id) == row_before
    assert _canary_files_snapshot(paths) == files_before


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


def test_catalog_repair_supports_legacy_database_without_writing(
    sqlite_path, mac_jobs_root
):
    with sqlite3.connect(sqlite_path) as connection:
        connection.execute(
            """
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY,
                movie_number TEXT NOT NULL,
                normalized_movie_number TEXT NOT NULL,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO jobs (
                id, movie_number, normalized_movie_number, status,
                priority, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-job",
                "ABC11",
                "abc-011",
                "english_srt_ready",
                1,
                "2025-01-01T00:00:00+00:00",
                "2025-01-01T00:00:00+00:00",
            ),
        )
    paths = _write_subtitles(mac_jobs_root, "abc-011")
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    before_database_files = {
        path.name: path.read_bytes()
        for path in sqlite_path.parent.glob(f"{sqlite_path.name}*")
    }

    plans = plan_catalog_repairs(store, allowlist=None, limit=100)

    assert [plan.job_id for plan in plans] == ["legacy-job"]
    assert plans[0].current_status == "english_srt_ready"
    assert plans[0].metadata_path is None
    assert plans[0].english_srt == str(paths.english_srt_path_mac)
    assert {
        path.name: path.read_bytes()
        for path in sqlite_path.parent.glob(f"{sqlite_path.name}*")
    } == before_database_files


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


def test_prepare_catalog_publication_canary_cli_requires_exact_arguments():
    parser = build_parser()

    parsed = parser.parse_args(
        [
            "prepare-catalog-publication-canary",
            "--allowlist-file",
            "approved.txt",
            "--movie",
            "ABC22",
            "--limit",
            "1",
            "--confirm-job-id",
            "job_exact",
        ]
    )

    assert parsed.command == "prepare-catalog-publication-canary"
    assert parsed.allowlist_file == Path("approved.txt")
    assert parsed.movie == "ABC22"
    assert parsed.limit == 1
    assert parsed.confirm_job_id == "job_exact"
    for forbidden in ("force", "batch", "preferred_movie"):
        assert not hasattr(parsed, forbidden)


def test_prepare_catalog_publication_canary_main_dispatches_exact_arguments(
    monkeypatch,
):
    import orchestrator.__main__ as main_module

    calls = []

    def record_prepare(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        "sys.argv",
        [
            "python -m orchestrator",
            "prepare-catalog-publication-canary",
            "--allowlist-file",
            "approved.txt",
            "--movie",
            "MIST166",
            "--limit",
            "1",
            "--confirm-job-id",
            "job_exact",
        ],
    )
    monkeypatch.setattr(main_module, "configure_logging", lambda: None)
    monkeypatch.setattr(
        main_module,
        "run_prepare_catalog_publication_canary",
        record_prepare,
    )

    main_module.main()

    assert calls == [
        {
            "allowlist_file": Path("approved.txt"),
            "movie": "MIST166",
            "limit": 1,
            "confirm_job_id": "job_exact",
        }
    ]


def test_prepare_catalog_publication_canary_cli_runner_is_sanitized_and_local(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch, capsys
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-022")
    paths.metadata_path_mac.write_text(
        json.dumps({"number": "ABC22", "title": "Secret Adult Metadata"}),
        encoding="utf-8",
    )
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "ABC22")
    files_before = _canary_files_snapshot(paths)
    directory_before = {
        path.relative_to(paths.job_dir_mac): path.read_bytes()
        for path in paths.job_dir_mac.rglob("*")
        if path.is_file()
    }
    english_sha256 = hashlib.sha256(
        paths.english_srt_path_mac.read_bytes()
    ).hexdigest()

    class Settings:
        db_path = sqlite_path
        jobs_root_mac = mac_jobs_root
        jobs_root_windows = "M:\\"

    def unexpected(*args, **kwargs):
        raise AssertionError("CLI preparation invoked external work")

    monkeypatch.setattr("orchestrator.config.MacSettings", Settings)
    monkeypatch.setattr(requests.Session, "request", unexpected)
    monkeypatch.setattr(
        "orchestrator.translation.SubtitleTranslator.translate_to_english",
        unexpected,
    )
    monkeypatch.setattr(
        "orchestrator.supabase_publisher.SupabaseSubtitlePublisher.publish_english_ai",
        unexpected,
    )
    from orchestrator.__main__ import run_prepare_catalog_publication_canary

    run_prepare_catalog_publication_canary(
        allowlist_file=allowlist,
        movie="ABC22",
        limit=1,
        confirm_job_id=job.id,
    )

    assert capsys.readouterr().out == (
        f"prepared=true job_id={job.id} movie=abc-022 prior_status=failed "
        "new_status=publish_pending translation_attempt_count=3 "
        f"english_sha256={english_sha256} quality_passed=true cues=25 "
        "unique_ratio=1.000 known_bad=0\n"
    )
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert _canary_files_snapshot(paths) == files_before
    assert {
        path.relative_to(paths.job_dir_mac): path.read_bytes()
        for path in paths.job_dir_mac.rglob("*")
        if path.is_file()
    } == directory_before


def test_prepare_catalog_publication_canary_is_exact_and_preserves_translation(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-021")
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "ABC21")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)
    english_before = paths.english_srt_path_mac.read_bytes()

    def unexpected(*args, **kwargs):
        raise AssertionError("canary preparation invoked external work")

    monkeypatch.setattr(requests.Session, "request", unexpected)
    monkeypatch.setattr(
        "orchestrator.translation.SubtitleTranslator.translate_to_english",
        unexpected,
    )
    monkeypatch.setattr(
        "orchestrator.supabase_publisher.SupabaseSubtitlePublisher.publish_english_ai",
        unexpected,
    )

    receipt = prepare_catalog_publication_canary(
        store,
        allowlist,
        movie="ABC21",
        limit=1,
        confirm_job_id=job.id,
    )

    assert receipt == CatalogPublicationCanaryReceipt(
        job_id=job.id,
        movie_code="abc-021",
        prior_status=JobStatus.FAILED,
        new_status=JobStatus.PUBLISH_PENDING,
        translation_attempt_count=3,
        english_sha256=hashlib.sha256(english_before).hexdigest(),
        quality_passed=True,
        english_cue_count=25,
        english_unique_ratio=1.0,
        known_bad_phrase_count=0,
    )
    with pytest.raises(FrozenInstanceError):
        receipt.new_status = JobStatus.FAILED
    assert not hasattr(receipt, "__dict__")
    assert "Private translated sentence" not in repr(receipt)

    prepared = store.get_job(job.id)
    assert prepared.status is JobStatus.PUBLISH_PENDING
    assert prepared.translation_attempt_count == row_before.translation_attempt_count
    assert prepared.publish_attempt_count == 0
    assert prepared.next_publish_attempt_at is None
    assert prepared.catalog_movie_uuid is None
    assert prepared.metadata_status is None
    assert prepared.metadata_source is None
    assert prepared.error is None
    assert _canary_files_snapshot(paths) == files_before


@pytest.mark.parametrize("limit", [0, 2, -1])
def test_prepare_catalog_publication_canary_requires_exactly_one(
    sqlite_path, mac_jobs_root, tmp_path, limit
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-030")
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-030")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)

    with pytest.raises(ValueError, match="exactly 1"):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-030",
            limit=limit,
            confirm_job_id=job.id,
        )

    _assert_canary_failure_is_atomic(
        store, job.id, paths, row_before, files_before
    )


@pytest.mark.parametrize("allowlist_state", ["empty", "duplicate", "invalid", "symlink"])
def test_prepare_catalog_publication_canary_reuses_strict_allowlist_loader(
    sqlite_path, mac_jobs_root, tmp_path, allowlist_state
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-031")
    allowlist = tmp_path / "allowlist.txt"
    if allowlist_state == "empty":
        allowlist.write_bytes(b"")
    elif allowlist_state == "duplicate":
        allowlist.write_text("ABC31\nabc-031\n", encoding="utf-8")
    elif allowlist_state == "invalid":
        allowlist.write_text("not-a-movie\n", encoding="utf-8")
    else:
        target = _write_canary_allowlist(tmp_path / "real.txt", "abc-031")
        allowlist.symlink_to(target)
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)

    with pytest.raises(ValueError, match="allowlist"):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-031",
            limit=1,
            confirm_job_id=job.id,
        )

    _assert_canary_failure_is_atomic(
        store, job.id, paths, row_before, files_before
    )


def test_prepare_catalog_publication_canary_rejects_movie_absent_from_allowlist(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-032")
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-999")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)

    with pytest.raises(ValueError, match="explicit allowlist"):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="ABC32",
            limit=1,
            confirm_job_id=job.id,
        )

    _assert_canary_failure_is_atomic(
        store, job.id, paths, row_before, files_before
    )


def test_prepare_catalog_publication_canary_rejects_missing_exact_job(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-033")
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-033")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)

    with pytest.raises(ValueError, match="does not exist"):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-033",
            limit=1,
            confirm_job_id="job_does_not_exist",
        )

    _assert_canary_failure_is_atomic(
        store, job.id, paths, row_before, files_before
    )


def test_prepare_catalog_publication_canary_rejects_job_movie_mismatch(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = _store(sqlite_path, mac_jobs_root)
    requested_job, requested_paths = _prepare_canary_candidate(
        store, mac_jobs_root, "abc-034"
    )
    other_job, other_paths = _prepare_canary_candidate(
        store, mac_jobs_root, "xyz-034"
    )
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-034")
    requested_before = store.get_job(requested_job.id)
    requested_files_before = _canary_files_snapshot(requested_paths)
    other_before = store.get_job(other_job.id)
    other_files_before = _canary_files_snapshot(other_paths)

    with pytest.raises(ValueError, match="does not match"):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-034",
            limit=1,
            confirm_job_id=other_job.id,
        )

    _assert_canary_failure_is_atomic(
        store,
        requested_job.id,
        requested_paths,
        requested_before,
        requested_files_before,
    )
    _assert_canary_failure_is_atomic(
        store, other_job.id, other_paths, other_before, other_files_before
    )


@pytest.mark.parametrize(
    ("status", "claimed_by"),
    [
        (JobStatus.FAILED, "publisher"),
        (JobStatus.QUEUED, None),
        (JobStatus.PUBLISH_PENDING, None),
        (JobStatus.PUBLISHING, None),
    ],
)
def test_prepare_catalog_publication_canary_rejects_claimed_or_ineligible_row(
    sqlite_path, mac_jobs_root, tmp_path, status, claimed_by
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(
        store,
        mac_jobs_root,
        "abc-035",
        status=status,
        claimed_by=claimed_by,
    )
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-035")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)

    with pytest.raises(ValueError, match="claimed|status"):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-035",
            limit=1,
            confirm_job_id=job.id,
        )

    _assert_canary_failure_is_atomic(
        store, job.id, paths, row_before, files_before
    )


def test_prepare_catalog_publication_canary_rejects_modern_verified_ready(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(
        store,
        mac_jobs_root,
        "abc-036",
        status=JobStatus.ENGLISH_SRT_READY,
        catalog_movie_uuid="f1bd9932-5697-4f16-865a-c56edc73d491",
        metadata_status="complete",
        metadata_source="public",
    )
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-036")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)

    with pytest.raises(ValueError, match="verified publication"):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-036",
            limit=1,
            confirm_job_id=job.id,
        )

    _assert_canary_failure_is_atomic(
        store, job.id, paths, row_before, files_before
    )


def test_prepare_catalog_publication_canary_rejects_bad_quality_safely(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(
        store, mac_jobs_root, "abc-037", quality_passes=False
    )
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-037")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)

    with pytest.raises(ValueError) as error:
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-037",
            limit=1,
            confirm_job_id=job.id,
        )

    message = str(error.value)
    assert message.startswith("quality_gate_failed:")
    reason_codes = message.removeprefix("quality_gate_failed:").split(",")
    assert reason_codes
    assert all(code.replace("_", "").isalnum() for code in reason_codes)
    assert "Cannot translate" not in message
    assert "日本語" not in message
    _assert_canary_failure_is_atomic(
        store, job.id, paths, row_before, files_before
    )


@pytest.mark.parametrize("language", ["japanese", "english"])
@pytest.mark.parametrize("file_state", ["missing", "empty", "symlink"])
def test_prepare_catalog_publication_canary_rejects_unsafe_canonical_subtitles(
    sqlite_path, mac_jobs_root, tmp_path, language, file_state
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-038")
    subtitle = getattr(paths, f"{language}_srt_path_mac")
    if file_state == "missing":
        subtitle.unlink()
    elif file_state == "empty":
        subtitle.write_bytes(b"")
    else:
        external = tmp_path / f"external-{language}.srt"
        external.write_bytes(subtitle.read_bytes())
        subtitle.unlink()
        subtitle.symlink_to(external)
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-038")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)

    expected_state = "not_regular" if file_state == "symlink" else file_state
    with pytest.raises(
        ValueError,
        match=rf"^quality_gate_failed:{language}_srt_{expected_state}$",
    ):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-038",
            limit=1,
            confirm_job_id=job.id,
        )

    _assert_canary_failure_is_atomic(
        store, job.id, paths, row_before, files_before
    )


def test_prepare_catalog_publication_canary_rechecks_allowlist_before_mutation(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-039")
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-039")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)
    reads = iter((frozenset({"abc-039"}), frozenset({"abc-999"})))
    monkeypatch.setattr(
        "orchestrator.catalog_repair.load_repair_allowlist",
        lambda path: next(reads),
    )

    with pytest.raises(RuntimeError, match="allowlist changed"):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-039",
            limit=1,
            confirm_job_id=job.id,
        )

    _assert_canary_failure_is_atomic(
        store, job.id, paths, row_before, files_before
    )


@pytest.mark.parametrize("changed_language", ["english", "japanese"])
def test_prepare_catalog_publication_canary_rejects_subtitle_changed_during_quality(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch, changed_language
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-040")
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-040")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)
    changed_bytes = b"changed while the quality gate was returning\n"
    changed_path = getattr(paths, f"{changed_language}_srt_path_mac")
    real_validate = catalog_repair.validate_translation_quality

    def validate_then_change(japanese_path, english_path):
        report = real_validate(japanese_path, english_path)
        assert report.passed is True
        changed_path.write_bytes(changed_bytes)
        return report

    def unexpected_prepare(*args, **kwargs):
        raise AssertionError("changed subtitle reached store transition")

    monkeypatch.setattr(
        catalog_repair,
        "validate_translation_quality",
        validate_then_change,
    )
    monkeypatch.setattr(
        store,
        "prepare_catalog_publication_repair",
        unexpected_prepare,
    )

    with pytest.raises(
        ValueError,
        match="^quality_gate_failed:subtitle_changed_during_validation$",
    ):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-040",
            limit=1,
            confirm_job_id=job.id,
        )

    assert store.get_job(job.id) == row_before
    expected_files = {**files_before, changed_language: changed_bytes}
    assert _canary_files_snapshot(paths) == expected_files


@pytest.mark.parametrize("changed_language", ["english", "japanese"])
def test_prepare_catalog_publication_canary_rejects_subtitle_changed_before_transition(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch, changed_language
):
    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _prepare_canary_candidate(store, mac_jobs_root, "abc-041")
    allowlist = _write_canary_allowlist(tmp_path / "allowlist.txt", "abc-041")
    row_before = store.get_job(job.id)
    files_before = _canary_files_snapshot(paths)
    changed_bytes = b"changed after validation while rechecking allowlist\n"
    changed_path = getattr(paths, f"{changed_language}_srt_path_mac")
    real_load_allowlist = catalog_repair.load_repair_allowlist
    allowlist_reads = 0

    def load_then_change(path):
        nonlocal allowlist_reads
        loaded = real_load_allowlist(path)
        allowlist_reads += 1
        if allowlist_reads == 2:
            changed_path.write_bytes(changed_bytes)
        return loaded

    def unexpected_prepare(*args, **kwargs):
        raise AssertionError("changed subtitle reached store transition")

    monkeypatch.setattr(
        catalog_repair,
        "load_repair_allowlist",
        load_then_change,
    )
    monkeypatch.setattr(
        store,
        "prepare_catalog_publication_repair",
        unexpected_prepare,
    )

    with pytest.raises(
        ValueError,
        match="^quality_gate_failed:subtitle_changed_after_validation$",
    ):
        prepare_catalog_publication_canary(
            store,
            allowlist,
            movie="abc-041",
            limit=1,
            confirm_job_id=job.id,
        )

    assert allowlist_reads == 2
    assert store.get_job(job.id) == row_before
    expected_files = {**files_before, changed_language: changed_bytes}
    assert _canary_files_snapshot(paths) == expected_files
