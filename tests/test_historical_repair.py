from __future__ import annotations

from pathlib import Path

import pytest

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore


def _srt_lines(*, bad: bool) -> str:
    blocks = []
    for index in range(1, 26):
        line = "Cannot translate" if bad else f"Distinct translation {index}"
        blocks.append(
            f"{index}\n00:00:{index - 1:02d},000 --> "
            f"00:00:{index:02d},000\n{line}\n"
        )
    return "\n".join(blocks)


def _prepare_local_job(
    store: JobStore,
    root: Path,
    movie: str,
    *,
    status: JobStatus = JobStatus.FAILED,
    bad: bool = True,
    audio: bool = True,
    japanese_file: bool = True,
    claimed: bool = False,
):
    job = store.submit_job(movie, priority=100, force=False).job
    paths = build_job_paths(movie, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    japanese_text = _srt_lines(bad=False).replace("Distinct translation", "日本語")
    paths.japanese_srt_path_mac.write_text(japanese_text, encoding="utf-8")
    paths.english_srt_path_mac.write_text(_srt_lines(bad=bad), encoding="utf-8")
    if audio:
        paths.audio_path_mac.write_bytes(b"synthetic-audio")
    if not japanese_file:
        paths.japanese_srt_path_mac.unlink()
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = ?, audio_path_mac = ?, "
            "japanese_srt_path_mac = ?, english_srt_path_mac = ? WHERE id = ?",
            (
                status.value,
                "active-worker" if claimed else None,
                str(paths.audio_path_mac),
                str(paths.japanese_srt_path_mac),
                str(paths.english_srt_path_mac),
                job.id,
            ),
        )
    return store.get_job(job.id), paths


def test_selector_prefers_eligible_movie_and_is_read_only(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_repair import select_historical_repair_canary

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    preferred, _ = _prepare_local_job(store, mac_jobs_root, "abf-279")
    fallback, _ = _prepare_local_job(
        store,
        mac_jobs_root,
        "abc-001",
        status=JobStatus.ENGLISH_SRT_READY,
    )
    allowlist = tmp_path / "repair-allowlist.txt"
    allowlist.write_text("abc-001\nabf-279\n", encoding="utf-8")

    candidate = select_historical_repair_canary(
        store, allowlist, preferred_movie="abf-279"
    )

    assert candidate.job_id == preferred.id
    assert candidate.movie_number == "abf-279"
    assert candidate.reason_codes
    assert candidate.audio_preexisting is True
    assert store.get_job(preferred.id).status is JobStatus.FAILED
    assert store.get_job(fallback.id).status is JobStatus.ENGLISH_SRT_READY
    assert "Cannot translate" not in repr(candidate)


def test_selector_skips_ineligible_jobs(sqlite_path, mac_jobs_root, tmp_path):
    from orchestrator.historical_repair import select_historical_repair_canary

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    _prepare_local_job(store, mac_jobs_root, "abc-001", japanese_file=False)
    _prepare_local_job(store, mac_jobs_root, "abc-002", bad=False)
    _prepare_local_job(
        store,
        mac_jobs_root,
        "abc-003",
        status=JobStatus.TRANSLATING,
        claimed=True,
    )
    _prepare_local_job(store, mac_jobs_root, "abc-004")
    allowlist = tmp_path / "repair-allowlist.txt"
    allowlist.write_text("abc-001\nabc-002\nabc-003\n", encoding="utf-8")

    assert select_historical_repair_canary(store, allowlist) is None


def test_selector_allows_preexisting_missing_audio(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_repair import select_historical_repair_canary

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = _prepare_local_job(
        store,
        mac_jobs_root,
        "abc-001",
        audio=False,
    )
    allowlist = tmp_path / "repair-allowlist.txt"
    allowlist.write_text("abc-001\n", encoding="utf-8")

    candidate = select_historical_repair_canary(store, allowlist)

    assert candidate.job_id == job.id
    assert candidate.audio_preexisting is False
    assert candidate.audio_path == str(paths.audio_path_mac)
    assert not paths.audio_path_mac.exists()


def test_prepare_requires_exact_limit_and_job_confirmation(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_repair import prepare_historical_repair_canary

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = _prepare_local_job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "repair-allowlist.txt"
    allowlist.write_text("abc-001\n", encoding="utf-8")
    before = {
        "japanese": paths.japanese_srt_path_mac.read_bytes(),
        "english": paths.english_srt_path_mac.read_bytes(),
        "audio": paths.audio_path_mac.read_bytes(),
    }

    with pytest.raises(ValueError, match="limit must be exactly 1"):
        prepare_historical_repair_canary(
            store,
            allowlist,
            movie="abc-001",
            limit=2,
            confirm_job_id=job.id,
        )
    with pytest.raises(ValueError, match="confirmed job does not match"):
        prepare_historical_repair_canary(
            store,
            allowlist,
            movie="abc-001",
            limit=1,
            confirm_job_id="wrong-job",
        )

    assert store.get_job(job.id).status is JobStatus.FAILED
    assert paths.japanese_srt_path_mac.read_bytes() == before["japanese"]
    assert paths.english_srt_path_mac.read_bytes() == before["english"]
    assert paths.audio_path_mac.read_bytes() == before["audio"]


def test_prepare_resets_only_selected_translation_stage(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_repair import prepare_historical_repair_canary

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = _prepare_local_job(store, mac_jobs_root, "abc-001")
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET worker_attempt_count = 2, "
            "translation_attempt_count = 2 WHERE id = ?",
            (job.id,),
        )
    allowlist = tmp_path / "repair-allowlist.txt"
    allowlist.write_text("abc-001\n", encoding="utf-8")
    japanese_hash = paths.japanese_srt_path_mac.read_bytes()
    audio_hash = paths.audio_path_mac.read_bytes()

    prepared = prepare_historical_repair_canary(
        store,
        allowlist,
        movie="abc-001",
        limit=1,
        confirm_job_id=job.id,
    )

    assert prepared.status is JobStatus.TRANSCRIPTION_DONE
    assert prepared.worker_attempt_count == 2
    assert prepared.translation_attempt_count == 0
    assert prepared.english_srt_path_mac is None
    assert paths.english_srt_path_mac.exists()
    assert paths.japanese_srt_path_mac.read_bytes() == japanese_hash
    assert paths.audio_path_mac.read_bytes() == audio_hash


def test_allowlist_rejects_duplicates_and_symlinks(tmp_path):
    from orchestrator.historical_repair import load_repair_allowlist

    duplicate = tmp_path / "duplicate.txt"
    duplicate.write_text("abc-001\nABC-001\n", encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        load_repair_allowlist(duplicate)

    target = tmp_path / "target.txt"
    target.write_text("abc-001\n", encoding="utf-8")
    symlink = tmp_path / "allowlist.txt"
    symlink.symlink_to(target)
    with pytest.raises(ValueError, match="regular file"):
        load_repair_allowlist(symlink)


def test_canary_cli_requires_explicit_safe_arguments(tmp_path):
    from orchestrator.__main__ import build_parser

    parser = build_parser()
    selector = parser.parse_args(
        [
            "select-historical-repair-canary",
            "--allowlist-file",
            str(tmp_path / "allowlist.txt"),
            "--preferred-movie",
            "abf-279",
            "--output",
            str(tmp_path / "selection.json"),
        ]
    )
    prepare = parser.parse_args(
        [
            "prepare-historical-repair-canary",
            "--allowlist-file",
            str(tmp_path / "allowlist.txt"),
            "--movie",
            "abf-279",
            "--limit",
            "1",
            "--confirm-job-id",
            "job-safe",
        ]
    )

    assert selector.command == "select-historical-repair-canary"
    assert prepare.command == "prepare-historical-repair-canary"
    for parsed in (selector, prepare):
        for forbidden in ("force", "delete", "batch", "upload", "overwrite"):
            assert not hasattr(parsed, forbidden)


def test_one_shot_translation_cli_requires_exact_job_id():
    from orchestrator.__main__ import build_parser

    args = build_parser().parse_args(
        ["mac-translation-worker-once", "--job-id", "job-safe"]
    )

    assert args.command == "mac-translation-worker-once"
    assert args.job_id == "job-safe"
