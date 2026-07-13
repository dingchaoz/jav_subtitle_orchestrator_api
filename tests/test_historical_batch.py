from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import HistoricalRepairState, JobStore


def _srt(*, bad: bool) -> bytes:
    blocks = []
    for index in range(1, 26):
        text = "Cannot translate" if bad else f"Distinct translation {index}"
        blocks.append(
            f"{index}\n00:00:{index - 1:02d},000 --> "
            f"00:00:{index:02d},000\n{text}\n"
        )
    return "\n".join(blocks).encode()


def _job(
    store: JobStore,
    root: Path,
    movie: str,
    *,
    bad: bool = True,
    status: JobStatus = JobStatus.FAILED,
    audio: bool = True,
    claimed: bool = False,
):
    job = store.submit_job(movie, priority=100, force=False).job
    assert job is not None
    paths = build_job_paths(job.normalized_movie_number, root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True, exist_ok=True)
    paths.japanese_srt_path_mac.write_bytes(
        _srt(bad=False).replace(b"Distinct translation", "日本語".encode())
    )
    paths.english_srt_path_mac.write_bytes(_srt(bad=bad))
    if audio:
        paths.audio_path_mac.write_bytes(b"synthetic-audio")
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = ?, audio_path_mac = ?, "
            "japanese_srt_path_mac = ?, english_srt_path_mac = ? WHERE id = ?",
            (
                status.value,
                "busy-worker" if claimed else None,
                str(paths.audio_path_mac) if audio else None,
                str(paths.japanese_srt_path_mac),
                str(paths.english_srt_path_mac),
                job.id,
            ),
        )
    return store.get_job(job.id), paths


def _store(sqlite_path: Path, mac_jobs_root: Path) -> JobStore:
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    return store


def _database_snapshot(path: Path) -> bytes:
    return path.read_bytes()


def _tree_snapshot(root: Path) -> dict[str, tuple[bytes, int]]:
    return {
        str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in root.rglob("*")
        if path.is_file()
    }


def test_plan_is_read_only_bounded_counts_full_allowlist_and_has_stable_digest(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import plan_historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    for index in range(1, 8):
        _job(store, mac_jobs_root, f"abc-{index:03d}")
    _job(store, mac_jobs_root, "good-001", bad=False)
    _job(store, mac_jobs_root, "busy-001", claimed=True)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_bytes(
        b"\n".join(
            [*(f"abc-{index:03d}".encode() for index in range(1, 8)),
             b"good-001", b"busy-001", b"missing-001"]
        )
        + b"\n"
    )
    before_db = _database_snapshot(sqlite_path)
    before_files = _tree_snapshot(mac_jobs_root)

    first = plan_historical_batch(store, allowlist, limit=5)
    second = plan_historical_batch(store, allowlist, limit=5)

    assert len(first.items) == 5
    assert first.eligible_total == 7
    assert first.ineligible == 1
    assert first.blocked == 2
    assert first.already_repaired == 0
    assert [item.movie_code for item in first.items] == [
        "abc-001", "abc-002", "abc-003", "abc-004", "abc-005"
    ]
    assert first.allowlist_sha256 == hashlib.sha256(allowlist.read_bytes()).hexdigest()
    assert first.plan_sha256 == first.recalculate_sha256()
    assert first == second
    assert _database_snapshot(sqlite_path) == before_db
    assert _tree_snapshot(mac_jobs_root) == before_files
    assert "Cannot translate" not in repr(first)


@pytest.mark.parametrize("limit", [0, 21, -1, True])
def test_plan_rejects_unsafe_limit(sqlite_path, mac_jobs_root, tmp_path, limit):
    from orchestrator.historical_batch import plan_historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    with pytest.raises(ValueError, match="between 1 and 20"):
        plan_historical_batch(store, allowlist, limit=limit)


def test_allowlist_is_explicit_strict_and_rejects_alias_duplicates_and_symlinks(
    tmp_path,
):
    from orchestrator.historical_batch import load_repair_allowlist

    duplicate = tmp_path / "duplicate.txt"
    duplicate.write_text("abc-7\nABC007\n")
    with pytest.raises(ValueError, match="allowlist_invalid"):
        load_repair_allowlist(duplicate)
    invalid = tmp_path / "invalid.txt"
    invalid.write_text("abc-001\n\n")
    with pytest.raises(ValueError, match="allowlist_invalid"):
        load_repair_allowlist(invalid)
    target = tmp_path / "target.txt"
    target.write_text("abc-001\n")
    symlink = tmp_path / "symlink.txt"
    symlink.symlink_to(target)
    with pytest.raises(ValueError, match="allowlist_invalid"):
        load_repair_allowlist(symlink)


def test_plan_blocks_missing_audio_and_ambiguous_legacy_aliases(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import plan_historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001", audio=False)
    first, _ = _job(store, mac_jobs_root, "legacy-007")
    second, _ = _job(store, mac_jobs_root, "legacy-008")
    assert first and second
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET normalized_movie_number = 'legacy-7' WHERE id = ?",
            (second.id,),
        )
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\nlegacy-007\n")

    plan = plan_historical_batch(store, allowlist, limit=2)

    assert plan.eligible_total == 0
    assert plan.blocked == 2
    assert plan.items == ()


def test_plan_rejects_symlinked_job_directory_without_reading_outside_root(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import plan_historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _, paths = _job(store, mac_jobs_root, "abc-001")
    outside = tmp_path / "outside-job"
    paths.job_dir_mac.rename(outside)
    paths.job_dir_mac.symlink_to(outside, target_is_directory=True)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")

    plan = plan_historical_batch(store, allowlist, limit=1)

    assert plan.blocked == 1
    assert plan.items == ()


def test_private_plan_write_is_0600_atomic_and_rejects_any_overwrite_or_symlink(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import (
        HistoricalBatchPlan,
        plan_historical_batch,
        write_private_plan,
    )

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = plan_historical_batch(store, allowlist, limit=1)
    output = tmp_path / "plan.json"

    write_private_plan(output, plan)

    assert output.stat().st_mode & 0o777 == 0o600
    assert HistoricalBatchPlan.from_json_bytes(output.read_bytes()) == plan
    with pytest.raises(ValueError, match="plan_output_unsafe"):
        write_private_plan(output, plan)
    target = tmp_path / "other.json"
    target.write_text("do not replace")
    linked = tmp_path / "linked.json"
    linked.symlink_to(target)
    with pytest.raises(ValueError, match="plan_output_unsafe"):
        write_private_plan(linked, plan)
    assert target.read_text() == "do not replace"


def test_plan_json_parser_rejects_extra_missing_bool_counts_and_tampering(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalBatchPlan, plan_historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = plan_historical_batch(store, allowlist, limit=1)
    payload = json.loads(plan.to_json_bytes())

    for mutate in (
        lambda value: value.update(extra="unsafe"),
        lambda value: value.pop("blocked"),
        lambda value: value.update(limit=True),
        lambda value: value.update(version=99),
        lambda value: value.update(plan_sha256="A" * 64),
        lambda value: value["items"][0].update(extra="unsafe"),
    ):
        changed = json.loads(json.dumps(payload))
        mutate(changed)
        with pytest.raises(ValueError, match="historical_plan_invalid"):
            HistoricalBatchPlan.from_json_bytes(json.dumps(changed).encode())


def test_enqueue_inserts_pending_without_mutating_job_and_is_idempotent(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        plan_historical_batch,
    )

    store = _store(sqlite_path, mac_jobs_root)
    job, paths = _job(store, mac_jobs_root, "abc-001")
    assert job is not None
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = plan_historical_batch(store, allowlist, limit=1)
    before_job = store.get_job(job.id)
    before_files = _tree_snapshot(mac_jobs_root)

    first = enqueue_historical_batch(
        store, plan, allowlist, confirm_plan_sha256=plan.plan_sha256
    )
    second = enqueue_historical_batch(
        store, plan, allowlist, confirm_plan_sha256=plan.plan_sha256
    )

    assert first == second
    assert len(first) == 1
    assert first[0].job_id == job.id
    assert first[0].movie_code == "abc-001"
    assert first[0].state is HistoricalRepairState.PENDING
    assert store.get_job(job.id) == before_job
    assert _tree_snapshot(mac_jobs_root) == before_files
    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 1
    assert paths.english_srt_path_mac.exists()

    next_plan = plan_historical_batch(store, allowlist, limit=1)
    assert next_plan.eligible_total == 0
    assert next_plan.already_repaired == 1
    assert next_plan.items == ()


def test_safe_report_names_exact_jobs_and_actions_without_paths_or_subtitle_text(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import (
        plan_historical_batch,
        render_historical_batch_report,
    )

    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _job(store, mac_jobs_root, "abc-001")
    assert job is not None
    allowlist = tmp_path / "operator-private" / "allowlist.txt"
    allowlist.parent.mkdir()
    allowlist.write_text("abc-001\n")
    plan = plan_historical_batch(store, allowlist, limit=1)

    report = render_historical_batch_report(plan)

    assert f"job_id={job.id}" in report
    assert "movie=abc-001" in report
    assert (
        "actions=quarantine_english,reset_translation_stage,upsert_english_subtitle"
        in report
    )
    assert "eligible_total=1" in report
    assert plan.plan_sha256 in report
    assert str(allowlist) not in report
    assert "Cannot translate" not in report


@pytest.mark.parametrize("change", ["digest", "allowlist", "snapshot", "job"])
def test_enqueue_revalidates_everything_and_rolls_back_atomically(
    sqlite_path, mac_jobs_root, tmp_path, change
):
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        plan_historical_batch,
    )

    store = _store(sqlite_path, mac_jobs_root)
    first, first_paths = _job(store, mac_jobs_root, "abc-001")
    second, _ = _job(store, mac_jobs_root, "abc-002")
    assert first and second
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\nabc-002\n")
    plan = plan_historical_batch(store, allowlist, limit=2)
    confirm = plan.plan_sha256
    if change == "digest":
        confirm = "0" * 64
    elif change == "allowlist":
        allowlist.write_text("abc-001\n")
    elif change == "snapshot":
        first_paths.japanese_srt_path_mac.write_bytes(_srt(bad=False) + b"\n")
    else:
        with store.connection() as conn:
            conn.execute(
                "UPDATE jobs SET claimed_by = 'raced-worker' WHERE id = ?",
                (first.id,),
            )
    jobs_before = store.list_jobs()

    with pytest.raises(ValueError, match="historical_plan_changed"):
        enqueue_historical_batch(
            store, plan, allowlist, confirm_plan_sha256=confirm
        )

    assert store.list_jobs() == jobs_before
    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0


def test_enqueue_rejects_tampered_item_and_never_selects_outside_allowlist_or_limit(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        plan_historical_batch,
    )

    store = _store(sqlite_path, mac_jobs_root)
    for index in range(1, 8):
        _job(store, mac_jobs_root, f"abc-{index:03d}")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("".join(f"abc-{index:03d}\n" for index in range(1, 8)))
    plan = plan_historical_batch(store, allowlist, limit=5)
    tampered = replace(
        plan,
        items=(replace(plan.items[0], job_id="job_outside"), *plan.items[1:]),
    )

    with pytest.raises(ValueError, match="historical_plan_changed"):
        enqueue_historical_batch(
            store, tampered, allowlist, confirm_plan_sha256=plan.plan_sha256
        )

    records = enqueue_historical_batch(
        store, plan, allowlist, confirm_plan_sha256=plan.plan_sha256
    )
    assert len(records) == 5
    assert {record.movie_code for record in records} <= {
        f"abc-{index:03d}" for index in range(1, 8)
    }


def test_cli_has_only_explicit_bounded_plan_and_confirmed_enqueue_arguments(tmp_path):
    from orchestrator.__main__ import build_parser

    parser = build_parser()
    planned = parser.parse_args(
        [
            "plan-historical-repair-batch",
            "--allowlist-file", str(tmp_path / "allowlist.txt"),
            "--limit", "5",
            "--output", str(tmp_path / "plan.json"),
        ]
    )
    enqueued = parser.parse_args(
        [
            "enqueue-historical-repair-batch",
            "--allowlist-file", str(tmp_path / "allowlist.txt"),
            "--plan-file", str(tmp_path / "plan.json"),
            "--confirm-plan-sha256", "a" * 64,
        ]
    )

    assert planned.limit == 5
    assert enqueued.confirm_plan_sha256 == "a" * 64
    for parsed in (planned, enqueued):
        for forbidden in (
            "force", "delete", "upload", "overwrite", "all", "selector", "movie"
        ):
            assert not hasattr(parsed, forbidden)
