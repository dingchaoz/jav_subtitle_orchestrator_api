from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import struct
import threading
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


def _wav(*, data_size: int = 64) -> bytes:
    byte_rate = 16_000 * 2
    block_align = 2
    fmt = struct.pack("<HHIIHH", 1, 1, 16_000, byte_rate, block_align, 16)
    riff_size = 4 + (8 + len(fmt)) + (8 + data_size)
    return (
        b"RIFF"
        + struct.pack("<I", riff_size)
        + b"WAVEfmt "
        + struct.pack("<I", len(fmt))
        + fmt
        + b"data"
        + struct.pack("<I", data_size)
        + (b"\0" * data_size)
    )


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
        paths.audio_path_mac.write_bytes(_wav())
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


def test_sparse_tens_of_gigabytes_audio_uses_bounded_wav_probe(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    _, paths = _job(store, mac_jobs_root, "zzz-001")
    with paths.audio_path_mac.open("r+b") as audio_file:
        audio_file.truncate(32 * 1024**3)
    audio_inode = paths.audio_path_mac.stat().st_ino
    bytes_requested = 0
    real_read = historical_batch.os.read
    real_pread = historical_batch.os.pread

    def bounded_read(fd, size):
        nonlocal bytes_requested
        if os.fstat(fd).st_ino == audio_inode:
            bytes_requested += size
            assert bytes_requested <= historical_batch.MAX_AUDIO_PROBE_BYTES
        return real_read(fd, size)

    def bounded_pread(fd, size, offset):
        nonlocal bytes_requested
        if os.fstat(fd).st_ino == audio_inode:
            bytes_requested += size
            assert bytes_requested <= historical_batch.MAX_AUDIO_PROBE_BYTES
        return real_pread(fd, size, offset)

    monkeypatch.setattr(historical_batch.os, "read", bounded_read)
    monkeypatch.setattr(historical_batch.os, "pread", bounded_pread)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\nzzz-001\n")

    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)

    assert len(plan.items) == 1
    item = plan.items[0]
    assert item.movie_code == "abc-001"
    assert len(item.audio_probe_snapshot_sha256) == 64
    assert len(item.audio_sha256) == 64
    assert 0 < bytes_requested <= historical_batch.MAX_AUDIO_PROBE_BYTES


def test_plan_full_hashes_every_byte_only_for_selected_limit(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    paths_by_inode = {}
    for index in range(1, 8):
        _, paths = _job(store, mac_jobs_root, f"abc-{index:03d}")
        paths.audio_path_mac.write_bytes(_wav(data_size=64 + 2 * index))
        paths_by_inode[paths.audio_path_mac.stat().st_ino] = (
            index,
            paths.audio_path_mac.stat().st_size,
        )
    bytes_read = {index: 0 for index in range(1, 8)}
    real_read = historical_batch.os.read

    def tracked_read(fd, size):
        chunk = real_read(fd, size)
        tracked = paths_by_inode.get(os.fstat(fd).st_ino)
        if tracked is not None:
            bytes_read[tracked[0]] += len(chunk)
        return chunk

    monkeypatch.setattr(historical_batch.os, "read", tracked_read)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("".join(f"abc-{index:03d}\n" for index in range(1, 8)))

    plan = historical_batch.plan_historical_batch(store, allowlist, limit=5)

    assert [item.movie_code for item in plan.items] == [
        "abc-001", "abc-002", "abc-003", "abc-004", "abc-005"
    ]
    assert all(
        item.audio_sha256
        == hashlib.sha256(
            (mac_jobs_root / item.path_movie_number / "audio.wav").read_bytes()
        ).hexdigest()
        for item in plan.items
    )
    assert bytes_read == {
        index: (size if index <= 5 else 0)
        for index, size in paths_by_inode.values()
    }


def test_selected_full_hash_does_not_block_unrelated_normal_writer(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch
    from orchestrator.job_files_lock import exclusive_job_files_lock

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    _job(store, mac_jobs_root, "abc-002")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    hashing = threading.Event()
    release = threading.Event()
    real_open = historical_batch._open_stable_regular_file_at

    def paused_open(directory_fd, basename, *, keep_content, max_bytes=None):
        if basename == "audio.wav" and max_bytes is None:
            hashing.set()
            assert release.wait(2)
        return real_open(
            directory_fd,
            basename,
            keep_content=keep_content,
            max_bytes=max_bytes,
        )

    monkeypatch.setattr(historical_batch, "_open_stable_regular_file_at", paused_open)
    outcome = []
    planner = threading.Thread(
        target=lambda: outcome.append(
            historical_batch.plan_historical_batch(store, allowlist, limit=1)
        )
    )
    planner.start()
    assert hashing.wait(2)
    with exclusive_job_files_lock(mac_jobs_root, "abc-002", blocking=False):
        pass
    release.set()
    planner.join(timeout=2)
    assert not planner.is_alive()
    assert len(outcome) == 1


def test_enqueue_rejects_audio_byte_mutation_even_when_mtime_is_restored(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _, paths = _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    original_stat = paths.audio_path_mac.stat()
    real_hash = historical_batch._hash_selected_audio

    def hash_then_mutate(*args, **kwargs):
        snapshots = real_hash(*args, **kwargs)
        with paths.audio_path_mac.open("r+b") as stream:
            stream.seek(-1, os.SEEK_END)
            stream.write(b"X")
            stream.flush()
            os.fsync(stream.fileno())
        os.utime(
            paths.audio_path_mac,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )
        return snapshots

    monkeypatch.setattr(historical_batch, "_hash_selected_audio", hash_then_mutate)

    with pytest.raises(ValueError, match="historical_plan_changed"):
        historical_batch.enqueue_historical_batch(
            store,
            plan,
            allowlist,
            confirm_plan_sha256=plan.plan_sha256,
        )
    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0


def test_enqueue_rejects_selected_audio_inode_change_after_full_hash(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _, paths = _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    real_hash = historical_batch._hash_selected_audio

    def hash_then_replace(*args, **kwargs):
        snapshots = real_hash(*args, **kwargs)
        before = paths.audio_path_mac.stat()
        replacement = paths.job_dir_mac / ".replacement.audio.wav"
        replacement.write_bytes(paths.audio_path_mac.read_bytes())
        os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
        os.replace(replacement, paths.audio_path_mac)
        return snapshots

    monkeypatch.setattr(historical_batch, "_hash_selected_audio", hash_then_replace)

    with pytest.raises(ValueError, match="historical_plan_changed"):
        historical_batch.enqueue_historical_batch(
            store,
            plan,
            allowlist,
            confirm_plan_sha256=plan.plan_sha256,
        )


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


def test_private_plan_rejects_ancestor_symlink_and_leaves_no_artifact(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import plan_historical_batch, write_private_plan

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = plan_historical_batch(store, allowlist, limit=1)
    real = tmp_path / "real"
    real.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)

    with pytest.raises(ValueError, match="plan_output_unsafe"):
        write_private_plan(linked / "nested" / "plan.json", plan)

    assert not (real / "nested" / "plan.json").exists()
    assert not list(real.rglob("*.tmp"))


def test_private_plan_parent_swap_after_link_is_detected_and_cleaned(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    parent = tmp_path / "reports"
    parent.mkdir()
    moved = tmp_path / "reports-moved"
    calls = 0
    real_require = historical_batch._require_parent_path_bound

    def swap_after_link(path, expected_stat):
        nonlocal calls
        calls += 1
        if calls == 2:
            parent.rename(moved)
            parent.mkdir()
        return real_require(path, expected_stat)

    monkeypatch.setattr(
        historical_batch,
        "_require_parent_path_bound",
        swap_after_link,
    )

    with pytest.raises(ValueError, match="plan_output_unsafe"):
        historical_batch.write_private_plan(parent / "plan.json", plan)

    assert not (parent / "plan.json").exists()
    assert not (moved / "plan.json").exists()
    assert not list(moved.glob("*.tmp"))


def test_private_plan_parent_swap_after_second_check_is_detected_and_cleaned(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    parent = tmp_path / "reports"
    parent.mkdir()
    moved = tmp_path / "reports-moved"
    calls = 0
    real_require = historical_batch._require_parent_path_bound

    def swap_after_second_success(path, expected_stat):
        nonlocal calls
        calls += 1
        result = real_require(path, expected_stat)
        if calls == 2:
            parent.rename(moved)
            parent.mkdir()
        return result

    monkeypatch.setattr(
        historical_batch,
        "_require_parent_path_bound",
        swap_after_second_success,
    )

    with pytest.raises(ValueError, match="plan_output_unsafe"):
        historical_batch.write_private_plan(parent / "plan.json", plan)

    assert calls == 3
    assert not (parent / "plan.json").exists()
    assert not (moved / "plan.json").exists()
    assert not list(moved.glob("*.tmp"))


def test_private_plan_is_0600_even_under_restrictive_umask(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import plan_historical_batch, write_private_plan

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = plan_historical_batch(store, allowlist, limit=1)
    output = tmp_path / "private-plan.json"
    prior_umask = os.umask(0o777)
    try:
        write_private_plan(output, plan)
    finally:
        os.umask(prior_umask)

    assert output.stat().st_mode & 0o777 == 0o600
    assert output.read_bytes() == plan.to_json_bytes()
    observed_umask = os.umask(prior_umask)
    os.umask(observed_umask)
    assert observed_umask == prior_umask


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
        lambda value: value["items"][0].pop("audio_probe_snapshot_sha256"),
        lambda value: value["items"][0].pop("audio_sha256"),
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
    assert first[0].source_english_sha256 == plan.items[0].english_sha256
    assert first[0].audio_probe_snapshot_sha256 == (
        plan.items[0].audio_probe_snapshot_sha256
    )
    assert first[0].audio_sha256 == hashlib.sha256(
        paths.audio_path_mac.read_bytes()
    ).hexdigest()
    assert first[0].audio_sha256 == plan.items[0].audio_sha256
    assert first[0].english_sha256 is None
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


def test_idempotent_replay_ignores_current_job_allowlist_and_files(
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
    first = enqueue_historical_batch(
        store,
        plan,
        allowlist,
        confirm_plan_sha256=plan.plan_sha256,
    )
    rejected = paths.job_dir_mac / "rejected"
    rejected.mkdir()
    paths.english_srt_path_mac.replace(rejected / "old-English.srt")
    allowlist.write_text("changed-999\n")
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = 'historical-worker', "
            "updated_at = 'claimed-after-enqueue' WHERE id = ?",
            (JobStatus.TRANSLATING.value, job.id),
        )
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ?, "
            "attempt_count = 1, updated_at = 'claimed-after-enqueue' "
            "WHERE batch_id = ?",
            (HistoricalRepairState.RUNNING.value, plan.batch_id),
        )
    before = sqlite_path.read_bytes()

    replayed = enqueue_historical_batch(
        store,
        plan,
        allowlist,
        confirm_plan_sha256=plan.plan_sha256,
    )

    assert [record.id for record in replayed] == [record.id for record in first]
    assert replayed[0].state is HistoricalRepairState.RUNNING
    assert sqlite_path.read_bytes() == before
    assert not paths.english_srt_path_mac.exists()
    assert (rejected / "old-English.srt").exists()


def test_enqueue_prescan_does_not_hold_sqlite_write_lock(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    unrelated, _ = _job(store, mac_jobs_root, "abc-002")
    assert unrelated is not None
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    prescan_complete = threading.Event()
    release_prescan = threading.Event()
    real_scan = historical_batch._scan_filesystem

    def paused_scan(*args, **kwargs):
        snapshot = real_scan(*args, **kwargs)
        prescan_complete.set()
        assert release_prescan.wait(2)
        return snapshot

    monkeypatch.setattr(historical_batch, "_scan_filesystem", paused_scan)
    outcome: list[object] = []

    def enqueue():
        try:
            outcome.extend(
                historical_batch.enqueue_historical_batch(
                    store,
                    plan,
                    allowlist,
                    confirm_plan_sha256=plan.plan_sha256,
                )
            )
        except Exception as exc:  # pragma: no cover - asserted below
            outcome.append(exc)

    thread = threading.Thread(target=enqueue)
    thread.start()
    assert prescan_complete.wait(2)
    started = threading.Event()

    def unrelated_sqlite_writer():
        with store.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            started.set()
            conn.execute(
                "UPDATE jobs SET updated_at = 'concurrent-write' WHERE id = ?",
                (unrelated.id,),
            )

    writer = threading.Thread(target=unrelated_sqlite_writer)
    writer.start()
    assert started.wait(1)
    writer.join(timeout=1)
    assert not writer.is_alive()
    release_prescan.set()
    thread.join(timeout=3)

    assert not thread.is_alive()
    assert len(outcome) == 1
    assert not isinstance(outcome[0], Exception)


def test_plan_and_enqueue_read_database_only_inside_explicit_transactions(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    transaction_modes: list[str] = []
    real_read = historical_batch._read_database_snapshot

    def checked_read(conn):
        assert conn.in_transaction
        transaction_modes.append("transaction")
        return real_read(conn)

    monkeypatch.setattr(historical_batch, "_read_database_snapshot", checked_read)

    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    records = historical_batch.enqueue_historical_batch(
        store,
        plan,
        allowlist,
        confirm_plan_sha256=plan.plan_sha256,
    )

    assert len(records) == 1
    assert transaction_modes == ["transaction", "transaction", "transaction"]


def test_plan_jobs_and_repairs_share_one_sqlite_snapshot(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _job(store, mac_jobs_root, "abc-001")
    assert job is not None
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    real_open = historical_batch._read_only_connection
    inserted = False

    def interleaving_connection(db_path):
        conn = real_open(db_path)

        def trace(statement):
            nonlocal inserted
            if (
                not inserted
                and statement.startswith(
                    "SELECT * FROM historical_translation_repairs"
                )
            ):
                inserted = True
                with store.connection() as writer:
                    writer.execute("BEGIN IMMEDIATE")
                    writer.execute(
                        """
                        INSERT INTO historical_translation_repairs (
                          id, batch_id, job_id, movie_code, allowlist_sha256,
                          state, japanese_sha256, audio_probe_snapshot_sha256,
                          audio_sha256, source_english_sha256, english_sha256,
                          created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                        """,
                        (
                            "repair_interleaved",
                            "batch_interleaved",
                            job.id,
                            "abc-001",
                            "a" * 64,
                            "pending",
                            "j" * 64,
                            "f" * 64,
                            "b" * 64,
                            "e" * 64,
                            "2026-07-01T00:00:00+00:00",
                            "2026-07-01T00:00:00+00:00",
                        ),
                    )

        conn.set_trace_callback(trace)
        return conn

    monkeypatch.setattr(
        historical_batch,
        "_read_only_connection",
        interleaving_connection,
    )

    with pytest.raises(ValueError, match="historical_plan_changed"):
        historical_batch.plan_historical_batch(store, allowlist, limit=1)

    assert inserted is True
    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs "
            "WHERE id = 'repair_interleaved'"
        ).fetchone()[0] == 1


def test_enqueue_performs_no_filesystem_scan_inside_sqlite_write_transaction(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    base_store = _store(sqlite_path, mac_jobs_root)
    _job(base_store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(
        base_store,
        allowlist,
        limit=1,
    )
    write_transaction_active = [False]

    class TransactionTrackingStore(JobStore):
        def connect(self):
            conn = super().connect()

            def trace(statement):
                normalized = statement.strip().upper()
                if normalized == "BEGIN IMMEDIATE":
                    write_transaction_active[0] = True
                elif normalized in {"COMMIT", "ROLLBACK"}:
                    write_transaction_active[0] = False

            conn.set_trace_callback(trace)
            return conn

    store = TransactionTrackingStore(sqlite_path, mac_jobs_root, "M:\\")
    real_scan = historical_batch._scan_filesystem
    real_quality = historical_batch.validate_translation_quality_snapshots
    real_pread = historical_batch.os.pread
    allowlist_inode = allowlist.stat().st_ino
    allowlist_bytes_read_in_transaction = [0]

    def checked_scan(*args, **kwargs):
        assert write_transaction_active[0] is False
        result = real_scan(*args, **kwargs)
        assert write_transaction_active[0] is False
        return result

    def checked_quality(*args, **kwargs):
        assert write_transaction_active[0] is False
        return real_quality(*args, **kwargs)

    def checked_pread(fd, size, offset):
        content = real_pread(fd, size, offset)
        if write_transaction_active[0]:
            assert os.fstat(fd).st_ino == allowlist_inode
            allowlist_bytes_read_in_transaction[0] += len(content)
            assert allowlist_bytes_read_in_transaction[0] <= (
                historical_batch.MAX_ALLOWLIST_VERIFY_BYTES
            )
        return content

    monkeypatch.setattr(historical_batch, "_scan_filesystem", checked_scan)
    monkeypatch.setattr(
        historical_batch,
        "validate_translation_quality_snapshots",
        checked_quality,
    )
    monkeypatch.setattr(historical_batch.os, "pread", checked_pread)

    records = historical_batch.enqueue_historical_batch(
        store,
        plan,
        allowlist,
        confirm_plan_sha256=plan.plan_sha256,
    )

    assert len(records) == 1
    assert write_transaction_active[0] is False
    assert allowlist_bytes_read_in_transaction[0] == len(allowlist.read_bytes())


@pytest.mark.parametrize(
    "mutation",
    ["replace", "in_place", "parent_swap", "restore_different_inode"],
)
def test_enqueue_rejects_allowlist_mutation_after_filesystem_scan(
    sqlite_path,
    mac_jobs_root,
    tmp_path,
    monkeypatch,
    mutation,
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    _job(store, mac_jobs_root, "abc-002")
    operator_dir = tmp_path / "operator"
    operator_dir.mkdir()
    allowlist = operator_dir / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    real_scan = historical_batch._scan_filesystem

    def mutate_after_scan(*args, **kwargs):
        snapshot = real_scan(*args, **kwargs)
        if mutation == "replace":
            replacement = operator_dir / "replacement.txt"
            replacement.write_text("abc-002\n")
            os.replace(replacement, allowlist)
        elif mutation == "in_place":
            with allowlist.open("r+b") as output:
                output.seek(0)
                output.write(b"abc-002\n")
                output.flush()
                os.fsync(output.fileno())
        elif mutation == "parent_swap":
            moved = tmp_path / "operator-moved"
            operator_dir.rename(moved)
            operator_dir.mkdir()
            allowlist.write_text("abc-001\n")
        else:
            changed = operator_dir / "changed.txt"
            changed.write_text("abc-002\n")
            os.replace(changed, allowlist)
            restored = operator_dir / "restored.txt"
            restored.write_text("abc-001\n")
            os.replace(restored, allowlist)
        return snapshot

    monkeypatch.setattr(historical_batch, "_scan_filesystem", mutate_after_scan)

    with pytest.raises(ValueError, match="historical_plan_changed"):
        historical_batch.enqueue_historical_batch(
            store,
            plan,
            allowlist,
            confirm_plan_sha256=plan.plan_sha256,
        )

    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0


def test_enqueue_rejects_allowlist_mutation_inside_write_transaction(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    _job(store, mac_jobs_root, "abc-002")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    real_read = historical_batch._read_database_snapshot

    def mutate_after_database_snapshot(conn):
        snapshot = real_read(conn)
        with allowlist.open("r+b") as output:
            output.seek(0)
            output.write(b"abc-002\n")
            output.flush()
            os.fsync(output.fileno())
        return snapshot

    monkeypatch.setattr(
        historical_batch,
        "_read_database_snapshot",
        mutate_after_database_snapshot,
    )

    with pytest.raises(ValueError, match="historical_plan_changed"):
        historical_batch.enqueue_historical_batch(
            store,
            plan,
            allowlist,
            confirm_plan_sha256=plan.plan_sha256,
        )

    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0


def test_plan_rejects_allowlist_replacement_after_filesystem_scan(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    real_scan = historical_batch._scan_filesystem

    def replace_after_scan(*args, **kwargs):
        snapshot = real_scan(*args, **kwargs)
        replacement = tmp_path / "replacement.txt"
        replacement.write_text("abc-001\n")
        os.replace(replacement, allowlist)
        return snapshot

    monkeypatch.setattr(historical_batch, "_scan_filesystem", replace_after_scan)

    with pytest.raises(ValueError, match="historical_plan_changed"):
        historical_batch.plan_historical_batch(store, allowlist, limit=1)


def test_plan_rejects_allowlist_mutation_during_database_snapshot(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    real_read = historical_batch._read_database_snapshot

    def mutate_during_read(conn):
        snapshot = real_read(conn)
        with allowlist.open("r+b") as output:
            output.seek(0)
            output.write(b"abc-002\n")
            output.flush()
            os.fsync(output.fileno())
        return snapshot

    monkeypatch.setattr(
        historical_batch,
        "_read_database_snapshot",
        mutate_during_read,
    )

    with pytest.raises(ValueError, match="historical_plan_changed"):
        historical_batch.plan_historical_batch(store, allowlist, limit=1)


@pytest.mark.parametrize(
    ("terminal_state", "reason_code"),
    [
        (HistoricalRepairState.SUCCEEDED, None),
        (HistoricalRepairState.PERMANENT_FAILED, "quality_gate_failed:known_bad"),
    ],
)
def test_terminal_replay_uses_immutable_source_hash_and_performs_zero_writes(
    sqlite_path,
    mac_jobs_root,
    tmp_path,
    terminal_state,
    reason_code,
):
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        plan_historical_batch,
    )

    store = _store(sqlite_path, mac_jobs_root)
    _, paths = _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = plan_historical_batch(store, allowlist, limit=1)
    first = enqueue_historical_batch(
        store,
        plan,
        allowlist,
        confirm_plan_sha256=plan.plan_sha256,
    )
    result_english_sha256 = "f" * 64
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ?, "
            "attempt_count = 1, updated_at = 'running' WHERE id = ?",
            (HistoricalRepairState.RUNNING.value, first[0].id),
        )
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ?, "
            "english_sha256 = ?, reason_code = ?, updated_at = 'terminal' "
            "WHERE id = ?",
            (
                terminal_state.value,
                result_english_sha256,
                reason_code,
                first[0].id,
            ),
        )
    paths.english_srt_path_mac.unlink()
    allowlist.write_text("changed-999\n")
    before = sqlite_path.read_bytes()

    replayed = enqueue_historical_batch(
        store,
        plan,
        allowlist,
        confirm_plan_sha256=plan.plan_sha256,
    )

    assert replayed[0].state is terminal_state
    assert replayed[0].source_english_sha256 == plan.items[0].english_sha256
    assert replayed[0].english_sha256 == result_english_sha256
    assert replayed[0].reason_code == reason_code
    assert sqlite_path.read_bytes() == before


@pytest.mark.parametrize(
    ("field", "tampered"),
    [
        ("id", "repair_" + "0" * 32),
        ("batch_id", "batch_" + "0" * 32),
        ("job_id", "use_second_job"),
        ("movie_code", "tampered-999"),
        ("allowlist_sha256", "0" * 64),
        ("audio_probe_snapshot_sha256", "0" * 64),
        ("audio_sha256", "0" * 64),
        ("source_english_sha256", "0" * 64),
    ],
)
def test_idempotent_replay_rejects_tampered_immutable_identity(
    sqlite_path,
    mac_jobs_root,
    tmp_path,
    field,
    tampered,
):
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        plan_historical_batch,
    )

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    second, _ = _job(store, mac_jobs_root, "abc-002")
    assert second is not None
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = plan_historical_batch(store, allowlist, limit=1)
    records = enqueue_historical_batch(
        store,
        plan,
        allowlist,
        confirm_plan_sha256=plan.plan_sha256,
    )
    if tampered == "use_second_job":
        tampered = second.id
    with store.connection() as conn:
        conn.execute(
            f"UPDATE historical_translation_repairs SET {field} = ? WHERE id = ?",
            (tampered, records[0].id),
        )

    with pytest.raises(ValueError, match="historical_plan_changed"):
        enqueue_historical_batch(
            store,
            plan,
            allowlist,
            confirm_plan_sha256=plan.plan_sha256,
        )


def test_enqueue_uses_bounded_descriptors_under_low_rlimit(
    sqlite_path, mac_jobs_root, tmp_path
):
    resource = pytest.importorskip("resource")
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        plan_historical_batch,
        render_historical_batch_report,
    )

    store = _store(sqlite_path, mac_jobs_root)
    movies = [f"bulk-{index:03d}" for index in range(1, 341)]
    for movie in movies:
        _job(store, mac_jobs_root, movie)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("".join(f"{movie}\n" for movie in movies))
    plan = plan_historical_batch(store, allowlist, limit=20)
    report = render_historical_batch_report(plan)
    assert "scan_entries=340" in report
    assert "audio_probe_max_bytes=4096" in report
    assert "allowlist_verify_max_bytes=1048576" in report
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = min(64, hard)
    if target < 48:
        pytest.skip("RLIMIT_NOFILE hard limit is already too low for pytest")
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    try:
        records = enqueue_historical_batch(
            store,
            plan,
            allowlist,
            confirm_plan_sha256=plan.plan_sha256,
        )
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))

    assert len(records) == 20


def test_idempotent_replay_rejects_partial_existing_batch(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        plan_historical_batch,
    )

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    _job(store, mac_jobs_root, "abc-002")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\nabc-002\n")
    plan = plan_historical_batch(store, allowlist, limit=2)
    records = enqueue_historical_batch(
        store,
        plan,
        allowlist,
        confirm_plan_sha256=plan.plan_sha256,
    )
    with store.connection() as conn:
        conn.execute(
            "DELETE FROM historical_translation_repairs WHERE id = ?",
            (records[0].id,),
        )

    with pytest.raises(ValueError, match="historical_plan_changed"):
        enqueue_historical_batch(
            store,
            plan,
            allowlist,
            confirm_plan_sha256=plan.plan_sha256,
        )


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
    assert "scan_entries=1" in report
    assert "audio_probe_max_bytes=4096" in report
    assert "elapsed" not in report
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


@pytest.mark.parametrize("replace_after", ["Japanese.srt", "English.srt"])
def test_enqueue_detects_path_replacement_after_individual_file_hash(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch, replace_after
):
    import orchestrator.historical_batch as historical_batch

    store = _store(sqlite_path, mac_jobs_root)
    _, paths = _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    real_snapshot = historical_batch._open_stable_regular_file_at
    replaced = False

    def replace_path_after_hash(directory_fd, basename, **kwargs):
        nonlocal replaced
        snapshot = real_snapshot(directory_fd, basename, **kwargs)
        if not replaced and basename.endswith(replace_after):
            replacement = paths.job_dir_mac / f".{basename}.replacement"
            replacement.write_bytes(
                _srt(bad=False)
                if replace_after == "English.srt"
                else _srt(bad=False).replace(
                    b"Distinct translation", "変更".encode()
                )
            )
            os.replace(replacement, paths.job_dir_mac / basename)
            replaced = True
        return snapshot

    monkeypatch.setattr(
        historical_batch,
        "_open_stable_regular_file_at",
        replace_path_after_hash,
    )

    with pytest.raises(ValueError, match="historical_plan_changed"):
        historical_batch.enqueue_historical_batch(
            store,
            plan,
            allowlist,
            confirm_plan_sha256=plan.plan_sha256,
        )

    assert replaced is True
    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0


def test_cooperating_writer_is_blocked_from_final_validation_through_commit(
    sqlite_path, mac_jobs_root, tmp_path, monkeypatch
):
    import orchestrator.historical_batch as historical_batch
    from orchestrator.job_files_lock import exclusive_job_files_lock

    store = _store(sqlite_path, mac_jobs_root)
    _, paths = _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = historical_batch.plan_historical_batch(store, allowlist, limit=1)
    attempted = threading.Event()
    acquired = threading.Event()
    writer: threading.Thread | None = None
    real_now = historical_batch.utc_now_iso

    def write_after_validation():
        attempted.set()
        with exclusive_job_files_lock(
            mac_jobs_root,
            "abc-001",
            blocking=True,
        ):
            acquired.set()
            replacement = paths.job_dir_mac / ".replacement.English.srt"
            replacement.write_bytes(_srt(bad=False))
            os.replace(replacement, paths.english_srt_path_mac)

    def start_writer_at_insert_boundary():
        nonlocal writer
        writer = threading.Thread(target=write_after_validation)
        writer.start()
        assert attempted.wait(1)
        assert not acquired.wait(0.05)
        return real_now()

    monkeypatch.setattr(
        historical_batch,
        "utc_now_iso",
        start_writer_at_insert_boundary,
    )

    records = historical_batch.enqueue_historical_batch(
        store,
        plan,
        allowlist,
        confirm_plan_sha256=plan.plan_sha256,
    )

    assert len(records) == 1
    assert acquired.wait(2)
    assert writer is not None
    writer.join(timeout=2)
    assert not writer.is_alive()


def test_audio_exclusive_lock_can_commit_before_enqueue_takes_database_lock(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.audio_lock import exclusive_audio_job_lock
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        plan_historical_batch,
    )

    store = _store(sqlite_path, mac_jobs_root)
    job, _ = _job(store, mac_jobs_root, "abc-001")
    assert job is not None
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    plan = plan_historical_batch(store, allowlist, limit=1)
    outcome: list[object] = []

    def enqueue_while_audio_is_locked():
        try:
            outcome.extend(
                enqueue_historical_batch(
                    store,
                    plan,
                    allowlist,
                    confirm_plan_sha256=plan.plan_sha256,
                )
            )
        except ValueError as exc:
            outcome.append(exc)

    with exclusive_audio_job_lock(
        mac_jobs_root,
        "abc-001",
        blocking=True,
    ):
        thread = threading.Thread(target=enqueue_while_audio_is_locked)
        thread.start()
        threading.Event().wait(0.05)
        conn = sqlite3.connect(sqlite_path, timeout=0.2)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE jobs SET updated_at = 'snapshot-raced' WHERE id = ?",
                (job.id,),
            )
            conn.commit()
        finally:
            conn.close()

    thread.join(timeout=2)
    assert not thread.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], ValueError)
    assert str(outcome[0]) == "historical_plan_changed"
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


def test_enqueue_rejects_unselected_classification_swap_even_when_counts_match(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        plan_historical_batch,
    )

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001", bad=True)
    _, second_paths = _job(store, mac_jobs_root, "abc-002", bad=True)
    _, third_paths = _job(store, mac_jobs_root, "abc-003", bad=False)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\nabc-002\nabc-003\n")
    plan = plan_historical_batch(store, allowlist, limit=1)
    assert plan.eligible_total == 2
    assert plan.ineligible == 1
    second_paths.english_srt_path_mac.write_bytes(_srt(bad=False))
    third_paths.english_srt_path_mac.write_bytes(_srt(bad=True))

    with pytest.raises(ValueError, match="historical_plan_changed"):
        enqueue_historical_batch(
            store,
            plan,
            allowlist,
            confirm_plan_sha256=plan.plan_sha256,
        )

    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0


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


def _set_repair_batch_terminal(store: JobStore, batch_id: str) -> None:
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs "
            "SET state = ?, reason_code = NULL WHERE batch_id = ?",
            (HistoricalRepairState.SUCCEEDED.value, batch_id),
        )


def _add_normal_stage(
    store: JobStore,
    movie: str,
    status: JobStatus,
    *,
    claimed: bool,
) -> None:
    job = store.submit_job(movie, priority=1, force=False).job
    assert job is not None
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = 'normal', "
            "claimed_by = ?, lease_expires_at = ? WHERE id = ?",
            (
                status.value,
                "normal-worker" if claimed else None,
                "2999-01-01T00:00:00+00:00" if claimed else None,
                job.id,
            ),
        )


def test_controller_enqueues_five_then_twenty(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    for index in range(1, 26):
        _job(store, mac_jobs_root, f"abc-{index:03d}")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("".join(f"abc-{index:03d}\n" for index in range(1, 26)))
    controller = HistoricalRepairController(
        store,
        allowlist,
        initial_batch_size=5,
        batch_size=20,
    )

    first = controller.run_once()

    assert first.action == "enqueued"
    assert first.enqueued == 5
    assert first.counts["eligible_total"] == 25
    assert first.batch_id is not None
    _set_repair_batch_terminal(store, first.batch_id)

    second = controller.run_once()

    assert second.action == "enqueued"
    assert second.enqueued == 20
    assert second.counts["eligible_total"] == 20
    assert second.batch_id != first.batch_id


def test_controller_waits_until_every_previous_repair_is_terminal(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    for index in range(1, 8):
        _job(store, mac_jobs_root, f"abc-{index:03d}")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("".join(f"abc-{index:03d}\n" for index in range(1, 8)))
    controller = HistoricalRepairController(store, allowlist)
    first = controller.run_once()

    waiting = controller.run_once()

    assert first.enqueued == 5
    assert waiting.action == "waiting"
    assert waiting.reason_code == "waiting_previous_batch"
    assert waiting.hard_pause is False
    assert waiting.enqueued == 0
    assert waiting.counts["pending"] == 5


@pytest.mark.parametrize(
    ("status", "claimed"),
    [
        (JobStatus.TRANSCRIPTION_DONE, False),
        (JobStatus.TRANSLATING, True),
        (JobStatus.PUBLISH_PENDING, False),
        (JobStatus.PUBLISHING, True),
        (JobStatus.CATALOG_SYNC_PENDING, False),
        (JobStatus.CATALOG_SYNCING, True),
    ],
)
def test_controller_yields_to_each_normal_pending_or_inflight_stage(
    sqlite_path, mac_jobs_root, tmp_path, status, claimed
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    _add_normal_stage(store, "new-001", status, claimed=claimed)

    result = HistoricalRepairController(store, allowlist).run_once()

    assert result.action == "waiting"
    assert result.reason_code == "normal_backlog"
    assert result.hard_pause is False
    assert result.enqueued == 0
    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0


@pytest.mark.parametrize(
    "reason_code",
    [
        "quality_failure_limit",
        "catalog_auth_failed",
        "publication_configuration_missing",
        "supabase_verification_failed",
        "public_visibility_mismatch",
        "preservation_hash_changed",
        "quarantine_failed",
        "catalog_response_invalid",
        "catalog_response_mismatch",
    ],
)
def test_controller_hard_pauses_for_durable_lane_blockers(
    sqlite_path, mac_jobs_root, tmp_path, reason_code
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    store.pause_historical_lane(reason_code)

    result = HistoricalRepairController(store, allowlist).run_once()

    assert result.action == "paused"
    assert result.reason_code == reason_code
    assert result.hard_pause is True
    assert result.enqueued == 0


@pytest.mark.parametrize(
    "reason_code",
    ["worker_process_count_invalid", "worker_health_stale"],
)
def test_controller_hard_pauses_for_injected_worker_health_failure(
    sqlite_path, mac_jobs_root, tmp_path, reason_code
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")

    result = HistoricalRepairController(
        store,
        allowlist,
        worker_health_probe=lambda: reason_code,
    ).run_once()

    assert result.reason_code == reason_code
    assert result.hard_pause is True
    assert result.enqueued == 0


def test_process_inventory_uses_exact_ps_tokens_and_excludes_other_commands(
    monkeypatch
):
    import subprocess

    from orchestrator.__main__ import PsProcessInventory

    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                "101 /usr/bin/python -m orchestrator mac-translation-worker\n"
                "102 /usr/bin/python -m orchestrator mac-translation-worker-once --job-id x\n"
                "103 /usr/bin/python -m orchestrator "
                "historical-repair-controller --allowlist-file x\n"
                "104 /usr/bin/python -c broken\\ command\n"
                "105 /bin/echo -m orchestrator mac-translation-worker\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    processes = PsProcessInventory().list_processes()

    assert [process.pid for process in processes] == [101, 102, 103, 104, 105]
    assert captured["command"] == ["ps", "-axo", "pid=,command="]
    assert captured["kwargs"]["shell"] is False
    assert captured["kwargs"]["timeout"] == 5
    exact = [process for process in processes if process.is_translation_worker]
    assert [process.pid for process in exact] == [101]


def test_translation_worker_health_requires_one_os_process_and_one_fresh_heartbeat(
    sqlite_path, mac_jobs_root
):
    from orchestrator.__main__ import (
        ProcessRecord,
        TranslationWorkerHealthProbe,
    )

    store = _store(sqlite_path, mac_jobs_root)
    store.record_worker_idle("fresh-worker", role="mac_translator")
    with store.connection() as conn:
        conn.execute(
            "INSERT INTO worker_statuses (worker_id, role, state, last_seen_at, "
            "updated_at) VALUES ('stale-worker', 'mac_translator', 'idle', ?, ?)",
            ("2000-01-01T00:00:00+00:00", "2000-01-01T00:00:00+00:00"),
        )

    class Inventory:
        def __init__(self, count):
            self.count = count

        def list_processes(self):
            return tuple(
                ProcessRecord(
                    100 + index,
                    ("/usr/bin/python", "-m", "orchestrator", "mac-translation-worker"),
                )
                for index in range(self.count)
            )

    assert TranslationWorkerHealthProbe(store, Inventory(1))() is None
    assert (
        TranslationWorkerHealthProbe(store, Inventory(2))()
        == "translation_worker_count_mismatch"
    )
    assert (
        TranslationWorkerHealthProbe(store, Inventory(0))()
        == "translation_worker_count_mismatch"
    )


def test_translation_worker_health_fails_closed_when_process_inventory_errors(
    sqlite_path, mac_jobs_root
):
    from orchestrator.__main__ import TranslationWorkerHealthProbe

    store = _store(sqlite_path, mac_jobs_root)
    store.record_worker_idle("fresh-worker", role="mac_translator")

    class BrokenInventory:
        def list_processes(self):
            raise RuntimeError("ps unavailable")

    assert (
        TranslationWorkerHealthProbe(store, BrokenInventory())()
        == "translation_worker_count_mismatch"
    )


@pytest.mark.parametrize("first_decision", ["normal_backlog", "hard_health", "complete"])
def test_controller_first_run_pins_identity_before_any_decision(
    sqlite_path, mac_jobs_root, tmp_path, first_decision
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    if first_decision == "complete":
        _job(store, mac_jobs_root, "abc-001", bad=False)
    else:
        _job(store, mac_jobs_root, "abc-001")
    if first_decision == "normal_backlog":
        _add_normal_stage(
            store,
            "new-001",
            JobStatus.TRANSCRIPTION_DONE,
            claimed=False,
        )
    allowlist = tmp_path / "approved" / "allowlist.txt"
    allowlist.parent.mkdir()
    allowlist.write_text("abc-001\n")
    health = (
        (lambda: "translation_worker_count_mismatch")
        if first_decision == "hard_health"
        else (lambda: None)
    )

    first = HistoricalRepairController(
        store,
        allowlist,
        worker_health_probe=health,
    ).run_once()

    expected_reason = {
        "normal_backlog": "normal_backlog",
        "hard_health": "translation_worker_count_mismatch",
        "complete": "allowlist_complete",
    }[first_decision]
    assert first.reason_code == expected_reason
    with store.connection() as conn:
        identity = conn.execute(
            "SELECT controller_allowlist_path_sha256, "
            "controller_allowlist_sha256, controller_allowlist_codes_sha256 "
            "FROM historical_repair_control WHERE singleton = 1"
        ).fetchone()
    assert all(identity)

    replacement = tmp_path / "replacement" / "allowlist.txt"
    replacement.parent.mkdir()
    replacement.write_bytes(allowlist.read_bytes())
    second = HistoricalRepairController(
        store,
        replacement,
        worker_health_probe=health,
    ).run_once()
    assert second.reason_code == "allowlist_changed"
    assert second.hard_pause is True
    assert second.enqueued == 0


def test_controller_identity_survives_crash_after_pin_before_enqueue(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "approved" / "allowlist.txt"
    allowlist.parent.mkdir()
    allowlist.write_text("abc-001\n")

    def crash():
        raise RuntimeError("simulated controller crash")

    with pytest.raises(RuntimeError, match="simulated controller crash"):
        HistoricalRepairController(
            store,
            allowlist,
            before_enqueue=crash,
        ).run_once()
    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT controller_allowlist_sha256 FROM historical_repair_control "
            "WHERE singleton = 1"
        ).fetchone()[0] is not None

    replacement = tmp_path / "replacement" / "allowlist.txt"
    replacement.parent.mkdir()
    replacement.write_bytes(allowlist.read_bytes())
    result = HistoricalRepairController(store, replacement).run_once()
    assert result.reason_code == "allowlist_changed"
    assert result.enqueued == 0


def test_controller_detects_allowlist_mutation_after_restart_without_writes(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    _job(store, mac_jobs_root, "abc-002")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    first = HistoricalRepairController(store, allowlist).run_once()
    assert first.enqueued == 1
    _set_repair_batch_terminal(store, first.batch_id)
    before = sqlite_path.read_bytes()
    allowlist.write_text("abc-001\nabc-002\n")

    result = HistoricalRepairController(store, allowlist).run_once()

    assert result.reason_code == "allowlist_changed"
    assert result.hard_pause is True
    assert result.enqueued == 0
    assert sqlite_path.read_bytes() == before


def test_manual_enqueue_pins_allowlist_path_for_controller_restart(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import (
        HistoricalRepairController,
        enqueue_historical_batch,
        plan_historical_batch,
    )

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    original = tmp_path / "approved" / "allowlist.txt"
    original.parent.mkdir()
    original.write_text("abc-001\n")
    plan = plan_historical_batch(store, original, limit=1)
    records = enqueue_historical_batch(
        store,
        plan,
        original,
        confirm_plan_sha256=plan.plan_sha256,
    )
    assert len(records) == 1
    _set_repair_batch_terminal(store, plan.batch_id)
    moved = tmp_path / "replacement" / "allowlist.txt"
    moved.parent.mkdir()
    moved.write_bytes(original.read_bytes())

    result = HistoricalRepairController(store, moved).run_once()

    assert result.hard_pause is True
    assert result.reason_code == "allowlist_changed"


def test_controller_completes_with_explicit_ineligible_skipped_count(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "good-001", bad=False)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("good-001\n")

    result = HistoricalRepairController(store, allowlist).run_once()

    assert result.complete is True
    assert result.hard_pause is False
    assert result.counts["ineligible"] == 1
    assert result.counts["succeeded"] == 0


def test_controller_does_not_claim_complete_with_blocked_allowlist_entry(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    mac_jobs_root.mkdir(parents=True, exist_ok=True)
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("missing-001\n")

    result = HistoricalRepairController(store, allowlist).run_once()

    assert result.complete is False
    assert result.hard_pause is True
    assert result.reason_code == "allowlist_blocked_entries"
    assert result.counts["blocked"] == 1


def test_controller_hard_pauses_when_historical_scan_root_is_unavailable(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    assert not mac_jobs_root.exists()
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("missing-001\n")

    result = HistoricalRepairController(store, allowlist).run_once()

    assert result.complete is False
    assert result.hard_pause is True
    assert result.reason_code == "historical_scan_failed"


def test_controller_complete_counts_permanent_failures_as_terminal(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")
    controller = HistoricalRepairController(store, allowlist)
    first = controller.run_once()
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET state = ?, "
            "reason_code = 'quality_gate_failed_known_bad' WHERE batch_id = ?",
            (HistoricalRepairState.PERMANENT_FAILED.value, first.batch_id),
        )

    complete = controller.run_once()

    assert complete.complete is True
    assert complete.action == "complete"
    assert complete.counts["permanent_failed"] == 1
    assert complete.counts["succeeded"] == 0


def test_controller_rechecks_normal_backlog_atomically_before_enqueue(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")

    def normal_arrives_after_plan():
        _add_normal_stage(
            store,
            "new-001",
            JobStatus.TRANSCRIPTION_DONE,
            claimed=False,
        )

    result = HistoricalRepairController(
        store,
        allowlist,
        before_enqueue=normal_arrives_after_plan,
    ).run_once()

    assert result.reason_code == "normal_backlog"
    assert result.hard_pause is False
    assert result.enqueued == 0
    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0


def test_controller_rejects_plan_digest_change_before_enqueue_without_records(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    _, paths = _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\n")

    def change_selected_snapshot():
        paths.english_srt_path_mac.write_bytes(_srt(bad=False))

    result = HistoricalRepairController(
        store,
        allowlist,
        before_enqueue=change_selected_snapshot,
    ).run_once()

    assert result.reason_code == "plan_digest_changed"
    assert result.hard_pause is True
    assert result.enqueued == 0
    with store.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM historical_translation_repairs"
        ).fetchone()[0] == 0


def test_two_controllers_never_create_overlapping_batches(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    for index in range(1, 8):
        _job(store, mac_jobs_root, f"abc-{index:03d}")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("".join(f"abc-{index:03d}\n" for index in range(1, 8)))
    barrier = threading.Barrier(2)
    results = []

    def run_controller():
        results.append(
            HistoricalRepairController(
                store,
                allowlist,
                before_enqueue=lambda: barrier.wait(timeout=5),
            ).run_once()
        )

    threads = [threading.Thread(target=run_controller) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(result.enqueued for result in results) == [0, 5]
    with store.connection() as conn:
        rows = conn.execute(
            "SELECT batch_id, job_id FROM historical_translation_repairs"
        ).fetchall()
    assert len(rows) == 5
    assert len({row["batch_id"] for row in rows}) == 1
    assert len({row["job_id"] for row in rows}) == 5


def test_controller_only_inserts_pending_repairs_and_preserves_jobs_and_files(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import HistoricalRepairController

    store = _store(sqlite_path, mac_jobs_root)
    for index in range(1, 3):
        _job(store, mac_jobs_root, f"abc-{index:03d}")
    allowlist = tmp_path / "allowlist.txt"
    allowlist.write_text("abc-001\nabc-002\n")
    before_files = _tree_snapshot(mac_jobs_root)
    with store.connection() as conn:
        before_jobs = [tuple(row) for row in conn.execute("SELECT * FROM jobs")]

    result = HistoricalRepairController(store, allowlist).run_once()

    assert result.enqueued == 2
    assert _tree_snapshot(mac_jobs_root) == before_files
    with store.connection() as conn:
        after_jobs = [tuple(row) for row in conn.execute("SELECT * FROM jobs")]
        states = {
            row[0]
            for row in conn.execute(
                "SELECT state FROM historical_translation_repairs"
            )
        }
    assert after_jobs == before_jobs
    assert states == {HistoricalRepairState.PENDING.value}


def test_controller_cli_is_bounded_and_has_no_mutating_escape_hatches(tmp_path):
    from orchestrator.__main__ import build_parser

    parsed = build_parser().parse_args(
        [
            "historical-repair-controller",
            "--allowlist-file",
            str(tmp_path / "allowlist.txt"),
            "--initial-batch-size",
            "5",
            "--batch-size",
            "20",
            "--poll-interval-seconds",
            "30",
        ]
    )

    assert parsed.initial_batch_size == 5
    assert parsed.batch_size == 20
    assert parsed.poll_interval_seconds == 30
    for forbidden in (
        "force",
        "delete",
        "upload",
        "overwrite",
        "all",
        "selector",
        "movie",
    ):
        assert not hasattr(parsed, forbidden)


def test_controller_report_is_deterministic_and_contains_no_path_or_subtitle_text(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.historical_batch import (
        HistoricalRepairController,
        render_historical_controller_report,
    )

    store = _store(sqlite_path, mac_jobs_root)
    _job(store, mac_jobs_root, "abc-001")
    allowlist = tmp_path / "secret-adult-allowlist.txt"
    allowlist.write_text("abc-001\n")

    result = HistoricalRepairController(store, allowlist).run_once()
    first = render_historical_controller_report(result)
    second = render_historical_controller_report(result)

    assert first == second
    assert str(tmp_path) not in first
    assert "secret-adult" not in first
    assert "Cannot translate" not in first
    payload = json.loads(first)
    assert payload["action"] == "enqueued"
    assert payload["enqueued"] == 1


def test_controller_loop_sleeps_for_healthy_wait_and_exits_zero_on_complete(
    capsys
):
    from orchestrator.__main__ import run_historical_repair_controller_loop
    from orchestrator.historical_batch import HistoricalControllerResult

    counts = {
        key: 0
        for key in (
            "eligible_total", "already_repaired", "ineligible", "blocked",
            "pending", "running", "retry_wait", "succeeded",
            "permanent_failed", "paused", "planned", "total_records",
        )
    }
    results = iter(
        [
            HistoricalControllerResult(
                "waiting", "normal_backlog", False, False, 0,
                None, None, "a" * 64, counts,
            ),
            HistoricalControllerResult(
                "complete", "allowlist_complete", False, True, 0,
                None, "b" * 64, "a" * 64, counts,
            ),
        ]
    )

    class FakeController:
        def run_once(self):
            return next(results)

    sleeps = []
    code = run_historical_repair_controller_loop(
        FakeController(),
        poll_interval_seconds=3,
        sleep_fn=sleeps.append,
    )

    assert code == 0
    assert sleeps == [3]
    output = capsys.readouterr().out
    assert '"reason_code":"normal_backlog"' in output
    assert '"complete":true' in output


def test_controller_loop_exits_nonzero_without_sleep_on_hard_pause(capsys):
    from orchestrator.__main__ import run_historical_repair_controller_loop
    from orchestrator.historical_batch import HistoricalControllerResult

    counts = {
        key: 0
        for key in (
            "eligible_total", "already_repaired", "ineligible", "blocked",
            "pending", "running", "retry_wait", "succeeded",
            "permanent_failed", "paused", "planned", "total_records",
        )
    }

    class FakeController:
        def run_once(self):
            return HistoricalControllerResult(
                "paused", "quality_failure_limit", True, False, 0,
                None, None, "a" * 64, counts,
            )

    sleeps = []
    code = run_historical_repair_controller_loop(
        FakeController(),
        poll_interval_seconds=3,
        sleep_fn=sleeps.append,
    )

    assert code == 2
    assert sleeps == []
    assert '"hard_pause":true' in capsys.readouterr().out


def test_controller_loop_finite_test_mode_never_returns_zero_while_incomplete():
    from orchestrator.__main__ import run_historical_repair_controller_loop
    from orchestrator.historical_batch import HistoricalControllerResult

    counts = {
        key: 0
        for key in (
            "eligible_total", "already_repaired", "ineligible", "blocked",
            "pending", "running", "retry_wait", "succeeded",
            "permanent_failed", "paused", "planned", "total_records",
        )
    }

    class FakeController:
        def run_once(self):
            return HistoricalControllerResult(
                "waiting", "waiting_previous_batch", False, False, 0,
                None, None, "a" * 64, counts,
            )

    assert run_historical_repair_controller_loop(
        FakeController(),
        poll_interval_seconds=1,
        sleep_fn=lambda _: None,
        max_cycles=1,
    ) == 3
