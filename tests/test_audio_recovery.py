from __future__ import annotations

import hashlib
import os
import stat
import struct
import threading
import wave
from contextlib import contextmanager
from pathlib import Path

import pytest

from orchestrator.audio_recovery import (
    AudioRecoveryError,
    recover_interrupted_audio,
    validate_pcm_wav,
)
from orchestrator.models import JobStatus
from orchestrator.paths import build_job_paths
from orchestrator.store import JobStore


def _write_pcm_wav(path: Path, *, frames: int = 16_000) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    samples = b"\x01\x00" * frames
    with wave.open(str(path), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(16_000)
        output.writeframes(samples)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _prepare_job(
    sqlite_path: Path,
    jobs_root: Path,
    movie: str = "abc-001",
) -> tuple[JobStore, object, object, Path, str]:
    store = JobStore(sqlite_path, jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job(movie, priority=100, force=False).job
    assert job is not None
    paths = build_job_paths(movie, jobs_root, "M:\\")
    paths.job_dir_mac.mkdir(parents=True)
    paths.metadata_path_mac.write_text("{}\n", encoding="utf-8")
    staged = paths.job_dir_mac / "audio" / f"{movie}.wav"
    digest = _write_pcm_wav(staged)
    store.update_download_status(job.id, JobStatus.DOWNLOADING_AUDIO)
    return store, store.get_job(job.id), paths, staged, digest


def _snapshot(store: JobStore, job_id: str, root: Path) -> tuple[dict, tuple]:
    with store.connection() as connection:
        row = connection.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        assert row is not None
        database = dict(row)

    files = []
    if root.exists():
        for path in sorted(root.rglob("*"), key=lambda item: str(item)):
            info = path.lstat()
            relative = str(path.relative_to(root))
            if stat.S_ISLNK(info.st_mode):
                payload = ("symlink", os.readlink(path))
            elif stat.S_ISREG(info.st_mode):
                payload = ("file", hashlib.sha256(path.read_bytes()).hexdigest())
            else:
                payload = ("directory", None)
            files.append((relative, info.st_mode, info.st_size, payload))
    return database, tuple(files)


def test_recover_moves_exact_staged_pcm_wav_and_finalizes_job(
    sqlite_path: Path, mac_jobs_root: Path
) -> None:
    store, job, paths, staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )

    receipt = recover_interrupted_audio(
        store,
        job_id=job.id,
        movie="abc-001",
        expected_sha256=digest,
    )

    assert receipt.job_id == job.id
    assert receipt.movie_code == "abc-001"
    assert receipt.status is JobStatus.AUDIO_READY
    assert receipt.final_path == paths.audio_path_mac
    assert receipt.sha256 == digest
    assert receipt.size_bytes == paths.audio_path_mac.stat().st_size
    assert receipt.duration_seconds == pytest.approx(1.0)
    assert receipt.reused_final is False
    assert paths.audio_path_mac.is_file()
    assert not staged.exists()

    refreshed = store.get_job(job.id)
    assert refreshed is not None
    assert refreshed.status is JobStatus.AUDIO_READY
    assert refreshed.metadata_path_mac == str(paths.metadata_path_mac)
    assert refreshed.audio_path_mac == str(paths.audio_path_mac)
    assert refreshed.audio_path_windows == paths.audio_path_windows
    assert refreshed.error is None


@pytest.mark.parametrize(
    "unsafe_case",
    [
        "wrong_hash",
        "symlink",
        "partial_wav",
        "wrong_job",
        "wrong_movie",
    ],
)
def test_unsafe_recovery_preserves_job_and_files_exactly(
    sqlite_path: Path,
    mac_jobs_root: Path,
    tmp_path: Path,
    unsafe_case: str,
) -> None:
    store, job, paths, staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    job_id = job.id
    movie = "abc-001"
    expected = digest

    if unsafe_case == "wrong_hash":
        expected = "0" * 64 if digest != "0" * 64 else "1" * 64
    elif unsafe_case == "symlink":
        staged.unlink()
        outside = tmp_path / "outside.wav"
        _write_pcm_wav(outside)
        staged.symlink_to(outside)
        expected = hashlib.sha256(outside.read_bytes()).hexdigest()
    elif unsafe_case == "partial_wav":
        staged.write_bytes(staged.read_bytes()[:32])
        expected = hashlib.sha256(staged.read_bytes()).hexdigest()
    elif unsafe_case == "wrong_job":
        job_id = "job_not_the_exact_job"
    elif unsafe_case == "wrong_movie":
        movie = "abc-002"

    before = _snapshot(store, job.id, mac_jobs_root)

    with pytest.raises(AudioRecoveryError):
        recover_interrupted_audio(
            store,
            job_id=job_id,
            movie=movie,
            expected_sha256=expected,
        )

    assert _snapshot(store, job.id, mac_jobs_root) == before
    assert not paths.audio_path_mac.exists()


def test_crash_resume_reuses_exact_final_without_overwrite(
    sqlite_path: Path, mac_jobs_root: Path
) -> None:
    store, job, paths, staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    os.replace(staged, paths.audio_path_mac)
    before = paths.audio_path_mac.stat()

    receipt = recover_interrupted_audio(
        store,
        job_id=job.id,
        movie="abc-001",
        expected_sha256=digest,
    )

    after = paths.audio_path_mac.stat()
    assert receipt.reused_final is True
    assert receipt.sha256 == digest
    assert (after.st_dev, after.st_ino, after.st_mtime_ns) == (
        before.st_dev,
        before.st_ino,
        before.st_mtime_ns,
    )
    assert store.get_job(job.id).status is JobStatus.AUDIO_READY


def test_post_move_cas_rejects_movie_path_race_and_rerun_reuses_final(
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, job, original_paths, staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    raced_paths = build_job_paths("def-002", mac_jobs_root, "M:\\")
    original_finalize = store.finalize_interrupted_audio
    race_injected = False

    def finalize_after_race(job_id: str, **kwargs):
        nonlocal race_injected
        if not race_injected:
            race_injected = True
            with store.connection() as connection:
                connection.execute(
                    "UPDATE jobs SET normalized_movie_number = ?, "
                    "job_dir_mac = ?, job_dir_windows = ? WHERE id = ?",
                    (
                        "def-002",
                        str(raced_paths.job_dir_mac),
                        raced_paths.job_dir_windows,
                        job_id,
                    ),
                )
        return original_finalize(job_id, **kwargs)

    monkeypatch.setattr(store, "finalize_interrupted_audio", finalize_after_race)

    with pytest.raises(
        AudioRecoveryError,
        match="^audio_recovery_state_changed$",
    ):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=digest,
        )

    raced = store.get_job(job.id)
    assert raced is not None
    assert raced.status is JobStatus.DOWNLOADING_AUDIO
    assert raced.normalized_movie_number == "def-002"
    assert original_paths.audio_path_mac.is_file()
    assert not staged.exists()

    with store.connection() as connection:
        connection.execute(
            "UPDATE jobs SET normalized_movie_number = ?, "
            "job_dir_mac = ?, job_dir_windows = ? WHERE id = ?",
            (
                "abc-001",
                str(original_paths.job_dir_mac),
                original_paths.job_dir_windows,
                job.id,
            ),
        )
    monkeypatch.setattr(store, "finalize_interrupted_audio", original_finalize)

    receipt = recover_interrupted_audio(
        store,
        job_id=job.id,
        movie="abc-001",
        expected_sha256=digest,
    )

    assert receipt.reused_final is True
    assert store.get_job(job.id).status is JobStatus.AUDIO_READY


def test_post_validation_final_replacement_cannot_be_marked_audio_ready(
    sqlite_path: Path,
    mac_jobs_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, job, paths, _staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    replacement = tmp_path / "replacement.wav"
    replacement.write_bytes(b"unvalidated replacement")
    replacement_digest = hashlib.sha256(replacement.read_bytes()).hexdigest()
    original_finalize = store.finalize_interrupted_audio

    def finalize_after_replacement(job_id: str, **kwargs):
        os.replace(replacement, paths.audio_path_mac)
        return original_finalize(job_id, **kwargs)

    monkeypatch.setattr(
        store,
        "finalize_interrupted_audio",
        finalize_after_replacement,
    )

    with pytest.raises(
        AudioRecoveryError,
        match="^audio_recovery_state_changed$",
    ):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=digest,
        )

    refreshed = store.get_job(job.id)
    assert refreshed is not None
    assert refreshed.status is JobStatus.DOWNLOADING_AUDIO
    assert hashlib.sha256(paths.audio_path_mac.read_bytes()).hexdigest() == (
        replacement_digest
    )
    assert not replacement.exists()


def test_replacement_after_preupdate_snapshot_check_rolls_back_cas(
    sqlite_path: Path,
    mac_jobs_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, job, paths, _staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    replacement = tmp_path / "replacement-after-check.wav"
    replacement.write_bytes(b"replacement after preupdate check")
    replacement_digest = hashlib.sha256(replacement.read_bytes()).hexdigest()
    original_finalize = store.finalize_interrupted_audio
    snapshot_checks = 0

    def finalize_with_replacement_after_first_check(job_id: str, **kwargs):
        original_check = kwargs["audio_snapshot_check"]

        def replace_after_check() -> None:
            nonlocal snapshot_checks
            snapshot_checks += 1
            original_check()
            if snapshot_checks == 1:
                os.replace(replacement, paths.audio_path_mac)

        kwargs["audio_snapshot_check"] = replace_after_check
        return original_finalize(job_id, **kwargs)

    monkeypatch.setattr(
        store,
        "finalize_interrupted_audio",
        finalize_with_replacement_after_first_check,
    )

    with pytest.raises(
        AudioRecoveryError,
        match="^audio_recovery_state_changed$",
    ):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=digest,
        )

    assert snapshot_checks == 2
    assert store.get_job(job.id).status is JobStatus.DOWNLOADING_AUDIO
    assert hashlib.sha256(paths.audio_path_mac.read_bytes()).hexdigest() == (
        replacement_digest
    )


def test_recovery_fails_safe_when_internal_audio_writer_holds_lock(
    sqlite_path: Path,
    mac_jobs_root: Path,
) -> None:
    from orchestrator.audio_lock import exclusive_audio_job_lock

    store, job, _paths, _staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )

    with exclusive_audio_job_lock(
        mac_jobs_root,
        "abc-001",
        blocking=True,
    ):
        with pytest.raises(AudioRecoveryError, match="^audio_recovery_busy$"):
            recover_interrupted_audio(
                store,
                job_id=job.id,
                movie="abc-001",
                expected_sha256=digest,
            )

    assert store.get_job(job.id).status is JobStatus.DOWNLOADING_AUDIO


def test_internal_audio_writer_waits_until_recovery_commit(
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orchestrator.audio_lock import exclusive_audio_job_lock
    from orchestrator.mac_worker import MacDownloadWorker

    store, job, paths, _staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    writer_attempting_lock = threading.Event()
    writer_entered_audio = threading.Event()
    writer_errors: list[BaseException] = []
    writer_payload = b"internal writer replacement"

    class ConcurrentAdapter:
        def download_metadata(self, _movie: str, output_path: Path) -> None:
            output_path.write_text("{}\n", encoding="utf-8")

        def download_audio(self, _movie: str, output_path: Path) -> None:
            writer_entered_audio.set()
            temporary = output_path.with_name("writer-audio.tmp")
            temporary.write_bytes(writer_payload)
            os.replace(temporary, output_path)

    worker = MacDownloadWorker(store, ConcurrentAdapter(), max_download_attempts=3)
    original_finalize = store.finalize_interrupted_audio

    @contextmanager
    def observed_worker_lock(root: Path, movie: str, *, blocking: bool):
        writer_attempting_lock.set()
        with exclusive_audio_job_lock(root, movie, blocking=blocking) as held:
            yield held

    monkeypatch.setattr(
        "orchestrator.mac_worker.exclusive_audio_job_lock",
        observed_worker_lock,
        raising=False,
    )

    writer_thread: threading.Thread | None = None

    def run_writer() -> None:
        try:
            worker._process_job(job)
        except BaseException as exc:
            writer_errors.append(exc)

    def finalize_while_writer_waits(job_id: str, **kwargs):
        nonlocal writer_thread
        writer_thread = threading.Thread(target=run_writer)
        writer_thread.start()
        assert writer_attempting_lock.wait(timeout=2)
        assert not writer_entered_audio.wait(timeout=0.1)
        finalized = original_finalize(job_id, **kwargs)
        assert not writer_entered_audio.is_set()
        return finalized

    monkeypatch.setattr(
        store,
        "finalize_interrupted_audio",
        finalize_while_writer_waits,
    )

    receipt = recover_interrupted_audio(
        store,
        job_id=job.id,
        movie="abc-001",
        expected_sha256=digest,
    )

    assert receipt.status is JobStatus.AUDIO_READY
    assert writer_entered_audio.wait(timeout=2)
    assert writer_thread is not None
    writer_thread.join(timeout=2)
    assert not writer_thread.is_alive()
    assert writer_errors == []
    refreshed = store.get_job(job.id)
    assert refreshed is not None
    assert refreshed.status is JobStatus.AUDIO_READY
    assert refreshed.audio_path_mac == str(paths.audio_path_mac)
    assert paths.audio_path_mac.read_bytes() == writer_payload


def test_audio_directory_symlink_loop_is_a_safe_unchanging_error(
    sqlite_path: Path,
    mac_jobs_root: Path,
) -> None:
    store, job, paths, staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    staged.unlink()
    staged.parent.rmdir()
    staged.parent.symlink_to(staged.parent.name)
    before = store.get_job(job.id)

    with pytest.raises(AudioRecoveryError, match="^job_path_mismatch$"):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=digest,
        )

    assert store.get_job(job.id) == before
    assert staged.parent.is_symlink()
    assert not paths.audio_path_mac.exists()


def test_job_directory_swap_after_validation_never_moves_external_staged_file(
    sqlite_path: Path,
    mac_jobs_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, job, paths, staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    external_job = tmp_path / "external-job"
    external_audio = external_job / "audio"
    external_audio.mkdir(parents=True)
    external_staged = external_audio / staged.name
    os.link(staged, external_staged)
    renamed_job = tmp_path / "held-original-job"
    original_validate = validate_pcm_wav
    swapped = False

    def validate_then_swap(path: Path, **kwargs):
        nonlocal swapped
        validated = original_validate(path, **kwargs)
        if not swapped and path.name == staged.name:
            swapped = True
            os.rename(paths.job_dir_mac, renamed_job)
            paths.job_dir_mac.symlink_to(external_job, target_is_directory=True)
        return validated

    monkeypatch.setattr(
        "orchestrator.audio_recovery.validate_pcm_wav",
        validate_then_swap,
    )

    with pytest.raises(AudioRecoveryError, match="^job_path_mismatch$"):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=digest,
        )

    assert swapped is True
    assert external_staged.is_file()
    assert not (external_job / "audio.wav").exists()
    assert (renamed_job / "audio" / staged.name).is_file()
    assert store.get_job(job.id).status is JobStatus.DOWNLOADING_AUDIO


def test_pcm_validation_reads_frame_payload_in_bounded_chunks(
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, job, _paths, _staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    original_readframes = wave.Wave_read.readframes
    requested_frame_counts: list[int] = []

    def bounded_readframes(reader, frame_count: int):
        requested_frame_counts.append(frame_count)
        assert frame_count <= 4_096
        return original_readframes(reader, frame_count)

    monkeypatch.setattr(wave.Wave_read, "readframes", bounded_readframes)

    receipt = recover_interrupted_audio(
        store,
        job_id=job.id,
        movie="abc-001",
        expected_sha256=digest,
    )

    assert receipt.status is JobStatus.AUDIO_READY
    assert requested_frame_counts
    assert max(requested_frame_counts) <= 4_096


def test_descriptor_io_failure_is_a_safe_unchanging_error(
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, job, _paths, _staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    before = _snapshot(store, job.id, mac_jobs_root)

    def fail_read(_descriptor: int, _size: int) -> bytes:
        raise OSError("unsafe internal descriptor detail")

    monkeypatch.setattr("orchestrator.audio_recovery.os.read", fail_read)

    with pytest.raises(AudioRecoveryError, match="^audio_unavailable$"):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=digest,
        )

    assert _snapshot(store, job.id, mac_jobs_root) == before


@pytest.mark.parametrize(
    ("precondition", "expected_reason"),
    [
        ("wrong_status", "job_status_mismatch"),
        ("claimed", "job_is_claimed"),
        ("job_path_outside", "job_path_mismatch"),
        ("audio_path_outside", "job_path_mismatch"),
    ],
)
def test_exact_job_preconditions_reject_without_changes(
    sqlite_path: Path,
    mac_jobs_root: Path,
    tmp_path: Path,
    precondition: str,
    expected_reason: str,
) -> None:
    store, job, _paths, _staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    with store.connection() as connection:
        if precondition == "wrong_status":
            connection.execute(
                "UPDATE jobs SET status = ? WHERE id = ?",
                (JobStatus.QUEUED.value, job.id),
            )
        elif precondition == "claimed":
            connection.execute(
                "UPDATE jobs SET claimed_by = ? WHERE id = ?",
                ("active-downloader", job.id),
            )
        elif precondition == "job_path_outside":
            connection.execute(
                "UPDATE jobs SET job_dir_mac = ? WHERE id = ?",
                (str(tmp_path / "outside"), job.id),
            )
        elif precondition == "audio_path_outside":
            connection.execute(
                "UPDATE jobs SET audio_path_mac = ? WHERE id = ?",
                (str(tmp_path / "outside.wav"), job.id),
            )
    before = _snapshot(store, job.id, mac_jobs_root)

    with pytest.raises(AudioRecoveryError, match=f"^{expected_reason}$"):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=digest,
        )

    assert _snapshot(store, job.id, mac_jobs_root) == before


@pytest.mark.parametrize(
    "expected_sha256",
    [
        "",
        "0" * 63,
        "0" * 65,
        "A" * 64,
        "g" * 64,
    ],
)
def test_expected_hash_must_be_64_lowercase_hex_without_changes(
    sqlite_path: Path,
    mac_jobs_root: Path,
    expected_sha256: str,
) -> None:
    store, job, _paths, _staged, _digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    before = _snapshot(store, job.id, mac_jobs_root)

    with pytest.raises(AudioRecoveryError, match="^invalid_expected_sha256$"):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=expected_sha256,
        )

    assert _snapshot(store, job.id, mac_jobs_root) == before


@pytest.mark.parametrize(
    "mutator",
    [
        lambda output: output.setnchannels(2),
        lambda output: output.setsampwidth(1),
        lambda output: output.setframerate(8_000),
    ],
    ids=["stereo", "wrong_width", "wrong_rate"],
)
def test_noncanonical_pcm_wav_is_rejected_without_changes(
    sqlite_path: Path,
    mac_jobs_root: Path,
    mutator,
) -> None:
    store, job, _paths, staged, _digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    with wave.open(str(staged), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(16_000)
        mutator(output)
        output.writeframes(b"\x00\x00" * 100)
    digest = hashlib.sha256(staged.read_bytes()).hexdigest()
    before = _snapshot(store, job.id, mac_jobs_root)

    with pytest.raises(AudioRecoveryError):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=digest,
        )

    assert _snapshot(store, job.id, mac_jobs_root) == before


def test_truncated_trailing_riff_chunk_is_rejected_without_changes(
    sqlite_path: Path,
    mac_jobs_root: Path,
) -> None:
    store, job, _paths, staged, _digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )
    payload = bytearray(staged.read_bytes())
    payload.extend(b"JUNK" + struct.pack("<I", 4) + b"xx")
    payload[4:8] = struct.pack("<I", len(payload) - 8)
    staged.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    before = _snapshot(store, job.id, mac_jobs_root)

    with pytest.raises(AudioRecoveryError, match="^invalid_pcm_wav$"):
        recover_interrupted_audio(
            store,
            job_id=job.id,
            movie="abc-001",
            expected_sha256=digest,
        )

    assert _snapshot(store, job.id, mac_jobs_root) == before


def test_cli_parser_requires_exact_audio_recovery_arguments() -> None:
    from orchestrator.__main__ import build_parser

    parser = build_parser()
    args = parser.parse_args(
        [
            "recover-interrupted-audio",
            "--job-id",
            "job_exact",
            "--movie",
            "abc-001",
            "--expected-sha256",
            "a" * 64,
        ]
    )

    assert args.command == "recover-interrupted-audio"
    assert args.job_id == "job_exact"
    assert args.movie == "abc-001"
    assert args.expected_sha256 == "a" * 64
    assert not hasattr(args, "force")
    assert not hasattr(args, "batch")
    assert not hasattr(args, "delete")

    for omitted in ("--job-id", "--movie", "--expected-sha256"):
        command = [
            "recover-interrupted-audio",
            "--job-id",
            "job_exact",
            "--movie",
            "abc-001",
            "--expected-sha256",
            "a" * 64,
        ]
        index = command.index(omitted)
        del command[index : index + 2]
        with pytest.raises(SystemExit):
            parser.parse_args(command)


def test_cli_prints_only_safe_audio_recovery_receipt_fields(
    sqlite_path: Path,
    mac_jobs_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    store, job, _paths, _staged, digest = _prepare_job(
        sqlite_path, mac_jobs_root
    )

    class Settings:
        db_path = sqlite_path
        jobs_root_mac = mac_jobs_root
        jobs_root_windows = "M:\\"

    monkeypatch.setattr("orchestrator.config.MacSettings", Settings)
    from orchestrator.__main__ import run_recover_interrupted_audio

    run_recover_interrupted_audio(
        job_id=job.id,
        movie="abc-001",
        expected_sha256=digest,
    )

    output = capsys.readouterr().out.strip()
    assert output == (
        f"job_id={job.id} movie=abc-001 status=audio_ready "
        f"sha256={digest} size=32044 duration=1.000000 reused_final=false"
    )
    assert "path=" not in output
