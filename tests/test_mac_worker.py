from pathlib import Path

import pytest

from orchestrator.mac_worker import (
    MacDownloadWorker,
    MacTranslationUnhealthyError,
    MacTranslationWorker,
)
from orchestrator.models import JobStatus
from orchestrator.store import JobStore
from orchestrator.translation_smoke import (
    TranslationRuntimeUnhealthyError,
    run_translation_startup_smoke_test,
)


class FakeMissAVAdapter:
    def download_metadata(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text('{"movie_number":"%s"}\n' % movie_number, encoding="utf-8")

    def download_audio(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.with_suffix(".wav.tmp").write_bytes(b"RIFFfakeWAVE")
        output_path.with_suffix(".wav.tmp").replace(output_path)


class FailingMissAVAdapter:
    def __init__(self, error: str) -> None:
        self.error = error

    def download_metadata(self, movie_number: str, output_path: Path) -> None:
        raise RuntimeError(self.error)

    def download_audio(self, movie_number: str, output_path: Path) -> None:
        raise AssertionError("audio should not run after metadata failure")


def test_mac_worker_processes_one_queued_job_to_audio_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    processed = worker.process_one()

    assert processed is True
    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.AUDIO_READY
    assert Path(refreshed.metadata_path_mac).exists()
    assert Path(refreshed.audio_path_mac).exists()
    assert refreshed.audio_path_windows == "M:\\ktb-096\\audio.wav"
    assert (mac_jobs_root / "ktb-096" / "job.json").exists()


def test_mac_worker_writes_download_log(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-096", priority=100, force=False)
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    assert worker.process_one() is True

    log_path = mac_jobs_root / "ktb-096" / "logs" / "mac-download.log"
    assert log_path.read_text(encoding="utf-8") == (
        "downloading_metadata ktb-096\n"
        "downloading_audio ktb-096\n"
        "audio_ready ktb-096\n"
    )


def test_mac_worker_audio_ready_log_failure_does_not_requeue_job(
    sqlite_path,
    mac_jobs_root,
    monkeypatch,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    def fail_on_audio_ready(job_dir, filename, message):
        if message == "audio_ready ktb-096":
            raise OSError("log disk full")

    monkeypatch.setattr("orchestrator.mac_worker.append_job_log", fail_on_audio_ready)

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.AUDIO_READY
    assert refreshed.attempt_count == 0
    assert refreshed.error is None


def test_mac_worker_recovers_interrupted_download_before_claiming_next_job(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    claimed = store.claim_next_download_job()
    assert claimed.status == JobStatus.DOWNLOADING_METADATA
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.AUDIO_READY
    assert refreshed.attempt_count == 1
    assert Path(refreshed.audio_path_mac).exists()


def test_mac_worker_promotes_stale_downloading_audio_when_audio_file_already_exists(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    claimed = store.claim_next_download_job()
    assert claimed.status == JobStatus.DOWNLOADING_METADATA
    store.update_download_status(
        job.id,
        JobStatus.DOWNLOADING_AUDIO,
        metadata_path_mac=str(mac_jobs_root / "ktb-112" / "metadata.json"),
    )
    audio_path = mac_jobs_root / "ktb-112" / "audio.wav"
    audio_path.parent.mkdir(parents=True, exist_ok=True)
    audio_path.write_bytes(b"RIFFfakeWAVE")
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    assert worker.process_one() is False

    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.AUDIO_READY
    assert refreshed.audio_path_mac == str(audio_path)
    assert refreshed.audio_path_windows == "M:\\ktb-112\\audio.wav"
    assert refreshed.error is None


def test_mac_worker_returns_false_when_no_queued_jobs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    assert worker.process_one() is False


def test_mac_worker_requeues_failure_below_max_attempts(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    worker = MacDownloadWorker(
        store,
        FailingMissAVAdapter("metadata failed"),
        max_download_attempts=3,
    )

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.QUEUED
    assert refreshed.attempt_count == 1
    assert refreshed.error == "metadata failed"
    assert (mac_jobs_root / "ktb-096" / "job.json").exists()


def test_mac_worker_marks_failed_at_max_attempts(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    worker = MacDownloadWorker(
        store,
        FailingMissAVAdapter("metadata failed"),
        max_download_attempts=1,
    )

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.FAILED
    assert refreshed.attempt_count == 1
    assert refreshed.error == "metadata failed"


class DiverseMacTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        lines = input_srt.read_text(encoding="utf-8").splitlines()
        for index in range(2, len(lines), 4):
            lines[index] = f"Distinct English translation {index}"
        output_srt.write_text("\n".join(lines) + "\n", encoding="utf-8")


class CollapsedMacTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        lines = input_srt.read_text(encoding="utf-8").splitlines()
        for index in range(2, len(lines), 4):
            lines[index] = "I don't know what to do"
        output_srt.write_text("\n".join(lines) + "\n", encoding="utf-8")


class FailingMacTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        raise RuntimeError("Mac translation runtime unavailable")


def prepare_transcription_done_job(store, mac_jobs_root, cue_count=20, movie="ktb-096"):
    job = store.submit_job(movie, priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=60)
    japanese = mac_jobs_root / movie / f"{movie}.Japanese.srt"
    japanese.parent.mkdir(parents=True, exist_ok=True)
    blocks = [
        f"{index}\n00:00:{index:02d},000 --> 00:00:{index:02d},900\n日本語{index}\n"
        for index in range(1, cue_count + 1)
    ]
    japanese.write_text("\n".join(blocks), encoding="utf-8")
    return store.complete_worker_transcription(
        claimed.id,
        "windows-gpu-1",
        f"M:\\{movie}\\{movie}.Japanese.srt",
        lambda path: Path(path).exists(),
    )


def test_mac_translation_worker_claims_transcription_and_marks_quality_pass_ready(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
    )

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.ENGLISH_SRT_READY
    assert refreshed.claimed_by is None
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    assert english.exists()
    assert "Distinct English" in english.read_text(encoding="utf-8")
    quality_log = mac_jobs_root / "ktb-096" / "logs" / "quality.log"
    assert '"passed": true' in quality_log.read_text(encoding="utf-8")


def test_mac_translation_worker_permanently_rejects_collapsed_output(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / "ktb-096" / "audio.wav"
    audio.write_bytes(b"keep-audio")
    worker = MacTranslationWorker(
        store,
        CollapsedMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
    )

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.FAILED
    assert refreshed.error.startswith("translating: quality_gate_failed:")
    assert "known_bad_collapse" in refreshed.error
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    assert not english.exists()
    assert len(list((english.parent / "rejected").glob("*.srt"))) == 1
    assert audio.read_bytes() == b"keep-audio"
    assert '"passed": false' in (
        english.parent / "logs" / "quality.log"
    ).read_text(encoding="utf-8")


def test_mac_translation_worker_retries_transient_failure_from_transcription_done(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    worker = MacTranslationWorker(
        store,
        FailingMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
    )

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.TRANSCRIPTION_DONE
    assert refreshed.worker_attempt_count == 1
    assert refreshed.claimed_by is None
    assert refreshed.error == "translating: Mac translation runtime unavailable"


def test_mac_translation_startup_smoke_accepts_diverse_runtime():
    report = run_translation_startup_smoke_test(DiverseMacTranslator())

    assert report.passed is True
    assert report.english_cue_count == 10
    assert report.english_unique_ratio == 1.0


def test_mac_translation_startup_smoke_rejects_collapsed_runtime():
    with pytest.raises(TranslationRuntimeUnhealthyError, match="startup_low_diversity"):
        run_translation_startup_smoke_test(CollapsedMacTranslator())


def test_mac_translation_worker_stops_after_three_consecutive_quality_failures(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    jobs = [
        prepare_transcription_done_job(store, mac_jobs_root, movie=f"ktb-{number}")
        for number in (96, 97, 98, 99)
    ]
    worker = MacTranslationWorker(
        store,
        CollapsedMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        quality_failure_limit=3,
    )

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert worker.process_one() is True
    with pytest.raises(MacTranslationUnhealthyError, match="3 consecutive"):
        worker.process_one()

    assert store.get_job(jobs[3].id).status == JobStatus.TRANSCRIPTION_DONE
