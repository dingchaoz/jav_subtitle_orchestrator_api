import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.catalog_repair import prepare_catalog_publication_canary
from orchestrator.mac_worker import (
    MacDownloadWorker,
    MacTranslationUnhealthyError,
    MacTranslationWorker,
)
from orchestrator.models import JobStatus
from orchestrator.store import JobStore
from orchestrator.subtitle_quality import SubtitleQualityGateError
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


class AudioOSErrorAdapter:
    def download_metadata(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("{}\n", encoding="utf-8")

    def download_audio(self, movie_number: str, output_path: Path) -> None:
        raise OSError("audio disk full")


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
    worker_status = store.get_worker_status("mac-downloader-1")
    assert worker_status is not None
    assert worker_status.role == "mac_downloader"
    assert worker_status.state == "idle"


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


def test_mac_worker_redownloads_stale_unverified_audio_before_marking_ready(
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
    audio_path.write_bytes(b"unverified stale audio")
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status == JobStatus.AUDIO_READY
    assert refreshed.attempt_count == 1
    assert refreshed.audio_path_mac == str(audio_path)
    assert refreshed.audio_path_windows == "M:\\ktb-112\\audio.wav"
    assert refreshed.error is None
    assert audio_path.read_bytes() == b"RIFFfakeWAVE"


def test_interrupted_download_sweep_never_promotes_unverified_final(
    sqlite_path,
    mac_jobs_root,
):
    from orchestrator.audio_lock import exclusive_audio_job_lock

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-113", priority=100, force=False).job
    store.update_download_status(job.id, JobStatus.DOWNLOADING_AUDIO)
    audio_path = mac_jobs_root / "ktb-113" / "audio.wav"
    audio_path.parent.mkdir(parents=True)
    audio_path.write_bytes(b"not a validated wav")

    with exclusive_audio_job_lock(
        mac_jobs_root,
        "ktb-113",
        blocking=True,
    ):
        assert store.recover_interrupted_downloads(3) == 1

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.QUEUED
    assert refreshed.attempt_count == 1
    assert refreshed.error == "download interrupted"


def test_mac_worker_preserves_audio_writer_oserror(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-114", priority=100, force=False).job
    worker = MacDownloadWorker(store, AudioOSErrorAdapter(), max_download_attempts=3)

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.QUEUED
    assert refreshed.error == "audio disk full"


def test_mac_worker_returns_false_when_no_queued_jobs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    worker = MacDownloadWorker(store, FakeMissAVAdapter(), max_download_attempts=3)

    assert worker.process_one() is False
    worker_status = store.get_worker_status("mac-downloader-1")
    assert worker_status is not None
    assert worker_status.state == "idle"


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


class NoOutputMacTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        return None


class RecordingPublisher:
    def __init__(
        self,
        events=None,
        *,
        errors=None,
        metadata_status="complete",
        metadata_source="missav",
        verified=True,
    ):
        self.events = events if events is not None else []
        self.errors = iter(errors or [])
        self.metadata_status = metadata_status
        self.metadata_source = metadata_source
        self.verified = verified

    def publish_english_ai(self, movie, path, metadata_path):
        self.events.append(("publish", movie, path.name, metadata_path.name))
        error = next(self.errors, None)
        if error is not None:
            raise error
        return SimpleNamespace(
            movie_uuid="00000000-0000-0000-0000-000000000001",
            content_sha256="a" * 64,
            file_size=path.stat().st_size,
            verified=self.verified,
            metadata_status=self.metadata_status,
            metadata_source=self.metadata_source,
        )


class RecordingTranslator(DiverseMacTranslator):
    def __init__(self, events):
        self.events = events

    def translate_to_english(self, input_srt, output_srt):
        self.events.append(("translate", input_srt.name, output_srt.name))
        super().translate_to_english(input_srt, output_srt)


def prepare_transcription_done_job(store, mac_jobs_root, cue_count=20, movie="ktb-096"):
    job = store.submit_job(movie, priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=60)
    stored_movie = job.normalized_movie_number
    japanese = mac_jobs_root / stored_movie / f"{stored_movie}.Japanese.srt"
    japanese.parent.mkdir(parents=True, exist_ok=True)
    blocks = [
        f"{index}\n00:00:{index:02d},000 --> 00:00:{index:02d},900\n日本語{index}\n"
        for index in range(1, cue_count + 1)
    ]
    japanese.write_text("\n".join(blocks), encoding="utf-8")
    return store.complete_worker_transcription(
        claimed.id,
        "windows-gpu-1",
        f"M:\\{stored_movie}\\{stored_movie}.Japanese.srt",
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
    worker_status = store.get_worker_status("mac-translation-1")
    assert worker_status is not None
    assert worker_status.role == "mac_translator"
    assert worker_status.state == "idle"


def test_good_translation_becomes_pending_then_publishes_without_retranslation(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
    )

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert [event[0] for event in events] == ["translate"]

    assert worker.process_one() is True
    assert [event[0] for event in events] == ["translate", "publish"]
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    log = mac_jobs_root / "ktb-096" / "logs" / "mac-translation.log"
    assert "publish_verified" in log.read_text(encoding="utf-8")
    assert "Distinct English" not in log.read_text(encoding="utf-8")


def test_placeholder_metadata_still_reaches_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(
            metadata_status="placeholder",
            metadata_source="placeholder",
        ),
    )

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.ENGLISH_SRT_READY
    assert refreshed.catalog_movie_uuid == "00000000-0000-0000-0000-000000000001"
    assert refreshed.metadata_status == "placeholder"
    assert refreshed.metadata_source == "placeholder"


def test_publish_retry_never_invokes_translator_again(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / "ktb-096" / "audio.wav"
    audio.write_bytes(b"keep-audio")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        max_publish_attempts=3,
        publish_retry_seconds=0,
        publisher=RecordingPublisher(
            events,
            errors=[RuntimeError("publish unavailable"), None],
        ),
    )

    assert worker.process_one() is True
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.PUBLISH_PENDING
    assert refreshed.translation_attempt_count == 0
    assert refreshed.publish_attempt_count == 1
    assert refreshed.next_publish_attempt_at is not None
    assert refreshed.claimed_by is None
    assert audio.read_bytes() == b"keep-audio"
    assert english.exists()
    assert worker.consecutive_quality_failures == 0

    assert worker.process_one() is True
    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.ENGLISH_SRT_READY
    assert [event[0] for event in events].count("translate") == 1
    assert [event[0] for event in events].count("publish") == 2


def test_publisher_quality_failure_is_permanent_and_quarantines_english(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / "ktb-096" / "audio.wav"
    audio.write_bytes(b"keep-audio")
    events = []
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        max_publish_attempts=3,
        publish_retry_seconds=0,
        publisher=RecordingPublisher(
            events,
            errors=[SubtitleQualityGateError(["subtitle_changed_after_validation"])],
        ),
    )
    assert worker.process_one() is True
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    rejected = english.parent / "rejected"
    assert refreshed.status is JobStatus.FAILED
    assert refreshed.error == (
        "publishing: quality_gate_failed:subtitle_changed_after_validation"
    )
    assert refreshed.publish_attempt_count == 1
    assert refreshed.translation_attempt_count == 0
    assert refreshed.next_publish_attempt_at is None
    assert [event[0] for event in events] == ["publish"]
    assert not english.exists()
    assert len(list(rejected.glob("*.srt"))) == 1
    assert audio.read_bytes() == b"keep-audio"
    assert worker.consecutive_quality_failures == 1


def test_three_publisher_quality_failures_stop_before_claiming_fourth_pending_job(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    events = []
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        quality_failure_limit=3,
        publisher=RecordingPublisher(
            events,
            errors=[
                SubtitleQualityGateError(["subtitle_changed_after_validation"])
                for _ in range(3)
            ],
        ),
    )
    pending_jobs = []
    for movie in ("abc-031", "abc-032", "abc-033", "abc-034"):
        job = prepare_transcription_done_job(store, mac_jobs_root, movie=movie)
        claimed = store.claim_translation_job(job.id, worker.worker_id, 60)
        assert claimed is not None
        assert worker._process_claimed_translation(claimed) is True
        assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
        pending_jobs.append(job)

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert worker.process_one() is True

    assert worker.consecutive_quality_failures == 3
    assert all(
        store.get_job(job.id).status is JobStatus.FAILED
        for job in pending_jobs[:3]
    )
    fourth = store.get_job(pending_jobs[3].id)
    assert fourth.status is JobStatus.PUBLISH_PENDING
    assert fourth.claimed_by is None
    assert [event[0] for event in events] == ["publish", "publish", "publish"]
    with pytest.raises(MacTranslationUnhealthyError, match="3 consecutive"):
        worker.process_one()


def test_final_publish_attempt_fails_but_preserves_validated_files(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / "ktb-096" / "audio.wav"
    audio.write_bytes(b"keep-audio")
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        max_publish_attempts=1,
        publish_retry_seconds=0,
        publisher=RecordingPublisher(errors=[RuntimeError("publish unavailable")]),
    )

    assert worker.process_one() is True
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.FAILED
    assert refreshed.error == "publishing: publish unavailable"
    assert refreshed.translation_attempt_count == 0
    assert refreshed.publish_attempt_count == 1
    assert english.exists()
    assert audio.read_bytes() == b"keep-audio"


def test_due_publication_has_priority_over_new_translation(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-001")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
    )
    assert worker.process_one() is True
    new_job = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-002")

    assert worker.process_one() is True

    assert store.get_job(pending.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_job(new_job.id).status is JobStatus.TRANSCRIPTION_DONE
    assert [event[0] for event in events] == ["translate", "publish"]


def test_future_publication_retry_does_not_block_new_translation(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    pending = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-003")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publish_retry_seconds=3600,
        publisher=RecordingPublisher(
            events,
            errors=[RuntimeError("publish unavailable")],
        ),
    )
    assert worker.process_one() is True
    assert worker.process_one() is True
    new_job = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-004")

    assert worker.process_one() is True

    assert store.get_job(pending.id).status is JobStatus.PUBLISH_PENDING
    assert store.get_job(new_job.id).status is JobStatus.PUBLISH_PENDING
    assert [event[0] for event in events] == ["translate", "publish", "translate"]


def test_exact_job_worker_does_not_claim_other_translation(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-001")
    second = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-002")
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-canary",
        lease_seconds=60,
        publisher=RecordingPublisher(),
    )

    assert worker.process_job_id(second.id) is True

    assert store.get_job(first.id).status is JobStatus.TRANSCRIPTION_DONE
    assert store.get_job(second.id).status is JobStatus.ENGLISH_SRT_READY


def test_exact_pending_job_publishes_without_claiming_other_pending(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-005")
    second = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-006")
    setup_worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-setup",
        lease_seconds=60,
        publisher=RecordingPublisher(),
    )
    assert setup_worker.process_job_id(first.id) is True
    first_ready = store.get_job(first.id)
    assert first_ready.status is JobStatus.ENGLISH_SRT_READY
    claimed_second = store.claim_translation_job(second.id, "mac-setup", 60)
    assert claimed_second is not None
    setup_worker._process_claimed_translation(claimed_second)
    assert store.get_job(second.id).status is JobStatus.PUBLISH_PENDING

    third = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-007")
    claimed_third = store.claim_translation_job(third.id, "mac-setup", 60)
    assert claimed_third is not None
    setup_worker._process_claimed_translation(claimed_third)
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-canary",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
    )

    assert worker.process_job_id(third.id) is True

    assert store.get_job(second.id).status is JobStatus.PUBLISH_PENDING
    assert store.get_job(third.id).status is JobStatus.ENGLISH_SRT_READY
    assert [event[0] for event in events] == ["publish"]


def test_prepared_catalog_canary_publication_only_exact_job_preserves_other_job(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = prepare_transcription_done_job(
        store, mac_jobs_root, cue_count=25, movie="abc-041"
    )
    first_dir = mac_jobs_root / first.normalized_movie_number
    japanese = first_dir / f"{first.normalized_movie_number}.Japanese.srt"
    english = first_dir / f"{first.normalized_movie_number}.English.srt"
    audio = first_dir / "audio.wav"
    audio.write_bytes(b"accepted-canary-audio")
    DiverseMacTranslator().translate_to_english(japanese, english)
    rejected_dir = first_dir / "rejected"
    rejected_dir.mkdir()
    (rejected_dir / "existing.srt").write_bytes(b"existing-rejected")
    with store.connection() as connection:
        connection.execute(
            "UPDATE jobs SET status = ?, translation_attempt_count = 2 WHERE id = ?",
            (JobStatus.FAILED.value, first.id),
        )
    second = prepare_transcription_done_job(
        store, mac_jobs_root, cue_count=25, movie="abc-042"
    )
    second_before = store.get_job(second.id)
    allowlist = tmp_path / "approved.txt"
    allowlist.write_text("ABC41\n", encoding="utf-8")
    japanese_sha256 = hashlib.sha256(japanese.read_bytes()).hexdigest()
    audio_sha256 = hashlib.sha256(audio.read_bytes()).hexdigest()
    english_before = english.read_bytes()
    rejected_before = {
        path.name: path.read_bytes() for path in sorted(rejected_dir.iterdir())
    }

    receipt = prepare_catalog_publication_canary(
        store,
        allowlist,
        movie="ABC41",
        limit=1,
        confirm_job_id=first.id,
    )
    translation_calls = []
    publication_calls = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(translation_calls),
        max_translation_attempts=3,
        worker_id="mac-catalog-canary",
        lease_seconds=60,
        publisher=RecordingPublisher(publication_calls, verified=True),
    )

    assert receipt.new_status is JobStatus.PUBLISH_PENDING
    assert worker.process_job_id(first.id) is True

    assert translation_calls == []
    assert [event[1] for event in publication_calls] == [
        first.normalized_movie_number
    ]
    assert store.get_job(first.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_job(second.id) == second_before
    assert hashlib.sha256(japanese.read_bytes()).hexdigest() == japanese_sha256
    assert hashlib.sha256(audio.read_bytes()).hexdigest() == audio_sha256
    assert english.exists()
    assert english.read_bytes() == english_before
    assert {
        path.name: path.read_bytes() for path in sorted(rejected_dir.iterdir())
    } == rejected_before


def test_publisher_receives_metadata_json_path(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    events = []
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True

    assert events == [
        ("publish", job.normalized_movie_number, "ktb-096.English.srt", "metadata.json")
    ]


def test_unverified_publication_never_becomes_ready(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publish_retry_seconds=0,
        publisher=RecordingPublisher(verified=False),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.PUBLISH_PENDING
    assert refreshed.error == "publishing: Supabase publication was not verified"


def test_changed_pending_subtitle_is_permanently_rejected_before_publisher(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / "ktb-096" / "audio.wav"
    audio.write_bytes(b"keep-audio")
    publisher = RecordingPublisher()
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=publisher,
    )
    assert worker.process_one() is True
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    japanese = mac_jobs_root / "ktb-096" / "ktb-096.Japanese.srt"
    rejected = english.parent / "rejected"
    rejected_before = len(list(rejected.glob("*.srt")))
    CollapsedMacTranslator().translate_to_english(japanese, english)

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert publisher.events == []
    assert refreshed.status is JobStatus.FAILED
    assert refreshed.error.startswith("publishing: quality_gate_failed:")
    assert "known_bad_collapse" in refreshed.error
    assert refreshed.publish_attempt_count == 1
    assert refreshed.translation_attempt_count == 0
    assert refreshed.next_publish_attempt_at is None
    assert not english.exists()
    assert len(list(rejected.glob("*.srt"))) == rejected_before + 1
    assert audio.read_bytes() == b"keep-audio"
    assert '"passed": false' in (
        english.parent / "logs" / "quality.log"
    ).read_text(encoding="utf-8")
    assert worker.consecutive_quality_failures == 1


def test_missing_pending_english_fails_quality_without_creating_rejected_dir(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / "ktb-096" / "audio.wav"
    audio.write_bytes(b"keep-audio")
    publisher = RecordingPublisher()
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=publisher,
    )
    assert worker.process_one() is True
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    english.unlink()

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.FAILED
    assert refreshed.error.startswith("publishing: quality_gate_failed:")
    assert "english_srt_missing" in refreshed.error
    assert refreshed.publish_attempt_count == 1
    assert refreshed.translation_attempt_count == 0
    assert publisher.events == []
    assert audio.read_bytes() == b"keep-audio"
    assert not english.exists()
    assert not (english.parent / "rejected").exists()
    assert worker.consecutive_quality_failures == 1


def test_three_changed_pending_subtitles_stop_worker_before_next_claim(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    publisher = RecordingPublisher()
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        quality_failure_limit=3,
        publisher=publisher,
    )
    pending_jobs = []
    for movie in ("abc-021", "abc-022", "abc-023"):
        job = prepare_transcription_done_job(store, mac_jobs_root, movie=movie)
        claimed = store.claim_translation_job(job.id, worker.worker_id, 60)
        assert claimed is not None
        assert worker._process_claimed_translation(claimed) is True
        english = mac_jobs_root / movie / f"{movie}.English.srt"
        japanese = mac_jobs_root / movie / f"{movie}.Japanese.srt"
        CollapsedMacTranslator().translate_to_english(japanese, english)
        pending_jobs.append(job)
    next_job = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-024")

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert worker.process_one() is True
    with pytest.raises(MacTranslationUnhealthyError, match="3 consecutive"):
        worker.process_one()

    assert publisher.events == []
    assert all(store.get_job(job.id).status is JobStatus.FAILED for job in pending_jobs)
    assert store.get_job(next_job.id).status is JobStatus.TRANSCRIPTION_DONE


def test_mac_translation_worker_permanently_rejects_collapsed_output(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / "ktb-096" / "audio.wav"
    audio.write_bytes(b"keep-audio")
    publisher = RecordingPublisher()
    worker = MacTranslationWorker(
        store,
        CollapsedMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=publisher,
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
    assert publisher.events == []
    assert '"passed": false' in (
        english.parent / "logs" / "quality.log"
    ).read_text(encoding="utf-8")


def test_translator_no_output_fails_quality_without_creating_rejected_dir(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / "ktb-096" / "audio.wav"
    audio.write_bytes(b"keep-audio")
    publisher = RecordingPublisher()
    worker = MacTranslationWorker(
        store,
        NoOutputMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=publisher,
    )

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    assert refreshed.status is JobStatus.FAILED
    assert refreshed.error.startswith("translating: quality_gate_failed:")
    assert "english_srt_missing" in refreshed.error
    assert refreshed.translation_attempt_count == 1
    assert refreshed.publish_attempt_count == 0
    assert publisher.events == []
    assert audio.read_bytes() == b"keep-audio"
    assert not english.exists()
    assert not (english.parent / "rejected").exists()
    assert worker.consecutive_quality_failures == 1


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
    assert refreshed.worker_attempt_count == 0
    assert refreshed.translation_attempt_count == 1
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
