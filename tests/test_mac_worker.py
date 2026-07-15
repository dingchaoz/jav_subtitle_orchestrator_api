import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
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
from orchestrator.paths import build_job_paths
from orchestrator.store import HistoricalRepairState, JobStore
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


class PartialFailingMacTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        output_srt.write_text("partial interrupted candidate\n", encoding="utf-8")
        raise RuntimeError("Mac translation runtime interrupted")


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
            subtitle_id="00000000-0000-0000-0000-000000000002",
            storage_path=f"{movie.split('-', 1)[0]}/{movie}/{movie}-English_AI.srt",
            content_sha256="a" * 64,
            file_size=path.stat().st_size,
            verified=self.verified,
            metadata_status=self.metadata_status,
            metadata_source=self.metadata_source,
        )


class RecordingCatalogSync:
    public_visibility_verification_enabled = True

    def __init__(self, events=None, *, errors=None):
        self.events = events if events is not None else []
        self.errors = iter(errors or [])
        self.receipts = []

    def sync(self, movie, **expected_receipt):
        self.events.append(("catalog", movie))
        self.receipts.append(expected_receipt)
        error = next(self.errors, None)
        if error is not None:
            raise error
        return SimpleNamespace(
            canonical_code=movie,
            d1_rows_updated=1,
            subtitle_count=1,
            kv_keys_deleted=(
                f"movie:full:{movie}",
                f"movie:light:{movie}",
            ),
            diagnostic=SimpleNamespace(
                http_status=200,
                response_json='{"success":true}',
            ),
        )


class RecordingCallbackNotifier:
    def __init__(self, events):
        self.events = events
        self.calls = []

    def notify_subtitle_ready(self, job):
        self.events.append(("webhook", job.normalized_movie_number))
        self.calls.append(job)


class ReclaimingCatalogSync:
    public_visibility_verification_enabled = True

    def __init__(self, store, job_id, *, fail=False):
        self.store = store
        self.job_id = job_id
        self.fail = fail
        self.reclaimed = None

    def sync(self, movie, **_expected_receipt):
        from orchestrator.catalog_sync import CatalogSyncError

        expired = (datetime.now(UTC) - timedelta(minutes=5)).replace(microsecond=0).isoformat()
        self.store.force_lease_expiry_for_test(self.job_id, expired)
        assert self.store.recover_expired_catalog_sync_leases(3, 0) == 1
        self.reclaimed = self.store.claim_catalog_sync_job(
            "replacement-worker",
            60,
            job_id=self.job_id,
        )
        assert self.reclaimed is not None
        if self.fail:
            raise CatalogSyncError("catalog_fetch_failed")
        return SimpleNamespace(
            canonical_code=movie,
            d1_rows_updated=1,
            subtitle_count=1,
            kv_keys_deleted=(
                f"movie:full:{movie}",
                f"movie:light:{movie}",
            ),
            diagnostic=SimpleNamespace(
                http_status=200,
                response_json='{"success":true}',
            ),
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


def enqueue_historical_worker_job(store, mac_jobs_root, movie):
    job = prepare_transcription_done_job(store, mac_jobs_root, movie=movie)
    paths = build_job_paths(movie, mac_jobs_root, "M:\\")
    paths.audio_path_mac.write_bytes(b"preserved historical audio")
    old_english = b"old rejected English subtitle\n"
    paths.english_srt_path_mac.write_bytes(old_english)
    japanese_sha256 = hashlib.sha256(paths.japanese_srt_path_mac.read_bytes()).hexdigest()
    audio_sha256 = hashlib.sha256(paths.audio_path_mac.read_bytes()).hexdigest()
    source_english_sha256 = hashlib.sha256(old_english).hexdigest()
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = NULL, lease_expires_at = NULL, "
            "english_srt_path_mac = ? WHERE id = ?",
            (JobStatus.ENGLISH_SRT_READY.value, str(paths.english_srt_path_mac), job.id),
        )
        conn.execute(
            """
            INSERT INTO historical_translation_repairs (
              id, batch_id, job_id, movie_code, allowlist_sha256, state,
              attempt_count, next_attempt_at, reason_code, japanese_sha256,
              audio_probe_snapshot_sha256, audio_sha256, source_english_sha256,
              english_sha256, created_at, updated_at
            ) VALUES (?, 'batch-worker', ?, ?, ?, ?, 0, NULL, NULL, ?, ?, ?, ?,
                      NULL, '2026-07-13T00:00:00+00:00',
                      '2026-07-13T00:00:00+00:00')
            """,
            (
                f"repair-{movie}", job.id, movie, "a" * 64,
                HistoricalRepairState.PENDING.value, japanese_sha256, "b" * 64,
                audio_sha256, source_english_sha256,
            ),
        )
    return store.get_job(job.id), paths


def test_normal_translation_wins_over_pending_historical_repair(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    historical, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-101")
    normal = prepare_transcription_done_job(store, mac_jobs_root, movie="new-101")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
    )

    assert worker.process_one() is True

    assert events[0][1] == "new-101.Japanese.srt"
    assert store.get_job(normal.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_job(historical.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_historical_repair(historical.id).state is HistoricalRepairState.PENDING


def test_normal_publication_wins_over_pending_historical_repair(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    historical, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-100")
    normal = prepare_transcription_done_job(store, mac_jobs_root, movie="new-100")
    claimed = store.claim_translation_job(normal.id, "setup", 60)
    normal_paths = build_job_paths("new-100", mac_jobs_root, "M:\\")
    DiverseMacTranslator().translate_to_english(
        normal_paths.japanese_srt_path_mac,
        normal_paths.english_srt_path_mac,
    )
    store.complete_mac_translation_quality(
        normal.id,
        "setup",
        lambda path: Path(path).exists(),
        lease_token=claimed.stage_lease_token,
    )
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )

    assert worker.process_one() is True

    assert store.get_job(normal.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_historical_repair(historical.id).state is HistoricalRepairState.PENDING
    assert [event[0] for event in events] == ["publish"]


def test_historical_job_runs_when_normal_lane_is_empty_and_quarantines_old_english(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-102")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )

    assert worker.process_one() is True

    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    repair = store.get_historical_repair(job.id)
    assert repair.state is HistoricalRepairState.RUNNING
    rejected = list((paths.job_dir_mac / "rejected").glob("*.rejected-old-*.srt"))
    assert len(rejected) == 1
    assert hashlib.sha256(rejected[0].read_bytes()).hexdigest() == repair.source_english_sha256
    assert paths.english_srt_path_mac.exists()
    assert [event[0] for event in events] == ["translate"]


def test_historical_repair_fails_closed_without_publication_pipeline(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-105")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
    )

    assert worker.process_one() is False

    assert events == []
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.PENDING
    status = store.get_worker_status("mac-translation-1")
    assert status is not None
    assert status.last_error == "publication_configuration_missing"
    lane = store.historical_lane_state()
    assert lane.paused is True
    assert lane.reason_code == "publication_configuration_missing"


def test_good_historical_repair_uploads_catalogs_and_marks_success(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-103")
    audio_before = hashlib.sha256(paths.audio_path_mac.read_bytes()).hexdigest()
    japanese_before = hashlib.sha256(paths.japanese_srt_path_mac.read_bytes()).hexdigest()
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    repair = store.get_historical_repair(job.id)
    assert refreshed.status is JobStatus.ENGLISH_SRT_READY
    assert repair.state is HistoricalRepairState.SUCCEEDED
    assert repair.english_sha256 == refreshed.published_content_sha256
    assert [event[0] for event in events] == ["translate", "publish", "catalog"]
    assert hashlib.sha256(paths.audio_path_mac.read_bytes()).hexdigest() == audio_before
    assert hashlib.sha256(paths.japanese_srt_path_mac.read_bytes()).hexdigest() == japanese_before


def test_publication_orphan_resumes_publisher_without_retranslation(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-321")
    setup_events = []
    setup_worker = MacTranslationWorker(
        store,
        RecordingTranslator(setup_events),
        max_translation_attempts=3,
        worker_id="mac-setup",
        lease_seconds=60,
        publisher=RecordingPublisher(setup_events),
        catalog_sync_client=RecordingCatalogSync(setup_events),
    )
    assert setup_worker.process_one() is True
    assert [event[0] for event in setup_events] == ["translate"]
    due = (datetime.now(UTC) - timedelta(minutes=1)).replace(
        microsecond=0
    ).isoformat()
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = NULL, "
            "lease_expires_at = NULL, stage_lease_token = NULL, error = ?, "
            "publish_attempt_count = 1, next_publish_attempt_at = ? "
            "WHERE id = ?",
            (
                JobStatus.FAILED.value,
                "publishing: publication lease expired",
                due,
                job.id,
            ),
        )
    before = store.get_job(job.id)

    assert store.reconcile_orphaned_historical_repairs() == 1

    resumed = store.get_job(job.id)
    repair = store.get_historical_repair(job.id)
    assert resumed.status is JobStatus.PUBLISH_PENDING
    assert repair.state is HistoricalRepairState.RUNNING
    assert resumed.publish_attempt_count == before.publish_attempt_count == 1
    assert resumed.next_publish_attempt_at == before.next_publish_attempt_at == due
    assert resumed.english_srt_path_mac == before.english_srt_path_mac
    assert resumed.published_subtitle_id == before.published_subtitle_id
    assert paths.english_srt_path_mac.exists()

    events = []
    restarted = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-restarted",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    assert restarted.process_one() is True
    assert [event[0] for event in events] == ["publish"]
    published = store.get_job(job.id)
    assert published.status is JobStatus.ENGLISH_SRT_READY
    assert published.publish_attempt_count == 1


def test_catalog_orphan_resumes_catalog_without_translation_or_reupload(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-322")
    setup_events = []
    setup_worker = MacTranslationWorker(
        store,
        RecordingTranslator(setup_events),
        max_translation_attempts=3,
        worker_id="mac-setup",
        lease_seconds=60,
        publisher=RecordingPublisher(setup_events),
        catalog_sync_client=RecordingCatalogSync(setup_events),
    )
    assert setup_worker.process_one() is True
    assert setup_worker.process_one() is True
    assert [event[0] for event in setup_events] == ["translate", "publish"]
    due = (datetime.now(UTC) - timedelta(minutes=1)).replace(
        microsecond=0
    ).isoformat()
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = NULL, "
            "lease_expires_at = NULL, catalog_lease_token = NULL, error = ?, "
            "catalog_sync_attempt_count = 1, next_catalog_sync_attempt_at = ? "
            "WHERE id = ?",
            (
                JobStatus.FAILED.value,
                "catalog_sync: catalog_sync_lease_expired",
                due,
                job.id,
            ),
        )
    before = store.get_job(job.id)
    receipt = (
        before.catalog_movie_uuid,
        before.metadata_status,
        before.metadata_source,
        before.published_subtitle_id,
        before.published_storage_path,
        before.published_content_sha256,
        before.published_file_size,
    )

    assert store.reconcile_orphaned_historical_repairs() == 1

    resumed = store.get_job(job.id)
    repair = store.get_historical_repair(job.id)
    assert resumed.status is JobStatus.ENGLISH_SRT_READY
    assert repair.state is HistoricalRepairState.SUCCEEDED
    assert resumed.catalog_sync_attempt_count == before.catalog_sync_attempt_count == 1
    assert resumed.next_catalog_sync_attempt_at == due
    assert (
        resumed.catalog_movie_uuid,
        resumed.metadata_status,
        resumed.metadata_source,
        resumed.published_subtitle_id,
        resumed.published_storage_path,
        resumed.published_content_sha256,
        resumed.published_file_size,
    ) == receipt

    events = []
    restarted = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-restarted",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    assert restarted.process_one() is True
    assert [event[0] for event in events] == ["catalog"]
    cataloged = store.get_job(job.id)
    assert cataloged.status is JobStatus.ENGLISH_SRT_READY
    assert cataloged.catalog_sync_attempt_count == 1


def test_catalog_orphan_with_invalid_receipt_fails_closed(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-323")
    setup_worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-setup",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert setup_worker.process_one() is True
    assert setup_worker.process_one() is True
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, claimed_by = NULL, "
            "lease_expires_at = NULL, catalog_lease_token = NULL, error = ?, "
            "published_storage_path = ? WHERE id = ?",
            (
                JobStatus.FAILED.value,
                "catalog_sync: catalog_sync_lease_expired",
                "wrong/path.srt",
                job.id,
            ),
        )

    assert store.reconcile_orphaned_historical_repairs() == 0

    repair = store.get_historical_repair(job.id)
    assert store.get_job(job.id).status is JobStatus.FAILED
    assert repair.state is HistoricalRepairState.SUCCEEDED
    assert store.claim_catalog_sync_job("mac-restarted", 60, job_id=job.id) is None


def test_bad_historical_candidate_is_permanent_rejected_and_never_published(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-104")
    audio_before = paths.audio_path_mac.read_bytes()
    japanese_before = paths.japanese_srt_path_mac.read_bytes()
    events = []
    worker = MacTranslationWorker(
        store,
        CollapsedMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )

    assert worker.process_one() is True
    assert worker.process_one() is False

    refreshed = store.get_job(job.id)
    repair = store.get_historical_repair(job.id)
    assert refreshed.status is JobStatus.FAILED
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code.startswith("quality_gate_failed:")
    assert events == []
    assert paths.audio_path_mac.read_bytes() == audio_before
    assert paths.japanese_srt_path_mac.read_bytes() == japanese_before
    rejected = list((paths.job_dir_mac / "rejected").glob("*.srt"))
    assert len(rejected) == 2
    assert not paths.english_srt_path_mac.exists()


def test_historical_local_quality_quarantine_is_durable_before_terminal(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-106")
    worker = MacTranslationWorker(
        store,
        CollapsedMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    original = store.fail_historical_translation_permanent

    def assert_quarantined_before_terminal(*args, **kwargs):
        repair = store.get_historical_repair(job.id)
        markers = list(
            (paths.job_dir_mac / "rejected").glob(
                f".quality-rejected-{repair.id}-*.json"
            )
        )
        assert not paths.english_srt_path_mac.exists()
        assert len(markers) == 1
        marker = json.loads(markers[0].read_text(encoding="utf-8"))
        assert marker["reason_code"].startswith("quality_gate_failed:")
        assert marker["candidate_sha256"] == kwargs["english_sha256"]
        assert "日本語" not in markers[0].read_text(encoding="utf-8")
        return original(*args, **kwargs)

    monkeypatch.setattr(
        store,
        "fail_historical_translation_permanent",
        assert_quarantined_before_terminal,
    )

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.FAILED
    assert (
        store.get_historical_repair(job.id).state
        is HistoricalRepairState.PERMANENT_FAILED
    )


def test_historical_local_quality_crash_after_quarantine_resumes_without_retranslation(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-107")
    store.record_historical_quality_failure(3)
    store.record_historical_quality_failure(3)
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=1,
        worker_id="mac-crash-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    worker.translator = CollapsedMacTranslator()
    claimed = store.claim_next_historical_repair(worker.worker_id, 60)
    assert claimed is not None
    original = store.fail_historical_translation_permanent

    def crash_before_database(*args, **kwargs):
        assert not paths.english_srt_path_mac.exists()
        raise KeyboardInterrupt("simulated crash after durable quarantine")

    monkeypatch.setattr(
        store,
        "fail_historical_translation_permanent",
        crash_before_database,
    )
    with pytest.raises(KeyboardInterrupt, match="durable quarantine"):
        worker._process_historical_translation(claimed)

    assert store.get_job(job.id).status is JobStatus.TRANSLATING
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.RUNNING
    assert list(
        (paths.job_dir_mac / "rejected").glob(".quality-rejected-*.json")
    )
    active_observer = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=1,
        worker_id="mac-crash-observer",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert active_observer.process_one() is False
    assert store.get_job(job.id).claimed_by == "mac-crash-1"
    monkeypatch.setattr(
        store,
        "fail_historical_translation_permanent",
        original,
    )
    expired = (datetime.now(UTC) - timedelta(seconds=1)).replace(
        microsecond=0
    ).isoformat()
    store.force_lease_expiry_for_test(job.id, expired)
    recovery_events = []
    recovering = MacTranslationWorker(
        store,
        RecordingTranslator(recovery_events),
        max_translation_attempts=1,
        worker_id="mac-crash-2",
        lease_seconds=60,
        publisher=RecordingPublisher(recovery_events),
        catalog_sync_client=RecordingCatalogSync(recovery_events),
    )

    assert recovering.process_one() is True

    assert store.get_job(job.id).status is JobStatus.FAILED
    assert (
        store.get_historical_repair(job.id).state
        is HistoricalRepairState.PERMANENT_FAILED
    )
    assert recovery_events == []
    assert not paths.english_srt_path_mac.exists()
    repair = store.get_historical_repair(job.id)
    lane = store.historical_lane_state()
    assert repair.reason_code.startswith("quality_gate_failed:")
    assert lane.consecutive_quality_failures == 3
    assert lane.paused is True
    assert lane.reason_code == "quality_failure_limit"
    assert recovering.process_one() is False
    assert store.historical_lane_state().consecutive_quality_failures == 3


def test_historical_local_quality_crash_before_quarantine_keeps_canonical_nonterminal(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-117")
    worker = MacTranslationWorker(
        store,
        CollapsedMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-before-quarantine",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    claimed = store.claim_next_historical_repair(worker.worker_id, 60)
    assert claimed is not None
    original = worker._historical_quarantine_locked

    def crash_before_quality_quarantine(*args, **kwargs):
        if kwargs.get("quality_marker") is not None:
            raise KeyboardInterrupt("simulated crash before quarantine")
        return original(*args, **kwargs)

    monkeypatch.setattr(
        worker,
        "_historical_quarantine_locked",
        crash_before_quality_quarantine,
    )

    with pytest.raises(KeyboardInterrupt, match="before quarantine"):
        worker._process_historical_translation(claimed)

    assert store.get_job(job.id).status is JobStatus.TRANSLATING
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.RUNNING
    assert store.historical_lane_state().consecutive_quality_failures == 0
    assert paths.english_srt_path_mac.exists()
    assert not list(
        (paths.job_dir_mac / "rejected").glob(".quality-rejected-*.json")
    )


def test_historical_publisher_quality_crash_resumes_without_reupload(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-108")
    events = []
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        max_publish_attempts=1,
        worker_id="mac-publish-crash-1",
        lease_seconds=60,
        publisher=RecordingPublisher(
            events,
            errors=[
                SubtitleQualityGateError(
                    ["subtitle_changed_after_validation"]
                )
            ],
        ),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    assert worker.process_one() is True
    original = store.fail_historical_publication

    def crash_before_database(*args, **kwargs):
        assert not paths.english_srt_path_mac.exists()
        assert list(
            (paths.job_dir_mac / "rejected").glob(
                ".quality-rejected-*.json"
            )
        )
        raise KeyboardInterrupt("simulated publisher crash before database")

    monkeypatch.setattr(
        store, "fail_historical_publication", crash_before_database
    )
    with pytest.raises(KeyboardInterrupt, match="publisher crash"):
        worker.process_one()

    assert [event[0] for event in events] == ["publish"]
    assert store.get_job(job.id).status is JobStatus.PUBLISHING
    monkeypatch.setattr(store, "fail_historical_publication", original)
    expired = (datetime.now(UTC) - timedelta(seconds=1)).replace(
        microsecond=0
    ).isoformat()
    store.force_lease_expiry_for_test(job.id, expired)
    recovery_events = []
    recovering = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        max_publish_attempts=1,
        worker_id="mac-publish-crash-2",
        lease_seconds=60,
        publish_retry_seconds=0,
        publisher=RecordingPublisher(
            recovery_events,
            errors=[AssertionError("quality marker must prevent reupload")],
        ),
        catalog_sync_client=RecordingCatalogSync(recovery_events),
    )

    assert recovering.process_one() is True
    assert store.get_job(job.id).status is JobStatus.FAILED
    assert (
        store.get_historical_repair(job.id).state
        is HistoricalRepairState.PERMANENT_FAILED
    )
    assert recovery_events == []
    repair = store.get_historical_repair(job.id)
    lane = store.historical_lane_state()
    assert repair.reason_code == (
        "quality_gate_failed:subtitle_changed_after_validation"
    )
    assert lane.consecutive_quality_failures == 1
    assert recovering.process_one() is False
    assert store.historical_lane_state().consecutive_quality_failures == 1


def test_historical_publisher_quarantine_symlink_failure_pauses_without_terminal(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-109")
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-symlink-1",
        lease_seconds=60,
        publisher=RecordingPublisher(
            errors=[
                SubtitleQualityGateError(
                    ["subtitle_changed_after_validation"]
                )
            ],
        ),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert worker.process_one() is True
    outside = paths.job_dir_mac.parent / "outside-bad.srt"
    outside.write_bytes(b"outside must stay untouched")
    paths.english_srt_path_mac.unlink()
    paths.english_srt_path_mac.symlink_to(outside)

    assert worker.process_one() is True

    current = store.get_job(job.id)
    repair = store.get_historical_repair(job.id)
    lane = store.historical_lane_state()
    assert current.status is JobStatus.PUBLISH_PENDING
    assert current.error == "publishing: quarantine_failed"
    assert repair.state is HistoricalRepairState.RUNNING
    assert repair.reason_code == "quarantine_failed"
    assert lane.paused is True
    assert lane.reason_code == "quarantine_failed"
    assert lane.consecutive_quality_failures == 0
    assert paths.english_srt_path_mac.is_symlink()
    assert outside.read_bytes() == b"outside must stay untouched"


def test_historical_local_quarantine_io_failure_pauses_without_terminal(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-110")
    worker = MacTranslationWorker(
        store,
        CollapsedMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-io-1",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    original = worker._historical_quarantine_locked

    def fail_quality_quarantine(*args, **kwargs):
        if kwargs.get("quality_marker") is not None:
            raise OSError("simulated rejected directory I/O failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(
        worker, "_historical_quarantine_locked", fail_quality_quarantine
    )

    assert worker.process_one() is True

    current = store.get_job(job.id)
    repair = store.get_historical_repair(job.id)
    lane = store.historical_lane_state()
    assert current.status is JobStatus.FAILED
    assert current.error == "historical_repair: quarantine_failed"
    assert repair.state is HistoricalRepairState.RETRY_WAIT
    assert repair.reason_code == "quarantine_failed"
    assert lane.paused is True
    assert lane.reason_code == "quarantine_failed"
    assert lane.consecutive_quality_failures == 0
    assert paths.english_srt_path_mac.exists()


@pytest.mark.parametrize("changed_file", ["audio", "japanese"])
def test_historical_bad_quality_with_preservation_change_is_preservation_permanent(
    sqlite_path, mac_jobs_root, changed_file
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-118")
    target = (
        paths.audio_path_mac
        if changed_file == "audio"
        else paths.japanese_srt_path_mac
    )

    class TamperingCollapsedTranslator(CollapsedMacTranslator):
        def translate_to_english(self, input_srt, output_srt):
            super().translate_to_english(input_srt, output_srt)
            with target.open("ab") as handle:
                handle.write(b"tampered")

    publish_events = []
    worker = MacTranslationWorker(
        store,
        TamperingCollapsedTranslator(),
        max_translation_attempts=3,
        worker_id=f"mac-preservation-{changed_file}",
        lease_seconds=60,
        publisher=RecordingPublisher(publish_events),
        catalog_sync_client=RecordingCatalogSync(publish_events),
    )

    assert worker.process_one() is True

    current = store.get_job(job.id)
    repair = store.get_historical_repair(job.id)
    lane = store.historical_lane_state()
    assert current.status is JobStatus.FAILED
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code == "preservation_hash_changed"
    assert lane.paused is True
    assert lane.reason_code == "preservation_hash_changed"
    assert lane.consecutive_quality_failures == 0
    assert publish_events == []


def test_historical_publication_local_gate_quarantines_before_permanent(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-116")
    events = []
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-publication-local-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    assert worker.process_one() is True
    CollapsedMacTranslator().translate_to_english(
        paths.japanese_srt_path_mac,
        paths.english_srt_path_mac,
    )

    assert worker.process_one() is True

    assert store.get_job(job.id).status is JobStatus.FAILED
    repair = store.get_historical_repair(job.id)
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code.startswith("quality_gate_failed:")
    assert not paths.english_srt_path_mac.exists()
    assert list(
        (paths.job_dir_mac / "rejected").glob(".quality-rejected-*.json")
    )
    assert events == []


def test_historical_publication_preservation_change_blocks_publisher_and_pauses(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-119")
    events = []
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-publication-preservation",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    assert worker.process_one() is True
    paths.audio_path_mac.write_bytes(b"changed before publication")

    assert worker.process_one() is True

    repair = store.get_historical_repair(job.id)
    lane = store.historical_lane_state()
    assert store.get_job(job.id).status is JobStatus.FAILED
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code == "preservation_hash_changed"
    assert lane.paused is True
    assert lane.reason_code == "preservation_hash_changed"
    assert lane.consecutive_quality_failures == 0
    assert events == []


def test_historical_catalog_failure_does_not_reopen_published_artifact(
    sqlite_path, mac_jobs_root
):
    from orchestrator.catalog_sync import CatalogSyncError

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-120")
    events = []
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-catalog-preservation",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(
            events,
            errors=[CatalogSyncError("catalog_sync_failed")],
        ),
    )
    assert worker.process_one() is True
    assert worker.process_one() is True
    published = store.get_job(job.id)
    receipt = (
        published.published_subtitle_id,
        published.published_storage_path,
        published.published_content_sha256,
        published.published_file_size,
    )
    paths.audio_path_mac.write_bytes(b"changed before catalog failure")

    assert worker.process_one() is True

    current = store.get_job(job.id)
    repair = store.get_historical_repair(job.id)
    lane = store.historical_lane_state()
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert current.catalog_sync_status == "pending"
    assert current.catalog_sync_warning_code == "catalog_sync_failed"
    assert repair.state is HistoricalRepairState.SUCCEEDED
    assert repair.reason_code is None
    assert lane.paused is False
    assert lane.reason_code is None
    assert lane.consecutive_quality_failures == 0
    assert (
        current.published_subtitle_id,
        current.published_storage_path,
        current.published_content_sha256,
        current.published_file_size,
    ) == receipt
    assert [event[0] for event in events] == ["publish", "catalog"]


def test_three_historical_quality_failures_pause_history_but_normal_still_runs(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    for movie in ("old-111", "old-112", "old-113"):
        enqueue_historical_worker_job(store, mac_jobs_root, movie)
    workers = [
        MacTranslationWorker(
            store,
            CollapsedMacTranslator(),
            max_translation_attempts=3,
            worker_id=f"mac-translation-{index}",
            lease_seconds=60,
            quality_failure_limit=3,
            publisher=RecordingPublisher(),
            catalog_sync_client=RecordingCatalogSync(),
        )
        for index in range(1, 4)
    ]

    assert [worker.process_one() for worker in workers] == [True, True, True]
    assert workers[-1].historical_quality_failures == 3
    assert store.historical_lane_state().reason_code == "quality_failure_limit"
    normal = prepare_transcription_done_job(store, mac_jobs_root, movie="new-111")
    worker = workers[-1]
    worker.translator = DiverseMacTranslator()

    assert worker.process_one() is True
    assert store.get_job(normal.id).status is JobStatus.PUBLISH_PENDING


def test_good_historical_quality_pass_resets_durable_failure_streak(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    enqueue_historical_worker_job(store, mac_jobs_root, "old-114")
    good, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-115")
    worker = MacTranslationWorker(
        store,
        CollapsedMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert worker.process_one() is True
    assert store.historical_lane_state().consecutive_quality_failures == 1

    worker.translator = DiverseMacTranslator()
    assert worker.process_one() is True

    assert store.get_job(good.id).status is JobStatus.PUBLISH_PENDING
    assert store.historical_lane_state().consecutive_quality_failures == 1
    assert worker.process_one() is True
    assert store.get_job(good.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.historical_lane_state().consecutive_quality_failures == 0


def test_historical_preservation_uses_full_audio_hash_not_probe(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-121")
    original = paths.audio_path_mac.stat()
    with paths.audio_path_mac.open("r+b") as audio:
        audio.seek(-1, 2)
        audio.write(b"X")
    paths.audio_path_mac.touch()
    import os
    os.utime(paths.audio_path_mac, ns=(original.st_atime_ns, original.st_mtime_ns))
    events = []
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )

    assert worker.process_one() is True

    repair = store.get_historical_repair(job.id)
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code == "preservation_hash_changed"
    assert events == []


def test_inflight_historical_unit_finishes_then_normal_wins_before_next_repair(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-131")
    second, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-132")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    assert worker.process_one() is True  # historical translation
    normal = prepare_transcription_done_job(store, mac_jobs_root, movie="new-131")

    assert worker.process_one() is True  # same historical publication
    assert worker.process_one() is True  # same historical catalog
    assert store.get_historical_repair(first.id).state is HistoricalRepairState.SUCCEEDED
    assert worker.process_one() is True  # normal translation before next repair

    assert store.get_job(normal.id).status is JobStatus.PUBLISH_PENDING
    assert store.get_historical_repair(second.id).state is HistoricalRepairState.PENDING
    assert [event[0] for event in events] == [
        "translate", "publish", "catalog", "translate"
    ]


def _assert_historical_controller_pause(
    store,
    tmp_path,
    movie,
    expected_reason,
):
    from orchestrator.historical_batch import HistoricalRepairController

    allowlist = tmp_path / f"{movie}-allowlist.txt"
    allowlist.write_text(f"{movie}\n")
    allowlist_sha = hashlib.sha256(allowlist.read_bytes()).hexdigest()
    with store.connection() as conn:
        conn.execute(
            "UPDATE historical_translation_repairs SET allowlist_sha256 = ? "
            "WHERE movie_code = ?",
            (allowlist_sha, movie),
        )
    controller = HistoricalRepairController(
        store,
        allowlist,
        worker_health_probe=lambda: None,
    )
    result = controller.run_once()
    assert result.hard_pause is True
    assert result.reason_code == expected_reason
    assert result.enqueued == 0
    return controller


def test_historical_publication_exhaustion_is_terminal_without_retranslation(
    sqlite_path, mac_jobs_root, tmp_path
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-141")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        max_publish_attempts=1,
        publisher=RecordingPublisher(events, errors=[RuntimeError("offline")]),
        catalog_sync_client=RecordingCatalogSync(events),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True

    assert store.get_job(job.id).status is JobStatus.FAILED
    repair = store.get_historical_repair(job.id)
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code == "supabase_verification_failed"
    lane = store.historical_lane_state()
    assert lane.paused is True
    assert lane.reason_code == "supabase_verification_failed"
    controller = _assert_historical_controller_pause(
        store,
        tmp_path,
        "old-141",
        "supabase_verification_failed",
    )
    assert [event[0] for event in events] == ["translate", "publish"]
    store.resume_historical_lane()
    resumed = controller.run_once()
    assert resumed.complete is True
    assert resumed.counts["permanent_failed"] == 1


def test_historical_catalog_exhaustion_only_fails_catalog_substate(
    sqlite_path, mac_jobs_root, tmp_path
):
    from orchestrator.catalog_sync import CatalogSyncError

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-142")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        max_catalog_sync_attempts=1,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(
            events, errors=[CatalogSyncError("catalog_fetch_failed")]
        ),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert worker.process_one() is True

    current = store.get_job(job.id)
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert current.catalog_sync_status == "failed"
    assert current.catalog_sync_warning_code == "catalog_fetch_failed"
    repair = store.get_historical_repair(job.id)
    assert repair.state is HistoricalRepairState.SUCCEEDED
    assert repair.reason_code is None
    lane = store.historical_lane_state()
    assert lane.paused is False
    assert lane.reason_code is None
    assert [event[0] for event in events] == ["translate", "publish", "catalog"]


@pytest.mark.parametrize(
    "reason_code",
    [
        "catalog_auth_failed",
        "catalog_redirect_rejected",
        "catalog_response_invalid",
        "catalog_response_mismatch",
        "public_visibility_redirect_rejected",
        "public_visibility_response_invalid",
        "public_visibility_mismatch",
    ],
)
def test_historical_structural_catalog_failure_is_only_a_warning(
    sqlite_path, mac_jobs_root, tmp_path, reason_code
):
    from orchestrator.catalog_sync import CatalogSyncError

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    movie = "old-171"
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, movie)
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        max_catalog_sync_attempts=3,
        catalog_sync_retry_seconds=0,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(
            events,
            errors=[CatalogSyncError(reason_code), None],
        ),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert worker.process_one() is True

    current = store.get_job(job.id)
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.SUCCEEDED
    lane = store.historical_lane_state()
    assert lane.paused is False
    assert lane.reason_code is None
    assert current.catalog_sync_warning_code == reason_code
    assert current.error is None


@pytest.mark.parametrize(
    ("reason_code", "pause_reason"),
    [
        ("catalog_fetch_failed", "catalog_sync_failed"),
        ("catalog_sync_failed", "catalog_sync_failed"),
        ("public_visibility_fetch_failed", "public_visibility_failed"),
        ("public_visibility_not_found", "public_visibility_failed"),
    ],
)
def test_historical_transient_catalog_exhaustion_preserves_ready_artifact(
    sqlite_path, mac_jobs_root, tmp_path, reason_code, pause_reason
):
    from orchestrator.catalog_sync import CatalogSyncError

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    movie = "old-172"
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, movie)
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        max_catalog_sync_attempts=1,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(
            events,
            errors=[CatalogSyncError(reason_code)],
        ),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert worker.process_one() is True

    current = store.get_job(job.id)
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert current.catalog_sync_status == "failed"
    assert current.catalog_sync_warning_code == reason_code
    repair = store.get_historical_repair(job.id)
    assert repair.state is HistoricalRepairState.SUCCEEDED
    assert repair.reason_code is None
    lane = store.historical_lane_state()
    assert lane.paused is False
    assert lane.reason_code is None


def test_historical_catalog_auth_failure_does_not_pause_artifact_lane(
    sqlite_path, mac_jobs_root
):
    from orchestrator.catalog_sync import CatalogSyncError

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-149")
    second, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-150")
    events = []
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        max_catalog_sync_attempts=1,
        catalog_sync_retry_seconds=0,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(
            events,
            errors=[CatalogSyncError("catalog_auth_failed")],
        ),
    )
    assert worker.process_one() is True
    assert worker.process_one() is True
    assert worker.process_one() is True

    current = store.get_job(job.id)
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert current.catalog_sync_status == "failed"
    assert current.catalog_sync_warning_code == "catalog_auth_failed"
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.SUCCEEDED
    lane = store.historical_lane_state()
    assert lane.paused is False
    assert lane.reason_code is None
    assert store.get_historical_repair(second.id).state is HistoricalRepairState.PENDING

    normal = prepare_transcription_done_job(store, mac_jobs_root, movie="new-149")
    assert worker.process_one() is True
    assert store.get_job(normal.id).status is JobStatus.PUBLISH_PENDING


@pytest.mark.parametrize("stage", ["publication", "catalog"])
def test_historical_terminal_stage_rolls_back_job_if_repair_update_fails(
    sqlite_path, mac_jobs_root, stage
):
    from orchestrator.catalog_sync import CatalogSyncError

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-151")
    publication_errors = [RuntimeError("offline")] if stage == "publication" else []
    catalog_errors = (
        [CatalogSyncError("catalog_sync_failed")] if stage == "catalog" else []
    )
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        max_publish_attempts=1,
        max_catalog_sync_attempts=1,
        publisher=RecordingPublisher(errors=publication_errors),
        catalog_sync_client=RecordingCatalogSync(errors=catalog_errors),
    )
    assert worker.process_one() is True
    if stage == "catalog":
        assert worker.process_one() is True
    with store.connection() as conn:
        conn.execute(
            """
            CREATE TRIGGER reject_historical_terminal
            BEFORE UPDATE OF state ON historical_translation_repairs
            WHEN NEW.state = 'permanent_failed'
            BEGIN
              SELECT RAISE(ABORT, 'injected repair failure');
            END
            """
        )

    if stage == "publication":
        with pytest.raises(sqlite3.IntegrityError, match="injected repair failure"):
            worker.process_one()
    else:
        assert worker.process_one() is True

    current = store.get_job(job.id)
    expected = (
        JobStatus.PUBLISHING
        if stage == "publication"
        else JobStatus.ENGLISH_SRT_READY
    )
    assert current.status is expected
    expected_repair = (
        HistoricalRepairState.RUNNING
        if stage == "publication"
        else HistoricalRepairState.SUCCEEDED
    )
    assert store.get_historical_repair(job.id).state is expected_repair
    assert store.historical_lane_state().paused is False


def test_catalog_completion_atomically_marks_historical_success(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-143")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    assert worker.process_one() is True
    assert worker.process_one() is True
    monkeypatch.setattr(
        store,
        "mark_historical_success",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("catalog completion must not need a second commit")
        ),
    )
    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.SUCCEEDED
    assert [event[0] for event in events] == ["translate", "publish", "catalog"]


def test_restart_after_old_quarantine_rejects_partial_and_retries_translation(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-144")
    worker = MacTranslationWorker(
        store,
        PartialFailingMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publish_retry_seconds=0,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.RETRY_WAIT
    assert paths.english_srt_path_mac.exists()
    worker.translator = DiverseMacTranslator()

    assert worker.process_one() is True

    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.RUNNING
    rejected = list((paths.job_dir_mac / "rejected").glob("*.srt"))
    assert len(rejected) == 2
    assert any("interrupted" in path.name for path in rejected)


def test_historical_quarantine_never_overwrites_preexisting_collision(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-145")
    repair = store.get_historical_repair(job.id)
    desired = store.historical_source_quarantine_path(repair)
    desired.parent.mkdir(parents=True, exist_ok=True)
    desired.write_bytes(b"preexisting unrelated quarantine")
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True

    assert desired.read_bytes() == b"preexisting unrelated quarantine"
    old_copies = [
        path
        for path in desired.parent.glob("*.srt")
        if hashlib.sha256(path.read_bytes()).hexdigest() == repair.source_english_sha256
    ]
    assert len(old_copies) == 1
    assert paths.english_srt_path_mac.exists()


def test_historical_collision_copy_is_discovered_on_retry(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-148")
    repair = store.get_historical_repair(job.id)
    canonical = store.historical_source_quarantine_path(repair)
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_bytes(b"unrelated canonical collision")
    worker = MacTranslationWorker(
        store,
        PartialFailingMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publish_retry_seconds=0,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.RETRY_WAIT
    worker.translator = DiverseMacTranslator()

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert canonical.read_bytes() == b"unrelated canonical collision"
    preserved = [
        path
        for path in canonical.parent.glob("*.srt")
        if hashlib.sha256(path.read_bytes()).hexdigest()
        == repair.source_english_sha256
    ]
    assert len(preserved) == 1
    assert "collision" in preserved[0].name


def test_historical_quarantine_resumes_after_link_before_unlink_crash(
    sqlite_path, mac_jobs_root, monkeypatch
):
    import os

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-146")
    repair = store.get_historical_repair(job.id)
    real_unlink = os.unlink
    crashed = False

    def crash_once(path, *args, **kwargs):
        nonlocal crashed
        if path == paths.english_srt_path_mac.name and not crashed:
            crashed = True
            raise OSError("simulated crash after durable link")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr("orchestrator.mac_worker.os.unlink", crash_once)
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publish_retry_seconds=0,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True
    assert crashed is True
    assert paths.english_srt_path_mac.exists()
    assert store.historical_source_quarantine_path(repair).exists()
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.RETRY_WAIT

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert (
        hashlib.sha256(store.historical_source_quarantine_path(repair).read_bytes()).hexdigest()
        == repair.source_english_sha256
    )


def test_translation_side_effect_safely_yields_after_same_worker_reclaim(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root, movie="new-151")

    class ReclaimingTranslator(DiverseMacTranslator):
        def translate_to_english(self, input_srt, output_srt):
            super().translate_to_english(input_srt, output_srt)
            expired = (datetime.now(UTC) - timedelta(seconds=1)).replace(
                microsecond=0
            ).isoformat()
            store.force_lease_expiry_for_test(job.id, expired)
            assert store.recover_expired_translation_leases(3) == 1
            self.reclaimed = store.claim_translation_job(
                job.id, "mac-translation-1", 60
            )

    translator = ReclaimingTranslator()
    worker = MacTranslationWorker(
        store,
        translator,
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
    )

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.TRANSLATING
    assert refreshed.stage_lease_token == translator.reclaimed.stage_lease_token
    assert refreshed.translation_attempt_count == 1
    assert worker.consecutive_quality_failures == 0


def test_publication_side_effect_safely_yields_after_same_worker_reclaim(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root, movie="new-152")
    setup = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert setup.process_one() is True

    class ReclaimingPublisher(RecordingPublisher):
        def publish_english_ai(self, movie, path, metadata_path):
            result = super().publish_english_ai(movie, path, metadata_path)
            expired = (datetime.now(UTC) - timedelta(seconds=1)).replace(
                microsecond=0
            ).isoformat()
            store.force_lease_expiry_for_test(job.id, expired)
            assert store.recover_expired_publication_leases(3, 0) == 1
            self.reclaimed = store.claim_publication_job(
                "mac-translation-1", 60, job_id=job.id
            )
            return result

    publisher = ReclaimingPublisher()
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=publisher,
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.PUBLISHING
    assert refreshed.stage_lease_token == publisher.reclaimed.stage_lease_token
    assert refreshed.publish_attempt_count == 1
    assert refreshed.catalog_sync_attempt_count == 0
    assert worker.consecutive_quality_failures == 0


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
    catalog = RecordingCatalogSync(events)
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=catalog,
    )

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert [event[0] for event in events] == ["translate"]

    assert worker.process_one() is True
    assert [event[0] for event in events] == ["translate", "publish"]
    published = store.get_job(job.id)
    assert published.status is JobStatus.ENGLISH_SRT_READY
    assert published.published_subtitle_id == "00000000-0000-0000-0000-000000000002"
    assert published.published_storage_path == "ktb/ktb-096/ktb-096-English_AI.srt"

    assert worker.process_one() is True
    assert [event[0] for event in events] == ["translate", "publish", "catalog"]
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert catalog.receipts == [
        {
            "expected_subtitle_id": published.published_subtitle_id,
            "expected_content_sha256": published.published_content_sha256,
        }
    ]
    log = mac_jobs_root / "ktb-096" / "logs" / "mac-translation.log"
    assert "publish_verified" in log.read_text(encoding="utf-8")
    assert "Distinct English" not in log.read_text(encoding="utf-8")


@pytest.mark.parametrize("catalog_failure", [False, True])
def test_ready_webhook_fires_after_publish_before_catalog_and_never_downgrades(
    sqlite_path,
    mac_jobs_root,
    catalog_failure,
):
    from orchestrator.catalog_sync import CatalogSyncError

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-201")
    events = []
    notifier = RecordingCallbackNotifier(events)
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(
            events,
            errors=(
                [
                    CatalogSyncError(
                        "catalog_sync_failed",
                        http_status=500,
                        response_json='{"success":false}',
                    )
                ]
                if catalog_failure
                else None
            ),
        ),
        callback_notifier=notifier,
        max_catalog_sync_attempts=1,
    )

    assert worker.process_one() is True
    assert worker.process_one() is True
    published = store.get_job(job.id)
    assert published.status is JobStatus.ENGLISH_SRT_READY
    assert len(notifier.calls) == 1
    assert worker.process_one() is True

    assert [event[0] for event in events] == [
        "translate",
        "publish",
        "webhook",
        "catalog",
    ]
    current = store.get_job(job.id)
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert current.error is None
    assert len(notifier.calls) == 1
    assert current.catalog_sync_status == (
        "failed" if catalog_failure else "succeeded"
    )
    catalog_log = (
        mac_jobs_root / "abc-201" / "logs" / "mac-translation.log"
    ).read_text(encoding="utf-8")
    if catalog_failure:
        assert 'http_status=500 response={"success":false}' in catalog_log
    else:
        assert 'http_status=200 response={"success":true}' in catalog_log


def test_ready_webhook_is_not_sent_when_supabase_publication_fails(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-202")
    events = []
    notifier = RecordingCallbackNotifier(events)
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(
            events,
            errors=[RuntimeError("supabase unavailable")],
        ),
        catalog_sync_client=RecordingCatalogSync(events),
        callback_notifier=notifier,
        max_publish_attempts=1,
    )

    assert worker.process_one() is True
    assert worker.process_one() is True

    assert [event[0] for event in events] == ["translate", "publish"]
    assert notifier.calls == []
    assert store.get_job(job.id).status is JobStatus.FAILED


def test_missing_catalog_client_cannot_block_verified_supabase_readiness(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-203")
    events = []
    notifier = RecordingCallbackNotifier(events)
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=None,
        callback_notifier=notifier,
    )

    assert worker.process_one() is True
    assert worker.process_one() is True

    current = store.get_job(job.id)
    assert [event[0] for event in events] == ["translate", "publish", "webhook"]
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert current.artifact_status == "ready"
    assert current.catalog_sync_status == "failed"
    assert current.catalog_sync_warning_code == "catalog_sync_failed"
    assert current.error is None
    assert len(notifier.calls) == 1


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
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.ENGLISH_SRT_READY
    assert refreshed.catalog_movie_uuid == "00000000-0000-0000-0000-000000000001"
    assert refreshed.metadata_status == "placeholder"
    assert refreshed.metadata_source == "placeholder"


@pytest.mark.parametrize(
    "reason_code", ["catalog_fetch_failed", "public_visibility_mismatch"]
)
def test_catalog_failure_retries_without_retranslation_or_reupload(
    sqlite_path, mac_jobs_root, reason_code
):
    from orchestrator.catalog_sync import CatalogSyncError

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
        catalog_sync_client=RecordingCatalogSync(
            events,
            errors=[CatalogSyncError(reason_code), None],
        ),
        catalog_sync_retry_seconds=0,
    )

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert worker.process_one() is True

    failed = store.get_job(job.id)
    assert failed.status is JobStatus.ENGLISH_SRT_READY
    assert failed.catalog_sync_attempt_count == 1
    assert failed.next_catalog_sync_attempt_at is not None
    assert failed.error is None
    assert failed.catalog_sync_warning_code == reason_code
    assert [event[0] for event in events].count("translate") == 1
    assert [event[0] for event in events].count("publish") == 1

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert [event[0] for event in events] == [
        "translate",
        "publish",
        "catalog",
        "catalog",
    ]


def test_due_catalog_sync_has_priority_over_unrelated_translation(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-021")
    events = []
    worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    assert worker.process_one() is True
    assert worker.process_one() is True
    second = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-022")

    assert worker.process_one() is True

    assert store.get_job(first.id).status is JobStatus.ENGLISH_SRT_READY
    assert store.get_job(second.id).status is JobStatus.TRANSCRIPTION_DONE
    assert [event[0] for event in events] == ["translate", "publish", "catalog"]


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
        catalog_sync_client=RecordingCatalogSync(events),
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
        catalog_sync_client=RecordingCatalogSync(events),
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


def test_historical_publisher_quality_quarantine_is_deterministic_no_clobber(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, paths = enqueue_historical_worker_job(store, mac_jobs_root, "old-147")
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(
            errors=[SubtitleQualityGateError(["subtitle_changed_after_validation"])]
        ),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert worker.process_one() is True
    repair = store.get_historical_repair(job.id)
    candidate_hash = hashlib.sha256(paths.english_srt_path_mac.read_bytes()).hexdigest()
    desired = (
        paths.job_dir_mac
        / "rejected"
        / (
            f"{paths.english_srt_path_mac.stem}.rejected-quality-publisher-"
            f"{repair.id}-{candidate_hash[:12]}.srt"
        )
    )
    desired.parent.mkdir(exist_ok=True)
    desired.write_bytes(b"unrelated existing quarantine")

    assert worker.process_one() is True

    assert desired.read_bytes() == b"unrelated existing quarantine"
    candidate_copies = [
        path
        for path in desired.parent.glob("*.srt")
        if hashlib.sha256(path.read_bytes()).hexdigest() == candidate_hash
    ]
    assert len(candidate_copies) == 1
    assert "collision" in candidate_copies[0].name


def test_historical_publisher_quality_failures_accumulate_across_restarts(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    for movie in ("old-161", "old-162", "old-163"):
        enqueue_historical_worker_job(store, mac_jobs_root, movie)

    for index in range(1, 4):
        worker = MacTranslationWorker(
            store,
            DiverseMacTranslator(),
            max_translation_attempts=3,
            worker_id=f"mac-restart-{index}",
            lease_seconds=60,
            quality_failure_limit=3,
            publisher=RecordingPublisher(
                errors=[
                    SubtitleQualityGateError(
                        ["subtitle_changed_after_validation"]
                    )
                ]
            ),
            catalog_sync_client=RecordingCatalogSync(),
        )
        assert worker.process_one() is True
        assert worker.process_one() is True

    lane = store.historical_lane_state()
    assert lane.consecutive_quality_failures == 3
    assert lane.paused is True
    assert lane.reason_code == "quality_failure_limit"


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
        catalog_sync_client=RecordingCatalogSync(events),
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
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True
    english = mac_jobs_root / "ktb-096" / "ktb-096.English.srt"
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.FAILED
    assert refreshed.error == "publishing: publication_failed"
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
        catalog_sync_client=RecordingCatalogSync(events),
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
        catalog_sync_client=RecordingCatalogSync(events),
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
        catalog_sync_client=RecordingCatalogSync(),
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
        catalog_sync_client=RecordingCatalogSync(),
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
        catalog_sync_client=RecordingCatalogSync(events),
    )

    assert worker.process_job_id(third.id) is True

    assert store.get_job(second.id).status is JobStatus.PUBLISH_PENDING
    assert store.get_job(third.id).status is JobStatus.ENGLISH_SRT_READY
    assert [event[0] for event in events] == ["publish", "catalog"]


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
        catalog_sync_client=RecordingCatalogSync(),
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
        catalog_sync_client=RecordingCatalogSync(events),
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
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.PUBLISH_PENDING
    assert refreshed.error == "publishing: publication_failed"


def test_publication_exception_does_not_persist_token_or_response_body(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    secret = "admin-token-and-adult-subtitle-response-body"
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publish_retry_seconds=0,
        publisher=RecordingPublisher(errors=[RuntimeError(secret)]),
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    log = (mac_jobs_root / "ktb-096" / "logs" / "mac-translation.log").read_text(
        encoding="utf-8"
    )
    assert refreshed.error == "publishing: publication_failed"
    assert secret not in refreshed.error
    assert secret not in log


def test_worker_restart_resumes_only_catalog_stage(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    events = []
    first_worker = MacTranslationWorker(
        store,
        RecordingTranslator(events),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(events),
        catalog_sync_client=RecordingCatalogSync(events),
    )
    assert first_worker.process_one() is True
    assert first_worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY

    class MustNotRun:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected restart call: {name}")

    restarted_worker = MacTranslationWorker(
        store,
        MustNotRun(),
        max_translation_attempts=3,
        worker_id="mac-translation-2",
        lease_seconds=60,
        publisher=MustNotRun(),
        catalog_sync_client=RecordingCatalogSync(events),
    )

    assert restarted_worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY
    assert [event[0] for event in events] == ["translate", "publish", "catalog"]


def test_publication_snapshot_failure_cannot_undo_committed_receipt(
    sqlite_path, mac_jobs_root, monkeypatch
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
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert worker.process_one() is True
    monkeypatch.setattr(
        "orchestrator.mac_worker.write_job_snapshot",
        lambda job: (_ for _ in ()).throw(OSError("snapshot secret body")),
    )

    assert worker.process_one() is True

    committed = store.get_job(job.id)
    assert committed.status is JobStatus.ENGLISH_SRT_READY
    assert committed.publish_attempt_count == 0
    assert committed.error is None
    assert "snapshot secret body" not in (
        mac_jobs_root / "ktb-096" / "logs" / "mac-translation.log"
    ).read_text(encoding="utf-8")
    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY


def test_catalog_snapshot_failure_keeps_ready_and_worker_continues(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-030")
    second = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-031")
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert worker.process_one() is True
    assert worker.process_one() is True
    assert store.get_job(first.id).status is JobStatus.ENGLISH_SRT_READY
    monkeypatch.setattr(
        "orchestrator.mac_worker.write_job_snapshot",
        lambda job: (_ for _ in ()).throw(OSError("snapshot secret body")),
    )

    assert worker.process_one() is True

    ready = store.get_job(first.id)
    assert ready.status is JobStatus.ENGLISH_SRT_READY
    assert ready.catalog_sync_attempt_count == 0
    assert ready.error is None
    assert "snapshot secret body" not in (
        mac_jobs_root / "abc-030" / "logs" / "mac-translation.log"
    ).read_text(encoding="utf-8")
    assert worker.process_one() is True
    assert store.get_job(second.id).status is JobStatus.PUBLISH_PENDING


@pytest.mark.parametrize("catalog_fails", [False, True])
def test_catalog_worker_safely_yields_when_lease_is_recovered_and_reclaimed(
    sqlite_path, mac_jobs_root, catalog_fails
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-035")
    setup = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="setup-worker",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert setup.process_one() is True
    assert setup.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY

    catalog = ReclaimingCatalogSync(store, job.id, fail=catalog_fails)
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="lease-losing-worker",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=catalog,
        catalog_sync_retry_seconds=0,
    )

    assert worker.process_one() is True

    current = store.get_job(job.id)
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert current.claimed_by == "replacement-worker"
    assert current.catalog_lease_token == catalog.reclaimed.catalog_lease_token
    assert current.catalog_sync_attempt_count == 1
    log = (
        mac_jobs_root / "abc-035" / "logs" / "mac-translation.log"
    ).read_text(encoding="utf-8")
    assert "catalog_lease_lost" in log
    assert catalog.reclaimed.catalog_lease_token not in log


def test_historical_catalog_preservation_failure_is_fenced_after_side_effect(
    sqlite_path, mac_jobs_root, monkeypatch
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job, _ = enqueue_historical_worker_job(store, mac_jobs_root, "old-153")
    setup = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="setup-worker",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert setup.process_one() is True
    assert setup.process_one() is True
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY

    catalog = ReclaimingCatalogSync(store, job.id)
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="lease-losing-worker",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=catalog,
        catalog_sync_retry_seconds=0,
    )
    monkeypatch.setattr(
        worker,
        "_require_historical_preservation_locked",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("changed")),
    )

    assert worker.process_one() is True

    current = store.get_job(job.id)
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert current.claimed_by == "replacement-worker"
    assert current.catalog_lease_token == catalog.reclaimed.catalog_lease_token
    assert store.get_historical_repair(job.id).state is HistoricalRepairState.SUCCEEDED


@pytest.mark.parametrize("database_failure_stage", ["complete", "fail"])
def test_catalog_worker_does_not_swallow_unknown_database_errors(
    sqlite_path, mac_jobs_root, monkeypatch, database_failure_stage
):
    from orchestrator.catalog_sync import CatalogSyncError

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root, movie="abc-036")
    setup = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="setup-worker",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )
    assert setup.process_one() is True
    assert setup.process_one() is True
    catalog = RecordingCatalogSync(
        errors=(
            [CatalogSyncError("catalog_fetch_failed")]
            if database_failure_stage == "fail"
            else None
        )
    )
    method_name = (
        "complete_catalog_sync"
        if database_failure_stage == "complete"
        else "fail_catalog_sync"
    )
    monkeypatch.setattr(
        store,
        method_name,
        lambda *args, **kwargs: (_ for _ in ()).throw(
            sqlite3.OperationalError("database unavailable")
        ),
    )
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="database-error-worker",
        lease_seconds=60,
        publisher=RecordingPublisher(),
        catalog_sync_client=catalog,
    )

    with pytest.raises(sqlite3.OperationalError, match="database unavailable"):
        worker.process_one()

    current = store.get_job(job.id)
    assert current.status is JobStatus.ENGLISH_SRT_READY
    assert current.claimed_by == "database-error-worker"


def test_changed_pending_subtitle_is_permanently_rejected_before_publisher(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = prepare_transcription_done_job(store, mac_jobs_root)
    audio = mac_jobs_root / "ktb-096" / "audio.wav"
    audio.write_bytes(b"keep-audio")
    downstream_events = []
    publisher = RecordingPublisher(downstream_events)
    worker = MacTranslationWorker(
        store,
        DiverseMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        publisher=publisher,
        catalog_sync_client=RecordingCatalogSync(downstream_events),
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
    assert downstream_events == []
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
        catalog_sync_client=RecordingCatalogSync(),
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
        catalog_sync_client=RecordingCatalogSync(),
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
        catalog_sync_client=RecordingCatalogSync(),
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
        catalog_sync_client=RecordingCatalogSync(),
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
