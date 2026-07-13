import hashlib
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
    def __init__(self, events=None, *, errors=None):
        self.events = events if events is not None else []
        self.errors = iter(errors or [])

    def sync(self, movie):
        self.events.append(("catalog", movie))
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
        )


class ReclaimingCatalogSync:
    def __init__(self, store, job_id, *, fail=False):
        self.store = store
        self.job_id = job_id
        self.fail = fail
        self.reclaimed = None

    def sync(self, movie):
        from orchestrator.catalog_sync import CatalogSyncError

        expired = (datetime.now(UTC) - timedelta(minutes=5)).replace(
            microsecond=0
        ).isoformat()
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

    assert store.get_job(normal.id).status is JobStatus.CATALOG_SYNC_PENDING
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


def test_good_historical_repair_uploads_catalogs_and_marks_success(
    sqlite_path, mac_jobs_root
):
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


def test_three_historical_quality_failures_pause_history_but_normal_still_runs(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    for movie in ("old-111", "old-112", "old-113"):
        enqueue_historical_worker_job(store, mac_jobs_root, movie)
    worker = MacTranslationWorker(
        store,
        CollapsedMacTranslator(),
        max_translation_attempts=3,
        worker_id="mac-translation-1",
        lease_seconds=60,
        quality_failure_limit=3,
        publisher=RecordingPublisher(),
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert [worker.process_one() for _ in range(3)] == [True, True, True]
    assert worker.historical_quality_failures == 3
    assert store.historical_lane_state().reason_code == "quality_failure_limit"
    normal = prepare_transcription_done_job(store, mac_jobs_root, movie="new-111")
    worker.translator = DiverseMacTranslator()

    assert worker.process_one() is True
    assert store.get_job(normal.id).status is JobStatus.PUBLISH_PENDING


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


def test_historical_publication_exhaustion_is_terminal_without_retranslation(
    sqlite_path, mac_jobs_root
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
    assert repair.reason_code == "publication_attempts_exhausted"
    assert [event[0] for event in events] == ["translate", "publish"]


def test_historical_catalog_exhaustion_is_terminal_without_reupload(
    sqlite_path, mac_jobs_root
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

    assert store.get_job(job.id).status is JobStatus.FAILED
    repair = store.get_historical_repair(job.id)
    assert repair.state is HistoricalRepairState.PERMANENT_FAILED
    assert repair.reason_code == "catalog_fetch_failed"
    assert [event[0] for event in events] == ["translate", "publish", "catalog"]


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
    assert published.status is JobStatus.CATALOG_SYNC_PENDING
    assert published.published_subtitle_id == "00000000-0000-0000-0000-000000000002"
    assert published.published_storage_path == "ktb/ktb-096/ktb-096-English_AI.srt"

    assert worker.process_one() is True
    assert [event[0] for event in events] == ["translate", "publish", "catalog"]
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
        catalog_sync_client=RecordingCatalogSync(),
    )

    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.PUBLISH_PENDING
    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.CATALOG_SYNC_PENDING
    assert worker.process_one() is True

    refreshed = store.get_job(job.id)
    assert refreshed.status is JobStatus.ENGLISH_SRT_READY
    assert refreshed.catalog_movie_uuid == "00000000-0000-0000-0000-000000000001"
    assert refreshed.metadata_status == "placeholder"
    assert refreshed.metadata_source == "placeholder"


def test_catalog_failure_retries_without_retranslation_or_reupload(
    sqlite_path, mac_jobs_root
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
            errors=[CatalogSyncError("catalog_fetch_failed"), None],
        ),
        catalog_sync_retry_seconds=0,
    )

    assert worker.process_one() is True
    assert worker.process_one() is True
    assert store.get_job(job.id).status is JobStatus.CATALOG_SYNC_PENDING
    assert worker.process_one() is True

    failed = store.get_job(job.id)
    assert failed.status is JobStatus.CATALOG_SYNC_PENDING
    assert failed.catalog_sync_attempt_count == 1
    assert failed.next_catalog_sync_attempt_at is not None
    assert failed.error == "catalog_sync: catalog_fetch_failed"
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
    assert refreshed.status is JobStatus.CATALOG_SYNC_PENDING

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

    assert store.get_job(pending.id).status is JobStatus.CATALOG_SYNC_PENDING
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
    assert store.get_job(job.id).status is JobStatus.CATALOG_SYNC_PENDING

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
    assert committed.status is JobStatus.CATALOG_SYNC_PENDING
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
    assert store.get_job(first.id).status is JobStatus.CATALOG_SYNC_PENDING
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
    assert store.get_job(job.id).status is JobStatus.CATALOG_SYNC_PENDING

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
    assert current.status is JobStatus.CATALOG_SYNCING
    assert current.claimed_by == "replacement-worker"
    assert current.catalog_lease_token == catalog.reclaimed.catalog_lease_token
    assert current.catalog_sync_attempt_count == 1
    log = (
        mac_jobs_root / "abc-035" / "logs" / "mac-translation.log"
    ).read_text(encoding="utf-8")
    assert "catalog_lease_lost" in log
    assert catalog.reclaimed.catalog_lease_token not in log


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
    assert current.status is JobStatus.CATALOG_SYNCING
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
