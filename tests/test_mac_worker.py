from pathlib import Path

from orchestrator.mac_worker import MacDownloadWorker
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


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
