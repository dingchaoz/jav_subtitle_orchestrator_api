from pathlib import Path
from threading import Event

import pytest
import requests

from orchestrator.windows_worker import WindowsWorker, run_forever


class FailedCalls(list):
    def __init__(self):
        super().__init__()
        self.permanent = []

    def __call__(self, job_id, stage, error, permanent=False):
        self.append((job_id, stage, error))
        self.permanent.append(permanent)


class FakeClient:
    def __init__(self, job):
        self.job = job
        self.next_job_calls = 0
        self.heartbeats = []
        self.completed = []
        self.transcriptions_completed = []
        self.failed = FailedCalls()

    def next_job(self):
        self.next_job_calls += 1
        return self.job

    def heartbeat(self, job_id, stage):
        self.heartbeats.append((job_id, stage))

    def complete(self, job_id, japanese_srt_path_windows, english_srt_path_windows):
        self.completed.append((job_id, japanese_srt_path_windows, english_srt_path_windows))

    def transcription_complete(self, job_id, japanese_srt_path_windows):
        self.transcriptions_completed.append((job_id, japanese_srt_path_windows))

class FakeTranscriber:
    def transcribe_to_srt(self, audio_path: Path, output_path: Path) -> None:
        assert audio_path.name == "audio.wav"
        output_path.write_text(
            "1\n00:00:00,000 --> 00:00:01,000\nこんにちは\n\n",
            encoding="utf-8",
        )


class FakeTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        assert input_srt.name == "ktb-096.Japanese.srt"
        output_srt.write_text(
            "1\n00:00:00,000 --> 00:00:01,000\nHello\n\n",
            encoding="utf-8",
        )


class CollapsedTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        source = input_srt.read_text(encoding="utf-8")
        lines = source.splitlines()
        for index in range(2, len(lines), 4):
            lines[index] = "I don't know what to do"
        output_srt.write_text("\n".join(lines) + "\n", encoding="utf-8")


class DiverseStartupTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        lines = input_srt.read_text(encoding="utf-8").splitlines()
        for index in range(2, len(lines), 4):
            lines[index] = f"Distinct safe translation {index}"
        output_srt.write_text("\n".join(lines) + "\n", encoding="utf-8")


class NeverTranslator:
    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        raise AssertionError("Windows must not translate")


class BlockingFakeClient(FakeClient):
    def __init__(self, job, stage: str):
        super().__init__(job)
        self.stage = stage
        self.operation_started = Event()
        self.periodic_heartbeat_seen = Event()

    def heartbeat(self, job_id, stage):
        super().heartbeat(job_id, stage)
        if stage == self.stage and self.operation_started.is_set():
            self.periodic_heartbeat_seen.set()


class BlockingTranscriber(FakeTranscriber):
    def __init__(self, client: BlockingFakeClient):
        self.client = client

    def transcribe_to_srt(self, audio_path: Path, output_path: Path) -> None:
        self.client.operation_started.set()
        assert self.client.periodic_heartbeat_seen.wait(timeout=1)
        super().transcribe_to_srt(audio_path, output_path)


class BlockingTranslator(FakeTranslator):
    def __init__(self, client: BlockingFakeClient):
        self.client = client

    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        self.client.operation_started.set()
        assert self.client.periodic_heartbeat_seen.wait(timeout=1)
        super().translate_to_english(input_srt, output_srt)


class TransientPeriodicHeartbeatFailureClient(FakeClient):
    def __init__(self, job, stage: str):
        super().__init__(job)
        self.stage = stage
        self.operation_started = Event()
        self.first_periodic_failure_seen = Event()
        self.later_periodic_success_seen = Event()
        self.periodic_heartbeat_count = 0

    def heartbeat(self, job_id, stage):
        if stage == self.stage and self.operation_started.is_set():
            self.periodic_heartbeat_count += 1
            if self.periodic_heartbeat_count == 1:
                self.first_periodic_failure_seen.set()
                raise TimeoutError("temporary heartbeat timeout")
            self.later_periodic_success_seen.set()
        super().heartbeat(job_id, stage)


class TransientFailureBlockingTranscriber(FakeTranscriber):
    def __init__(self, client: TransientPeriodicHeartbeatFailureClient):
        self.client = client

    def transcribe_to_srt(self, audio_path: Path, output_path: Path) -> None:
        self.client.operation_started.set()
        assert self.client.first_periodic_failure_seen.wait(timeout=1)
        assert self.client.later_periodic_success_seen.wait(timeout=1)
        super().transcribe_to_srt(audio_path, output_path)


class FailingTranscriber:
    def __init__(self, error: str) -> None:
        self.error = error

    def transcribe_to_srt(self, audio_path: Path, output_path: Path) -> None:
        raise RuntimeError(self.error)


class FlakyApiWorker:
    def __init__(self):
        self.client = type("Client", (), {"base_url": "http://192.168.1.247:8000"})()
        self.calls = 0

    def process_one(self):
        self.calls += 1
        if self.calls == 1:
            raise requests.exceptions.ConnectTimeout("timed out")
        raise KeyboardInterrupt


def make_job(tmp_path):
    job_dir = tmp_path / "ktb-096"
    job_dir.mkdir()
    audio = job_dir / "audio.wav"
    audio.write_bytes(b"RIFFfakeWAVE")
    return {
        "id": "job_1",
        "audio_path_windows": str(audio),
        "japanese_srt_path_windows": str(job_dir / "ktb-096.Japanese.srt"),
        "english_srt_path_windows": str(job_dir / "ktb-096.English.srt"),
    }


def test_windows_worker_processes_one_job(tmp_path):
    job_dir = tmp_path / "ktb-096"
    job_dir.mkdir()
    audio = job_dir / "audio.wav"
    audio.write_bytes(b"RIFFfakeWAVE")
    job = {
        "id": "job_1",
        "audio_path_windows": str(audio),
        "japanese_srt_path_windows": str(job_dir / "ktb-096.Japanese.srt"),
        "english_srt_path_windows": str(job_dir / "ktb-096.English.srt"),
    }
    client = FakeClient(job)
    worker = WindowsWorker(client, FakeTranscriber(), FakeTranslator())

    processed = worker.process_one()

    assert processed is True
    assert client.heartbeats == [
        ("job_1", "transcribing"),
        ("job_1", "transcription_done"),
    ]
    assert client.transcriptions_completed == [
        ("job_1", str(job_dir / "ktb-096.Japanese.srt")),
    ]
    assert client.completed == []


def test_windows_worker_hands_off_after_transcription_without_translation(tmp_path):
    job = make_job(tmp_path)
    client = FakeClient(job)
    worker = WindowsWorker(client, FakeTranscriber(), NeverTranslator())

    assert worker.process_one() is True

    assert client.transcriptions_completed == [
        ("job_1", job["japanese_srt_path_windows"]),
    ]
    assert client.completed == []
    assert not Path(job["english_srt_path_windows"]).exists()


def test_windows_worker_skips_transcription_when_japanese_srt_exists(tmp_path):
    job = make_job(tmp_path)
    japanese_srt = Path(job["japanese_srt_path_windows"])
    japanese_srt.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nexisting transcript\n\n",
        encoding="utf-8",
    )
    client = FakeClient(job)
    worker = WindowsWorker(client, FailingTranscriber("should not transcribe"), FakeTranslator())

    processed = worker.process_one()

    assert processed is True
    assert client.heartbeats == [
        ("job_1", "transcription_done"),
    ]
    assert client.failed == []
    assert client.transcriptions_completed == [
        ("job_1", job["japanese_srt_path_windows"]),
    ]
    assert client.completed == []
    assert "skipping existing transcript" in (
        japanese_srt.parent / "logs" / "whisper.log"
    ).read_text(encoding="utf-8")


def test_windows_worker_writes_worker_and_whisper_logs(tmp_path):
    job = make_job(tmp_path)
    client = FakeClient(job)
    worker = WindowsWorker(client, FakeTranscriber(), FakeTranslator())

    assert worker.process_one() is True

    job_dir = Path(job["audio_path_windows"]).parent
    audio_path = Path(job["audio_path_windows"])
    japanese_srt = Path(job["japanese_srt_path_windows"])

    assert (job_dir / "logs" / "windows-worker.log").read_text(encoding="utf-8") == (
        "claimed job_1\n"
        "transcription_completed job_1\n"
    )
    assert (job_dir / "logs" / "whisper.log").read_text(encoding="utf-8") == (
        f"transcribing {audio_path}\n"
    )
    assert not (job_dir / "logs" / "translate.log").exists()


def test_windows_worker_completed_log_failure_does_not_fail_completed_job(
    tmp_path,
    monkeypatch,
):
    job = make_job(tmp_path)
    client = FakeClient(job)
    worker = WindowsWorker(client, FakeTranscriber(), FakeTranslator())

    def fail_on_completed(job_dir, filename, message):
        if message == "transcription_completed job_1":
            raise OSError("log disk full")

    monkeypatch.setattr("orchestrator.windows_worker.append_job_log", fail_on_completed)

    assert worker.process_one() is True
    assert client.transcriptions_completed == [
        ("job_1", job["japanese_srt_path_windows"]),
    ]
    assert client.completed == []
    assert client.failed == []


def test_windows_worker_writes_failure_log_on_transcription_error(tmp_path):
    job = make_job(tmp_path)
    client = FakeClient(job)
    worker = WindowsWorker(client, FailingTranscriber("whisper crashed"), FakeTranslator())

    assert worker.process_one() is True

    job_dir = Path(job["audio_path_windows"]).parent
    assert (job_dir / "logs" / "windows-worker.log").read_text(encoding="utf-8") == (
        "claimed job_1\n"
        "failed job_1 transcribing: whisper crashed\n"
    )
    assert client.failed == [("job_1", "transcribing", "whisper crashed")]


def test_windows_worker_failure_log_failure_still_reports_original_failure(
    tmp_path,
    monkeypatch,
):
    job = make_job(tmp_path)
    client = FakeClient(job)
    worker = WindowsWorker(client, FailingTranscriber("whisper crashed"), FakeTranslator())

    def fail_on_failed_log(job_dir, filename, message):
        if message.startswith("failed "):
            raise OSError("log disk full")

    monkeypatch.setattr("orchestrator.windows_worker.append_job_log", fail_on_failed_log)

    assert worker.process_one() is True
    assert client.completed == []
    assert client.failed == [("job_1", "transcribing", "whisper crashed")]


def test_windows_worker_returns_false_when_no_job():
    client = FakeClient(None)
    worker = WindowsWorker(client, FakeTranscriber(), FakeTranslator())

    assert worker.process_one() is False


def test_windows_worker_heartbeats_during_long_transcription(tmp_path):
    job = make_job(tmp_path)
    client = BlockingFakeClient(job, "transcribing")
    worker = WindowsWorker(
        client,
        BlockingTranscriber(client),
        FakeTranslator(),
        heartbeat_interval_seconds=0.001,
    )

    processed = worker.process_one()

    assert processed is True
    assert client.periodic_heartbeat_seen.is_set()
    assert client.heartbeats.count(("job_1", "transcribing")) >= 2


def test_windows_worker_never_enters_translation_stage(tmp_path):
    job = make_job(tmp_path)
    client = BlockingFakeClient(job, "translating")
    worker = WindowsWorker(client, FakeTranscriber(), NeverTranslator())

    processed = worker.process_one()

    assert processed is True
    assert not client.periodic_heartbeat_seen.is_set()
    assert ("job_1", "translating") not in client.heartbeats
    assert client.completed == []


def test_windows_worker_continues_after_transient_periodic_heartbeat_failure(tmp_path):
    job = make_job(tmp_path)
    client = TransientPeriodicHeartbeatFailureClient(job, "transcribing")
    worker = WindowsWorker(
        client,
        TransientFailureBlockingTranscriber(client),
        FakeTranslator(),
        heartbeat_interval_seconds=0.001,
    )

    processed = worker.process_one()

    assert processed is True
    assert client.first_periodic_failure_seen.is_set()
    assert client.later_periodic_success_seen.is_set()
    assert client.transcriptions_completed == [
        ("job_1", job["japanese_srt_path_windows"]),
    ]
    assert client.completed == []
    assert client.failed == []


def test_run_forever_retries_after_mac_api_timeout(caplog):
    worker = FlakyApiWorker()

    with caplog.at_level("WARNING"):
        with pytest.raises(KeyboardInterrupt):
            run_forever(worker, poll_interval_seconds=0)

    assert worker.calls == 2
    assert "could not reach Mac API" in caplog.text
    assert "192.168.1.247:8000" in caplog.text
