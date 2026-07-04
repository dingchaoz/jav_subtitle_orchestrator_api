from pathlib import Path
from threading import Event

from orchestrator.windows_worker import WindowsWorker


class FakeClient:
    def __init__(self, job):
        self.job = job
        self.heartbeats = []
        self.completed = []
        self.failed = []

    def next_job(self):
        return self.job

    def heartbeat(self, job_id, stage):
        self.heartbeats.append((job_id, stage))

    def complete(self, job_id, japanese_srt_path_windows, english_srt_path_windows):
        self.completed.append((job_id, japanese_srt_path_windows, english_srt_path_windows))

    def failed(self, job_id, stage, error):
        self.failed.append((job_id, stage, error))


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
        ("job_1", "translating"),
    ]
    assert client.completed == [
        (
            "job_1",
            str(job_dir / "ktb-096.Japanese.srt"),
            str(job_dir / "ktb-096.English.srt"),
        )
    ]


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


def test_windows_worker_heartbeats_during_long_translation(tmp_path):
    job = make_job(tmp_path)
    client = BlockingFakeClient(job, "translating")
    worker = WindowsWorker(
        client,
        FakeTranscriber(),
        BlockingTranslator(client),
        heartbeat_interval_seconds=0.001,
    )

    processed = worker.process_one()

    assert processed is True
    assert client.periodic_heartbeat_seen.is_set()
    assert client.heartbeats.count(("job_1", "translating")) >= 2


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
    assert client.completed == [
        (
            "job_1",
            job["japanese_srt_path_windows"],
            job["english_srt_path_windows"],
        )
    ]
    assert client.failed == []
