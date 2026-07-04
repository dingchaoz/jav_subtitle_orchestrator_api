from pathlib import Path

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
