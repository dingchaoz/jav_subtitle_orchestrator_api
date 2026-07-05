from pathlib import Path

from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.store import JobStore


class RecordingPublisher:
    def __init__(self):
        self.calls = []

    def publish_english_ai(self, movie_code, srt_path):
        self.calls.append((movie_code, Path(srt_path)))


def test_worker_complete_publishes_english_ai(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    result = store.submit_job("ktb-112", priority=100, force=False)
    job = result.job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    assert claimed is not None

    english_srt = mac_jobs_root / "ktb-112" / "ktb-112.English.srt"
    japanese_srt = mac_jobs_root / "ktb-112" / "ktb-112.Japanese.srt"
    english_srt.parent.mkdir(parents=True, exist_ok=True)
    english_srt.write_text("translated\n", encoding="utf-8")
    japanese_srt.write_text("japanese\n", encoding="utf-8")
    publisher = RecordingPublisher()
    app = create_app(store, publisher=publisher)
    client = TestClient(app)

    response = client.post(
        f"/worker/jobs/{job.id}/complete",
        json={
            "worker_id": "windows-gpu-1",
            "japanese_srt_path_windows": "M:\\ktb-112\\ktb-112.Japanese.srt",
            "english_srt_path_windows": "M:\\ktb-112\\ktb-112.English.srt",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "english_srt_ready"
    assert publisher.calls == [("ktb-112", english_srt)]
