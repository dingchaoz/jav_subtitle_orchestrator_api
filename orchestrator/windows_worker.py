import time
from pathlib import Path

import requests


class MacApiClient:
    def __init__(self, base_url: str, worker_id: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id

    def next_job(self):
        response = requests.get(
            f"{self.base_url}/worker/next-job",
            params={"worker_id": self.worker_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["job"]

    def heartbeat(self, job_id: str, stage: str) -> None:
        response = requests.post(
            f"{self.base_url}/worker/jobs/{job_id}/heartbeat",
            json={"worker_id": self.worker_id, "stage": stage},
            timeout=30,
        )
        response.raise_for_status()

    def complete(
        self,
        job_id: str,
        japanese_srt_path_windows: str,
        english_srt_path_windows: str,
    ) -> None:
        response = requests.post(
            f"{self.base_url}/worker/jobs/{job_id}/complete",
            json={
                "worker_id": self.worker_id,
                "japanese_srt_path_windows": japanese_srt_path_windows,
                "english_srt_path_windows": english_srt_path_windows,
            },
            timeout=30,
        )
        response.raise_for_status()

    def failed(self, job_id: str, stage: str, error: str) -> None:
        response = requests.post(
            f"{self.base_url}/worker/jobs/{job_id}/failed",
            json={"worker_id": self.worker_id, "stage": stage, "error": error},
            timeout=30,
        )
        response.raise_for_status()


class WindowsWorker:
    def __init__(self, client, transcriber, translator) -> None:
        self.client = client
        self.transcriber = transcriber
        self.translator = translator

    def process_one(self) -> bool:
        job = self.client.next_job()
        if job is None:
            return False

        job_id = job["id"]
        stage = "transcribing"
        try:
            audio_path = Path(job["audio_path_windows"])
            japanese_srt = Path(job["japanese_srt_path_windows"])
            english_srt = Path(job["english_srt_path_windows"])

            self.client.heartbeat(job_id, stage)
            self.transcriber.transcribe_to_srt(audio_path, japanese_srt)

            stage = "transcription_done"
            self.client.heartbeat(job_id, stage)

            stage = "translating"
            self.client.heartbeat(job_id, stage)
            self.translator.translate_to_english(japanese_srt, english_srt)

            self.client.complete(job_id, str(japanese_srt), str(english_srt))
            return True
        except Exception as exc:
            self.client.failed(job_id, stage, str(exc))
            return True


def run_forever(worker: WindowsWorker, poll_interval_seconds: int) -> None:
    while True:
        worker.process_one()
        time.sleep(poll_interval_seconds)
