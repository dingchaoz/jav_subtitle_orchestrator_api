import time
from pathlib import Path
from threading import Event, Thread

import requests

from orchestrator.job_logs import append_job_log


def _append_job_log_safely(job_dir: Path, filename: str, message: str) -> None:
    try:
        append_job_log(job_dir, filename, message)
    except Exception:
        return


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
    def __init__(
        self,
        client,
        transcriber,
        translator,
        heartbeat_interval_seconds: float = 60,
    ) -> None:
        self.client = client
        self.transcriber = transcriber
        self.translator = translator
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

    def process_one(self) -> bool:
        job = self.client.next_job()
        if job is None:
            return False

        job_id = job["id"]
        stage = "transcribing"
        job_dir: Path | None = None
        try:
            audio_path = Path(job["audio_path_windows"])
            job_dir = audio_path.parent
            japanese_srt = Path(job["japanese_srt_path_windows"])
            english_srt = Path(job["english_srt_path_windows"])

            _append_job_log_safely(job_dir, "windows-worker.log", f"claimed {job_id}")
            self.client.heartbeat(job_id, stage)
            _append_job_log_safely(job_dir, "whisper.log", f"transcribing {audio_path}")
            self._run_with_periodic_heartbeat(
                job_id,
                stage,
                self.transcriber.transcribe_to_srt,
                audio_path,
                japanese_srt,
            )

            stage = "transcription_done"
            self.client.heartbeat(job_id, stage)

            stage = "translating"
            self.client.heartbeat(job_id, stage)
            _append_job_log_safely(
                job_dir,
                "translate.log",
                f"translating {japanese_srt}",
            )
            self._run_with_periodic_heartbeat(
                job_id,
                stage,
                self.translator.translate_to_english,
                japanese_srt,
                english_srt,
            )

            self.client.complete(job_id, str(japanese_srt), str(english_srt))
            _append_job_log_safely(job_dir, "windows-worker.log", f"completed {job_id}")
            return True
        except Exception as exc:
            if job_dir is not None:
                _append_job_log_safely(
                    job_dir,
                    "windows-worker.log",
                    f"failed {job_id} {stage}: {exc}",
                )
            self.client.failed(job_id, stage, str(exc))
            return True

    def _run_with_periodic_heartbeat(self, job_id: str, stage: str, operation, *args) -> None:
        stop = Event()
        thread = Thread(
            target=self._heartbeat_until_stopped,
            args=(job_id, stage, stop),
            daemon=True,
        )
        thread.start()
        try:
            operation(*args)
        finally:
            stop.set()
            thread.join()

    def _heartbeat_until_stopped(
        self,
        job_id: str,
        stage: str,
        stop: Event,
    ) -> None:
        while not stop.wait(self.heartbeat_interval_seconds):
            try:
                self.client.heartbeat(job_id, stage)
            except Exception:
                continue


def run_forever(worker: WindowsWorker, poll_interval_seconds: int) -> None:
    while True:
        worker.process_one()
        time.sleep(poll_interval_seconds)
