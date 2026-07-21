import json
import logging
import time
from pathlib import Path
from threading import Event, Thread

import requests

from orchestrator.job_logs import append_job_log


LOGGER = logging.getLogger(__name__)


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

    def transcription_complete(
        self,
        job_id: str,
        japanese_srt_path_windows: str,
    ) -> None:
        response = requests.post(
            f"{self.base_url}/worker/jobs/{job_id}/transcription-complete",
            json={
                "worker_id": self.worker_id,
                "japanese_srt_path_windows": japanese_srt_path_windows,
            },
            timeout=30,
        )
        response.raise_for_status()

    def failed(self, job_id: str, stage: str, error: str, permanent: bool = False) -> None:
        response = requests.post(
            f"{self.base_url}/worker/jobs/{job_id}/failed",
            json={
                "worker_id": self.worker_id,
                "stage": stage,
                "error": error,
                "permanent": permanent,
            },
            timeout=30,
        )
        response.raise_for_status()


class WindowsWorker:
    def __init__(
        self,
        client,
        transcriber,
        translator=None,
        heartbeat_interval_seconds: float = 60,
    ) -> None:
        self.client = client
        self.transcriber = transcriber
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

            _append_job_log_safely(job_dir, "windows-worker.log", f"claimed {job_id}")
            if japanese_srt.exists():
                _append_job_log_safely(
                    job_dir,
                    "whisper.log",
                    f"skipping existing transcript {japanese_srt}",
                )
            else:
                self.client.heartbeat(job_id, stage)
                _append_job_log_safely(job_dir, "whisper.log", f"transcribing {audio_path}")
                transcription_report = self._run_with_periodic_heartbeat(
                    job_id,
                    stage,
                    self.transcriber.transcribe_to_srt,
                    audio_path,
                    japanese_srt,
                )
                if hasattr(transcription_report, "as_dict"):
                    _append_job_log_safely(
                        job_dir,
                        "whisper.log",
                        "transcription_stats "
                        + json.dumps(
                            transcription_report.as_dict(),
                            ensure_ascii=True,
                            sort_keys=True,
                        ),
                    )

            stage = "transcription_done"
            self.client.heartbeat(job_id, stage)
            self.client.transcription_complete(job_id, str(japanese_srt))
            _append_job_log_safely(
                job_dir,
                "windows-worker.log",
                f"transcription_completed {job_id}",
            )
            return True
        except Exception as exc:
            if job_dir is not None:
                _append_job_log_safely(
                    job_dir,
                    "windows-worker.log",
                    f"failed {job_id} {stage}: {exc}",
                )
            self.client.failed(job_id, stage, str(exc), permanent=False)
            return True

    def _run_with_periodic_heartbeat(self, job_id: str, stage: str, operation, *args):
        stop = Event()
        thread = Thread(
            target=self._heartbeat_until_stopped,
            args=(job_id, stage, stop),
            daemon=True,
        )
        thread.start()
        try:
            return operation(*args)
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
        try:
            worker.process_one()
        except requests.RequestException as exc:
            base_url = getattr(getattr(worker, "client", None), "base_url", "unknown")
            LOGGER.warning("windows worker could not reach Mac API at %s: %s", base_url, exc)
        time.sleep(poll_interval_seconds)
