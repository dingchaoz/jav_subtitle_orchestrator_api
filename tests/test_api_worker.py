from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def test_worker_next_job_returns_null_when_no_work(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    response = client.get("/worker/next-job?worker_id=windows-gpu-1")

    assert response.status_code == 200
    assert response.json() == {"job": None}


def test_worker_next_job_claims_audio_ready_job(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    client = TestClient(create_app(store, worker_lease_seconds=1800))

    response = client.get("/worker/next-job?worker_id=windows-gpu-1")

    assert response.status_code == 200
    body = response.json()["job"]
    assert body["id"] == job.id
    assert body["audio_path_windows"] == "M:\\ktb-096\\audio.wav"
    assert body["japanese_srt_path_windows"] == "M:\\ktb-096\\ktb-096.Japanese.srt"
    assert body["english_srt_path_windows"] == "M:\\ktb-096\\ktb-096.English.srt"


def test_worker_next_job_recovers_expired_lease_before_claiming(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=-1)
    client = TestClient(create_app(store, worker_lease_seconds=1800, max_worker_attempts=3))

    response = client.get("/worker/next-job?worker_id=windows-gpu-2")

    assert response.status_code == 200
    assert response.json()["job"]["id"] == claimed.id
    refreshed = store.get_job(claimed.id)
    assert refreshed.status == JobStatus.TRANSCRIPTION_CLAIMED
    assert refreshed.claimed_by == "windows-gpu-2"
    assert refreshed.worker_attempt_count == 1


def test_worker_heartbeat_and_failure(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    client = TestClient(create_app(store))

    heartbeat = client.post(
        f"/worker/jobs/{claimed.id}/heartbeat",
        json={"worker_id": "windows-gpu-1", "stage": "transcribing"},
    )
    failed = client.post(
        f"/worker/jobs/{claimed.id}/failed",
        json={"worker_id": "windows-gpu-1", "stage": "transcribing", "error": "CUDA out of memory"},
    )

    assert heartbeat.status_code == 200
    assert heartbeat.json()["status"] == "transcribing"
    assert failed.status_code == 200
    assert failed.json()["status"] == "audio_ready"
    assert failed.json()["error"] == "transcribing: CUDA out of memory"


def test_worker_heartbeat_missing_job_returns_404(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store), raise_server_exceptions=False)

    response = client.post(
        "/worker/jobs/missing-job/heartbeat",
        json={"worker_id": "windows-gpu-1", "stage": "transcribing"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "job not found"


def test_worker_failed_wrong_worker_returns_409(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    client = TestClient(create_app(store), raise_server_exceptions=False)

    response = client.post(
        f"/worker/jobs/{claimed.id}/failed",
        json={"worker_id": "windows-gpu-2", "stage": "transcribing", "error": "wrong worker"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "job is not claimed by worker"


def test_worker_complete_missing_final_file_returns_409(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    client = TestClient(create_app(store), raise_server_exceptions=False)

    response = client.post(
        f"/worker/jobs/{claimed.id}/complete",
        json={
            "worker_id": "windows-gpu-1",
            "japanese_srt_path_windows": "M:\\ktb-096\\ktb-096.Japanese.srt",
            "english_srt_path_windows": "M:\\ktb-096\\ktb-096.English.srt",
        },
    )

    assert response.status_code == 409
    assert "final file not found" in response.json()["detail"]


def test_worker_complete_success_with_existing_final_file(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    client = TestClient(create_app(store, final_file_exists=lambda path: True))

    response = client.post(
        f"/worker/jobs/{claimed.id}/complete",
        json={
            "worker_id": "windows-gpu-1",
            "japanese_srt_path_windows": "M:\\ktb-096\\ktb-096.Japanese.srt",
            "english_srt_path_windows": "M:\\ktb-096\\ktb-096.English.srt",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "english_srt_ready"
