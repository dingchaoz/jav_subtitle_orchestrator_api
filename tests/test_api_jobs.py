from pathlib import Path

from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.store import JobStore


def test_post_job_creates_job(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    response = client.post("/jobs", json={"movie_number": "ktb-096", "priority": 100, "force": False})

    assert response.status_code == 200
    body = response.json()
    assert body["movie_number"] == "ktb-096"
    assert body["status"] == "queued"
    assert Path(body["job_dir_mac"]).name == "ktb-096"
    assert body["job_dir_windows"] == "M:\\ktb-096"


def test_post_batch_groups_created_existing_invalid(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-095", priority=100, force=False)
    client = TestClient(create_app(store))

    response = client.post(
        "/jobs/batch",
        json={"movie_numbers": ["ktb-096", "ktb-095", "bad id"], "priority": 100, "force": False},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["created"][0]["movie_number"] == "ktb-096"
    assert body["existing"][0]["movie_number"] == "ktb-095"
    assert body["invalid"] == ["bad id"]


def test_get_jobs_filters_by_status(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    first = store.submit_job("ktb-096", priority=100, force=False).job
    store.submit_job("ktb-095", priority=100, force=False)
    store.mark_audio_ready(first.id)
    client = TestClient(create_app(store))

    response = client.get("/jobs?status=audio_ready")

    assert response.status_code == 200
    assert [job["movie_number"] for job in response.json()] == ["ktb-096"]
