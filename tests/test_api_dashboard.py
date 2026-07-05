from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def test_dashboard_state_endpoint_returns_counts_latest_jobs_and_errors(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-096", priority=100, force=False)
    failed = store.submit_job("ktb-095", priority=100, force=False).job
    store.record_download_failure(failed.id, JobStatus.FAILED, 3, "download interrupted")
    client = TestClient(create_app(store))

    response = client.get("/dashboard/state")

    assert response.status_code == 200
    body = response.json()
    assert body["api"]["online"] is True
    assert body["counts"]["queued"] == 1
    assert body["counts"]["failed"] == 1
    assert [job["movie_number"] for job in body["active_errors"]] == ["ktb-095"]
    assert {job["movie_number"] for job in body["latest_jobs"]} == {"ktb-096", "ktb-095"}


def test_job_detail_endpoint_returns_full_paths(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=50, force=False).job
    store.mark_audio_ready(job.id)
    client = TestClient(create_app(store))

    response = client.get(f"/jobs/{job.id}/detail")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == job.id
    assert body["movie_number"] == "ktb-112"
    assert body["normalized_movie_number"] == "ktb-112"
    assert body["status"] == "audio_ready"
    assert body["priority"] == 50
    assert body["job_dir_mac"].endswith("/ktb-112")
    assert body["job_dir_windows"] == "M:\\ktb-112"
    assert body["audio_path_windows"] == "M:\\ktb-112\\audio.wav"


def test_log_endpoints_list_and_tail_allowlisted_logs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "translate.log").write_text("one\ntwo\nthree\n", encoding="utf-8")
    client = TestClient(create_app(store))

    list_response = client.get(f"/jobs/{job.id}/logs")
    tail_response = client.get(f"/jobs/{job.id}/logs/translate.log?tail=2")

    assert list_response.status_code == 200
    assert list_response.json()["logs"] == [
        {"name": "translate.log", "size_bytes": len("one\ntwo\nthree\n"), "available": True}
    ]
    assert tail_response.status_code == 200
    assert tail_response.json()["lines"] == ["two", "three"]


def test_log_tail_endpoint_rejects_unknown_and_traversal_names(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    client = TestClient(create_app(store))

    unknown = client.get(f"/jobs/{job.id}/logs/secret.log")
    traversal = client.get(f"/jobs/{job.id}/logs/..%2Ftranslate.log")

    assert unknown.status_code == 404
    assert traversal.status_code in {404, 422}


def test_dashboard_routes_return_404_for_missing_job(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    detail = client.get("/jobs/job_missing/detail")
    logs = client.get("/jobs/job_missing/logs")

    assert detail.status_code == 404
    assert logs.status_code == 404
