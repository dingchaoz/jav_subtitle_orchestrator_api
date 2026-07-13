import json

from orchestrator.job_snapshot import write_job_snapshot
from orchestrator.store import JobStore


def test_write_job_snapshot_creates_human_readable_job_json(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-096", priority=100, force=False).job

    snapshot_path = write_job_snapshot(job)

    assert snapshot_path == mac_jobs_root / "ktb-096" / "job.json"
    data = json.loads(snapshot_path.read_text())
    assert data["id"] == job.id
    assert data["movie_number"] == "ktb-096"
    assert data["status"] == "queued"
    assert data["job_dir_windows"] == "M:\\ktb-096"
    assert "stage_lease_token" not in data
    assert "catalog_lease_token" not in data
