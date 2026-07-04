from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def test_submit_job_creates_sqlite_row(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    result = store.submit_job("KTB-096", priority=100, force=False)

    assert result.kind == "created"
    assert result.job.movie_number == "KTB-096"
    assert result.job.normalized_movie_number == "ktb-096"
    assert result.job.status == JobStatus.QUEUED
    assert result.job.priority == 100
    assert result.job.job_dir_mac == str(mac_jobs_root / "ktb-096")
    assert result.job.job_dir_windows == "M:\\ktb-096"


def test_duplicate_submit_returns_existing(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    created = store.submit_job("ktb-096", priority=100, force=False)

    existing = store.submit_job("KTB096", priority=10, force=False)

    assert existing.kind == "existing"
    assert existing.job.id == created.job.id
    assert existing.job.priority == 100


def test_submit_invalid_movie_number(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()

    result = store.submit_job("bad id", priority=100, force=False)

    assert result.kind == "invalid"
    assert result.job is None
    assert result.movie_number == "bad id"


def test_batch_submission_groups_created_existing_and_invalid(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-095", priority=100, force=False)

    result = store.submit_batch(["ktb-096", "ktb-095", "bad id"], priority=100, force=False)

    assert [item.job.normalized_movie_number for item in result.created] == ["ktb-096"]
    assert [item.job.normalized_movie_number for item in result.existing] == ["ktb-095"]
    assert [item.movie_number for item in result.invalid] == ["bad id"]
