import pytest

from orchestrator.dashboard import list_job_logs, read_job_log_tail
from orchestrator.store import JobStore


def test_list_job_logs_returns_existing_allowlisted_logs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "mac-download.log").write_text("download ok\n", encoding="utf-8")
    (logs_dir / "translate.log").write_text("translate ok\n", encoding="utf-8")
    (logs_dir / "secret.log").write_text("hidden\n", encoding="utf-8")

    response = list_job_logs(job)

    assert [log.name for log in response.logs] == ["mac-download.log", "translate.log"]
    assert response.logs[0].size_bytes == len("download ok\n")
    assert response.logs[0].available is True


def test_read_job_log_tail_returns_last_lines_and_caps_tail(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "translate.log").write_text(
        "\n".join(f"line {index}" for index in range(1, 1205)) + "\n",
        encoding="utf-8",
    )

    response = read_job_log_tail(job, "translate.log", tail=1200)

    assert response.log_name == "translate.log"
    assert response.tail == 1000
    assert response.lines[0] == "line 205"
    assert response.lines[-1] == "line 1204"


def test_read_job_log_tail_rejects_unknown_or_traversal_log_names(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job

    with pytest.raises(FileNotFoundError):
        read_job_log_tail(job, "secret.log")

    with pytest.raises(FileNotFoundError):
        read_job_log_tail(job, "../translate.log")


def test_read_job_log_tail_rejects_missing_allowlisted_log(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job

    with pytest.raises(FileNotFoundError):
        read_job_log_tail(job, "whisper.log")


def test_allowlisted_log_symlink_outside_logs_is_not_listed_or_tailed(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    logs_dir.mkdir(parents=True)
    outside_log = mac_jobs_root / "outside-translate.log"
    outside_log.write_text("outside\n", encoding="utf-8")
    (logs_dir / "translate.log").symlink_to(outside_log)

    response = list_job_logs(job)

    assert [log.name for log in response.logs] == []
    with pytest.raises(FileNotFoundError):
        read_job_log_tail(job, "translate.log")


def test_allowlisted_log_directory_is_not_listed_or_tailed(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    (logs_dir / "whisper.log").mkdir(parents=True)

    response = list_job_logs(job)

    assert [log.name for log in response.logs] == []
    with pytest.raises(FileNotFoundError):
        read_job_log_tail(job, "whisper.log")


@pytest.mark.parametrize("tail", [0, -10])
def test_read_job_log_tail_clamps_zero_and_negative_tail_to_one(
    sqlite_path,
    mac_jobs_root,
    tail,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "translate.log").write_text("first\nsecond\n", encoding="utf-8")

    response = read_job_log_tail(job, "translate.log", tail=tail)

    assert response.tail == 1
    assert response.lines == ["second"]
