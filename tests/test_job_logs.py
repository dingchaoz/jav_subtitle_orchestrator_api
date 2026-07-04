from orchestrator.job_logs import append_job_log


def test_append_job_log_creates_logs_dir_and_appends_lines_exactly(tmp_path):
    job_dir = tmp_path / "ktb-096"

    log_path = append_job_log(job_dir, "mac-download.log", "first line\n")
    second_path = append_job_log(job_dir, "mac-download.log", "second line")

    assert log_path == job_dir / "logs" / "mac-download.log"
    assert second_path == log_path
    assert log_path.read_text(encoding="utf-8") == "first line\nsecond line\n"
