from pathlib import Path


def append_job_log(job_dir: Path, filename: str, message: str) -> Path:
    logs_dir = job_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / filename
    with log_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(message.rstrip() + "\n")
    return log_path
