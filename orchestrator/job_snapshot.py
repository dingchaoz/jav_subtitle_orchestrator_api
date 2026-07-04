import json
from dataclasses import asdict
from pathlib import Path

from orchestrator.store import JobRecord


def write_job_snapshot(job: JobRecord) -> Path:
    job_dir = Path(job.job_dir_mac)
    job_dir.mkdir(parents=True, exist_ok=True)
    snapshot = asdict(job)
    snapshot["status"] = job.status.value
    path = job_dir / "job.json"
    path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
