from pathlib import Path

import pytest


@pytest.fixture
def mac_jobs_root(tmp_path: Path) -> Path:
    return tmp_path / "MissAVJobs"


@pytest.fixture
def sqlite_path(tmp_path: Path) -> Path:
    return tmp_path / "jobs.sqlite3"
