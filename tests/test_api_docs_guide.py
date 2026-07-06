from pathlib import Path

from orchestrator.api import create_app
from orchestrator.store import JobStore


def test_api_client_guide_documents_current_openapi_routes(sqlite_path, mac_jobs_root):
    docs = Path("docs/api-client-guide.md").read_text(encoding="utf-8")
    app = create_app(JobStore(sqlite_path, mac_jobs_root, "M:\\"))
    schema = app.openapi()

    documented_routes = {
        "GET /dashboard",
        "GET /dashboard/state",
        "POST /jobs",
        "POST /jobs/batch",
        "POST /jobs/import-subtitle-requests",
        "GET /jobs",
        "GET /jobs/{job_id}",
        "GET /jobs/{job_id}/detail",
        "GET /jobs/{job_id}/logs",
        "GET /jobs/{job_id}/logs/{log_name}",
        "GET /worker/next-job",
        "POST /worker/jobs/{job_id}/heartbeat",
        "POST /worker/jobs/{job_id}/complete",
        "POST /worker/jobs/{job_id}/failed",
    }

    actual_routes = {
        f"{method.upper()} {path}"
        for path, methods in schema["paths"].items()
        for method in methods
    }

    assert actual_routes == documented_routes
    for route in documented_routes:
        assert route in docs
    assert "CF-Access-Client-Id" in docs
    assert "CF-Access-Client-Secret" in docs
    assert 'status == "english_srt_ready"' in docs
    assert "CALLBACK_CLIENTS_JSON" in docs
