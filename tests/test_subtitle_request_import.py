import subprocess
import sys

from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.store import JobStore
from orchestrator.subtitle_request_importer import (
    RequestedSubtitleImportSelection,
    RequestedSubtitle,
    RequestedSubtitleImportResponse,
    RequestedSubtitleImporter,
)


class FakeRequestedSubtitleImporter:
    def __init__(self, items):
        self.items = items
        self.calls = []

    def fetch_requested_subtitles(self, *, min_count: int, limit: int):
        self.calls.append({"min_count": min_count, "limit": limit})
        return RequestedSubtitleImportSelection(
            requested=self.items,
            imported=self.items,
            skipped_available=[],
        )


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text if text is not None else ("" if payload is None else "json")
        self.ok = 200 <= status_code < 300

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self):
        self.requests = []

    def post(self, url, **kwargs):
        self.requests.append(("POST", url, kwargs))
        return FakeResponse(
            payload={
                "success": True,
                "result": [
                    {
                        "success": True,
                        "results": [
                            {
                                "code": "dandy-434",
                                "movie_id": "movie-dandy",
                                "request_count": 56,
                                "last_requested_at": "2026-07-06T00:00:00Z",
                            },
                            {
                                "code": "abc-002",
                                "movie_id": "movie-abc",
                                "request_count": 3,
                                "last_requested_at": "2026-07-05T00:00:00Z",
                            },
                            {
                                "code": "bad id",
                                "movie_id": None,
                                "request_count": 1,
                                "last_requested_at": None,
                            },
                        ],
                    }
                ],
            }
        )

    def get(self, url, **kwargs):
        self.requests.append(("GET", url, kwargs))
        return FakeResponse(
            payload=[
                {"canonical_code": "dandy-434", "language": "English_AI"},
            ]
        )


def test_import_requested_subtitles_endpoint_queues_valid_requested_movies(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("abc-001", priority=100, force=False)
    importer = FakeRequestedSubtitleImporter(
        [
            RequestedSubtitle(
                code="abc-002",
                movie_id="movie-b",
                request_count=5,
                last_requested_at="2026-07-06T00:00:00Z",
            ),
            RequestedSubtitle(
                code="abc-001",
                movie_id="movie-a",
                request_count=3,
                last_requested_at="2026-07-05T00:00:00Z",
            ),
            RequestedSubtitle(
                code="bad id",
                movie_id=None,
                request_count=1,
                last_requested_at=None,
            ),
        ]
    )
    client = TestClient(create_app(store, requested_subtitle_importer=importer))

    response = client.post("/jobs/import-subtitle-requests", json={"priority": 25})

    assert response.status_code == 200
    body = response.json()
    assert importer.calls == [{"min_count": 1, "limit": 500}]
    assert [item["movie_number"] for item in body["created"]] == ["abc-002"]
    assert [item["movie_number"] for item in body["existing"]] == ["abc-001"]
    assert body["invalid"] == ["bad id"]
    assert body["requested"] == [
        {
            "code": "abc-002",
            "movie_id": "movie-b",
            "request_count": 5,
            "last_requested_at": "2026-07-06T00:00:00Z",
        },
        {
            "code": "abc-001",
            "movie_id": "movie-a",
            "request_count": 3,
            "last_requested_at": "2026-07-05T00:00:00Z",
        },
        {
            "code": "bad id",
            "movie_id": None,
            "request_count": 1,
            "last_requested_at": None,
        },
    ]
    assert [item["code"] for item in body["imported"]] == ["abc-002", "abc-001", "bad id"]
    assert body["skipped_available"] == []


def test_import_requested_subtitles_endpoint_requires_configured_importer(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    response = client.post("/jobs/import-subtitle-requests")

    assert response.status_code == 503
    assert response.json()["detail"] == "requested subtitle importer is not configured"


def test_dashboard_page_includes_requested_subtitle_import_controls(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    response = client.get("/dashboard")

    assert response.status_code == 200
    html = response.text
    assert 'id="import-requested-form"' in html
    assert 'id="import-requested-message"' in html
    assert 'id="import-requested-limit" name="limit" type="number" value="500"' in html
    assert "/jobs/import-subtitle-requests" in html
    assert "Import requested subtitles" in html
    assert "skipped available" in html


def test_build_requested_subtitle_importer_requires_cloudflare_and_supabase_config():
    from orchestrator.__main__ import build_requested_subtitle_importer

    try:
        build_requested_subtitle_importer(
            cloudflare_account_id=None,
            cloudflare_d1_api_token="token",
            cloudflare_d1_database_id="database",
            supabase_url="https://example.supabase.co",
            supabase_service_role_key="service-key",
        )
    except RuntimeError as exc:
        assert "CLOUDFLARE_ACCOUNT_ID" in str(exc)
    else:
        raise AssertionError("expected missing Cloudflare account id to fail startup")


def test_module_cli_includes_import_subtitle_requests_command():
    result = subprocess.run(
        [sys.executable, "-m", "orchestrator", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "import-subtitle-requests" in result.stdout


def test_requested_subtitle_import_response_uses_snake_case_wire_fields():
    payload = RequestedSubtitleImportResponse(
        requested=[
            RequestedSubtitle(
                code="abc-001",
                movie_id="movie-a",
                request_count=2,
                last_requested_at="2026-07-06T00:00:00Z",
            )
        ],
        imported=[
            RequestedSubtitle(
                code="abc-001",
                movie_id="movie-a",
                request_count=2,
                last_requested_at="2026-07-06T00:00:00Z",
            )
        ],
        skipped_available=[],
        created=[],
        existing=[],
        invalid=[],
    )

    assert payload.model_dump(mode="json")["requested"][0]["request_count"] == 2
    assert payload.model_dump(mode="json")["imported"] == [
        {
            "code": "abc-001",
            "movie_id": "movie-a",
            "request_count": 2,
            "last_requested_at": "2026-07-06T00:00:00Z",
        }
    ]


def test_requested_subtitle_importer_reads_d1_and_skips_existing_english_ai():
    session = FakeSession()
    importer = RequestedSubtitleImporter(
        cloudflare_account_id="account",
        cloudflare_d1_api_token="token",
        cloudflare_d1_database_id="database",
        supabase_url="https://example.supabase.co",
        supabase_service_role_key="service-key",
        session=session,
    )

    selection = importer.fetch_requested_subtitles(min_count=1, limit=100)

    assert [item.code for item in selection.requested] == ["dandy-434", "abc-002", "bad id"]
    assert [item.code for item in selection.imported] == ["abc-002", "bad id"]
    assert [item.code for item in selection.skipped_available] == ["dandy-434"]
    d1_call = session.requests[0]
    assert d1_call[0] == "POST"
    assert d1_call[1] == (
        "https://api.cloudflare.com/client/v4/accounts/account/d1/database/database/query"
    )
    assert d1_call[2]["headers"]["Authorization"] == "Bearer token"
    assert d1_call[2]["json"]["params"] == [1, 100]
    assert "request_count >= ?" in d1_call[2]["json"]["sql"]
    assert "ORDER BY request_count DESC, last_requested_at DESC" in d1_call[2]["json"]["sql"]
    assert "LIMIT ?" in d1_call[2]["json"]["sql"]
    supabase_call = session.requests[1]
    assert supabase_call[0] == "GET"
    assert supabase_call[1] == "https://example.supabase.co/rest/v1/movie_subtitle_catalog"
    assert supabase_call[2]["headers"]["Authorization"] == "Bearer service-key"
    assert supabase_call[2]["params"]["language"] == "eq.English_AI"


def test_requested_subtitle_importer_reports_cloudflare_http_failure():
    class FailingSession(FakeSession):
        def post(self, url, **kwargs):
            self.requests.append(("POST", url, kwargs))
            return FakeResponse(status_code=403, payload={"success": False}, text="forbidden")

    importer = RequestedSubtitleImporter(
        cloudflare_account_id="account",
        cloudflare_d1_api_token="token",
        cloudflare_d1_database_id="database",
        supabase_url="https://example.supabase.co",
        supabase_service_role_key="service-key",
        session=FailingSession(),
    )

    try:
        importer.fetch_requested_subtitles(min_count=1, limit=100)
    except RuntimeError as exc:
        assert "Cloudflare D1 requested subtitle query failed (403): forbidden" in str(exc)
    else:
        raise AssertionError("expected Cloudflare failure to raise")
