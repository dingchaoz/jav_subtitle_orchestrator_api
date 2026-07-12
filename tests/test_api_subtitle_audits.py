from __future__ import annotations

import json
from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.store import JobStore


SUBTITLE_ID = "12345678-1234-5678-1234-567812345678"
SECRET = "never-return-this-service-key"


def _item() -> dict[str, object]:
    return {
        "id": 91,
        "subtitle_id": SUBTITLE_ID,
        "movie_id": "87654321-4321-8765-4321-876543218765",
        "canonical_code": "abc-123",
        "language": "English_AI",
        "file_path": "abc-123/English_AI.srt",
        "audit_version": "subtitle-quality-v1",
        "status": "bad",
        "score": 10,
        "reason_codes": ["KNOWN_BAD_TRANSLATION"],
        "metrics": {"cue_count": 40, "coverage_ratio": 0.22},
        "expected_duration_seconds": 7200.0,
        "duration_source": "sibling_median_last_end",
        "duration_confidence": "high",
        "scanned_at": "2026-07-12T12:00:00Z",
    }


class FakeAuditService:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def summary(self):
        self.calls.append(("summary",))
        return {
            "status_counts": {
                "pass": 4,
                "warning": 1,
                "review": 2,
                "bad": 3,
                "invalid": 1,
                "missing": 2,
            },
            "reason_counts": {"KNOWN_BAD_TRANSLATION": 3},
            "total_audited": 13,
            "catalog_total": 20,
            "progress_ratio": 0.65,
            "latest_scanned_at": "2026-07-12T12:00:00Z",
        }

    def list_findings(self, *, status, language, page, page_size):
        self.calls.append(("list", status, language, page, page_size))
        return {
            "items": [_item()],
            "total": 1,
            "page": page,
            "page_size": page_size,
            "pages": 1,
            "accessible_pages": 1,
        }

    def get_finding(self, subtitle_id):
        self.calls.append(("detail", subtitle_id))
        return _item() if subtitle_id == SUBTITLE_ID else None


@pytest.fixture
def audit_client(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    service = FakeAuditService()
    return TestClient(create_app(store, subtitle_audit_service=service)), service


def test_summary_endpoint_returns_all_statuses_reasons_and_progress(audit_client):
    client, service = audit_client

    response = client.get("/subtitle-audits/summary")

    assert response.status_code == 200
    assert response.json() == {
        "status_counts": {
            "pass": 4,
            "warning": 1,
            "review": 2,
            "bad": 3,
            "invalid": 1,
            "missing": 2,
        },
        "reason_counts": {"KNOWN_BAD_TRANSLATION": 3},
        "total_audited": 13,
        "catalog_total": 20,
        "progress_ratio": 0.65,
        "latest_scanned_at": "2026-07-12T12:00:00Z",
    }
    assert service.calls == [("summary",)]


def test_list_endpoint_forwards_valid_filters_and_returns_typed_page(audit_client):
    client, service = audit_client

    response = client.get(
        "/subtitle-audits?status=bad&language=English_AI&page=2&page_size=50"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["canonical_code"] == "abc-123"
    assert body["items"][0]["metrics"] == {"cue_count": 40, "coverage_ratio": 0.22}
    assert "subtitle_body" not in body["items"][0]
    assert SECRET not in response.text
    assert service.calls == [("list", "bad", "English_AI", 2, 50)]


@pytest.mark.parametrize(
    "query",
    [
        "page=0",
        "page_size=101",
        "page=1000002",
        "page=10002&page_size=100",
        "status=unknown",
        "language=English%0Aapikey%3Devil",
        f"language={'a' * 129}",
    ],
)
def test_list_endpoint_rejects_invalid_pagination_status_and_language(
    audit_client, query
):
    client, service = audit_client

    response = client.get(f"/subtitle-audits?{query}")

    assert response.status_code == 422
    assert service.calls == []


def test_detail_endpoint_validates_uuid_and_returns_404(audit_client):
    client, service = audit_client

    invalid = client.get("/subtitle-audits/not-a-uuid")
    missing = client.get("/subtitle-audits/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    found = client.get(f"/subtitle-audits/{SUBTITLE_ID}")

    assert invalid.status_code == 422
    assert missing.status_code == 404
    assert found.status_code == 200
    assert found.json()["subtitle_id"] == SUBTITLE_ID
    assert service.calls == [
        ("detail", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        ("detail", SUBTITLE_ID),
    ]


def test_unconfigured_audit_api_is_503_without_breaking_dashboard_state(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    summary = client.get("/subtitle-audits/summary")
    listing = client.get("/subtitle-audits")
    state = client.get("/dashboard/state")

    assert summary.status_code == listing.status_code == 503
    assert summary.json() == {"detail": "subtitle audit visibility is unavailable"}
    assert state.status_code == 200
    assert state.json()["api"]["online"] is True


def test_service_errors_are_sanitized_and_do_not_echo_credentials(
    sqlite_path, mac_jobs_root
):
    class BrokenService(FakeAuditService):
        def summary(self):
            raise RuntimeError(f"upstream failed with {SECRET}")

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store, subtitle_audit_service=BrokenService()))

    response = client.get("/subtitle-audits/summary")

    assert response.status_code == 503
    assert response.json() == {"detail": "subtitle audit visibility is unavailable"}
    assert SECRET not in response.text


def test_app_closes_owned_audit_service_on_shutdown(sqlite_path, mac_jobs_root):
    class ClosableService(FakeAuditService):
        def __init__(self):
            super().__init__()
            self.closed = False

        def close(self):
            self.closed = True

    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    service = ClosableService()

    with TestClient(create_app(store, subtitle_audit_service=service)):
        assert service.closed is False

    assert service.closed is True


@dataclass
class FakeResponse:
    status_code: int = 200
    payload: object = None
    headers: dict[str, str] | None = None
    text: str = ""
    raw_body: bytes | None = None
    chunks: list[bytes] | None = None

    def __post_init__(self):
        self.json_calls = 0
        self.closed = False
        self.chunks_yielded = 0

    def json(self):
        self.json_calls += 1
        return self.payload

    def iter_content(self, chunk_size):
        chunks = self.chunks
        if chunks is None:
            body = self.raw_body
            if body is None:
                body = json.dumps(
                    self.payload, ensure_ascii=False, allow_nan=True
                ).encode("utf-8")
            chunks = [
                body[index:index + chunk_size]
                for index in range(0, len(body), chunk_size)
            ]
        for chunk in chunks:
            self.chunks_yielded += 1
            yield chunk

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, *, get=(), post=()):
        self.get_responses = list(get)
        self.post_responses = list(post)
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.closed = False

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self.get_responses.pop(0)

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self.post_responses.pop(0)

    def close(self):
        self.closed = True


def _audit_row() -> dict[str, object]:
    item = _item()
    for key in ("movie_id", "canonical_code", "language", "file_path"):
        item.pop(key)
    item["language"] = "English_AI"
    return item


def _catalog_row() -> dict[str, object]:
    return {
        "id": SUBTITLE_ID,
        "movie_id": "87654321-4321-8765-4321-876543218765",
        "language": "English_AI",
        "file_path": "abc-123/English_AI.srt",
        "movies": {"standard_movie_id": "abc-123"},
    }


def test_supabase_service_summary_uses_private_rpc_and_validates_counts():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    payload = {
        "status_counts": {status: 0 for status in (
            "pass", "warning", "review", "bad", "invalid", "missing"
        )},
        "reason_counts": {"KNOWN_BAD_TRANSLATION": 3},
        "total_audited": 13,
        "catalog_total": 20,
        "latest_scanned_at": "2026-07-12T12:00:00Z",
    }
    payload["status_counts"]["bad"] = 13
    session = FakeSession(post=[FakeResponse(payload=payload)])
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET, session=session, timeout_seconds=7
    )

    result = service.summary()

    assert result.progress_ratio == 0.65
    method, url, kwargs = session.calls[0]
    assert method == "POST"
    assert url.endswith("/rest/v1/rpc/subtitle_quality_latest_summary")
    assert kwargs["json"] == {}
    assert kwargs["allow_redirects"] is False
    assert kwargs["stream"] is True
    assert kwargs["timeout"] == 7
    assert kwargs["headers"]["Authorization"] == f"Bearer {SECRET}"
    assert kwargs["headers"]["Accept-Encoding"] == "identity"


def test_supabase_service_lists_latest_rows_then_batches_catalog_metadata():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    session = FakeSession(
        get=[
            FakeResponse(payload=[_audit_row()], headers={"Content-Range": "50-50/101"}),
            FakeResponse(payload=[_catalog_row()]),
        ]
    )
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET, session=session
    )

    result = service.list_findings(
        status="bad", language="English_AI", page=2, page_size=50
    )

    assert result.total == 101
    assert result.pages == 3
    assert result.items[0].canonical_code == "abc-123"
    latest_call, metadata_call = session.calls
    assert latest_call[1].endswith("/rest/v1/subtitle_quality_latest_catalog")
    assert latest_call[2]["params"]["status"] == "eq.bad"
    assert latest_call[2]["params"]["language"] == "eq.English_AI"
    assert "movie_languages!inner" not in latest_call[2]["params"]["select"]
    assert latest_call[2]["params"]["order"] == "scanned_at.desc,id.desc"
    assert latest_call[2]["headers"]["Range"] == "50-99"
    assert latest_call[2]["headers"]["Prefer"] == "count=exact"
    assert metadata_call[1].endswith("/rest/v1/movie_languages")
    assert metadata_call[2]["params"]["language"] == "eq.English_AI"
    assert metadata_call[2]["params"]["id"].startswith("in.(")
    assert all(call[2]["allow_redirects"] is False for call in session.calls)
    assert all(call[2]["stream"] is True for call in session.calls)


def test_supabase_service_detail_is_one_latest_row_and_one_catalog_batch():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    session = FakeSession(
        get=[FakeResponse(payload=[_audit_row()]), FakeResponse(payload=[_catalog_row()])]
    )
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET, session=session
    )

    result = service.get_finding(SUBTITLE_ID)

    assert result is not None
    assert str(result.subtitle_id) == SUBTITLE_ID
    assert session.calls[0][1].endswith("/rest/v1/subtitle_quality_latest_catalog")
    assert session.calls[0][2]["params"]["subtitle_id"] == f"eq.{SUBTITLE_ID}"
    assert session.calls[0][2]["params"]["limit"] == "1"


@pytest.mark.parametrize(
    "payload",
    [
        {"status_counts": {"bad": True}, "reason_counts": {}, "total_audited": 1,
         "catalog_total": 1, "latest_scanned_at": None},
        {"status_counts": {"bad": 1}, "reason_counts": {"UNKNOWN": 1},
         "total_audited": 1, "catalog_total": 1, "latest_scanned_at": None},
        {"status_counts": {"bad": 2}, "reason_counts": {}, "total_audited": 1,
         "catalog_total": 1, "latest_scanned_at": None},
    ],
)
def test_supabase_service_rejects_malformed_summary_without_secret(payload):
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    service = SubtitleAuditApiService(
        "https://example.supabase.co",
        SECRET,
        session=FakeSession(post=[FakeResponse(payload=payload)]),
    )

    with pytest.raises(ValueError) as error:
        service.summary()

    assert SECRET not in str(error.value)


def test_supabase_service_rejects_redirects_bad_content_range_and_raw_metrics():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    redirect = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(post=[FakeResponse(status_code=302, text=SECRET)]),
    )
    with pytest.raises(RuntimeError) as redirect_error:
        redirect.summary()
    assert SECRET not in str(redirect_error.value)

    bad_range = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(payload=[_audit_row()], headers={"Content-Range": "bad"})
        ]),
    )
    with pytest.raises(ValueError, match="Content-Range"):
        bad_range.list_findings(status=None, language=None, page=1, page_size=50)

    raw = _audit_row()
    raw["metrics"] = {"cue_count": 1, "dominant_normalized_text": "private body"}
    raw_service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(payload=[raw], headers={"Content-Range": "0-0/1"}),
            FakeResponse(payload=[_catalog_row()]),
        ]),
    )
    item = raw_service.list_findings(
        status=None, language=None, page=1, page_size=50
    ).items[0]
    assert item.metrics == {"cue_count": 1}


def test_supabase_service_streams_bounded_json_and_closes_without_response_json():
    from orchestrator.subtitle_audit_api import MAX_AUDIT_RESPONSE_BYTES, SubtitleAuditApiService

    chunk = b"x" * (64 * 1024)
    response = FakeResponse(
        headers={}, chunks=[chunk] * ((MAX_AUDIT_RESPONSE_BYTES // len(chunk)) + 2)
    )
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(post=[response]),
    )

    with pytest.raises(ValueError, match="size limit"):
        service.summary()

    assert response.chunks_yielded == (MAX_AUDIT_RESPONSE_BYTES // len(chunk)) + 1
    assert response.closed is True
    assert response.json_calls == 0


@pytest.mark.parametrize(
    "raw_body",
    [
        b'{"status_counts":{},"status_counts":{}}',
        b'{"value":NaN}',
        b'{"value":Infinity}',
    ],
)
def test_supabase_service_rejects_duplicate_keys_and_nonfinite_json(raw_body):
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    response = FakeResponse(raw_body=raw_body)
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(post=[response]),
    )

    with pytest.raises(ValueError, match="invalid JSON"):
        service.summary()

    assert response.closed is True
    assert response.json_calls == 0


@pytest.mark.parametrize(
    ("declared", "body"),
    [
        ("5", b"{}"),
        ("1", b"{}"),
        ("not-an-int", b"{}"),
        ("-1", b"{}"),
        ("+2", b"{}"),
        (" 2", b"{}"),
    ],
)
def test_supabase_service_rejects_lying_or_invalid_content_length(declared, body):
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    response = FakeResponse(headers={"Content-Length": declared}, raw_body=body)
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(post=[response]),
    )

    with pytest.raises(ValueError, match="Content-Length"):
        service.summary()

    assert response.closed is True
    assert response.json_calls == 0


@pytest.mark.parametrize("content_range", ["1-1/1", "0-3/1", "*/1"])
def test_supabase_service_rejects_content_range_inconsistent_with_page(
    content_range
):
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(payload=[_audit_row()], headers={"Content-Range": content_range})
        ]),
    )

    with pytest.raises(ValueError, match="Content-Range"):
        service.list_findings(status=None, language=None, page=1, page_size=50)


@pytest.mark.parametrize(
    ("page", "content_range", "expected_total"),
    [(1, "*/0", 0), (2, "*/25", 25)],
)
def test_supabase_service_accepts_valid_empty_wildcard_ranges(
    page, content_range, expected_total
):
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(payload=[], headers={"Content-Range": content_range})
        ]),
    )

    result = service.list_findings(
        status=None, language=None, page=page, page_size=50
    )

    assert result.total == expected_total
    assert result.pages == 1


def test_supabase_service_rejects_offset_above_shared_bound_without_request():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    session = FakeSession()
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET, session=session
    )

    with pytest.raises(ValueError, match="offset"):
        service.list_findings(
            status=None, language=None, page=10002, page_size=100
        )

    assert session.calls == []


def test_subtitle_audit_page_model_blocks_only_absurd_page_integer():
    from pydantic import ValidationError

    from orchestrator.models import SubtitleAuditPageResponse

    with pytest.raises(ValidationError):
        SubtitleAuditPageResponse(
            items=[], total=0, page=1_000_002, page_size=1,
            pages=1, accessible_pages=1,
        )


def test_api_allows_current_catalog_last_page_and_forwards_to_service(audit_client):
    client, service = audit_client

    response = client.get("/subtitle-audits?page=12634&page_size=50")

    assert response.status_code == 200
    assert service.calls == [("list", None, None, 12634, 50)]


def test_service_reaches_current_catalog_last_pages_with_exact_counts():
    import uuid

    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    rows = []
    catalog = []
    for index in range(32):
        subtitle_id = str(uuid.UUID(int=index + 1))
        movie_id = str(uuid.UUID(int=index + 100))
        audit = _audit_row()
        audit["id"] = index + 1
        audit["subtitle_id"] = subtitle_id
        rows.append(audit)
        metadata = _catalog_row()
        metadata["id"] = subtitle_id
        metadata["movie_id"] = movie_id
        catalog.append(metadata)

    page_50 = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(
                payload=rows,
                headers={"Content-Range": "631650-631681/631682"},
            ),
            FakeResponse(payload=catalog),
        ]),
    ).list_findings(status=None, language=None, page=12634, page_size=50)

    single_row = rows[-1]
    single_metadata = catalog[-1]
    page_1 = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(
                payload=[single_row],
                headers={"Content-Range": "631681-631681/631682"},
            ),
            FakeResponse(payload=[single_metadata]),
        ]),
    ).list_findings(status=None, language=None, page=631682, page_size=1)

    assert page_50.pages == page_50.accessible_pages == 12634
    assert len(page_50.items) == 32
    assert page_1.pages == page_1.accessible_pages == 631682
    assert len(page_1.items) == 1


def test_supabase_service_cross_checks_requested_status_and_language():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    wrong_status = _audit_row()
    wrong_status["status"] = "pass"
    wrong_language = _audit_row()
    wrong_language["language"] = "Japanese"
    for row in (wrong_status, wrong_language):
        service = SubtitleAuditApiService(
            "https://example.supabase.co", SECRET,
            session=FakeSession(get=[
                FakeResponse(payload=[row], headers={"Content-Range": "0-0/1"})
            ]),
        )
        with pytest.raises(ValueError, match="requested filter"):
            service.list_findings(
                status="bad", language="English_AI", page=1, page_size=50
            )


def test_supabase_service_rejects_duplicate_subtitles_in_latest_page():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(
                payload=[_audit_row(), _audit_row()],
                headers={"Content-Range": "0-1/2"},
            )
        ]),
    )

    with pytest.raises(ValueError, match="duplicate subtitle"):
        service.list_findings(status=None, language=None, page=1, page_size=50)


def test_supabase_service_detail_cross_checks_requested_subtitle_id():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    row = _audit_row()
    row["subtitle_id"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[FakeResponse(payload=[row])]),
    )

    with pytest.raises(ValueError, match="requested subtitle"):
        service.get_finding(SUBTITLE_ID)


def test_supabase_service_does_not_propagate_session_errors_with_secret():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    class RaisingSession(FakeSession):
        def post(self, url, **kwargs):
            raise RuntimeError(f"connection detail included {SECRET}")

    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET, session=RaisingSession()
    )

    with pytest.raises(RuntimeError) as error:
        service.summary()

    assert str(error.value) == "subtitle audit summary request failed"
    assert error.value.__cause__ is None


def test_supabase_service_rejects_missing_or_malformed_catalog_metadata():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    for metadata in ([], [{**_catalog_row(), "language": None}]):
        service = SubtitleAuditApiService(
            "https://example.supabase.co", SECRET,
            session=FakeSession(get=[
                FakeResponse(payload=[_audit_row()], headers={"Content-Range": "0-0/1"}),
                FakeResponse(payload=metadata),
            ]),
        )
        with pytest.raises(ValueError, match="catalog metadata"):
            service.list_findings(
                status=None, language=None, page=1, page_size=50
            )


@pytest.mark.parametrize(
    ("metrics", "expected"),
    [
        ({"dominant_text_sha256": None}, {"dominant_text_sha256": None}),
        ({"encoding": "utf-16", "parse_mode": "tolerant"},
         {"encoding": "utf-16", "parse_mode": "tolerant"}),
    ],
)
def test_supabase_service_accepts_valid_parser_and_nullable_hash_metrics(
    metrics, expected
):
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    raw = _audit_row()
    raw["metrics"] = metrics
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(payload=[raw], headers={"Content-Range": "0-0/1"}),
            FakeResponse(payload=[_catalog_row()]),
        ]),
    )

    item = service.list_findings(
        status=None, language=None, page=1, page_size=50
    ).items[0]

    assert item.metrics == expected


@pytest.mark.parametrize(
    "metrics",
    [
        {"encoding": "unknown-codec"},
        {"parse_mode": "raw"},
        {"dominant_text_sha256": "not-a-sha"},
    ],
)
def test_supabase_service_rejects_invalid_parser_and_hash_metrics(metrics):
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    raw = _audit_row()
    raw["metrics"] = metrics
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(payload=[raw], headers={"Content-Range": "0-0/1"}),
        ]),
    )

    with pytest.raises(ValueError, match="metric"):
        service.list_findings(status=None, language=None, page=1, page_size=50)


def test_supabase_service_rejects_zero_expected_duration():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    raw = _audit_row()
    raw["expected_duration_seconds"] = 0
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(payload=[raw], headers={"Content-Range": "0-0/1"})
        ]),
    )

    with pytest.raises(ValueError, match="expected duration"):
        service.list_findings(status=None, language=None, page=1, page_size=50)


def test_supabase_service_rejects_unknown_duration_source():
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    raw = _audit_row()
    raw["duration_source"] = "untrusted subtitle body"
    service = SubtitleAuditApiService(
        "https://example.supabase.co", SECRET,
        session=FakeSession(get=[
            FakeResponse(payload=[raw], headers={"Content-Range": "0-0/1"})
        ]),
    )

    with pytest.raises(ValueError, match="duration source"):
        service.list_findings(status=None, language=None, page=1, page_size=50)


def test_api_service_builder_requires_enabled_visibility_and_both_credentials():
    from types import SimpleNamespace

    from orchestrator.__main__ import build_subtitle_audit_api_service

    base = {
        "subtitle_audit_visibility_enabled": True,
        "supabase_url": "https://example.supabase.co",
        "supabase_service_role_key": SECRET,
        "subtitle_audit_timeout_seconds": 9,
    }
    assert build_subtitle_audit_api_service(SimpleNamespace(**base)) is not None
    for override in (
        {"subtitle_audit_visibility_enabled": False},
        {"supabase_url": None},
        {"supabase_service_role_key": None},
    ):
        settings = SimpleNamespace(**{**base, **override})
        assert build_subtitle_audit_api_service(settings) is None


def test_latest_view_migration_is_service_role_only_and_security_invoker():
    from pathlib import Path

    migrations = sorted(
        Path("supabase/migrations").glob("*_subtitle_quality_latest_view.sql")
    )
    assert len(migrations) == 1
    sql = migrations[0].read_text(encoding="utf-8").lower()

    assert "with (security_invoker = true)" in sql
    assert "distinct on (audit.subtitle_id)" in sql
    assert "audit.scanned_at desc, audit.id desc" in sql
    core = sql.split("create view public.subtitle_quality_latest_catalog", 1)[0]
    expected_core_select = """select distinct on (audit.subtitle_id)
  audit.id,
  audit.subtitle_id,
  audit.audit_version,
  audit.content_sha256,
  audit.storage_etag,
  audit.status,
  audit.score,
  audit.reason_codes,
  audit.metrics,
  audit.expected_duration_seconds,
  audit.duration_source,
  audit.duration_confidence,
  audit.scanned_at
from public.subtitle_quality_audits audit"""
    assert expected_core_select in core
    assert "catalog.language" not in core
    assert "revoke all on public.subtitle_quality_latest from public, anon, authenticated" in sql
    assert "grant select on public.subtitle_quality_latest to service_role" in sql
    assert "create view public.subtitle_quality_latest_catalog" in sql
    assert "with (security_invoker = true)" in sql.split(
        "create view public.subtitle_quality_latest_catalog", 1
    )[1]
    assert "catalog.language" in sql.split(
        "create view public.subtitle_quality_latest_catalog", 1
    )[1]
    assert "revoke all on public.subtitle_quality_latest_catalog from public, anon, authenticated" in sql
    assert "grant select on public.subtitle_quality_latest_catalog to service_role" in sql
    assert "security invoker" in sql
    assert "security definer" not in sql
    assert "set search_path = public" in sql
    assert "revoke execute on function public.subtitle_quality_latest_summary() from public, anon, authenticated" in sql
    assert "grant execute on function public.subtitle_quality_latest_summary() to service_role" in sql
    summary_body = sql.split(
        "create or replace function public.subtitle_quality_latest_summary()", 1
    )[1]
    assert "with latest as materialized" in summary_body
    assert "from public.subtitle_quality_latest" in summary_body
    assert "select distinct latest.id, reason" in summary_body
    assert "reason is not null" in summary_body
    assert "btrim(reason) <> ''" in summary_body

