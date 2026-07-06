import logging

import pytest

from orchestrator.catalog_sync import CatalogSyncClient, CatalogSyncError


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text="OK"):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"success": True}
        self.text = text

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_catalog_sync_posts_canonical_codes_to_admin_endpoint():
    session = FakeSession([FakeResponse(payload={"success": True, "synced": 1})])
    client = CatalogSyncClient(
        "https://javsubtitle.com/",
        "secret-token",
        session=session,
    )

    result = client.sync_subtitles(
        ["KTB-112"],
        source="jav-subtitle-orchestrator",
        reason="orchestrator_ai_subtitle_publish",
    )

    assert result == {"success": True, "synced": 1}
    assert len(session.calls) == 1
    url, kwargs = session.calls[0]
    assert url == "https://javsubtitle.com/api/admin/catalog/sync-subtitles"
    assert kwargs["headers"]["Authorization"] == "Bearer secret-token"
    assert kwargs["headers"]["Content-Type"] == "application/json"
    assert kwargs["json"] == {
        "canonicalCodes": ["ktb-112"],
        "source": "jav-subtitle-orchestrator",
        "reason": "orchestrator_ai_subtitle_publish",
    }


def test_catalog_sync_retries_then_succeeds_after_transient_failure():
    session = FakeSession(
        [
            FakeResponse(status_code=503, text="temporarily unavailable"),
            FakeResponse(payload={"success": True, "synced": 1}),
        ]
    )
    client = CatalogSyncClient(
        "https://javsubtitle.com",
        "secret-token",
        session=session,
        max_attempts=2,
    )

    result = client.sync_subtitles(["ktb-112"], source="test", reason="retry_test")

    assert result["success"] is True
    assert len(session.calls) == 2


def test_catalog_sync_retries_when_endpoint_reports_failed_codes():
    session = FakeSession(
        [
            FakeResponse(
                payload={
                    "success": False,
                    "failed": [
                        {
                            "canonicalCode": "ktb-112",
                            "code": "catalog_movie_missing",
                            "error": "No catalog row",
                        }
                    ],
                }
            ),
            FakeResponse(payload={"success": True, "synced": 1}),
        ]
    )
    client = CatalogSyncClient(
        "https://javsubtitle.com",
        "secret-token",
        session=session,
        max_attempts=2,
    )

    result = client.sync_subtitles(["ktb-112"], source="test", reason="retry_test")

    assert result["success"] is True
    assert len(session.calls) == 2


def test_catalog_sync_raises_after_failed_retry_attempts():
    session = FakeSession(
        [
            FakeResponse(status_code=503, text="temporarily unavailable"),
            FakeResponse(status_code=500, text="still unavailable"),
        ]
    )
    client = CatalogSyncClient(
        "https://javsubtitle.com",
        "secret-token",
        session=session,
        max_attempts=2,
    )

    with pytest.raises(CatalogSyncError, match="ktb-112"):
        client.sync_subtitles(["ktb-112"], source="test", reason="retry_test")

    assert len(session.calls) == 2


def test_catalog_sync_disabled_skips_http_request():
    session = FakeSession([])
    client = CatalogSyncClient(
        "https://javsubtitle.com",
        "secret-token",
        session=session,
        enabled=False,
    )

    assert client.sync_subtitles(["ktb-112"], source="test", reason="disabled") is None
    assert session.calls == []


def test_publisher_logs_catalog_sync_failure_without_failing_publication(tmp_path, caplog):
    from tests.test_supabase_publisher import FakeSession as SupabaseSession
    from orchestrator.supabase_publisher import SupabaseSubtitlePublisher

    srt = tmp_path / "ktb-112.English.srt"
    srt.write_text("translated\n", encoding="utf-8")

    class FailingSync:
        def sync_subtitles(self, canonical_codes, *, source, reason):
            raise CatalogSyncError(canonical_codes, "sync failed")

    caplog.set_level(logging.WARNING)
    publisher = SupabaseSubtitlePublisher(
        "https://example.supabase.co",
        "service-key",
        session=SupabaseSession(),
        catalog_sync=FailingSync(),
    )

    result = publisher.publish_english_ai("ktb-112", srt)

    assert result.movie_code == "ktb-112"
    assert "Catalog sync failed for ktb-112" in caplog.text
