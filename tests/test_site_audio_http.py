import pytest

from orchestrator.site_audio.errors import DownloadFailure
from orchestrator.site_audio.http import CurlCffiTransport


class FakeResponse:
    def __init__(self, status_code, text, url="https://example.com/final"):
        self.status_code = status_code
        self.text = text
        self.url = url


def test_transport_forwards_headers_timeout_and_redirect_setting():
    calls = []

    class FakeSession:
        def get(self, url, **kwargs):
            calls.append((url, kwargs))
            return FakeResponse(200, "ok")

    response = CurlCffiTransport(session=FakeSession(), timeout=17).get_text(
        "https://example.com/page",
        {"User-Agent": "Browser"},
    )

    assert response.status_code == 200
    assert calls == [
        (
            "https://example.com/page",
            {
                "headers": {"User-Agent": "Browser"},
                "timeout": 17,
                "allow_redirects": True,
            },
        )
    ]


def test_transport_retries_transient_status_then_returns_success():
    responses = iter([FakeResponse(503, "busy"), FakeResponse(200, "ready")])
    sleeps = []

    class FakeSession:
        def get(self, url, **kwargs):
            return next(responses)

    response = CurlCffiTransport(
        session=FakeSession(),
        max_attempts=3,
        sleep=sleeps.append,
    ).get_text("https://example.com/page", {})

    assert response.text == "ready"
    assert sleeps == [1.0]


def test_transport_does_not_retry_forbidden_response():
    calls = 0

    class FakeSession:
        def get(self, url, **kwargs):
            nonlocal calls
            calls += 1
            return FakeResponse(403, "challenge")

    response = CurlCffiTransport(session=FakeSession()).get_text("https://example.com/page", {})

    assert response.status_code == 403
    assert calls == 1


def test_transport_maps_network_exception_to_typed_failure():
    class FakeSession:
        def get(self, url, **kwargs):
            raise OSError("network stack failed")

    with pytest.raises(DownloadFailure, match="HTTP request failed"):
        CurlCffiTransport(session=FakeSession(), max_attempts=1).get_text(
            "https://example.com/page",
            {},
        )
