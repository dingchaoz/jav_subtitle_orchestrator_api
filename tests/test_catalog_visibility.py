from __future__ import annotations

import math
from dataclasses import fields

import pytest
import requests

from orchestrator.catalog_visibility import (
    MAX_CATALOG_TIMEOUT_SECONDS,
    PublicCatalogVisibilityClient,
    PublicVisibilityResult,
    VisibilityStatus,
    normalize_catalog_api_origin,
)


CANONICAL_CODE = "ktb-111"
SUBTITLE_ID = "00000000-0000-0000-0000-000000000001"
CONTENT_SHA256 = "a" * 64
SECRET = "never-expose-this-detail"


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        body: object | None = None,
        *,
        json_error: Exception | None = None,
    ) -> None:
        self.status_code = status_code
        self._body = (
            {"canonicalCode": CANONICAL_CODE, "subtitles": [{"id": SUBTITLE_ID}]}
            if body is None
            else body
        )
        self._json_error = json_error

    def json(self) -> object:
        if self._json_error is not None:
            raise self._json_error
        return self._body


class FakeSession:
    def __init__(
        self,
        response: FakeResponse | None = None,
        *,
        error: requests.RequestException | None = None,
    ) -> None:
        self.response = response or FakeResponse()
        self.error = error
        self.requests: list[tuple[str, dict[str, object]]] = []

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        self.requests.append((url, kwargs))
        if self.error is not None:
            raise self.error
        return self.response


class PreparedRequestSession(requests.Session):
    def __init__(self) -> None:
        super().__init__()
        self.sent: list[tuple[requests.PreparedRequest, dict[str, object]]] = []

    def send(self, request: requests.PreparedRequest, **kwargs: object) -> requests.Response:
        self.sent.append((request, kwargs))
        response = requests.Response()
        response.status_code = 404
        response.request = request
        response.url = request.url
        return response


def check(session: FakeSession) -> PublicVisibilityResult:
    return PublicCatalogVisibilityClient(
        "https://javsubtitle.example/", session=session
    ).check("KTB111", SUBTITLE_ID, CONTENT_SHA256)


def test_exact_expected_subtitle_once_is_visible_and_uses_bounded_get():
    session = FakeSession()

    result = check(session)

    assert result == PublicVisibilityResult(
        status=VisibilityStatus.VISIBLE,
        canonical_code=CANONICAL_CODE,
        expected_subtitle_id=SUBTITLE_ID,
        observed_subtitle_ids=(SUBTITLE_ID,),
    )
    assert session.requests == [
        (
            f"https://javsubtitle.example/api/movie/{CANONICAL_CODE}"
            f"?cacheNonce={CONTENT_SHA256}",
            {"timeout": 30, "allow_redirects": False},
        )
    ]
    assert [field.name for field in fields(PublicVisibilityResult)] == [
        "status",
        "canonical_code",
        "expected_subtitle_id",
        "observed_subtitle_ids",
        "reason_code",
    ]
    with pytest.raises(AttributeError):
        result.status = VisibilityStatus.MISSING  # type: ignore[misc]


@pytest.mark.parametrize(
    "base_url",
    [
        "https://javsubtitle.example:",
        "https://javsubtitle.example:not-a-port",
        "https://javsubtitle.example:65536",
    ],
)
def test_origin_normalization_rejects_malformed_ports(base_url: str):
    with pytest.raises(ValueError, match="catalog API base URL is invalid"):
        normalize_catalog_api_origin(base_url)


@pytest.mark.parametrize(
    ("base_url", "expected_origin"),
    [
        ("https://example.com:0443", "https://example.com:443"),
        ("http://localhost:03000", "http://localhost:3000"),
    ],
)
def test_origin_normalization_accepts_leading_zero_numeric_ports(
    base_url: str,
    expected_origin: str,
):
    assert normalize_catalog_api_origin(base_url) == expected_origin


@pytest.mark.parametrize(
    ("base_url", "expected_origin"),
    [
        ("https://[v1.fe]", "https://[v1.fe]"),
        ("https://[vF.a:b]:0443", "https://[vF.a:b]:443"),
    ],
)
def test_origin_normalization_preserves_valid_ipvfuture_brackets(
    base_url: str,
    expected_origin: str,
):
    assert normalize_catalog_api_origin(base_url) == expected_origin


@pytest.mark.parametrize(
    "base_url",
    [
        "https://example.com\x00",
        "https://example.com\\evil",
        "https://[::1]evil",
        "https://[::1].example",
        "https://[::1]]",
        "https://[v.fe]",
        "https://[v1]",
        "https://[v1.]",
        "https://[v1.fe%]",
        "https://[v1.fe]junk",
        "https://[:::1]",
        "https://[v1.fe",
        "https://[example.com]",
        "https://[127.0.0.1]",
        "http://[v1.fe]",
        "https://０.example.com",
    ],
)
def test_origin_normalization_rejects_invalid_hostname_characters(base_url: str):
    with pytest.raises(ValueError) as raised:
        normalize_catalog_api_origin(base_url)

    assert str(raised.value) == "catalog API base URL is invalid"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.parametrize(
    "timeout",
    [
        0,
        -1,
        True,
        math.nan,
        math.inf,
        -math.inf,
        MAX_CATALOG_TIMEOUT_SECONDS + 1,
        MAX_CATALOG_TIMEOUT_SECONDS + 0.1,
        1e20,
    ],
)
def test_timeout_must_be_a_positive_non_bool_number(timeout):
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        PublicCatalogVisibilityClient(
            "https://javsubtitle.example", timeout_seconds=timeout, session=FakeSession()
        )


@pytest.mark.parametrize("timeout", [1, 0.25, 300, 300.0])
def test_positive_finite_timeout_is_preserved(timeout):
    assert MAX_CATALOG_TIMEOUT_SECONDS == 300
    client = PublicCatalogVisibilityClient(
        "https://javsubtitle.example", timeout_seconds=timeout, session=FakeSession()
    )

    assert client.timeout_seconds == timeout


def test_requests_prepares_exact_validated_url_and_receives_finite_timeout():
    session = PreparedRequestSession()
    client = PublicCatalogVisibilityClient(
        "https://javsubtitle.example:8443/",
        timeout_seconds=2.5,
        session=session,
    )

    result = client.check("KTB111", SUBTITLE_ID, CONTENT_SHA256)

    assert result.status is VisibilityStatus.NOT_FOUND
    assert len(session.sent) == 1
    request, kwargs = session.sent[0]
    assert request.url == (
        f"https://javsubtitle.example:8443/api/movie/{CANONICAL_CODE}"
        f"?cacheNonce={CONTENT_SHA256}"
    )
    assert kwargs["timeout"] == 2.5
    assert kwargs["allow_redirects"] is False


@pytest.mark.parametrize(
    ("hostname", "ascii_hostname"),
    [
        ("faß.de", "xn--fa-hia.de"),
        ("βόλος.com", "xn--nxasmm1c.com"),
        ("例え.テスト", "xn--r8jz45g.xn--zckzah"),
    ],
)
def test_requests_and_origin_normalization_agree_on_unicode_idn_destination(
    hostname: str,
    ascii_hostname: str,
):
    session = PreparedRequestSession()
    client = PublicCatalogVisibilityClient(
        f"https://{hostname}",
        session=session,
    )

    result = client.check("KTB111", SUBTITLE_ID, CONTENT_SHA256)

    assert result.status is VisibilityStatus.NOT_FOUND
    assert client.base_url == f"https://{ascii_hostname}"
    assert len(session.sent) == 1
    request, _kwargs = session.sent[0]
    assert request.url == (
        f"https://{ascii_hostname}/api/movie/{CANONICAL_CODE}"
        f"?cacheNonce={CONTENT_SHA256}"
    )


def test_visibility_get_preserves_bracketed_ipvfuture_url():
    session = FakeSession(FakeResponse(status_code=404))
    client = PublicCatalogVisibilityClient(
        "https://[v1.fe]:8443",
        session=session,
    )

    result = client.check("KTB111", SUBTITLE_ID, CONTENT_SHA256)

    assert result.status is VisibilityStatus.NOT_FOUND
    assert client.base_url == "https://[v1.fe]:8443"
    assert session.requests == [
        (
            f"https://[v1.fe]:8443/api/movie/{CANONICAL_CODE}"
            f"?cacheNonce={CONTENT_SHA256}",
            {"timeout": 30, "allow_redirects": False},
        )
    ]


def test_requests_ipvfuture_parse_failure_is_safely_classified():
    request_url = (
        f"https://[v1.fe]:8443/api/movie/{CANONICAL_CODE}"
        f"?cacheNonce={CONTENT_SHA256}"
    )
    with pytest.raises(requests.exceptions.InvalidURL):
        requests.Request("GET", request_url).prepare()

    session = PreparedRequestSession()
    client = PublicCatalogVisibilityClient(
        "https://[v1.fe]:8443",
        session=session,
    )

    result = client.check("KTB111", SUBTITLE_ID, CONTENT_SHA256)

    assert result.status is VisibilityStatus.FETCH_FAILED
    assert result.reason_code == "public_visibility_fetch_failed"
    assert client.base_url == "https://[v1.fe]:8443"
    assert session.sent == []
    assert "Failed to parse" not in repr(result)


@pytest.mark.parametrize(
    ("expected_subtitle_id", "content_sha256"),
    [
        ("", CONTENT_SHA256),
        (SUBTITLE_ID, "a" * 63),
        (SUBTITLE_ID, "A" * 64),
        (SUBTITLE_ID, "g" * 64),
    ],
)
def test_invalid_receipt_is_classified_without_request(expected_subtitle_id, content_sha256):
    session = FakeSession()

    result = PublicCatalogVisibilityClient(
        "https://javsubtitle.example", session=session
    ).check(CANONICAL_CODE, expected_subtitle_id, content_sha256)

    assert result.status is VisibilityStatus.INVALID_RECEIPT
    assert result.reason_code == "invalid_receipt"
    assert session.requests == []


@pytest.mark.parametrize(
    "subtitles",
    [
        [],
        [{"id": "another-id"}],
        [{"id": SUBTITLE_ID}, {"id": SUBTITLE_ID}],
    ],
)
def test_zero_or_duplicate_expected_subtitle_is_missing(subtitles):
    session = FakeSession(
        FakeResponse(body={"canonicalCode": CANONICAL_CODE, "subtitles": subtitles})
    )

    result = check(session)

    assert result.status is VisibilityStatus.MISSING
    assert result.reason_code == "public_visibility_mismatch"
    assert result.observed_subtitle_ids == tuple(row["id"] for row in subtitles)


def test_404_is_not_found():
    result = check(FakeSession(FakeResponse(status_code=404)))

    assert result.status is VisibilityStatus.NOT_FOUND
    assert result.reason_code == "public_visibility_not_found"


@pytest.mark.parametrize("status", [300, 302, 399, 500])
def test_redirect_or_other_http_failure_is_fetch_failed(status: int):
    result = check(FakeSession(FakeResponse(status_code=status)))

    assert result.status is VisibilityStatus.FETCH_FAILED
    assert result.reason_code == (
        "public_visibility_redirect_rejected"
        if 300 <= status < 400
        else "public_visibility_fetch_failed"
    )


def test_network_failure_is_safe_fetch_failed():
    session = FakeSession(error=requests.ConnectionError(f"failed at https://{SECRET}"))

    result = check(session)

    assert result.status is VisibilityStatus.FETCH_FAILED
    assert result.reason_code == "public_visibility_fetch_failed"
    assert SECRET not in repr(result)


@pytest.mark.parametrize(
    ("body", "json_error"),
    [
        (None, ValueError(f"invalid JSON {SECRET}")),
        ({"canonicalCode": "abc-123", "subtitles": []}, None),
        ({"canonicalCode": CANONICAL_CODE, "subtitles": "not-an-array"}, None),
        ({"canonicalCode": CANONICAL_CODE, "subtitles": [SECRET]}, None),
        ({"canonicalCode": CANONICAL_CODE, "subtitles": [{}]}, None),
        ({"canonicalCode": CANONICAL_CODE, "subtitles": [{"id": 123}]}, None),
    ],
)
def test_invalid_payload_is_response_invalid_without_leaking_details(body, json_error):
    result = check(FakeSession(FakeResponse(body=body, json_error=json_error)))

    assert result.status is VisibilityStatus.RESPONSE_INVALID
    assert result.reason_code == "public_visibility_response_invalid"
    assert SECRET not in repr(result)
