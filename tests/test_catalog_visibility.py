from __future__ import annotations

import math
from dataclasses import fields

import pytest
import requests

from orchestrator.catalog_visibility import (
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
    "base_url",
    [
        "https://example.com\x00",
        "https://example.com\\evil",
    ],
)
def test_origin_normalization_rejects_invalid_hostname_characters(base_url: str):
    with pytest.raises(ValueError) as raised:
        normalize_catalog_api_origin(base_url)

    assert str(raised.value) == "catalog API base URL is invalid"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.parametrize("timeout", [0, -1, True, math.nan])
def test_timeout_must_be_a_positive_non_bool_number(timeout):
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        PublicCatalogVisibilityClient(
            "https://javsubtitle.example", timeout_seconds=timeout, session=FakeSession()
        )


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
