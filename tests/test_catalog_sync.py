from __future__ import annotations

import hashlib
from dataclasses import fields

import pytest
import requests

from orchestrator.catalog_sync import (
    CatalogSyncClient,
    CatalogSyncDiagnostic,
    CatalogSyncError,
    CatalogSyncResult,
)


CANONICAL_CODE = "roe-291"
SUBTITLE_ID = "00000000-0000-0000-0000-000000000002"
CONTENT_SHA256 = "a" * 64
TOKEN = "never-log-this-token"
ADULT_TEXT = "sensitive-response-subtitle-text"
CREDENTIAL_URL = f"https://user:{TOKEN}@javsubtitle.example/private"
SECRET_NETLOC = "private-adult-host.example"
IDEMPOTENCY_KEY = "jso-catalog-" + hashlib.sha256(
    f"{CANONICAL_CODE}\0{SUBTITLE_ID}\0{CONTENT_SHA256}".encode()
).hexdigest()


def valid_body() -> dict[str, object]:
    return {
        "success": True,
        "requested": 1,
        "synced": 1,
        "failed": [],
        "results": [
            {
                "canonicalCode": CANONICAL_CODE,
                "d1RowsUpdated": 2,
                "subtitleCount": 3,
                "kvKeysDeleted": [
                    f"movie:full:{CANONICAL_CODE}",
                    f"movie:light:{CANONICAL_CODE}",
                ],
                "dryRun": False,
            }
        ],
    }


def valid_current_body() -> dict[str, object]:
    return {
        "success": True,
        "requested": 1,
        "synced": 1,
        "failed": [],
        "dryRun": False,
        "results": [
            {
                "canonicalCode": CANONICAL_CODE,
                "d1RowsUpdated": 2,
                "subtitleCount": 3,
                "kvKeysTouched": [
                    f"movie:full:{CANONICAL_CODE}",
                    f"movie:light:{CANONICAL_CODE}",
                    f"movie:full:{CANONICAL_CODE}-uncensored-leak",
                    f"movie:light:{CANONICAL_CODE}-uncensored-leak",
                ],
            }
        ],
    }


def valid_public_body() -> dict[str, object]:
    return {
        "canonicalCode": CANONICAL_CODE,
        "subtitles": [
            {
                "id": SUBTITLE_ID,
                "lang": "English_AI",
                "label": "English AI",
                "languageTag": "en",
                "vttUrlSigned": "https://signed.example/subtitle.vtt",
                "expiresAt": "2026-07-13T22:00:00Z",
                "expiresIn": 3600,
            }
        ],
    }


def production_body() -> dict[str, object]:
    keys = [
        f"movie:full:{CANONICAL_CODE}",
        f"movie:light:{CANONICAL_CODE}",
    ]
    return {
        "success": True,
        "requested": 1,
        "synced": 1,
        "failed": [],
        "results": [
            {
                "canonicalCode": CANONICAL_CODE.upper(),
                "d1RowsUpdated": 1,
                "kvKeysTouched": keys,
                "kvKeysDeleted": keys,
                "subtitleCount": 1,
                "futureField": ADULT_TEXT,
            }
        ],
        "fence": {"value": 42, "accepted": True},
        "action": "sync",
        "futureTopLevel": ADULT_TEXT,
    }


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        body: object | None = None,
        *,
        json_error: Exception | None = None,
        text: str = ADULT_TEXT,
    ) -> None:
        self.status_code = status_code
        self._body = valid_body() if body is None else body
        self._json_error = json_error
        self.text = text

    def json(self) -> object:
        if self._json_error is not None:
            raise self._json_error
        return self._body


class FakeSession:
    def __init__(
        self,
        response: FakeResponse | None = None,
        *,
        public_response: FakeResponse | None = None,
        error: requests.RequestException | None = None,
        public_error: requests.RequestException | None = None,
    ) -> None:
        self.response = response or FakeResponse()
        self.public_response = public_response or FakeResponse(body=valid_public_body())
        self.error = error
        self.public_error = public_error
        self.requests: list[tuple[str, dict[str, object]]] = []

    def post(self, url: str, **kwargs: object) -> FakeResponse:
        self.requests.append((url, kwargs))
        if self.error is not None:
            raise self.error
        return self.response

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        self.requests.append((url, kwargs))
        if self.public_error is not None:
            raise self.public_error
        return self.public_response


def sync(client: CatalogSyncClient, movie_code: str = CANONICAL_CODE):
    return client.sync(
        movie_code,
        expected_subtitle_id=SUBTITLE_ID,
        expected_content_sha256=CONTENT_SHA256,
    )


def sync_with_response(
    status: int,
    *,
    body: object | None = None,
    json_error: Exception | None = None,
    token: str = TOKEN,
) -> CatalogSyncResult:
    client = CatalogSyncClient(
        "https://javsubtitle.example/",
        token,
        session=FakeSession(FakeResponse(status, body, json_error=json_error)),
    )
    return sync(client)


def test_sync_uses_exact_bounded_request_and_returns_frozen_public_result():
    session = FakeSession()

    result = sync(
        CatalogSyncClient(
            "https://javsubtitle.example/", TOKEN, timeout_seconds=17, session=session
        ),
        "ROE291",
    )

    assert result == CatalogSyncResult(
        canonical_code=CANONICAL_CODE,
        d1_rows_updated=2,
        subtitle_count=3,
        kv_keys_deleted=(
            f"movie:full:{CANONICAL_CODE}",
            f"movie:light:{CANONICAL_CODE}",
        ),
        diagnostic=result.diagnostic,
    )
    assert isinstance(result.diagnostic, CatalogSyncDiagnostic)
    assert result.diagnostic.http_status == 200
    assert [field.name for field in fields(CatalogSyncResult)] == [
        "canonical_code",
        "d1_rows_updated",
        "subtitle_count",
        "kv_keys_deleted",
        "diagnostic",
    ]
    with pytest.raises(AttributeError):
        result.subtitle_count = 4  # type: ignore[misc]
    assert session.requests == [
        (
            "https://javsubtitle.example/api/admin/catalog/sync-subtitles",
            {
                "headers": {
                    "Authorization": f"Bearer {TOKEN}",
                    "Content-Type": "application/json",
                    "Idempotency-Key": IDEMPOTENCY_KEY,
                },
                "json": {
                    "canonicalCodes": [CANONICAL_CODE],
                    "reason": "subtitle_ingest",
                    "source": "jav-subtitle-orchestrator",
                    "dryRun": False,
                },
                "timeout": 17,
                "allow_redirects": False,
            },
        ),
        (
            f"https://javsubtitle.example/api/movie/{CANONICAL_CODE}"
            f"?cacheNonce={CONTENT_SHA256}",
            {"timeout": 17, "allow_redirects": False},
        ),
    ]


def test_sync_accepts_current_catalog_response_schema():
    result = sync_with_response(200, body=valid_current_body())

    assert result == CatalogSyncResult(
        canonical_code=CANONICAL_CODE,
        d1_rows_updated=2,
        subtitle_count=3,
        kv_keys_deleted=(
            f"movie:full:{CANONICAL_CODE}",
            f"movie:light:{CANONICAL_CODE}",
            f"movie:full:{CANONICAL_CODE}-uncensored-leak",
            f"movie:light:{CANONICAL_CODE}-uncensored-leak",
        ),
        diagnostic=result.diagnostic,
    )


def test_sync_accepts_production_schema_semantically_and_redacts_unknown_values():
    result = sync_with_response(200, body=production_body())

    assert result.canonical_code == CANONICAL_CODE
    assert result.d1_rows_updated == 1
    assert result.subtitle_count == 1
    assert result.diagnostic.http_status == 200
    assert "futureField" in result.diagnostic.response_json
    assert "futureTopLevel" in result.diagnostic.response_json
    assert ADULT_TEXT not in result.diagnostic.response_json


def test_http_207_is_retryable_and_retains_only_safe_partial_failure_metadata():
    body = {
        "success": False,
        "requested": 1,
        "synced": 0,
        "failed": [
            {"canonicalCode": CANONICAL_CODE, "error": "movie_not_found"}
        ],
        "results": [],
        "secret": ADULT_TEXT,
    }

    with pytest.raises(CatalogSyncError) as raised:
        sync_with_response(207, body=body)

    assert raised.value.reason_code == "catalog_sync_failed"
    assert raised.value.retryable is True
    assert raised.value.http_status == 207
    assert "movie_not_found" in raised.value.response_json
    assert "secret" in raised.value.response_json
    assert ADULT_TEXT not in raised.value.response_json


def test_http_500_is_retryable_with_safe_status_diagnostic():
    with pytest.raises(CatalogSyncError) as raised:
        sync_with_response(500, body={"error": ADULT_TEXT})

    assert raised.value.retryable is True
    assert raised.value.http_status == 500
    assert ADULT_TEXT not in raised.value.response_json


def test_non_json_diagnostic_contains_only_size_and_sha256():
    with pytest.raises(CatalogSyncError) as raised:
        sync_with_response(
            500,
            json_error=ValueError("invalid"),
        )

    diagnostic = raised.value.response_json
    assert "bodyBytes" in diagnostic
    assert "bodySha256" in diagnostic
    assert ADULT_TEXT not in diagnostic
    assert TOKEN not in diagnostic


def test_idempotency_key_is_stable_for_same_verified_receipt():
    first_session = FakeSession()
    second_session = FakeSession()
    sync(CatalogSyncClient("https://javsubtitle.example", TOKEN, session=first_session))
    sync(CatalogSyncClient("https://javsubtitle.example", TOKEN, session=second_session))

    assert first_session.requests[0][1]["headers"]["Idempotency-Key"] == IDEMPOTENCY_KEY
    assert second_session.requests[0][1]["headers"]["Idempotency-Key"] == IDEMPOTENCY_KEY


def test_sync_accepts_versioned_full_cache_key_from_deployed_catalog():
    body = valid_body()
    body["results"][0]["kvKeysDeleted"] = [
        f"movie:full:v3:{CANONICAL_CODE}",
        f"movie:light:{CANONICAL_CODE}",
    ]

    result = sync_with_response(200, body=body)

    assert result.kv_keys_deleted == (
        f"movie:full:v3:{CANONICAL_CODE}",
        f"movie:light:{CANONICAL_CODE}",
    )


def test_sync_accepts_versioned_alias_keys_from_multirow_catalog():
    body = valid_body()
    body["results"][0].update(
        d1RowsUpdated=2,
        kvKeysDeleted=[
            f"movie:full:v3:{CANONICAL_CODE}",
            f"movie:light:{CANONICAL_CODE}",
            f"movie:full:v3:{CANONICAL_CODE}-uncensored-leak",
            f"movie:light:{CANONICAL_CODE}-uncensored-leak",
        ],
    )

    result = sync_with_response(200, body=body)

    assert result.kv_keys_deleted == tuple(body["results"][0]["kvKeysDeleted"])


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (300, "catalog_redirect_rejected"),
        (302, "catalog_redirect_rejected"),
        (399, "catalog_redirect_rejected"),
        (401, "catalog_auth_failed"),
        (403, "catalog_auth_failed"),
        (400, "catalog_sync_failed"),
        (500, "catalog_sync_failed"),
    ],
)
def test_http_failures_are_classified_without_leaking_secrets(status: int, reason: str):
    with pytest.raises(CatalogSyncError) as raised:
        sync_with_response(status)

    error = raised.value
    assert error.reason_code == reason
    assert str(error) == reason
    assert error.args == (reason,)
    for rendered in (str(error), repr(error)):
        assert TOKEN not in rendered
        assert ADULT_TEXT not in rendered


def test_request_exception_is_safe_catalog_fetch_failure():
    session = FakeSession(error=requests.ConnectionError(f"failed {CREDENTIAL_URL} {ADULT_TEXT}"))

    with pytest.raises(CatalogSyncError) as raised:
        sync(CatalogSyncClient("https://javsubtitle.example", TOKEN, session=session))

    assert raised.value.reason_code == "catalog_fetch_failed"
    assert str(raised.value) == "catalog_fetch_failed"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    for sensitive in (TOKEN, CREDENTIAL_URL, ADULT_TEXT, session.response.text):
        assert sensitive not in str(raised.value)
        assert sensitive not in repr(raised.value)


@pytest.mark.parametrize(
    ("body", "json_error"),
    [
        ([], None),
        ("not-an-object", None),
        (
            None,
            ValueError(f"invalid JSON containing {TOKEN} {CREDENTIAL_URL} {ADULT_TEXT}"),
        ),
        (
            None,
            TypeError(f"invalid JSON containing {TOKEN} {CREDENTIAL_URL} {ADULT_TEXT}"),
        ),
    ],
)
def test_invalid_json_or_non_object_response_is_rejected_safely(body, json_error):
    with pytest.raises(CatalogSyncError) as raised:
        sync_with_response(200, body=body, json_error=json_error)

    assert raised.value.reason_code == "catalog_response_invalid"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    for sensitive in (TOKEN, CREDENTIAL_URL, ADULT_TEXT):
        assert sensitive not in str(raised.value)
        assert sensitive not in repr(raised.value)


def response_mutations() -> list[tuple[str, object]]:
    return [
        ("missing success", lambda body: body.pop("success")),
        ("false success", lambda body: body.update(success=False)),
        ("requested count", lambda body: body.update(requested=2)),
        ("requested bool", lambda body: body.update(requested=True)),
        ("synced count", lambda body: body.update(synced=0)),
        ("synced bool", lambda body: body.update(synced=True)),
        ("failed not empty", lambda body: body.update(failed=[CANONICAL_CODE])),
        ("failed wrong type", lambda body: body.update(failed={})),
        ("results wrong type", lambda body: body.update(results={})),
        ("results wrong count", lambda body: body.update(results=[])),
        ("result not object", lambda body: body.update(results=[ADULT_TEXT])),
        (
            "canonical code mismatch",
            lambda body: body["results"][0].update(canonicalCode="abc-123"),
        ),
        ("dry run true", lambda body: body["results"][0].update(dryRun=True)),
        ("dry run non-bool", lambda body: body["results"][0].update(dryRun=0)),
        ("D1 rows missing", lambda body: body["results"][0].pop("d1RowsUpdated")),
        ("D1 rows zero", lambda body: body["results"][0].update(d1RowsUpdated=0)),
        ("D1 rows bool", lambda body: body["results"][0].update(d1RowsUpdated=True)),
        ("subtitle count zero", lambda body: body["results"][0].update(subtitleCount=0)),
        (
            "subtitle count bool",
            lambda body: body["results"][0].update(subtitleCount=True),
        ),
        (
            "KV keys missing one",
            lambda body: body["results"][0].update(
                kvKeysDeleted=[f"movie:full:{CANONICAL_CODE}"]
            ),
        ),
        (
            "KV keys duplicate",
            lambda body: body["results"][0].update(
                kvKeysDeleted=[
                    f"movie:full:{CANONICAL_CODE}",
                    f"movie:light:{CANONICAL_CODE}",
                    f"movie:light:{CANONICAL_CODE}",
                ]
            ),
        ),
        (
            "KV keys wrong type",
            lambda body: body["results"][0].update(kvKeysDeleted=ADULT_TEXT),
        ),
    ]


@pytest.mark.parametrize("_name,mutate", response_mutations(), ids=lambda value: str(value))
def test_malformed_or_mismatched_success_response_is_rejected(_name, mutate):
    body = valid_body()
    mutate(body)

    with pytest.raises(CatalogSyncError) as raised:
        sync_with_response(200, body=body)

    assert raised.value.reason_code == "catalog_response_mismatch"
    assert str(raised.value) == "catalog_response_mismatch"
    assert ADULT_TEXT not in repr(raised.value)


@pytest.mark.parametrize(
    "base_url",
    [
        "",
        "javsubtitle.example",
        "ftp://javsubtitle.example",
        "http://javsubtitle.example",
        f"https://user:{TOKEN}@javsubtitle.example",
        f"https://user:{TOKEN}@[{SECRET_NETLOC}/{ADULT_TEXT}",
        "https://javsubtitle.example/base",
        "https://javsubtitle.example/path?token=private",
        "https://javsubtitle.example/path#fragment",
        "https://javsubtitle.example?",
        "https://javsubtitle.example#",
        "https://javsubtitle.example/?",
        "https://javsubtitle.example/#",
    ],
)
def test_base_url_validation_fails_closed_without_leaking_input(base_url: str):
    with pytest.raises(ValueError) as raised:
        CatalogSyncClient(base_url, TOKEN, session=FakeSession())

    assert str(raised.value) == "catalog API base URL is invalid"
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None
    for sensitive in (TOKEN, SECRET_NETLOC, ADULT_TEXT):
        assert sensitive not in str(raised.value)
        assert sensitive not in repr(raised.value)


@pytest.mark.parametrize(
    "base_url",
    [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://[::1]:3000",
        "https://javsubtitle.example",
        "https://javsubtitle.example/",
    ],
)
def test_base_url_accepts_https_and_explicit_localhost_http(base_url: str):
    client = CatalogSyncClient(base_url, TOKEN, session=FakeSession())

    assert sync(client).canonical_code == CANONICAL_CODE


@pytest.mark.parametrize("token", ["", "   ", None, 123])
def test_admin_token_must_be_nonempty_string(token):
    with pytest.raises(ValueError, match="catalog admin token is required"):
        CatalogSyncClient("https://javsubtitle.example", token, session=FakeSession())


@pytest.mark.parametrize("timeout", [0, -1, True])
def test_timeout_must_be_positive_number(timeout):
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        CatalogSyncClient(
            "https://javsubtitle.example", TOKEN, timeout_seconds=timeout, session=FakeSession()
        )


def test_invalid_movie_code_is_rejected_before_request_without_echoing_input():
    session = FakeSession()
    sensitive_movie_code = f"{TOKEN} {SECRET_NETLOC} {ADULT_TEXT}"

    with pytest.raises(ValueError) as raised:
        sync(
            CatalogSyncClient("https://javsubtitle.example", TOKEN, session=session),
            sensitive_movie_code,
        )

    assert str(raised.value) == "invalid movie code"
    assert raised.value.__context__ is None
    assert raised.value.__cause__ is None
    for sensitive in (TOKEN, SECRET_NETLOC, ADULT_TEXT, sensitive_movie_code):
        assert sensitive not in str(raised.value)
        assert sensitive not in repr(raised.value)
    assert session.requests == []


def test_public_movie_verification_accepts_real_public_subtitle_shape():
    session = FakeSession(public_response=FakeResponse(body=valid_public_body()))

    result = sync(CatalogSyncClient("https://javsubtitle.example", TOKEN, session=session))

    assert result.canonical_code == CANONICAL_CODE
    assert session.requests[-1] == (
        f"https://javsubtitle.example/api/movie/{CANONICAL_CODE}"
        f"?cacheNonce={CONTENT_SHA256}",
        {"timeout": 30, "allow_redirects": False},
    )


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        ({"canonicalCode": "abc-123", "subtitles": []}, "public_visibility_mismatch"),
        ({"canonicalCode": CANONICAL_CODE, "subtitles": []}, "public_visibility_mismatch"),
        (
            {
                "canonicalCode": CANONICAL_CODE,
                "subtitles": [
                    {"id": SUBTITLE_ID},
                    {"id": SUBTITLE_ID},
                ],
            },
            "public_visibility_mismatch",
        ),
        (
            {
                "canonicalCode": CANONICAL_CODE,
                "subtitles": "not-an-array",
            },
            "public_visibility_response_invalid",
        ),
        ([], "public_visibility_response_invalid"),
    ],
)
def test_public_movie_verification_fails_closed_on_mismatch(body, reason):
    session = FakeSession(public_response=FakeResponse(body=body))

    with pytest.raises(CatalogSyncError) as raised:
        sync(CatalogSyncClient("https://javsubtitle.example", TOKEN, session=session))

    assert raised.value.reason_code == reason
    assert TOKEN not in repr(raised.value)
    assert ADULT_TEXT not in repr(raised.value)


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (302, "public_visibility_redirect_rejected"),
        (404, "public_visibility_not_found"),
        (500, "public_visibility_fetch_failed"),
    ],
)
def test_public_movie_http_failure_has_structured_safe_reason(status, reason):
    session = FakeSession(public_response=FakeResponse(status_code=status))

    with pytest.raises(CatalogSyncError) as raised:
        sync(CatalogSyncClient("https://javsubtitle.example", TOKEN, session=session))

    assert raised.value.reason_code == reason


def test_public_movie_request_exception_does_not_leak_details():
    session = FakeSession(public_error=requests.ConnectionError(f"failed {TOKEN} {ADULT_TEXT}"))

    with pytest.raises(CatalogSyncError) as raised:
        sync(CatalogSyncClient("https://javsubtitle.example", TOKEN, session=session))

    assert raised.value.reason_code == "public_visibility_fetch_failed"
    assert TOKEN not in repr(raised.value)
    assert ADULT_TEXT not in repr(raised.value)
