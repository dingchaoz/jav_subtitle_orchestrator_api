from __future__ import annotations

from dataclasses import fields

import pytest
import requests

from orchestrator.catalog_sync import CatalogSyncClient, CatalogSyncError, CatalogSyncResult


CANONICAL_CODE = "roe-291"
TOKEN = "never-log-this-token"
ADULT_TEXT = "sensitive-response-subtitle-text"


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
        error: requests.RequestException | None = None,
    ) -> None:
        self.response = response or FakeResponse()
        self.error = error
        self.requests: list[tuple[str, dict[str, object]]] = []

    def post(self, url: str, **kwargs: object) -> FakeResponse:
        self.requests.append((url, kwargs))
        if self.error is not None:
            raise self.error
        return self.response


def sync_with_response(
    status: int,
    *,
    body: object | None = None,
    json_error: Exception | None = None,
    token: str = TOKEN,
) -> CatalogSyncResult:
    return CatalogSyncClient(
        "https://javsubtitle.example/",
        token,
        session=FakeSession(FakeResponse(status, body, json_error=json_error)),
    ).sync(CANONICAL_CODE)


def test_sync_uses_exact_bounded_request_and_returns_frozen_public_result():
    session = FakeSession()

    result = CatalogSyncClient(
        "https://javsubtitle.example/", TOKEN, timeout_seconds=17, session=session
    ).sync("ROE291")

    assert result == CatalogSyncResult(
        canonical_code=CANONICAL_CODE,
        d1_rows_updated=2,
        subtitle_count=3,
        kv_keys_deleted=(
            f"movie:full:{CANONICAL_CODE}",
            f"movie:light:{CANONICAL_CODE}",
        ),
    )
    assert [field.name for field in fields(CatalogSyncResult)] == [
        "canonical_code",
        "d1_rows_updated",
        "subtitle_count",
        "kv_keys_deleted",
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
        )
    ]


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (300, "catalog_redirect_rejected"),
        (302, "catalog_redirect_rejected"),
        (399, "catalog_redirect_rejected"),
        (401, "catalog_auth_failed"),
        (403, "catalog_auth_failed"),
        (200 + 7, "catalog_sync_failed"),
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
    secret_url = f"https://user:{TOKEN}@javsubtitle.example/private"
    session = FakeSession(error=requests.ConnectionError(f"failed {secret_url} {ADULT_TEXT}"))

    with pytest.raises(CatalogSyncError) as raised:
        CatalogSyncClient("https://javsubtitle.example", TOKEN, session=session).sync(
            CANONICAL_CODE
        )

    assert raised.value.reason_code == "catalog_fetch_failed"
    assert str(raised.value) == "catalog_fetch_failed"
    assert raised.value.__cause__ is None
    assert TOKEN not in repr(raised.value)
    assert ADULT_TEXT not in repr(raised.value)


@pytest.mark.parametrize(
    ("body", "json_error"),
    [
        ([], None),
        ("not-an-object", None),
        (None, ValueError(f"invalid JSON containing {ADULT_TEXT}")),
        (None, TypeError(f"invalid JSON containing {ADULT_TEXT}")),
    ],
)
def test_invalid_json_or_non_object_response_is_rejected_safely(body, json_error):
    with pytest.raises(CatalogSyncError) as raised:
        sync_with_response(200, body=body, json_error=json_error)

    assert raised.value.reason_code == "catalog_response_invalid"
    assert raised.value.__cause__ is None
    assert ADULT_TEXT not in repr(raised.value)


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
        "https://javsubtitle.example/path?token=private",
        "https://javsubtitle.example/path#fragment",
    ],
)
def test_base_url_validation_fails_closed_without_leaking_input(base_url: str):
    with pytest.raises(ValueError) as raised:
        CatalogSyncClient(base_url, TOKEN, session=FakeSession())

    assert str(raised.value) == "catalog API base URL is invalid"
    assert TOKEN not in repr(raised.value)


@pytest.mark.parametrize(
    "base_url",
    [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://[::1]:3000",
        "https://javsubtitle.example/base",
    ],
)
def test_base_url_accepts_https_and_explicit_localhost_http(base_url: str):
    client = CatalogSyncClient(base_url, TOKEN, session=FakeSession())

    assert client.sync(CANONICAL_CODE).canonical_code == CANONICAL_CODE


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

    with pytest.raises(ValueError) as raised:
        CatalogSyncClient("https://javsubtitle.example", TOKEN, session=session).sync(
            ADULT_TEXT
        )

    assert str(raised.value) == "invalid movie code"
    assert ADULT_TEXT not in repr(raised.value)
    assert session.requests == []
