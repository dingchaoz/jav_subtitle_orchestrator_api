import json

import pytest

from orchestrator.movie_catalog import (
    SupabaseMovieCatalogEnsurer,
    load_publish_metadata,
)
from orchestrator.movie_code import canonical_movie_code


MOVIE_UUID = "87b76a84-e4c6-4416-bf77-e598997dce5c"


class FakeResponse:
    def __init__(self, payload, *, status_code=200):
        self._payload = payload
        self.status_code = status_code

    @property
    def ok(self):
        return self.status_code < 400

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.response


def test_canonical_movie_code_regression():
    assert canonical_movie_code("MIST166") == "mist-166"
    assert canonical_movie_code(" ABC-7 ") == "abc-007"

    with pytest.raises(ValueError, match="invalid movie code"):
        canonical_movie_code("123-abc")


def test_load_publish_metadata_allows_only_bounded_valid_fields(tmp_path):
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "number": "MIST166",
                "title": f"  {'x' * 501}  ",
                "release_date": "2024-02-29",
                "duration": "about 125 minutes",
                "description": "must not escape",
                "unexpected": "must not escape",
            }
        ),
        encoding="utf-8",
    )

    assert load_publish_metadata(metadata_path, "mist-166") == {
        "number": "mist-166",
        "title": "x" * 500,
        "release_date": "2024-02-29",
        "duration_minutes": 125,
    }


@pytest.mark.parametrize(
    "content",
    [
        None,
        "not json",
        json.dumps([]),
        json.dumps({"number": "other-123", "title": "Wrong movie"}),
        json.dumps({"number": "mist-166", "description": "only blocked data"}),
    ],
)
def test_load_publish_metadata_returns_empty_for_missing_or_unusable_metadata(
    tmp_path, content
):
    metadata_path = tmp_path / "metadata.json"
    if content is not None:
        metadata_path.write_text(content, encoding="utf-8")

    assert load_publish_metadata(metadata_path, "mist-166") == {}


def test_load_publish_metadata_drops_invalid_optional_values(tmp_path):
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "number": "mist-166",
                "title": 123,
                "release_date": "2023-02-29",
                "duration": "runtime 1441 minutes",
            }
        ),
        encoding="utf-8",
    )

    assert load_publish_metadata(metadata_path, "mist-166") == {}


def test_load_publish_metadata_ignores_overlong_duration_digits(tmp_path):
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "number": "mist-166",
                "title": "Local title",
                "duration": "9" * 5000,
            }
        ),
        encoding="utf-8",
    )

    assert load_publish_metadata(metadata_path, "mist-166") == {
        "number": "mist-166",
        "title": "Local title",
    }


def test_load_publish_metadata_uses_requested_number_when_number_is_absent(tmp_path):
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(
        json.dumps({"title": "  Local title  "}),
        encoding="utf-8",
    )

    assert load_publish_metadata(metadata_path, "mist-166") == {
        "number": "mist-166",
        "title": "Local title",
    }


def test_ensure_movie_posts_exact_rpc_request_and_accepts_placeholder(tmp_path):
    metadata_path = tmp_path / "metadata.json"
    response = FakeResponse(
        {
            "movie_uuid": MOVIE_UUID,
            "canonical_code": "mist-166",
            "metadata_status": "placeholder",
            "metadata_source": "placeholder",
        }
    )
    session = FakeSession(response)
    ensurer = SupabaseMovieCatalogEnsurer(
        "https://example.supabase.co/", "secret-service-key", session=session
    )

    result = ensurer.ensure_movie("MIST166", metadata_path)

    assert result.movie_uuid == MOVIE_UUID
    assert result.canonical_code == "mist-166"
    assert result.metadata_status == "placeholder"
    assert result.metadata_source == "placeholder"
    assert session.calls == [
        (
            "POST",
            "https://example.supabase.co/rest/v1/rpc/ensure_subtitle_movie",
            {
                "headers": {
                    "apikey": "secret-service-key",
                    "Authorization": "Bearer secret-service-key",
                    "Content-Type": "application/json",
                },
                "json": {
                    "p_movie_code": "mist-166",
                    "p_local_metadata": {},
                },
                "timeout": 30,
                "allow_redirects": False,
            },
        )
    ]


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {
            "movie_uuid": "not-a-uuid",
            "canonical_code": "mist-166",
            "metadata_status": "complete",
            "metadata_source": "local",
        },
        {
            "movie_uuid": MOVIE_UUID,
            "canonical_code": "other-166",
            "metadata_status": "complete",
            "metadata_source": "local",
        },
        {
            "movie_uuid": MOVIE_UUID,
            "canonical_code": "mist-166",
            "metadata_status": "unknown",
            "metadata_source": "local",
        },
        {
            "movie_uuid": MOVIE_UUID,
            "canonical_code": "mist-166",
            "metadata_status": "complete",
            "metadata_source": "unknown",
        },
        {
            "movie_uuid": MOVIE_UUID,
            "canonical_code": "mist-166",
            "metadata_status": [],
            "metadata_source": "local",
        },
    ],
)
def test_ensure_movie_rejects_malformed_rpc_responses(tmp_path, payload):
    ensurer = SupabaseMovieCatalogEnsurer(
        "https://example.supabase.co",
        "secret-service-key",
        session=FakeSession(FakeResponse(payload)),
    )

    with pytest.raises(
        RuntimeError, match=r"^catalog ensure returned invalid response$"
    ):
        ensurer.ensure_movie("mist-166", tmp_path / "missing.json")


def test_ensure_movie_rejects_non_ok_response(tmp_path):
    ensurer = SupabaseMovieCatalogEnsurer(
        "https://example.supabase.co",
        "secret-service-key",
        session=FakeSession(FakeResponse({}, status_code=503)),
    )

    with pytest.raises(RuntimeError, match=r"^catalog ensure failed \(503\)$"):
        ensurer.ensure_movie("mist-166", tmp_path / "missing.json")


def test_ensure_movie_rejects_redirect_response(tmp_path):
    ensurer = SupabaseMovieCatalogEnsurer(
        "https://example.supabase.co",
        "secret-service-key",
        session=FakeSession(FakeResponse({}, status_code=302)),
    )

    with pytest.raises(RuntimeError, match=r"^catalog ensure failed \(302\)$"):
        ensurer.ensure_movie("mist-166", tmp_path / "missing.json")
