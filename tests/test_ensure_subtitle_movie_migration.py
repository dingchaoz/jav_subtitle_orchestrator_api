import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def migration_sql() -> str:
    migrations = list(
        (ROOT / "supabase" / "migrations").glob("*_ensure_subtitle_movie.sql")
    )
    assert len(migrations) == 1, "expected exactly one ensure_subtitle_movie migration"
    return migrations[0].read_text(encoding="utf-8").lower()


def compact_sql() -> str:
    return re.sub(r"\s+", " ", migration_sql())


def test_declares_locked_down_security_definer_rpc():
    sql = compact_sql()
    assert re.search(
        r"create\s+or\s+replace\s+function\s+public\.ensure_subtitle_movie\s*\(\s*"
        r"p_movie_code\s+text\s*,\s*p_local_metadata\s+jsonb\s+default\s+'\{\}'::jsonb\s*\)"
        r"\s+returns\s+jsonb",
        sql,
    )
    assert "language plpgsql" in sql
    assert "security definer" in sql
    assert "set search_path = ''" in sql
    assert (
        "revoke execute on function public.ensure_subtitle_movie(text, jsonb) "
        "from public, anon, authenticated"
    ) in sql
    assert (
        "grant execute on function public.ensure_subtitle_movie(text, jsonb) "
        "to service_role"
    ) in sql


def test_validates_and_parses_canonical_movie_code_safely():
    sql = compact_sql()
    assert "lower(" in sql and "btrim(" in sql
    assert "^[a-z]+-[0-9]+$" in sql
    assert "split_part(" in sql
    assert "invalid_movie_code" in sql
    assert "22023" in sql
    assert re.search(r"movie_number[^;]{0,500}(bigint|numeric)", sql)
    assert re.search(r"movie_number[^;]{0,700}>\s*2147483647", sql)


def test_reads_ranked_missav_metadata_and_local_allowlist():
    sql = compact_sql()
    assert "from missav.movies" in sql
    assert "left join missav.makers" in sql
    assert "lower(m.number)" in sql
    assert re.search(
        r"order by\s+m\.published\s+desc[^;]+m\.release_date\s+desc\s+nulls\s+last"
        r"[^;]+m\.id\s+desc",
        sql,
    )
    assert "p_local_metadata" in sql
    assert "duration_minutes" in sql
    assert "release_date" in sql
    assert "jsonb_typeof(" in sql
    assert re.search(r"(left|substring)\([^;]{0,200}title[^;]{0,200}500", sql)
    assert re.search(r"duration[^;]{0,700}between\s+1\s+and\s+1440", sql)


def test_upserts_public_movie_atomically_without_overwriting_real_metadata():
    sql = compact_sql()
    assert "insert into public.movies" in sql
    assert "standard_movie_id" in sql
    for column in (
        "series",
        "movie_number",
        "title",
        "studio",
        "release_year",
        "duration_minutes",
    ):
        assert column in sql
    assert "on conflict (movie_id) do update" in sql
    assert "excluded." in sql
    assert "public.movies.title" in sql
    assert "returning" in sql
    assert "canonical_code" in sql


def test_returns_exact_catalog_result_contract_and_placeholder_markers():
    sql = compact_sql()
    for key in (
        "'movie_uuid'",
        "'canonical_code'",
        "'metadata_status'",
        "'metadata_source'",
    ):
        assert key in sql
    for status in ("'complete'", "'partial'", "'placeholder'"):
        assert status in sql
    for source in ("'public'", "'missav'", "'local'", "'placeholder'"):
        assert source in sql


def test_has_no_out_of_scope_database_changes_or_dynamic_sql():
    sql = compact_sql()
    assert "create trigger" not in sql
    assert "alter table" not in sql
    assert "create policy" not in sql
    assert "enable row level security" not in sql
    assert "storage." not in sql
    assert "movie_languages" not in sql
    assert "execute " not in sql.replace("revoke execute", "").replace(
        "grant execute", ""
    )


def test_security_definer_references_are_schema_qualified():
    sql = compact_sql()
    assert "public.movies" in sql
    assert "missav.movies" in sql
    assert "missav.makers" in sql
    assert "jsonb_build_object(" in sql
    assert "pg_catalog.jsonb_build_object(" in sql
