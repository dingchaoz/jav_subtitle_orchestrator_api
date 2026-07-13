import re
from pathlib import Path


# These are migration static contract guards. They read SQL text but do not compile
# or execute PostgreSQL. Real schema, ACL, and concurrency behavior must be verified
# at the Task 10 approval gate against a disposable/local or explicitly approved target.
ROOT = Path(__file__).resolve().parents[1]


def migration_sql() -> str:
    migrations = list(
        (ROOT / "supabase" / "migrations").glob("*_ensure_subtitle_movie.sql")
    )
    assert len(migrations) == 1, "expected exactly one ensure_subtitle_movie migration"
    return migrations[0].read_text(encoding="utf-8").lower()


def compact_sql() -> str:
    return re.sub(r"\s+", " ", migration_sql())


def test_migration_contains_locked_down_security_definer_rpc():
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
    signature = "public.ensure_subtitle_movie(text, jsonb)"
    assert (
        f"revoke execute on function {signature} "
        "from public, anon, authenticated;"
    ) in sql
    assert f"grant execute on function {signature} to service_role;" in sql
    execute_grantees = re.findall(
        rf"grant\s+execute\s+on\s+function\s+{re.escape(signature)}\s+"
        r"to\s+([^;]+);",
        sql,
    )
    assert execute_grantees == ["service_role"]
    assert not re.search(
        rf"grant\s+execute\s+on\s+function\s+{re.escape(signature)}\s+"
        r"to\s+[^;]*\b(public|anon|authenticated)\b[^;]*;",
        sql,
    )


def test_migration_contains_safe_canonical_movie_code_parsing():
    sql = compact_sql()
    assert "lower(" in sql and "btrim(" in sql
    assert "^[a-z]+-[0-9]+$" in sql
    assert "split_part(" in sql
    assert "invalid_movie_code" in sql
    assert "22023" in sql
    assert re.search(r"movie_number[^;]{0,500}(bigint|numeric)", sql)
    assert re.search(r"movie_number[^;]{0,700}>\s*2147483647", sql)


def test_migration_contains_numeric_alias_normalization_before_lookups():
    sql = compact_sql()
    normalization = re.search(
        r"v_movie_number\s*:=\s*v_movie_number_bigint::integer\s*;"
        r"(?P<body>.*?)select\s+p\.id",
        sql,
    )
    assert normalization, "canonical normalization must precede the public lookup"
    assert re.search(
        r"v_code\s*:=\s*v_series\s*\|\|\s*'-'\s*\|\|\s*case\s+when\s+"
        r"v_movie_number\s*<\s*100\s+then\s+pg_catalog\.lpad\(\s*"
        r"v_movie_number::text\s*,\s*3\s*,\s*'0'\s*\)\s+else\s+"
        r"v_movie_number::text\s+end\s*;",
        normalization.group("body"),
    )
    assert sql.index("v_code :=") < sql.index("from public.movies")
    assert sql.index("v_code :=") < sql.index("from missav.movies")
    assert "coalesce(v_missav_title, v_local_title, v_code)" in sql
    assert "= v_code" in sql
    assert re.search(
        r"'canonical_code'\s*,\s*coalesce\([^)]*v_code",
        sql,
    )


def test_migration_contains_existing_public_provenance_without_dead_variables():
    sql = compact_sql()
    provenance = re.search(
        r"if\s+v_missav_found\s+then(?P<body>.*?)"
        r"return\s+pg_catalog\.jsonb_build_object",
        sql,
    )
    assert provenance
    assert re.search(
        r"elsif\s+v_existing_id\s+is\s+not\s+null.*?"
        r"v_source\s*:=\s*'public'\s*;.*?"
        r"v_status\s*:=\s*'complete'\s*;",
        provenance.group("body"),
    )
    assert re.search(
        r"select\s+p\.id\s+into\s+v_existing_id\s+from\s+public\.movies",
        sql,
    )
    for dead_variable in (
        "v_existing_title",
        "v_existing_movie_code",
        "v_existing_standard_code",
    ):
        assert dead_variable not in sql


def test_migration_contains_ranked_missav_and_local_metadata_allowlist():
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


def test_migration_contains_catalog_upsert_with_title_protection():
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
    upsert = re.search(
        r"on\s+conflict\s*\(movie_id\)\s+do\s+update\s+"
        r"(?P<body>.*?)\s+returning\s+",
        sql,
    )
    assert upsert
    title_case = re.search(
        r"set\s+title\s*=\s*case\s+when\s+"
        r"(?P<conditions>.*?)\s+then\s+excluded\.title\s+"
        r"else\s+public\.movies\.title\s+end\s*,",
        upsert.group("body"),
    )
    assert title_case
    conditions = title_case.group("conditions")
    assert "public.movies.title is null" in conditions
    assert (
        "pg_catalog.lower(pg_catalog.btrim(public.movies.title)) = v_code"
        in conditions
    )
    assert (
        "pg_catalog.lower(pg_catalog.btrim(public.movies.title)) "
        "= public.movies.movie_id"
    ) in conditions
    assert (
        "pg_catalog.lower(pg_catalog.btrim(public.movies.title)) "
        "= public.movies.standard_movie_id"
    ) in conditions
    assert "canonical_code" in sql


def test_migration_contains_exact_catalog_json_contract_and_markers():
    sql = compact_sql()
    result = re.search(
        r"return\s+pg_catalog\.jsonb_build_object\((?P<body>.*?)\)\s*;\s*"
        r"end\s*;\s*\$\$",
        sql,
    )
    assert result
    assert re.findall(r"'([^']+)'\s*,", result.group("body")) == [
        "movie_uuid",
        "canonical_code",
        "metadata_status",
        "metadata_source",
    ]
    for status in ("'complete'", "'partial'", "'placeholder'"):
        assert status in sql
    for source in ("'public'", "'missav'", "'local'", "'placeholder'"):
        assert source in sql


def test_migration_contains_no_out_of_scope_database_changes_or_dynamic_sql():
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


def test_migration_contains_schema_qualified_security_definer_references():
    sql = compact_sql()
    assert "public.movies" in sql
    assert "missav.movies" in sql
    assert "missav.makers" in sql
    assert "jsonb_build_object(" in sql
    assert "pg_catalog.jsonb_build_object(" in sql
