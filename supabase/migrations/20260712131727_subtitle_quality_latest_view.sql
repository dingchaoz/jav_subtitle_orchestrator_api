-- Latest persisted finding per catalog subtitle. The view remains private and
-- honors the invoking role's permissions on subtitle_quality_audits.
create view public.subtitle_quality_latest
with (security_invoker = true)
as
select distinct on (audit.subtitle_id)
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
from public.subtitle_quality_audits audit
order by audit.subtitle_id, audit.scanned_at desc, audit.id desc;

revoke all on public.subtitle_quality_latest from public, anon, authenticated;
grant select on public.subtitle_quality_latest to service_role;

comment on view public.subtitle_quality_latest is
  'Private service-role view of the newest persisted finding per subtitle.';

-- Keep catalog metadata outside the exact latest-audit contract. This helper
-- provides the one safe filter column required by the operator API while the
-- API still batch-loads canonical identifiers from movie_languages.
create view public.subtitle_quality_latest_catalog
with (security_invoker = true)
as
select
  latest.id,
  latest.subtitle_id,
  latest.audit_version,
  latest.content_sha256,
  latest.storage_etag,
  latest.status,
  latest.score,
  latest.reason_codes,
  latest.metrics,
  latest.expected_duration_seconds,
  latest.duration_source,
  latest.duration_confidence,
  latest.scanned_at,
  catalog.language
from public.subtitle_quality_latest latest
join public.movie_languages catalog on catalog.id = latest.subtitle_id;

revoke all on public.subtitle_quality_latest_catalog from public, anon, authenticated;
grant select on public.subtitle_quality_latest_catalog to service_role;

comment on view public.subtitle_quality_latest_catalog is
  'Private service-role latest-audit helper with catalog language filtering.';

-- The dashboard needs one bounded aggregate rather than downloading the
-- complete latest view. This remains SECURITY INVOKER: it grants no access
-- beyond what the server-side service role already has.
create or replace function public.subtitle_quality_latest_summary()
returns jsonb
language sql
stable
security invoker
set search_path = public
as $$
  with latest as materialized (
    select id, status, reason_codes, scanned_at
    from public.subtitle_quality_latest
  ),
  status_names(status) as (
    values
      ('pass'::text),
      ('warning'::text),
      ('review'::text),
      ('bad'::text),
      ('invalid'::text),
      ('missing'::text)
  ),
  status_totals as (
    select status, count(*)::bigint as finding_count
    from latest
    group by status
  ),
  reason_rows as (
    select distinct latest.id, reason
    from latest
    cross join lateral unnest(latest.reason_codes) as reasons(reason)
    where reason is not null
      and btrim(reason) <> ''
  ),
  reason_totals as (
    select reason, count(*)::bigint as finding_count
    from reason_rows
    group by reason
  )
  select jsonb_build_object(
    'status_counts', (
      select jsonb_object_agg(names.status, coalesce(totals.finding_count, 0))
      from status_names names
      left join status_totals totals using (status)
    ),
    'reason_counts', coalesce(
      (select jsonb_object_agg(reason, finding_count) from reason_totals),
      '{}'::jsonb
    ),
    'total_audited', (
      select count(*)::bigint from latest
    ),
    'catalog_total', (
      select count(*)::bigint from public.movie_languages
    ),
    'latest_scanned_at', (
      select max(scanned_at) from latest
    )
  );
$$;

revoke execute on function public.subtitle_quality_latest_summary() from public, anon, authenticated;
grant execute on function public.subtitle_quality_latest_summary() to service_role;

comment on function public.subtitle_quality_latest_summary() is
  'Private bounded subtitle-quality status and reason summary for operators.';

