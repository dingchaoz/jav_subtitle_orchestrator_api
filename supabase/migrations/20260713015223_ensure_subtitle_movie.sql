create or replace function public.ensure_subtitle_movie(
  p_movie_code text,
  p_local_metadata jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_code text := pg_catalog.lower(pg_catalog.btrim(p_movie_code));
  v_series text;
  v_number_text text;
  v_number_significant text;
  v_movie_number_bigint bigint;
  v_movie_number integer;
  v_local jsonb := case
    when pg_catalog.jsonb_typeof(p_local_metadata) = 'object' then p_local_metadata
    else '{}'::jsonb
  end;
  v_local_title text;
  v_local_release_date date;
  v_local_duration integer;
  v_missav_found boolean := false;
  v_missav_title text;
  v_missav_release_date date;
  v_missav_studio text;
  v_existing_id uuid;
  v_title text;
  v_studio text;
  v_release_year integer;
  v_movie_uuid uuid;
  v_final_title text;
  v_final_studio text;
  v_final_release_year integer;
  v_final_movie_code text;
  v_final_standard_code text;
  v_source text;
  v_status text;
begin
  if v_code is null or v_code !~ '^[a-z]+-[0-9]+$' then
    raise exception using
      message = 'invalid_movie_code',
      errcode = '22023';
  end if;

  v_series := pg_catalog.split_part(v_code, '-', 1);
  v_number_text := pg_catalog.split_part(v_code, '-', 2);

  v_number_significant := pg_catalog.ltrim(v_number_text, '0');
  if v_number_significant = '' then
    v_number_significant := '0';
  end if;

  if pg_catalog.length(v_number_significant) > 10 then
    raise exception using
      message = 'invalid_movie_code',
      errcode = '22023';
  end if;

  v_movie_number_bigint := v_number_significant::bigint;
  if v_movie_number_bigint < 0 or v_movie_number_bigint > 2147483647 then
    raise exception using
      message = 'invalid_movie_code',
      errcode = '22023';
  end if;
  v_movie_number := v_movie_number_bigint::integer;
  v_code := v_series || '-' || case
    when v_movie_number < 100
      then pg_catalog.lpad(v_movie_number::text, 3, '0')
    else v_movie_number::text
  end;

  select p.id
    into v_existing_id
  from public.movies as p
  where p.standard_movie_id = v_code or p.movie_id = v_code
  order by case when p.standard_movie_id = v_code then 0 else 1 end
  limit 1;

  select
    true,
    nullif(pg_catalog.left(pg_catalog.btrim(m.title), 500), ''),
    m.release_date,
    nullif(pg_catalog.left(pg_catalog.btrim(mk.name), 255), '')
    into
      v_missav_found,
      v_missav_title,
      v_missav_release_date,
      v_missav_studio
  from missav.movies as m
  left join missav.makers as mk on mk.id = m.maker_id
  where pg_catalog.lower(m.number) = v_code
  order by
    m.published desc nulls last,
    m.release_date desc nulls last,
    m.id desc
  limit 1;

  if pg_catalog.jsonb_typeof(v_local -> 'title') = 'string' then
    v_local_title := nullif(
      pg_catalog.left(pg_catalog.btrim(v_local ->> 'title'), 500),
      ''
    );
  end if;

  if pg_catalog.jsonb_typeof(v_local -> 'release_date') = 'string'
     and (v_local ->> 'release_date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then
    begin
      v_local_release_date := (v_local ->> 'release_date')::date;
    exception when others then
      v_local_release_date := null;
    end;
  end if;

  if pg_catalog.jsonb_typeof(v_local -> 'duration_minutes') in ('number', 'string')
     and (v_local ->> 'duration_minutes') ~ '^[0-9]{1,4}$' then
    begin
      v_local_duration := (v_local ->> 'duration_minutes')::integer;
      if v_local_duration not between 1 and 1440 then
        v_local_duration := null;
      end if;
    exception when others then
      v_local_duration := null;
    end;
  end if;

  v_title := coalesce(v_missav_title, v_local_title, v_code);
  v_studio := v_missav_studio;
  v_release_year := pg_catalog.date_part(
    'year',
    coalesce(v_missav_release_date, v_local_release_date)
  )::integer;

  insert into public.movies (
    series,
    movie_number,
    title,
    studio,
    release_year,
    duration_minutes
  )
  values (
    v_series,
    v_movie_number,
    v_title,
    v_studio,
    v_release_year,
    v_local_duration
  )
  on conflict (movie_id) do update
  set
    title = case
      when public.movies.title is null
        or pg_catalog.lower(pg_catalog.btrim(public.movies.title)) = v_code
        or pg_catalog.lower(pg_catalog.btrim(public.movies.title)) = public.movies.movie_id
        or pg_catalog.lower(pg_catalog.btrim(public.movies.title)) = public.movies.standard_movie_id
      then excluded.title
      else public.movies.title
    end,
    studio = coalesce(public.movies.studio, excluded.studio),
    release_year = coalesce(
      public.movies.release_year,
      excluded.release_year
    ),
    duration_minutes = coalesce(
      public.movies.duration_minutes,
      excluded.duration_minutes
    )
  returning
    public.movies.id,
    public.movies.title,
    public.movies.studio,
    public.movies.release_year,
    public.movies.movie_id,
    public.movies.standard_movie_id
  into
    v_movie_uuid,
    v_final_title,
    v_final_studio,
    v_final_release_year,
    v_final_movie_code,
    v_final_standard_code;

  if v_missav_found then
    v_source := 'missav';
    if v_final_title is not null
       and pg_catalog.lower(pg_catalog.btrim(v_final_title)) <> v_code
       and pg_catalog.lower(pg_catalog.btrim(v_final_title))
         is distinct from v_final_movie_code
       and pg_catalog.lower(pg_catalog.btrim(v_final_title))
         is distinct from v_final_standard_code
       and v_final_studio is not null
       and v_final_release_year is not null then
      v_status := 'complete';
    else
      v_status := 'partial';
    end if;
  elsif v_local_title is not null
     or v_local_release_date is not null
     or v_local_duration is not null then
    v_source := 'local';
    v_status := 'partial';
  elsif v_existing_id is not null
     and v_final_title is not null
     and pg_catalog.lower(pg_catalog.btrim(v_final_title)) <> v_code
     and pg_catalog.lower(pg_catalog.btrim(v_final_title))
       is distinct from v_final_movie_code
     and pg_catalog.lower(pg_catalog.btrim(v_final_title))
       is distinct from v_final_standard_code then
    v_source := 'public';
    v_status := 'complete';
  else
    v_source := 'placeholder';
    v_status := 'placeholder';
  end if;

  return pg_catalog.jsonb_build_object(
    'movie_uuid', v_movie_uuid,
    'canonical_code', coalesce(
      v_final_standard_code,
      v_final_movie_code,
      v_code
    ),
    'metadata_status', v_status,
    'metadata_source', v_source
  );
end;
$$;

revoke execute on function public.ensure_subtitle_movie(text, jsonb)
from public, anon, authenticated;

grant execute on function public.ensure_subtitle_movie(text, jsonb)
to service_role;
