-- Run this whole file in the Supabase SQL editor for the project used by
-- public/index.html and public/admin/index.html.
-- Keeps public reads available while allowing only the configured admin email to write.
-- Analytics stay at zero until the site_visits grants and RLS policy below are
-- applied to the live project.

create or replace function public.is_cinema_admin()
returns boolean
language sql
stable
as $$
  select lower(coalesce(auth.jwt() ->> 'email', '')) = 'danymora131@hotmail.com'
$$;

alter table public.blurbs enable row level security;
alter table public.picks enable row level security;
alter table public.site_hidden enable row level security;

create table if not exists public.hidden_movies (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references auth.users not null,
  movie_id text not null,
  title text,
  created_at timestamptz default now(),
  unique(user_id, movie_id)
);

create table if not exists public.watched_movies (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references auth.users not null,
  movie_id text not null,
  title text,
  created_at timestamptz default now(),
  unique(user_id, movie_id)
);

create table if not exists public.seen_movies (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references auth.users not null,
  movie_id text not null,
  title text,
  created_at timestamptz default now(),
  unique(user_id, movie_id)
);

create table if not exists public.site_visits (
  id bigserial primary key,
  visitor_id text not null,
  user_id uuid references auth.users,
  path text,
  referrer_host text,
  client_hint text,
  visit_fingerprint text,
  visited_at timestamptz default now(),
  visit_date date default current_date
);

create table if not exists public.coming_soon_overrides (
  movie_key text primary key,
  source_title text not null default '',
  disabled boolean not null default false,
  title_override text,
  release_date_override date,
  poster_override text,
  synopsis_override text,
  director_override text,
  genres_override text[],
  studio_override text,
  release_scale_override text,
  letterboxd_url_override text,
  updated_at timestamptz not null default now()
);

alter table public.hidden_movies enable row level security;
alter table public.watched_movies enable row level security;
alter table public.seen_movies enable row level security;
alter table public.site_visits enable row level security;
alter table public.coming_soon_overrides enable row level security;
alter table public.site_visits add column if not exists referrer_host text;
alter table public.site_visits add column if not exists client_hint text;
alter table public.site_visits add column if not exists visit_fingerprint text;
alter table public.site_visits drop column if exists referrer;
alter table public.site_visits drop column if exists user_agent;

create index if not exists site_visits_visit_date_idx
on public.site_visits (visit_date);

create index if not exists site_visits_path_visit_date_idx
on public.site_visits (path, visit_date);

create index if not exists site_visits_fingerprint_visited_at_idx
on public.site_visits (visit_fingerprint, visited_at desc);

alter table public.blurbs
add column if not exists rt_url_override text,
add column if not exists letterboxd_url_override text,
add column if not exists imdb_id_override text,
add column if not exists poster_url_override text;

select pg_notify('pgrst', 'reload schema');

grant usage on schema public to anon, authenticated;
grant execute on function public.is_cinema_admin() to authenticated;

grant select on table public.blurbs to anon, authenticated;
grant insert, update, delete on table public.blurbs to authenticated;

grant select on table public.picks to anon, authenticated;
grant insert, update, delete on table public.picks to authenticated;

grant select on table public.site_hidden to anon, authenticated;
grant insert, update, delete on table public.site_hidden to authenticated;

grant select, insert, update, delete on table public.hidden_movies to authenticated;
grant select, insert, update, delete on table public.watched_movies to authenticated;
grant select, insert, update, delete on table public.seen_movies to authenticated;
grant select on table public.coming_soon_overrides to anon, authenticated;
grant insert, update, delete on table public.coming_soon_overrides to authenticated;
revoke insert on table public.site_visits from anon, authenticated;
revoke usage, select on sequence public.site_visits_id_seq from anon, authenticated;

drop policy if exists "public read blurbs" on public.blurbs;
create policy "public read blurbs"
on public.blurbs
for select
to anon, authenticated
using (true);

drop policy if exists "admin write blurbs" on public.blurbs;
create policy "admin write blurbs"
on public.blurbs
for all
to authenticated
using (public.is_cinema_admin())
with check (public.is_cinema_admin());

drop policy if exists "public read picks" on public.picks;
create policy "public read picks"
on public.picks
for select
to anon, authenticated
using (true);

drop policy if exists "admin write picks" on public.picks;
create policy "admin write picks"
on public.picks
for all
to authenticated
using (public.is_cinema_admin())
with check (public.is_cinema_admin());

drop policy if exists "public read site_hidden" on public.site_hidden;
create policy "public read site_hidden"
on public.site_hidden
for select
to anon, authenticated
using (true);

drop policy if exists "admin write site_hidden" on public.site_hidden;
create policy "admin write site_hidden"
on public.site_hidden
for all
to authenticated
using (public.is_cinema_admin())
with check (public.is_cinema_admin());

drop policy if exists "public read coming soon overrides" on public.coming_soon_overrides;
create policy "public read coming soon overrides"
on public.coming_soon_overrides
for select
to anon, authenticated
using (true);

drop policy if exists "admin write coming soon overrides" on public.coming_soon_overrides;
create policy "admin write coming soon overrides"
on public.coming_soon_overrides
for all
to authenticated
using (public.is_cinema_admin())
with check (public.is_cinema_admin());

drop policy if exists "own hidden_movies rows" on public.hidden_movies;
drop policy if exists "own rows" on public.hidden_movies;
create policy "own hidden_movies rows"
on public.hidden_movies
for all
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "own watched_movies rows" on public.watched_movies;
drop policy if exists "own rows" on public.watched_movies;
create policy "own watched_movies rows"
on public.watched_movies
for all
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "own seen_movies rows" on public.seen_movies;
drop policy if exists "own rows" on public.seen_movies;
create policy "own seen_movies rows"
on public.seen_movies
for all
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists "record site visits" on public.site_visits;
drop function if exists public.record_site_visit(text, uuid, text, text, text, text);
create or replace function public.record_site_visit(
  p_visitor_id text,
  p_user_id uuid,
  p_path text,
  p_referrer_host text,
  p_client_hint text,
  p_visit_fingerprint text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  normalized_path text;
  recent_count integer;
  inserted_id bigint;
begin
  normalized_path := left(nullif(trim(coalesce(p_path, '')), ''), 255);
  if normalized_path is null then
    normalized_path := '/';
  end if;

  if nullif(trim(coalesce(p_visitor_id, '')), '') is null then
    return jsonb_build_object('accepted', false, 'reason', 'invalid_visitor');
  end if;

  if nullif(trim(coalesce(p_visit_fingerprint, '')), '') is null then
    return jsonb_build_object('accepted', false, 'reason', 'missing_fingerprint');
  end if;

  select count(*)::int
  into recent_count
  from public.site_visits
  where visit_fingerprint = p_visit_fingerprint
    and path = normalized_path
    and visited_at >= now() - interval '15 minutes';

  if recent_count >= 5 then
    return jsonb_build_object('accepted', false, 'reason', 'rate_limited');
  end if;

  if exists (
    select 1
    from public.site_visits
    where visit_fingerprint = p_visit_fingerprint
      and path = normalized_path
      and visit_date = current_date
  ) then
    return jsonb_build_object('accepted', false, 'reason', 'already_recorded_today');
  end if;

  insert into public.site_visits (
    visitor_id,
    user_id,
    path,
    referrer_host,
    client_hint,
    visit_fingerprint
  ) values (
    left(trim(p_visitor_id), 128),
    p_user_id,
    normalized_path,
    left(nullif(trim(coalesce(p_referrer_host, '')), ''), 255),
    left(nullif(trim(coalesce(p_client_hint, '')), ''), 64),
    left(trim(p_visit_fingerprint), 128)
  )
  returning id into inserted_id;

  return jsonb_build_object('accepted', true, 'id', inserted_id);
end;
$$;

grant execute on function public.record_site_visit(text, uuid, text, text, text, text) to anon, authenticated;

drop function if exists public.purge_old_site_visits(integer);
create or replace function public.purge_old_site_visits(retention_days integer default 90)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  deleted_count integer;
begin
  delete from public.site_visits
  where visited_at < now() - make_interval(days => greatest(retention_days, 1));
  get diagnostics deleted_count = row_count;
  return deleted_count;
end;
$$;

drop function if exists public.get_admin_analytics();
drop function if exists public.movie_action_toplist(text);

create or replace function public.movie_action_toplist(table_name text)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  result jsonb;
begin
  if not public.is_cinema_admin() then
    raise exception 'admin access required';
  end if;

  if table_name not in ('hidden_movies', 'watched_movies', 'seen_movies') then
    raise exception 'unsupported analytics table: %', table_name;
  end if;

  execute format(
    $query$
      select coalesce(jsonb_agg(
        jsonb_build_object(
          'movie_id', movie_id,
          'title', title,
          'count', action_count,
          'last_at', last_at
        )
        order by action_count desc, last_at desc
      ), '[]'::jsonb)
      from (
        select
          movie_id,
          coalesce(max(nullif(title, '')), movie_id) as title,
          count(*)::int as action_count,
          max(created_at) as last_at
        from public.%I
        group by movie_id
        order by action_count desc, last_at desc
        limit 20
      ) ranked
    $query$,
    table_name
  )
  into result;

  return result;
end;
$$;

grant execute on function public.movie_action_toplist(text) to authenticated;

create or replace function public.get_admin_analytics()
returns jsonb
language plpgsql
security definer
set search_path = public, auth
as $$
declare
  result jsonb;
begin
  if not public.is_cinema_admin() then
    raise exception 'admin access required';
  end if;

  select jsonb_build_object(
    'generated_at', now(),
    'users', jsonb_build_object(
      'signed_up', (select count(*)::int from auth.users),
      'signed_up_7d', (select count(*)::int from auth.users where created_at >= now() - interval '7 days'),
      'signed_up_30d', (select count(*)::int from auth.users where created_at >= now() - interval '30 days')
    ),
    'visits', jsonb_build_object(
      'total', (select count(*)::int from public.site_visits),
      'unique_visitors', (select count(distinct visitor_id)::int from public.site_visits),
      'unique_signed_in_users', (select count(distinct user_id)::int from public.site_visits where user_id is not null),
      'today', (select count(*)::int from public.site_visits where visited_at >= date_trunc('day', now())),
      'unique_today', (select count(distinct visitor_id)::int from public.site_visits where visited_at >= date_trunc('day', now())),
      'last_7d', (select count(*)::int from public.site_visits where visited_at >= now() - interval '7 days'),
      'unique_7d', (select count(distinct visitor_id)::int from public.site_visits where visited_at >= now() - interval '7 days'),
      'last_30d', (select count(*)::int from public.site_visits where visited_at >= now() - interval '30 days'),
      'unique_30d', (select count(distinct visitor_id)::int from public.site_visits where visited_at >= now() - interval '30 days')
    ),
    'actions', jsonb_build_object(
      'hidden_total', (select count(*)::int from public.hidden_movies),
      'saved_total', (select count(*)::int from public.watched_movies),
      'seen_total', (select count(*)::int from public.seen_movies),
      'hidden_users', (select count(distinct user_id)::int from public.hidden_movies),
      'saved_users', (select count(distinct user_id)::int from public.watched_movies),
      'seen_users', (select count(distinct user_id)::int from public.seen_movies)
    ),
    'top', jsonb_build_object(
      'hidden', public.movie_action_toplist('hidden_movies'),
      'saved', public.movie_action_toplist('watched_movies'),
      'seen', public.movie_action_toplist('seen_movies')
    )
  )
  into result;

  return result;
end;
$$;

grant execute on function public.get_admin_analytics() to authenticated;

select pg_notify('pgrst', 'reload schema');
