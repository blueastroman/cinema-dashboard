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
  select lower(coalesce(auth.email(), auth.jwt() ->> 'email', '')) = 'danymora131@hotmail.com'
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
  referrer text,
  user_agent text,
  visited_at timestamptz default now(),
  visit_date date default current_date
);

alter table public.hidden_movies enable row level security;
alter table public.watched_movies enable row level security;
alter table public.seen_movies enable row level security;
alter table public.site_visits enable row level security;

alter table public.blurbs
add column if not exists rt_url_override text;

alter table public.site_visits
add column if not exists ip_address text;

alter table public.site_visits
add column if not exists country_code text;

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
grant insert on table public.site_visits to anon, authenticated;
grant usage, select on sequence public.site_visits_id_seq to anon, authenticated;

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
create policy "record site visits"
on public.site_visits
for insert
to anon, authenticated
with check (true);

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
      'unique_visitors', (select count(distinct visitor_id || '|' || coalesce(ip_address, ''))::int from public.site_visits),
      'unique_signed_in_users', (select count(distinct user_id)::int from public.site_visits where user_id is not null),
      'today', (select count(*)::int from public.site_visits where visited_at >= date_trunc('day', now())),
      'unique_today', (select count(distinct visitor_id || '|' || coalesce(ip_address, ''))::int from public.site_visits where visited_at >= date_trunc('day', now())),
      'last_7d', (select count(*)::int from public.site_visits where visited_at >= now() - interval '7 days'),
      'unique_7d', (select count(distinct visitor_id || '|' || coalesce(ip_address, ''))::int from public.site_visits where visited_at >= now() - interval '7 days'),
      'last_30d', (select count(*)::int from public.site_visits where visited_at >= now() - interval '30 days'),
      'unique_30d', (select count(distinct visitor_id || '|' || coalesce(ip_address, ''))::int from public.site_visits where visited_at >= now() - interval '30 days')
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

create or replace function public.get_visitor_locations()
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

  select coalesce(jsonb_agg(
    jsonb_build_object('country', country_code, 'count', cnt)
    order by cnt desc
  ), '[]'::jsonb)
  from (
    select
      country_code,
      count(distinct visitor_id || '|' || coalesce(ip_address, ''))::int as cnt
    from public.site_visits
    where country_code is not null
    group by country_code
    order by cnt desc
    limit 100
  ) t
  into result;

  return result;
end;
$$;

grant execute on function public.get_visitor_locations() to authenticated;

select pg_notify('pgrst', 'reload schema');
