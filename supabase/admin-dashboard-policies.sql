-- Run in the Supabase SQL editor for the project used by public/admin/index.html.
-- Keeps public reads available while allowing only the configured admin email to write.

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

alter table public.blurbs
add column if not exists rt_url_override text;

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
