# Cinema Dashboard — Full Codebase Review

**Scope:** entire application — scraper pipeline (`scripts/`), frontend (`public/index.html`, `public/admin/index.html`), serverless APIs (`public/api/`), Supabase policies, GitHub Actions workflows, and the test suite.

**Focus:** movie identification and record matching, external link/ID correctness, data fetching/merging/caching, search/filter/sort/selection state, UI rendering and accessibility, loading/empty/error states, auth/input validation/secrets, async/race conditions, performance, and test coverage. Findings are evidence-based and, where noted, verified by executing the relevant code against the live `public/data.json`.

No code has been changed as part of this review.

---

## 1. Architecture & data-flow overview

**Pipeline (GitHub Actions, daily/scheduled):** `scrape.py` pulls showtimes per theater — SerpAPI, AMC API, Paris API, Alamo Algolia, and regex/BeautifulSoup scrapers for Metrograph, IFC, Film Forum, FLC, Nitehawk, BAM, and Anthology. Each entry is a `(title, theater, day, times, ticket_urls, hint_year?, source_metadata?)` record. `resolve_movie_records` groups entries into movies (grouping key = normalized title + imdbID, else `title|year`), resolves metadata via OMDb (`resolve_omdb_record`) with RT/Letterboxd/TMDb HTML-scrape fallbacks, merges in prior-run metadata and `rating_cache.json`, applies manual overrides, and writes `public/data.json`. `generate_verdicts.py` (Anthropic) adds a per-movie verdict keyed by movie `id` (imdbID or slug). Satellite scripts (`refresh_amc.py`, `refresh_ifc.py`, `refresh_direct_sources.py`, `backfill_ratings.py`, `refresh_recent_ratings.py`) partially rewrite `data.json` between full scrapes. `refresh_coming_soon.py` builds `coming-soon.json` from Box Office Mojo plus OMDb/TMDb/Letterboxd enrichment.

**Frontend:** the static `public/index.html` fetches `data.json` + `coming-soon.json` + several Supabase tables (blurbs, picks, site_hidden, coming-soon overrides — all via the public anon key), de-duplicates movies client-side, and renders three views (Rankings, Theaters, Coming Soon). Movie identity in the browser is `movie.id` (imdbID or slug), but blurbs/score-overrides use a separate `"{title}-{year}"` key, and the saved/hidden lists store *both* id and title. External links (Letterboxd, Rotten Tomatoes, tickets) are constructed at render time with title-slug fallbacks. `public/admin/index.html` is a Supabase-authenticated editor writing to RLS-protected tables; `/api/blurbs` and `/api/site-visit` are Vercel serverless functions holding the real secrets (Anthropic key, Supabase service role key).

**Identity verdict:** the system has one strong identity anchor — imdbID — and honors it well wherever it's present (grouping key, same-IMDB dedupe, verdict cache, `letterboxd.com/imdb/<tt>` links). Everything else falls back to re-deriving identity from the title string at each boundary: previous-dataset merge, rating cache, partial refreshers, frontend dedupe, blurb/override keys, and link construction. Several of those title-only paths are year-blind and reachable in production: **76 of 309 movies in the live `data.json` currently have no imdbID and no year.**

---

## 2. Prioritized findings

Each finding lists severity, confidence, exact location, the failure path, impact, cause, fix, and a regression test.

### F1 — `clean_title` destroys real titles containing format words
**Severity:** High &nbsp;·&nbsp; **Confidence:** Confirmed (executed)

- **Location:** `scripts/cinema_backend/common.py:290-294` (`FORMAT_TAGS`) and `common.py:349` (`clean_title`)
- **Code/behavior:** The second alternative of `FORMAT_TAGS` — `\s*[\(\[]?(70mm|35mm|16mm|imax|rpx|4dx|screenx|4k|dcp)[\)\]]?` — has **no word boundaries**, and the first alternative strips words like `restoration`/`restored`/`digital`/`laser` anywhere in the string, not just as a trailing qualifier.
- **Verified:**
  ```
  clean_title("Climax")        -> "Cl"
  clean_title("Restoration")   -> ""
  clean_title("Digital Man")   -> "Man"
  clean_title("IMAX: Hubble")  -> "Hubble"
  ```
- **Failure path:** Gaspar Noé's *Climax* is scraped as `"Cl"`, then gets metadata-matched against a different film or fails OMDb lookup entirely. A title that clean-titles to `""` is silently dropped by every provider (`if not title: continue`).
- **Impact:** Real repertory titles are lost or mis-identified at the very first transformation step, before any matching logic runs.
- **Cause:** Format-tag stripping is applied globally instead of being end-anchored, unlike the (correctly end-anchored) `SCREENING_SUFFIX_PATTERNS`.
- **Fix:** Anchor format-tag stripping to suffix/bracketed positions only (e.g. `\s*[\(\[](70mm|...)​[\)\]]\s*$` plus explicit `\bin\s+70mm$` forms). Never let the result be empty — fall back to the raw title if stripping empties it.
- **Regression test:**
  ```python
  assert clean_title("Climax") == "Climax"
  assert clean_title("Restoration") == "Restoration"
  assert clean_title("Nosferatu IMAX") == "Nosferatu"
  ```

---

### F2 — Year-blind title matching accepts the wrong same-title film
**Severity:** High &nbsp;·&nbsp; **Confidence:** Confirmed (executed)

- **Location:** `scripts/scrape.py:2029-2078` (`title_match_score` / `title_result_is_compatible`) and `scrape.py:2469-2515` (`is_acceptable_omdb_match`); duplicated in `scripts/backfill_ratings.py:127-144`.
- **Verified:**
  ```
  is_acceptable_omdb_match("Nosferatu", {Title:"Nosferatu", Year:"2024"}, query_year=1922, minimum_score=0.85)
  -> True   (score = 1.32)
  ```
  Even at the strictest threshold in the codebase, a query for the 1922 film accepts the 2024 remake as a match.
- **Failure path:** Every guard built on these functions — OMDb cache validation (`scrape.py:2579`), `enrich_from_rating_cache` (`scrape.py:2417`), the RT fallback page check (`scrape.py:1871`), the Letterboxd fallback (`scrape.py:1908`), the TMDb poster fallback (`scrape.py:1975`) — can attach the remake's metadata to the original screening or vice versa.
- **Impact:** Directly hits the user's stated concern: originals/remakes/rereleases with identical titles (*Nosferatu*, *Suspiria*, *The Fly*, *Crash*, ...) get the wrong poster, ratings, plot, and links.
- **Cause:** A year mismatch is only a small additive penalty (−0.25 for >3 years), not a hard veto, even though the query year is known and reliable.
- **Fix:** When both `query_year` and `result_year` are known and differ by more than 2 years, reject the match outright in both `is_acceptable_omdb_match` and `title_result_is_compatible`, regardless of title score.
- **Regression test:** An exact-title pair 100 years apart must be rejected at every threshold used in the codebase; a ±1-year pair must still pass.

---

### F3 — Previous-dataset carry-forward is indexed by title without year
**Severity:** High &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `scripts/cinema_backend/runtime.py:30-45` (`_load_existing_movie_metadata`) and `:48-62` (`_load_existing_movie_records`) — both key by `exact_title_identity_key(title)`, which omits the year.
- **Failure path:** If yesterday's dataset holds both *Nosferatu* (1922) and *Nosferatu* (2024) as separate records (which the backend deliberately keeps distinct), the index retains only whichever was inserted last. Next scrape, `merge_existing_metadata` (`scrape.py:2146-2194`) backfills missing fields on the 1922 record from that single indexed entry, and `get_existing_movie_record` (`scrape.py:1640`, used at `scrape.py:3034-3044`) can revive the *2024 film's verdict* onto the 1922 screening. The year guard at `scrape.py:2157-2164` only activates when the *current* run's ratings already carry a year — precisely the case that least needs rescuing.
- **Impact:** Wrong poster, wrong review verdict, wrong external links (they derive from imdbID) — and it persists and compounds run over run because the corrupted record becomes next run's "existing" source.
- **Cause:** Yearless index keys collapse distinct films that share a title.
- **Fix:** Key both indexes by `exact_title_identity_key(title, year)`, only falling back to a yearless entry when the title is unique across the dataset.
- **Regression test:** Build a fake previous `data.json` with two same-title, different-year movies; assert `get_existing_metadata`/`get_existing_movie_record` either return nothing or the correct-year record for each query.

---

### F4 — `enrich_from_rating_cache` attaches cached imdbIDs with no year cross-check
**Severity:** High &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `scripts/scrape.py:2409-2425`
- **Code/behavior:** `get_best_cached_match` (`scrape.py:1662-1678`) falls back to the **yearless** cache key. Unlike the main resolver path (`scrape.py:2566-2581`), this function has no `abs(query_year - cached_year) > 2` skip before adopting the cached imdbID — its only protection is the broken matcher from F2. It also runs *after* `resolve_omdb_record` may have already declined a match, so a deliberate rejection can be silently overridden by a stale cache hit.
- **Impact:** `rating_cache.json` is committed to the repo and long-lived, so a single bad resolution becomes a **wrong-identity amplifier**, poisoning every future run for that title until manually purged.
- **Fix:** Apply the year-mismatch veto from F2 to this path. Store the query year alongside cached entries and refuse yearless-key fallback whenever a query year is available.
- **Regression test:** With a cache entry `nosferatu|2024`, an enrichment call with `hint_year=1922` must not adopt it.

---

### F5 — Partial refreshers join showtimes to dataset movies by bare title
**Severity:** High &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `scripts/refresh_amc.py:410-432` (`movie_lookup_keys`, `build_movie_index`) and `scripts/refresh_ifc.py:49-70` (same pattern). `ensure_movie` (`refresh_amc.py:452-455`) attaches to the *first* same-title record found via `setdefault` — i.e., whichever appears earlier in the `movies` array.
- **Failure path:** AMC screens the 2024 *Nosferatu*. If Film Forum's 1922 *Nosferatu* happens to sit earlier in `data.json`, AMC's showtimes, ticket links, and format tags (e.g. "IMAX") attach to the 1922 record instead.
- **Impact:** A direct wrong-movie/wrong-ticket-link association determined by array position — not a title collision edge case, a routine consequence of dataset ordering.
- **Cause:** Yearless lookup keys plus first-match-wins resolution.
- **Fix:** In `ensure_movie`, only accept a yearless key match when it is unambiguous (build the index while counting title collisions); otherwise create a new record and let the next full scrape reconcile it via imdbID.
- **Regression test:** Dataset with two same-title movies of different years; a merged AMC entry carrying a known year must attach to the year-matching record, and an ambiguous no-year entry must create a new record rather than picking array index 0.

---

### F6 — Frontend `deduplicateMovies` merges a yearless variant into any known-year same-title film
**Severity:** High &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `public/index.html:5713-5731`
- **Code/behavior:** A movie with an empty `ratings.year` adopts the year of "the sole known-year version of the canonical title" and is merged into that record — its poster, ratings, verdict, and links now front the *other* film's showtimes. With 76/309 live movies yearless, this fires whenever a repertory original (whose metadata lookup failed) coexists with a current release sharing its title.
- **Impact:** Exactly the reported concern — one card displaying Film A's poster/score/links above Film B's actual showtimes.
- **Fix:** Only merge when the two records' imdbIDs match, or when the yearless variant is a screening-suffix variant of the other (i.e. `getScreeningTitleInfo` finds non-empty attributes). Never merge two records the backend produced as distinct `id`s.
- **Regression test (JS):** Two movies — `{title:"Nosferatu", year:"2024", id:"tt5040012"}` and `{title:"Nosferatu", year:"", id:"nosferatu"}` — must remain two separate entries after `deduplicateMovies`.

---

### F7 — Letterboxd/RT links built from bare title slugs point at the wrong film
**Severity:** High &nbsp;·&nbsp; **Confidence:** Confirmed (by construction)

- **Location:** `public/index.html:7435-7453` (`getLetterboxdUrl`) and `:7368-7378` (`getRottenTomatoesUrl`)
- **Code/behavior:** When `imdbID` is missing (76 live movies), the title link becomes `letterboxd.com/film/<title-slug>/` and the score links to `rottentomatoes.com/m/<title_slug>`. Both sites assign the bare slug to one specific film and disambiguate others with `-1`/`_year` suffixes, so for any remake/rerelease the link opens a *different* film than the one whose showtimes/score are displayed. Worse: the RT score itself may have been scraped from a `slug_2026`-style candidate URL (`scrape.py:1846-1851`), while the frontend link goes to the bare slug — score and link can point at two different films.
- **Fix:** Persist the verified RT/Letterboxd URLs the backend already fetches into `ratings.rtUrl` / `ratings.letterboxdUrl` — the frontend already prefers those fields when present. Drop client-side slug guessing, or degrade to the existing `letterboxd.com/search/` fallback, which is honest about ambiguity.
- **Regression test:** A movie with no imdbID and a title matching a known multi-film slug must produce a `search/` URL, not a `/film/<slug>/` URL.

---

### F8 — Repertory year-bias: venues missing from `REPERTORY_THEATERS` get current-year-first OMDb lookups
**Severity:** High &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `scripts/scrape.py:2519-2525`
- **Code/behavior:** `REPERTORY_THEATERS` lists only Metrograph, IFC, Film Forum, FLC, and Paris. BAM, Anthology, Nitehawk, Alamo, and MoMA are heavy repertory programmers but are excluded. For a no-year title at one of these venues, `years_to_try = [2026, 2025]` (`scrape.py:2590`) — the resolver actively *prefers* a recent same-title film, and F2's broken matcher accepts it.
- **Fix:** Add the missing venues, or replace the allowlist entirely with a rule: no year hint ⇒ no year-biased probing, rely on stronger title/score validation instead.

---

### F9 — Nitehawk parser extracts the year hint and then discards it
**Severity:** Medium-High &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `scripts/cinema_backend/providers/nitehawk.py:97, 113-114`
- **Code/behavior:** `hint_year` is parsed from a `"(1978)"`-style title suffix, but the only subsequent use is `grouped_formats[title].update([])` — a no-op. The entries built from this data carry no `hint_year` field, feeding directly into F8.
- **Fix:** Carry `hint_year` into each built entry, matching the pattern already used by Film Forum and Paris.
- **Regression test:** Extend `tests/test_repertory_providers.py`'s Nitehawk test to assert `entries[0]["hint_year"] == 1978` for a fixture containing a year suffix.

---

### F10 — "Today"/"Tomorrow" day matching uses substring comparison
**Severity:** Medium &nbsp;·&nbsp; **Confidence:** Confirmed (executed)

- **Location:** `public/index.html:4157` (`dayMatchesActiveScope`)
- **Verified:** with `activeDayLabel = "Aug 1"`, `"Sat Aug 16".includes("Aug 1")` evaluates to `true`.
- **Code/behavior:** After the strict ISO comparison at line 4156 fails, `raw.includes(activeDayLabel)` still runs. On the 1st–3rd of any month, the "Today" view's showtime chips (via `buildScheduleHTML` → `buildInlineShowtimeHTML` → `getNextShowFromSchedule`) incorrectly include days 10–19 and 30–31 of that month.
- **Impact:** Users see, and can click through to buy tickets for, showtimes that are not actually today.
- **Fix:** When `rawISO` parses successfully and differs from `activeISO`, return `false` immediately. Keep the substring path only for genuinely unparseable labels, and make it a word-boundary match rather than plain substring.
- **Regression test (JS):** With today = Aug 1, `dayMatchesActiveScope("Sat Aug 16", "Aug 1", "")` must be `false`.

---

### F11 — "Letterboxd" scores are fabricated from IMDb in 187 of 188 cases
**Severity:** Medium &nbsp;·&nbsp; **Confidence:** Confirmed (measured on live data)

- **Location:** `scripts/scrape.py:1998-2011` (`letterboxd_score = imdb_num / 2`); repeated client-side at `public/index.html:5302-5308` (`getLetterboxdScore`)
- **Measured:** 187 of 188 populated `letterboxd` values in the live `data.json` are exactly `imdb / 2`.
- **Impact:** A labeled third-party rating, attributed and linked to the real Letterboxd brand, is almost always synthetic. It also double-counts IMDb inside composite scores like `getPriorityScore`/`getWorthItScore` as if it were an independent signal.
- **Fix:** Either visibly label the derived value (e.g. "≈ 4.2") and drop the Letterboxd branding when it's synthetic, or only display `letterboxd` when it came from the real Letterboxd fallback scrape.

---

### F12 — Coming-soon pipeline: title-only merge and unverified OMDb year
**Severity:** Medium-High &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `scripts/refresh_coming_soon.py:305-318` (`merge_movies`) and `:195-213` (`fetch_omdb_metadata`)
- **Code/behavior:** `merge_movies` keys by `normalize_title` alone, so two different upcoming films sharing a normalized title collide; `{**movie, **current}` also lets *stale existing* fields (poster, synopsis, director, `letterboxd_url`) silently override fresh calendar data. Separately, `fetch_omdb_metadata` never validates the returned `Year`/`Title` against the query — OMDb's `y=` parameter is only a hint — so a remake can be enriched with the original's poster/director/synopsis.
- **Fix:** Merge on `(normalized_title, release_year)`. Verify the OMDb response's `Title`/`Year` before adopting any field.
- **Regression test:** Two discovered movies with the same title but different release dates must both survive the merge; an OMDb payload with a mismatched `Year` must be discarded.

---

### F13 — Blurbs, prestige tags, and score/link overrides are keyed by mutable `"{title}-{year}"`
**Severity:** Medium &nbsp;·&nbsp; **Confidence:** Confirmed risk

- **Location:** `public/index.html:4696-4700` (`getMovieBlurbKey`), `public/admin/index.html:985-989` (`getKey`), applied at `index.html:7978-7991` — including `imdb_id_override` and `poster_url_override`.
- **Code/behavior:** The key is derived from whatever title/year the pipeline resolved *this run*. Any identity flip from F2–F6 silently detaches admin corrections or attaches them to whichever film currently bears that title+year. The manual-override system meant to *fix* identity mistakes is itself vulnerable to them. (Contrast: `picks` and `site_hidden` are correctly keyed by `movie_id`.)
- **Fix:** Key blurbs/overrides by `movie.id`, keeping title+year only as a display fallback, or store both and prefer `id`.

---

### F14 — Saved/hidden/watched state matches by title as well as id
**Severity:** Medium &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `public/index.html:5608-5615` (`getMovieStorageKeys`)
- **Code/behavior:** Returns `[id, title]`; every toggle stores both, and `hasStoredMovie` matches either. Hiding *Nosferatu* (2024) also hides the 1922 screening (and the corresponding Coming Soon entry — `buildComingSoonCard` at line 6648 checks title too). `isPickMovie` (`index.html:8016`) applies "Showtimes Recommended" by title as well.
- **Fix:** Store and match by `id` only, with a one-time migration path for legacy title-only entries already in users' `localStorage`.

---

### F15 — `/api/blurbs` is an unauthenticated, unmetered Anthropic proxy
**Severity:** Medium (security / cost) &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `public/api/blurbs.js:80-105`
- **Code/behavior:** No origin check, rate limit, or shared cache. Any client can `POST` an arbitrary "movie" payload and consume API credit; every fresh visitor re-generates blurbs already generated for other visitors, because results are only cached in each browser's `localStorage` (the Supabase `blurbs` table is written by the admin only, not by this endpoint).
- **Fix:** Add per-IP rate limiting, validate the movie payload against a known key in `data.json`, and cache generated blurbs server-side (KV or Supabase upsert via the service role) so each movie is generated once, not once per visitor.

---

### F16 — Scraper clock is naive `datetime.now()` (UTC on CI) while all schedule logic assumes New York
**Severity:** Low-Medium &nbsp;·&nbsp; **Confidence:** Confirmed (latent)

- **Location:** `scripts/cinema_backend/runtime.py:175` (`ctx.now = datetime.now()`)
- **Code/behavior:** `existing_showtime_entries` (`scrape.py:2789`) drops slots with `date < ctx.now.date()`, and `fetch_theater_showtimes` checks `ctx.now.weekday()` against the configured wed/sat SerpAPI refresh days. On GitHub-hosted runners this clock is UTC. Any run between 00:00–04:00 UTC treats the current NY evening as "yesterday," dropping today's remaining SerpAPI-cached showtimes, and can evaluate the wrong refresh weekday.
- **Impact:** Masked by the scheduled 12:00 UTC crons, but a manual `workflow_dispatch` run during ET evening hours will corrupt the carry-forward cache.
- **Fix:** Use `now=ny_now()` in `build_scrape_context` — providers already normalize via `ny_reference_now` elsewhere.

---

### F17 — Coming-soon `letterboxd_url` and admin-editable poster URLs bypass the URL allowlist
**Severity:** Low-Medium (defense-in-depth) &nbsp;·&nbsp; **Confidence:** Confirmed

- **Location:** `public/index.html:6658-6659` (title link), `:6666` (poster `src`)
- **Code/behavior:** `movie.letterboxd_url` (sourced from `coming-soon.json` or the admin-editable Supabase `coming_soon_overrides` table) is inserted into an `href` with only `escapeHTML` applied — no `safeExternalUrl` scheme/host check. `escapeHTML` does not neutralize a `javascript:` URI. Exploitation requires admin credentials or repo write access (writes are RLS-gated), but every *other* external URL in the app is rigorously host-allowlisted, and this one renders the page's title link.
- **Fix:** Route both values through `safeExternalUrl(value, SAFE_URL_HOSTS.letterboxd)` / an appropriate poster-host allowlist.

---

### F18 — Data-writing GitHub Actions workflows race on `data.json`
**Severity:** Low-Medium &nbsp;·&nbsp; **Confidence:** Confirmed risk

- **Location:** `.github/workflows/*.yml`
- **Code/behavior:** Only `weekly-scrape`, `refresh-direct-sources`, and `refresh-coming-soon` share the `cinema-data-writes` concurrency group. `refresh-amc.yml`, `refresh-ifc.yml`, `backfill-ratings.yml`, `refresh-recent-releases.yml`, `review-movies.yml`, and `apply-prestige-tags.yml` do not. Two overlapping jobs both rewrite the entire `data.json`; `git pull --rebase` on a conflicting single-file rewrite either fails the push or resolves via last-writer-wins (`refresh-direct-sources.yml` even has an explicit "Merge origin/main (keeping local changes)" commit message for this case).
- **Fix:** Put every data-writing workflow into the same concurrency group with `cancel-in-progress: false`.

---

### Lower-severity items

| # | Severity | Location | Issue | Fix |
|---|----------|----------|-------|-----|
| F19 | Low | `scrape.py:127` (`infer_date_iso_from_label`) | `current.replace(month=…, day=…)` raises uncaught `ValueError` on invalid day/month combinations, aborting the whole scrape | Wrap in try/except, skip the entry |
| F20 | Low | `backfill_ratings.py:43` | `_CURRENT_YEAR = 2026` hardcoded; RT year-suffixed URL candidates go stale in 2027 | Compute from `datetime.now()` |
| F21 | Low | `refresh_recent_ratings.py:110` | Passes lowercased/punctuation-stripped `normalize_title(title)` into OMDb `t=`, degrading matches (e.g. "M\*A\*S\*H" → "m a s h") | Pass the display title |
| F22 | Low | `scrape.py:2199-2213`, `prestige.py:179-181` | Rating/CinemaScore/prestige overrides keyed yearless — one override applies to every same-title film | Support `"title\|year"` override keys |
| F23 | Low | `prestige.py:133` | `lru_cache(maxsize=1)` on a loader called with 4 distinct file paths — cache thrashes, re-reading 3 of 4 files per movie | `maxsize=8` |
| F24 | Low | live `data.json` | Junk records present (e.g. "PRIVATE EVENT TODAY IN THEATER & COMMISSARY" treated as a movie) | Add a venue-noise blocklist before movie creation |
| F25 | Low | `public/index.html:7672` | Search input re-renders the entire board on every keystroke; `parseShowtimeDate`/`getNYNow` run per-slot per-render with no memoization | Debounce input; compute "now" once per render |
| F26 | Low | `public/index.html:7049` and elsewhere | Hover-only underline via inline `onmouseover`/`onmouseout` has no `:focus` equivalent; verdict tone sometimes conveyed by color class alone | Add CSS `:hover, :focus` rule; add a non-color indicator |
| F27 | Low | `scrape.py:1764` (FLC RSC parsing) | Unescapes `\"` before `\\` — wrong order for JS string unescaping, can corrupt payloads containing escaped backslashes | Reverse unescape order |

---

## 3. Movie-identity and link-association summary

The identity chain has one strong link — **imdbID** — and the code respects it wherever it exists: the grouping key (`scrape.py:2233-2245`), same-IMDB dedupe (`scrape.py:3091-3113`), the verdict cache (keyed by movie id), and `letterboxd.com/imdb/<tt>` links. Everything else is title-string re-derivation, and the title path fails in a mutually reinforcing loop:

1. **Acquisition** mangles titles (F1) and sometimes discards the venue's own year hint (F9).
2. **Resolution** prefers recent years for repertory venues missing from the allowlist (F8) and accepts an exact-title match across a century (F2).
3. **Persistence** amplifies mistakes: the previous-dataset index (F3) and rating cache (F4) are both yearless, so one bad match re-infects every subsequent run and survives the existing purge logic.
4. **Partial refreshers** re-join by bare title and array order (F5).
5. **The frontend** merges yearless variants into known-year films (F6), keys manual corrections by mutable title+year (F13), applies hide/save/picks by title (F14), and fabricates external links from title slugs (F7) — so even a correctly-separated backend pair can end up sharing links, blurbs, or user state on the client.
6. **Coming Soon** repeats the same title-only merge pattern independently (F12).

Posters, review blurbs/verdicts, and provider/ticket links can each be reused across records via a **different** one of these channels, which explains why such symptoms tend to look intermittent: a wrong poster traces to F3/F4, wrong showtimes to F5/F6, a wrong outbound link to F7, and wrong "hidden" state to F14.

**One place identity is solid:** per-showtime `ticket_urls` are captured directly from each venue at scrape time and passed through host-allowlisted (`SAFE_URL_HOSTS.ticket`) rendering — mis-association there requires an upstream grouping error (F5/F6), not a URL-construction bug.

---

## 4. Quick wins vs. architectural work

### Quick wins (small, isolated diffs)
- F1 — anchor `FORMAT_TAGS` (one regex change + tests)
- F2 — hard year veto in the two matcher functions
- F9 — pass Nitehawk `hint_year` through (one line)
- F8 — extend `REPERTORY_THEATERS`
- F10 — fix `dayMatchesActiveScope` fall-through
- F16 — `now=ny_now()`
- F18 — add missing workflows to the concurrency group
- F17 — apply `safeExternalUrl` to coming-soon links
- F19–F24 — each is a self-contained ≤10-line fix

### Medium effort
- F4 — year-aware cache adoption + cache-entry schema change (store query year)
- F5 — ambiguity-aware `ensure_movie` in both partial refreshers
- F6 — restrict frontend dedupe to suffix-variants / imdb matches
- F11 — honest Letterboxd labeling
- F12 — coming-soon merge/verify fixes
- F15 — blurb API caching + rate limiting

### Architectural
- **F3 / F13 / F14 — unify on a single canonical movie identity.** Use imdbID when known, otherwise a *persisted* stable slug written once by the scraper and reused thereafter (never re-derived from the title at each boundary). Apply that identity consistently to: previous-run carry-forward, the rating cache, Supabase blurb/override keys, and localStorage user state — with a one-time key migration for existing users. This is the change that closes the *class* of bugs rather than patching individual instances.
- **F7 — persist verified external URLs from the scraper** instead of guessing them from title slugs on the client. Belongs to the same effort since it depends on the same identity guarantee.

---

## 5. Recommended implementation order

1. **F1, F2** — stop creating new wrong identities. Highest leverage, smallest diffs.
2. **F4** + purge any `rating_cache.json` entries whose year conflicts — stop replaying old mistakes.
3. **F3** — year-aware previous-dataset index.
4. **F5, F6** — fix the two join layers that move showtimes across films.
5. **F7, F11** — link and score honesty in the frontend.
6. **F8, F9, F12** — resolution-quality improvements.
7. **F10, F16, F17, F18, F19–F24** — batch of quick correctness/ops fixes.
8. **F13 / F14** identity-key unification + migration, then **F15** and the performance/accessibility items (F25, F26).

---

## 6. Areas that could not be verified

- **Supabase Auth configuration** — whether email/password signup with unconfirmed emails is enabled. This determines whether `is_cinema_admin()`'s JWT-email trust (`supabase/admin-dashboard-policies.sql:7-13`) is exploitable. Needs direct inspection of the project's Auth settings.
- **Live RLS state** — whether the `blurbs` / `picks` / `site_hidden` tables' row-level security is actually enabled in the deployed project; the SQL file in the repo is prescriptive, and the comment block at `public/index.html:7815-7866` describes an older, more permissive schema that may not match production.
- **Real OMDb behavior for ambiguous `t=`+`y=` queries** — rate limits prevented live probing from this environment. F2/F8 were verified against the code's own acceptance logic, not against live OMDb responses.
- **Actual SerpAPI response shape for ticket URLs** — the code probes four different key names for ticket links, suggesting past API drift; current shape not independently confirmed.
- **Whether F10 (day substring bug) is currently visibly manifesting** — depends on today's date; verified by direct logic execution, not by a live screenshot on an affected date.

---

*Review performed by tracing real data and control flows through the codebase, not by pattern-matching for likely bug locations. High-confidence findings above were confirmed by executing the actual application code against the live `public/data.json`.*
