# Showtimes NYC

Static movie dashboard for New York repertory and selected commercial theaters. The scraper writes a single dataset to `public/data.json`, and the frontend in `public/index.html` renders from that file.

## What Is In This Repo

- `scripts/scrape.py`: showtime aggregation, metadata matching, verdict generation, and dataset assembly
- `scripts/refresh_coming_soon.py`: rolling six-month theatrical release discovery and metadata enrichment
- `scripts/rating_overrides.json`: hard overrides for title identity edge cases
- `scripts/cinemascore_overrides.json`: manual CinemaScore values for current releases
- `scripts/rating_cache.json`: resolved OMDb matches cached for stability
- `public/index.html`: production frontend
- `public/data.json`: live dataset consumed by the frontend
- `public/coming-soon.json`: upcoming wide and specialty theatrical releases
- `.github/workflows/weekly-scrape.yml`: daily scheduled scrape and commit
- `.github/workflows/refresh-coming-soon.yml`: coming-soon refresh on the first day of each month
- `.github/workflows/deploy.yml`: deploys production when `public/**` changes on `main`

## Data Sources

- Showtimes:
  - SerpAPI for some theaters
  - AMC API for AMC theaters
  - direct theater scraping for Metrograph, IFC, Film Forum, Film at Lincoln Center, Paris Theater, Alamo, BAM, Nitehawk, and Anthology
- Metadata:
  - OMDb as primary source
  - Box Office Mojo for upcoming U.S. theatrical dates, studios, scale, and genres
  - TMDB public movie pages as a fallback for upcoming posters, synopses, and directors
  - verified Letterboxd film pages for optional outgoing links
  - Rotten Tomatoes and Letterboxd fallbacks where OMDb is incomplete
  - manual overrides for ambiguous titles
- Audience signal:
  - manual `cinemascore_overrides.json` entries for supported new releases
- Verdicts:
  - Anthropic when configured
  - deterministic local fallback when not configured

## Current Theater Coverage

- Metrograph
- IFC Center
- Angelika Film Center
- Village East by Angelika
- Film Forum
- Film at Lincoln Center
- Paris Theater
- Alamo Drafthouse Lower Manhattan
- Alamo Drafthouse Downtown Brooklyn
- Alamo Drafthouse Staten Island
- BAM Rose Cinemas
- Nitehawk Cinema Williamsburg
- Nitehawk Cinema Prospect Park
- Anthology Film Archives
- Regal Union Square
- Regal Essex Crossing
- Regal Battery Park
- Regal Times Square
- Regal UA Sheepshead Bay
- Regal Bricktown Charleston
- AMC theaters returned by the configured AMC filters

## Environment

The scraper reads these environment variables:

- `SERPAPI_KEY`
- `SERPAPI_MONTHLY_BUDGET` optional hard safety cap, defaults to `200`
- `SERPAPI_REFRESH_WEEKDAYS` optional comma-separated live refresh days, defaults to `wed,sat`
- `OMDB_KEY`
- `ANTHROPIC_API_KEY`
- `AMC_VENDOR_KEY`
- `PARIS_API_USERNAME`
- `PARIS_API_PASSWORD`
- `PARIS_API_CLIENT_ID`
- `AMC_API_BASE` optional
- `AMC_THEATRE_IDS` optional comma-separated override
- `ALLOW_MOCK_DATA=1` optional local-only escape hatch for mock scraper runs

Frontend/serverless production also expects:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `ANALYTICS_FINGERPRINT_SALT`
- `ANTHROPIC_API_KEY`
- `ANTHROPIC_REVIEW_MODEL` optional

## Local Development

Create and use a virtualenv, then install the scraper dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/scrape.py
python scripts/refresh_coming_soon.py --dry-run
```

Production scrapes fail loudly when `SERPAPI_KEY`, `OMDB_KEY`, or `ANTHROPIC_API_KEY` is missing. For local layout work without API keys, run `ALLOW_MOCK_DATA=1 python scripts/scrape.py` and do not commit the generated mock dataset.

SerpAPI-backed theaters refresh live every Wednesday and Saturday by default. On the
other days the scraper carries forward still-future showtimes from the previous
dataset. Before spending a search, it checks SerpAPI's free Account API and
stops at the configured monthly safety cap. With eight SerpAPI-backed venues,
the default cadence uses 64–80 searches per month instead of up to 248. The cap
leaves 50 searches of headroom on the 250-search free plan.

To refresh direct/API sources without spending SerpAPI quota, run:

```bash
AMC_VENDOR_KEY=... python scripts/refresh_direct_sources.py
```

This live-fetches direct venue scrapers and AMC API, while carrying forward
still-future Angelika, Village East, and Regal schedules from `public/data.json`.

If Paris Theater credentials were previously committed, rotate them in the upstream provider before the next scrape or deploy.

Open `public/index.html` in a browser, or serve the `public/` directory with any static file server.

## Deployment Flow

Production deploys should follow one path:

1. `weekly-scrape.yml` runs daily on schedule or manually.
2. The scraper updates `public/data.json` and `scripts/rating_cache.json`.
3. The workflow commits and pushes to `main`.
4. `deploy.yml` deploys production when `public/**` changes on `main`.

The separate `refresh-coming-soon.yml` workflow runs at 09:00 UTC on the first
day of every month. It updates `public/coming-soon.json`, commits changes to
`main`, and triggers the same production deployment flow.

The authenticated `/admin` dashboard has a Coming Soon editor for disabling
titles and overriding release dates, posters, synopses, directors, genres,
studios, release scale, and Letterboxd links. Run
`supabase/admin-dashboard-policies.sql` in the Supabase SQL editor after schema
changes; these overrides live separately from the generated JSON and survive
monthly refreshes.

## Maintenance Notes

- If title matching drifts, fix the identity in `scripts/rating_overrides.json` instead of patching the frontend.
- If a current release needs CinemaScore, add it to `scripts/cinemascore_overrides.json`.
- `public/data.json` is generated output and should not be hand-edited unless debugging a one-off issue.
