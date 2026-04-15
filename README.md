# Showtimes NYC

Static movie dashboard for New York repertory and selected commercial theaters. The scraper writes a single dataset to `public/data.json`, and the frontend in `public/index.html` renders from that file.

## What Is In This Repo

- `scripts/scrape.py`: showtime aggregation, metadata matching, verdict generation, and dataset assembly
- `scripts/rating_overrides.json`: hard overrides for title identity edge cases
- `scripts/cinemascore_overrides.json`: manual CinemaScore values for current releases
- `scripts/rating_cache.json`: resolved OMDb matches cached for stability
- `public/index.html`: production frontend
- `public/data.json`: live dataset consumed by the frontend
- `.github/workflows/weekly-scrape.yml`: scheduled scrape and commit
- `.github/workflows/deploy.yml`: deploys production when `public/**` changes on `main`

## Data Sources

- Showtimes:
  - SerpAPI for some theaters
  - AMC API for AMC theaters
  - direct theater scraping for Metrograph, IFC, Alamo, and MoMA
- Metadata:
  - OMDb as primary source
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
- Museum of Modern Art
- Alamo Drafthouse Lower Manhattan
- Alamo Drafthouse Downtown Brooklyn
- Alamo Drafthouse Staten Island
- AMC theaters returned by the configured AMC filters

## Environment

The scraper reads these environment variables:

- `SERPAPI_KEY`
- `OMDB_KEY`
- `ANTHROPIC_API_KEY`
- `AMC_VENDOR_KEY`
- `AMC_API_BASE` optional
- `AMC_THEATRE_IDS` optional comma-separated override
- `ALLOW_MOCK_DATA=1` optional local-only escape hatch for mock scraper runs

## Local Development

Create and use a virtualenv, then install the scraper dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/scrape.py
```

Production scrapes fail loudly when `SERPAPI_KEY`, `OMDB_KEY`, or `ANTHROPIC_API_KEY` is missing. For local layout work without API keys, run `ALLOW_MOCK_DATA=1 python scripts/scrape.py` and do not commit the generated mock dataset.

Open `public/index.html` in a browser, or serve the `public/` directory with any static file server.

## Deployment Flow

Production deploys should follow one path:

1. `weekly-scrape.yml` runs on schedule or manually.
2. The scraper updates `public/data.json` and `scripts/rating_cache.json`.
3. The workflow commits and pushes to `main`.
4. `deploy.yml` deploys production when `public/**` changes on `main`.

## Maintenance Notes

- If title matching drifts, fix the identity in `scripts/rating_overrides.json` instead of patching the frontend.
- If a current release needs CinemaScore, add it to `scripts/cinemascore_overrides.json`.
- `public/data.json` is generated output and should not be hand-edited unless debugging a one-off issue.
