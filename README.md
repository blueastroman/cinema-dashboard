# NYC Cinema Dashboard

Weekly auto-refreshing dashboard for NYC indie cinema. Pulls showtimes, ratings, and Claude-generated verdicts every Wednesday morning.

Live production updates are deployed via the connected Vercel project.

## Theaters Tracked
- Metrograph
- IFC Center
- Angelika Film Center
- Film Forum
- Village East by Angelika
- Film at Lincoln Center
- Alamo Drafthouse Lower Manhattan
- Paris Theater
- Museum of Modern Art (MoMA)

## Stack
- **Showtimes**: SerpAPI (free tier — 100 searches/month, ~10/week)
- **Ratings**: OMDb API (free — RT score, IMDB, Metacritic)
- **Verdicts**: Claude API (claude-sonnet)
- **Scheduler**: GitHub Actions (every Wednesday 8am America/New_York)
- **Hosting**: Vercel (free static hosting)

## Setup

### 1. Fork & clone this repo

### 2. Get API keys
- **SerpAPI**: https://serpapi.com — free tier is enough
- **OMDb**: https://www.omdbapi.com/apikey.aspx — free
- **Anthropic**: https://console.anthropic.com — pay per use (~$0.50/month)

### 3. Add GitHub Secrets
In your repo → Settings → Secrets and variables → Actions:

| Secret | Value |
|--------|-------|
| `SERPAPI_KEY` | Your SerpAPI key |
| `OMDB_KEY` | Your OMDb key |
| `ANTHROPIC_API_KEY` | Your Anthropic key |

### 4. Deploy to Vercel
```bash
npm i -g vercel
vercel --prod
```
Point Vercel to the `public/` folder as the output directory.

### 5. Trigger first run
Go to Actions → "Weekly Cinema Scrape" → Run workflow manually.

## Local Development
```bash
pip install -r requirements.txt
python scripts/scrape.py  # runs with mock data if no API keys set
```
Open `public/index.html` in your browser.

### Ratings Matching
- `scripts/rating_overrides.json`: manual title → `imdbID` mapping for known edge cases.
- `scripts/rating_cache.json`: auto-populated cache of resolved OMDb IDs to improve weekly stability.
- If a title misses exact OMDb lookup, scraper now falls back to OMDb search (`s=`) and chooses the best candidate.

## How It Works
Every Wednesday at 8am New York time, GitHub Actions:
1. Runs `scripts/scrape.py`
2. Hits SerpAPI for showtimes at each theater
3. Hits OMDb for RT/IMDB/Metacritic scores per film
4. Asks Claude for a Watch/Skip/Depends verdict + one-line reason
5. Writes `public/data.json`
6. Commits and pushes
7. Vercel auto-deploys the updated static site

**Total cost: $0** (within free tiers)
