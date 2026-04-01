"""
NYC Cinema Dashboard - Weekly Scraper
Runs every Wednesday via GitHub Actions
Pulls showtimes via SerpAPI, ratings via OMDb, verdicts via Claude API
"""

import os
import json
import requests
import anthropic
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote
from typing import Optional

# ─── CONFIG ──────────────────────────────────────────────────────────────────

THEATERS = [
    {"name": "Metrograph", "serpapi_id": "metrograph new york"},
    {"name": "IFC Center", "serpapi_id": "ifc center new york"},
    {"name": "Angelika Film Center", "serpapi_id": "angelika film center new york"},
    {"name": "Film Forum", "serpapi_id": "film forum new york"},
    {"name": "Village East by Angelika", "serpapi_id": "village east cinema new york"},
    {"name": "Film at Lincoln Center", "serpapi_id": "film at lincoln center new york"},
    {"name": "Alamo Drafthouse Lower Manhattan", "serpapi_id": "alamo drafthouse lower manhattan new york"},
    {"name": "Paris Theater", "serpapi_id": "paris theater new york"},
]

SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "")
OMDB_KEY = os.environ.get("OMDB_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ─── TITLE CLEANING ───────────────────────────────────────────────────────────

import re

FORMAT_TAGS = re.compile(
    r'\b(70mm|35mm|imax|4k|dcp|digital|in\s+70mm|in\s+35mm|presented\s+in\s+\w+)\b'
    r'|\s*[\(\[]?(70mm|35mm|imax|4k|dcp)[\)\]]?',
    re.IGNORECASE
)

NON_ALNUM = re.compile(r"[^a-z0-9]+")
SCRIPT_DIR = Path(__file__).resolve().parent
RATING_OVERRIDES_PATH = SCRIPT_DIR / "rating_overrides.json"
RATING_CACHE_PATH = SCRIPT_DIR / "rating_cache.json"


def normalize_title(title: str) -> str:
    return NON_ALNUM.sub(" ", (title or "").lower()).strip()

def clean_title(raw: str) -> str:
    """Strip projection format tags from a showtime title before lookup."""
    return FORMAT_TAGS.sub('', raw).strip(' -–—·')

# ─── SHOWTIMES ────────────────────────────────────────────────────────────────

def fetch_showtimes(theater: dict) -> list[dict]:
    """Pull showtimes from Google via SerpAPI for a given theater."""
    if not SERPAPI_KEY:
        print(f"  [MOCK] No SerpAPI key — using mock data for {theater['name']}")
        return mock_showtimes(theater["name"])

    params = {
        "engine": "google",
        "q": f"showtimes {theater['serpapi_id']}",
        "api_key": SERPAPI_KEY,
    }
    try:
        r = requests.get("https://serpapi.com/search", params=params, timeout=15)
        data = r.json()
        movies = []
        for day in data.get("showtimes", []):
            for movie in day.get("movies", []):
                times = []
                for showing in movie.get("showing", []):
                    times.extend(showing.get("time", []))
                movies.append({
                    "title": clean_title(movie.get("name", "Unknown")),
                    "theater": theater["name"],
                    "day": f"{day.get('day', '')} {day.get('date', '')}".strip(),
                    "times": times,
                })
        return movies
    except Exception as e:
        print(f"  [ERROR] SerpAPI failed for {theater['name']}: {e}")
        return mock_showtimes(theater["name"])


def mock_showtimes(theater_name: str) -> list[dict]:
    """Fallback mock data for development without API keys."""
    days = ["Friday", "Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday"]
    sample = [
        {"title": "Caught by the Tides", "times": ["2:00 PM", "7:30 PM"]},
        {"title": "A Real Pain", "times": ["4:15 PM", "9:00 PM"]},
        {"title": "The Brutalist", "times": ["1:00 PM", "5:30 PM"]},
        {"title": "Hard Truths", "times": ["3:45 PM", "8:15 PM"]},
        {"title": "Nickel Boys", "times": ["2:30 PM", "7:00 PM"]},
        {"title": "September Says", "times": ["6:00 PM"]},
    ]
    results = []
    for movie in sample[:3]:
        for day in days[:4]:
            results.append({
                "title": movie["title"],
                "theater": theater_name,
                "day": day,
                "times": movie["times"],
            })
    return results


# ─── RATINGS ─────────────────────────────────────────────────────────────────

def load_json_file(path: Path) -> dict:
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except Exception as e:
        print(f"  [WARN] Failed to load {path.name}: {e}")
    return {}


def save_json_file(path: Path, data: dict) -> None:
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"  [WARN] Failed to save {path.name}: {e}")


RATING_OVERRIDES = load_json_file(RATING_OVERRIDES_PATH)
RATING_CACHE = load_json_file(RATING_CACHE_PATH)


def omdb_request(params: dict) -> Optional[dict]:
    try:
        r = requests.get("http://www.omdbapi.com/", params={"apikey": OMDB_KEY, **params}, timeout=10)
        data = r.json()
        if data.get("Response") == "False":
            return None
        return data
    except Exception:
        return None


def serpapi_google_search(query: str) -> Optional[dict]:
    if not SERPAPI_KEY:
        return None
    try:
        r = requests.get(
            "https://serpapi.com/search",
            params={
                "engine": "google",
                "q": query,
                "api_key": SERPAPI_KEY,
            },
            timeout=15,
        )
        return r.json()
    except Exception:
        return None


def fetch_rt_fallback(title: str) -> Optional[str]:
    data = serpapi_google_search(f"site:rottentomatoes.com {title} movie")
    if not data:
        return None

    organic = data.get("organic_results", []) or []
    target_url = None
    for result in organic:
        link = result.get("link", "")
        if "rottentomatoes.com/m/" in link:
            target_url = link
            break

    snippet_text = " ".join(
        [
            str(result.get("snippet", ""))
            for result in organic[:3]
            if result.get("snippet")
        ]
    )
    snippet_match = re.search(r"\b(\d{1,3})%\b", snippet_text)

    if not target_url:
        if snippet_match:
            pct = int(snippet_match.group(1))
            if 0 <= pct <= 100:
                return f"{pct}%"
        return None

    try:
        page = requests.get(
            target_url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        ).text
    except Exception:
        page = ""

    patterns = [
        r'tomatometerscore="(\d{1,3})"',
        r'"tomatometerScoreAll"\s*:\s*\{"score"\s*:\s*(\d{1,3})',
        r'"criticsScore"\s*:\s*(\d{1,3})',
        r'"criticsScore"\s*:\s*\{[^{}]{0,240}"score"\s*:\s*"(\d{1,3})"',
        r'"scorePercent"\s*:\s*"(\d{1,3})%"',
        r'"Tomatometer","ratingCount":\d+,"ratingValue":"(\d{1,3})"',
    ]
    for pat in patterns:
        m = re.search(pat, page)
        if m:
            pct = int(m.group(1))
            if 0 <= pct <= 100:
                return f"{pct}%"

    if snippet_match:
        pct = int(snippet_match.group(1))
        if 0 <= pct <= 100:
            return f"{pct}%"
    return None


def fetch_letterboxd_fallback(title: str) -> Optional[str]:
    try:
        search_url = f"https://letterboxd.com/search/{quote(title)}/"
        search_page = requests.get(
            search_url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        ).text
    except Exception:
        return None

    m = re.search(r'href="(/film/[^"/]+/)"', search_page)
    if not m:
        return None

    film_url = f"https://letterboxd.com{m.group(1)}"
    try:
        film_page = requests.get(
            film_url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        ).text
    except Exception:
        return None

    patterns = [
        r'"ratingValue"\s*:\s*"?(?P<score>\d(?:\.\d)?)"?',
        r'"averageRating"\s*:\s*"?(?P<score>\d(?:\.\d)?)"?',
    ]
    for pat in patterns:
        mm = re.search(pat, film_page)
        if mm:
            try:
                score = float(mm.group("score"))
                if 0.0 < score <= 5.0:
                    return f"{score:.1f}"
            except Exception:
                continue

    return None


def parse_omdb_ratings(data: dict) -> dict:
    rt = next((r["Value"] for r in data.get("Ratings", []) if r["Source"] == "Rotten Tomatoes"), None)
    cinema_score = next((r["Value"] for r in data.get("Ratings", []) if r["Source"] == "CinemaScore"), None)
    imdb_rating = data.get("imdbRating")
    letterboxd_score = None
    try:
        imdb_num = float(imdb_rating) if imdb_rating not in (None, "N/A") else None
        if imdb_num is not None:
            letterboxd_score = f"{(imdb_num / 2):.1f}"
    except Exception:
        letterboxd_score = None

    return {
        "rt": rt,
        "imdb": imdb_rating,
        "metacritic": data.get("Metascore"),
        "letterboxd": letterboxd_score,
        "cinemaScore": cinema_score,
        "poster": data.get("Poster"),
        "genre": data.get("Genre"),
        "runtime": data.get("Runtime"),
        "plot": data.get("Plot"),
        "year": data.get("Year"),
        "director": data.get("Director"),
    }


def title_match_score(query_title: str, result_title: str, query_year: Optional[int] = None, result_year: Optional[str] = None) -> float:
    q_norm = normalize_title(query_title)
    r_norm = normalize_title(result_title or "")
    if not q_norm or not r_norm:
        return 0.0

    q_tokens = set(q_norm.split())
    r_tokens = set(r_norm.split())
    overlap = len(q_tokens & r_tokens)
    coverage = overlap / max(1, len(q_tokens))

    exact_bonus = 0.45 if q_norm == r_norm else 0.0
    startswith_bonus = 0.20 if r_norm.startswith(q_norm) or q_norm.startswith(r_norm) else 0.0
    score = coverage + exact_bonus + startswith_bonus

    if query_year and result_year and result_year.isdigit():
        result_y = int(result_year)
        diff = abs(query_year - result_y)
        if diff == 0:
            score += 0.2
        elif diff == 1:
            score += 0.1
        elif diff > 3:
            score -= 0.25

    return score


def fetch_omdb_by_imdb_id(imdb_id: str) -> Optional[dict]:
    if not imdb_id:
        return None
    return omdb_request({"i": imdb_id, "tomatoes": "true"})


def resolve_omdb_record(title: str) -> Optional[dict]:
    normalized = normalize_title(title)
    override = RATING_OVERRIDES.get(normalized, {})
    if isinstance(override, str):
        override = {"imdbID": override}

    override_imdb = override.get("imdbID")
    if override_imdb:
        data = fetch_omdb_by_imdb_id(override_imdb)
        if data:
            RATING_CACHE[normalized] = {
                "imdbID": data.get("imdbID"),
                "title": data.get("Title"),
                "year": data.get("Year"),
                "source": "override",
            }
            return data
        print(f"  [WARN] Override imdbID failed for '{title}': {override_imdb}")

    cached_imdb = (RATING_CACHE.get(normalized) or {}).get("imdbID")
    if cached_imdb:
        data = fetch_omdb_by_imdb_id(cached_imdb)
        if data:
            return data

    exact = omdb_request({"t": title, "tomatoes": "true"})
    if exact:
        RATING_CACHE[normalized] = {
            "imdbID": exact.get("imdbID"),
            "title": exact.get("Title"),
            "year": exact.get("Year"),
            "source": "exact",
        }
        return exact

    query_year = None
    override_year = override.get("year")
    if isinstance(override_year, int):
        query_year = override_year
    elif isinstance(override_year, str) and override_year.isdigit():
        query_year = int(override_year)

    search = omdb_request({"s": title, "type": "movie"})
    if not search:
        return None

    candidates = search.get("Search", [])
    if not candidates:
        return None

    best = max(
        candidates,
        key=lambda c: title_match_score(
            title,
            c.get("Title", ""),
            query_year=query_year,
            result_year=c.get("Year"),
        ),
    )
    best_score = title_match_score(
        title,
        best.get("Title", ""),
        query_year=query_year,
        result_year=best.get("Year"),
    )
    if best_score < 0.55:
        return None

    best_data = fetch_omdb_by_imdb_id(best.get("imdbID"))
    if best_data:
        RATING_CACHE[normalized] = {
            "imdbID": best_data.get("imdbID"),
            "title": best_data.get("Title"),
            "year": best_data.get("Year"),
            "source": "search",
        }
    return best_data

def fetch_ratings(title: str) -> dict:
    """Fetch RT, IMDb, and CinemaScore via OMDb; include a Letterboxd-style score."""
    if not OMDB_KEY:
        return mock_ratings(title)

    try:
        data = resolve_omdb_record(title)
        if data:
            parsed = parse_omdb_ratings(data)
        else:
            parsed = {"rt": None, "imdb": None, "metacritic": None, "letterboxd": None, "poster": None, "genre": None, "runtime": None, "plot": None, "year": None, "director": None, "cinemaScore": None}

        # Fallbacks for new/edge releases where OMDb is lagging.
        if not parsed.get("rt"):
            parsed["rt"] = fetch_rt_fallback(title)
        if not parsed.get("letterboxd"):
            parsed["letterboxd"] = fetch_letterboxd_fallback(title)

        return parsed
    except Exception as e:
        print(f"  [ERROR] OMDb failed for '{title}': {e}")
        return mock_ratings(title)


def mock_ratings(title: str) -> dict:
    import random
    rt_scores = ["94%", "87%", "72%", "65%", "91%", "55%"]
    imdb_scores = ["7.8", "8.1", "6.9", "7.2", "8.4", "6.3"]
    genres = ["Drama", "Drama, History", "Comedy, Drama", "Documentary", "Thriller"]
    plots = [
        "A sweeping portrait of ambition, sacrifice, and the cost of greatness.",
        "Two cousins reunite in Poland and confront the weight of their family history.",
        "A Ghanaian immigrant navigates life in 1990s London with quiet determination.",
        "An epic meditation on the immigrant experience and the American Dream.",
        "Two brothers reckon with grief, distance, and what it means to belong.",
    ]
    idx = hash(title) % len(rt_scores)
    return {
        "rt": rt_scores[idx],
        "imdb": imdb_scores[idx],
        "metacritic": str(int(rt_scores[idx].replace("%", "")) - 5),
        "letterboxd": f"{(float(imdb_scores[idx]) / 2):.1f}",
        "poster": None,
        "genre": genres[idx % len(genres)],
        "runtime": f"{random.randint(90, 150)} min",
        "plot": plots[idx % len(plots)],
        "year": "2024",
        "director": "Various",
    }


# ─── VERDICTS ─────────────────────────────────────────────────────────────────

def fetch_verdict(title: str, ratings: dict) -> dict:
    """Ask Claude for a Watch/Skip verdict + one-line reason."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY) if ANTHROPIC_KEY else None

    rt = ratings.get("rt", "N/A")
    cinema_score = ratings.get("cinemaScore", "N/A")
    letterboxd = ratings.get("letterboxd", "N/A")
    imdb = ratings.get("imdb", "N/A")
    plot = ratings.get("plot", "")
    genre = ratings.get("genre", "")
    director = ratings.get("director", "")

    prompt = f"""You are a sharp, taste-driven film critic helping a cinephile decide what to watch at NYC indie theaters this week.

Movie: {title}
Director: {director}
Genre: {genre}
Plot: {plot}
Rotten Tomatoes: {rt}
CinemaScore: {cinema_score}
Letterboxd: {letterboxd}
IMDB: {imdb}

Return ONLY a JSON object with exactly these fields:
{{
  "verdict": "WATCH" | "SKIP",
  "reason": "One sharp sentence (max 15 words) explaining why.",
  "vibe": "One or two words describing the feeling/mood of the film"
}}

Be direct. No hedging. Use SKIP only for clear duds; otherwise choose WATCH."""

    if not client:
        return mock_verdict(title, ratings)

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        # Strip markdown fences if present
        text = text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        print(f"  [ERROR] Claude verdict failed for '{title}': {e}")
        return mock_verdict(title, ratings)


def mock_verdict(title: str, ratings: dict) -> dict:
    cinema_score = str(ratings.get("cinemaScore") or "").strip().upper()
    rt_str = ratings.get("rt")
    letterboxd_str = ratings.get("letterboxd")
    imdb_str = ratings.get("imdb")

    cinema_weight = {
        "A+": 130, "A": 120, "A-": 115,
        "B+": 105, "B": 100, "B-": 95,
        "C+": 85, "C": 80, "C-": 75,
        "D+": 65, "D": 60, "D-": 55,
        "F": 30,
    }.get(cinema_score)

    has_rt = isinstance(rt_str, str) and "%" in rt_str
    has_letterboxd = letterboxd_str not in (None, "N/A")
    has_imdb = imdb_str not in (None, "N/A")

    # If there is no critic/audience signal yet, avoid manufacturing a harsh skip.
    if cinema_weight is None and not has_rt and not has_letterboxd and not has_imdb:
        return {
            "verdict": "WATCH",
            "reason": "No clear consensus yet, but nothing suggests it's a dud.",
            "vibe": "Unscored",
        }

    if cinema_weight is not None:
        score = cinema_weight
    elif has_rt:
        score = int(rt_str.replace("%", ""))
    elif has_letterboxd:
        score = int(float(letterboxd_str) * 20)
    elif has_imdb:
        score = int(float(imdb_str) * 10)
    else:
        score = 0

    if score >= 55:
        return {"verdict": "WATCH", "reason": "Critics are united — this one earns its runtime.", "vibe": "Essential"}
    else:
        return {"verdict": "SKIP", "reason": "The scores don't lie — pass on this one.", "vibe": "Mediocre"}


# ─── ASSEMBLE ─────────────────────────────────────────────────────────────────

def build_dataset() -> dict:
    print("Starting weekly NYC cinema scrape...")
    all_movies: dict[str, dict] = {}  # keyed by title to deduplicate
    theater_schedule: dict[str, dict] = defaultdict(lambda: defaultdict(list))

    for theater in THEATERS:
        print(f"\nFetching: {theater['name']}")
        showtimes = fetch_showtimes(theater)

        for entry in showtimes:
            title = entry["title"]
            theater_name = entry["theater"]
            day = entry["day"]
            times = entry["times"]

            theater_schedule[theater_name][title].append({"day": day, "times": times})

            if title not in all_movies:
                print(f"  Fetching ratings for: {title}")
                ratings = fetch_ratings(title)
                print(f"  Fetching verdict for: {title}")
                verdict = fetch_verdict(title, ratings)
                all_movies[title] = {
                    "title": title,
                    "ratings": ratings,
                    "verdict": verdict,
                    "theaters": [],
                }

    # Attach theater + showtime info to each movie
    for theater_name, movies in theater_schedule.items():
        for title, schedule in movies.items():
            if title in all_movies:
                all_movies[title]["theaters"].append({
                    "name": theater_name,
                    "schedule": schedule,
                })

    movies_list = sorted(
        all_movies.values(),
        key=lambda m: (
            {"WATCH": 0, "SKIP": 1}.get(m["verdict"]["verdict"], 2),
            -(int((m["ratings"].get("rt") or "0%").replace("%", "")) if m["ratings"].get("rt") else 0)
        )
    )

    return {
        "generated_at": datetime.now().isoformat(),
        "week_of": (datetime.now() + timedelta(days=(4 - datetime.now().weekday()) % 7)).strftime("%B %d, %Y"),
        "theaters": [t["name"] for t in THEATERS],
        "movies": movies_list,
    }


if __name__ == "__main__":
    dataset = build_dataset()
    save_json_file(RATING_CACHE_PATH, RATING_CACHE)

    output_path = os.path.join(os.path.dirname(__file__), "../public/data.json")
    with open(output_path, "w") as f:
        json.dump(dataset, f, indent=2)

    print(f"\nDone. {len(dataset['movies'])} unique films written to public/data.json")
