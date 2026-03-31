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

# ─── CONFIG ──────────────────────────────────────────────────────────────────

THEATERS = [
    {"name": "Metrograph", "serpapi_id": "metrograph new york"},
    {"name": "IFC Center", "serpapi_id": "ifc center new york"},
    {"name": "Angelika Film Center", "serpapi_id": "angelika film center new york"},
    {"name": "Film Forum", "serpapi_id": "film forum new york"},
    {"name": "Village East by Angelika", "serpapi_id": "village east cinema new york"},
    {"name": "Lincoln Center - Film at Lincoln Center", "serpapi_id": "film at lincoln center new york"},
    {"name": "Alamo Drafthouse Lower Manhattan", "serpapi_id": "alamo drafthouse lower manhattan new york"},
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

def fetch_ratings(title: str) -> dict:
    """Fetch RT score, IMDB score, CinemaScore via OMDb."""
    if not OMDB_KEY:
        return mock_ratings(title)

    try:
        r = requests.get(
            "http://www.omdbapi.com/",
            params={"apikey": OMDB_KEY, "t": title, "tomatoes": "true"},
            timeout=10,
        )
        data = r.json()
        if data.get("Response") == "False":
            return {"rt": None, "imdb": None, "metacritic": None, "poster": None, "genre": None, "runtime": None, "plot": None, "year": None}

        rt = next((r["Value"] for r in data.get("Ratings", []) if r["Source"] == "Rotten Tomatoes"), None)
        cinema_score = next((r["Value"] for r in data.get("Ratings", []) if r["Source"] == "CinemaScore"), None)
        return {
            "rt": rt,
            "imdb": data.get("imdbRating"),
            "metacritic": data.get("Metascore"),
            "cinemaScore": cinema_score,
            "poster": data.get("Poster"),
            "genre": data.get("Genre"),
            "runtime": data.get("Runtime"),
            "plot": data.get("Plot"),
            "year": data.get("Year"),
            "director": data.get("Director"),
        }
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
        "poster": None,
        "genre": genres[idx % len(genres)],
        "runtime": f"{random.randint(90, 150)} min",
        "plot": plots[idx % len(plots)],
        "year": "2024",
        "director": "Various",
    }


# ─── VERDICTS ─────────────────────────────────────────────────────────────────

def fetch_verdict(title: str, ratings: dict) -> dict:
    """Ask Claude for a Watch/Skip/Depends verdict + one-line reason."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY) if ANTHROPIC_KEY else None

    rt = ratings.get("rt", "N/A")
    imdb = ratings.get("imdb", "N/A")
    meta = ratings.get("metacritic", "N/A")
    plot = ratings.get("plot", "")
    genre = ratings.get("genre", "")
    director = ratings.get("director", "")

    prompt = f"""You are a sharp, taste-driven film critic helping a cinephile decide what to watch at NYC indie theaters this week.

Movie: {title}
Director: {director}
Genre: {genre}
Plot: {plot}
Rotten Tomatoes: {rt}
IMDB: {imdb}
Metacritic: {meta}

Return ONLY a JSON object with exactly these fields:
{{
  "verdict": "WATCH" | "SKIP" | "DEPENDS",
  "reason": "One sharp sentence (max 15 words) explaining why.",
  "vibe": "One or two words describing the feeling/mood of the film"
}}

Be direct. No hedging. Think like someone with genuinely high standards who respects both arthouse and accessible cinema. DEPENDS means it's good but niche or divisive."""

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
    rt_str = ratings.get("rt") or "0%"
    score = int(rt_str.replace("%", "")) if rt_str and "%" in rt_str else 0
    if score >= 85:
        return {"verdict": "WATCH", "reason": "Critics are united — this one earns its runtime.", "vibe": "Essential"}
    elif score >= 70:
        return {"verdict": "DEPENDS", "reason": "Strong but divisive — know what you're signing up for.", "vibe": "Challenging"}
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
            {"WATCH": 0, "DEPENDS": 1, "SKIP": 2}.get(m["verdict"]["verdict"], 3),
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

    output_path = os.path.join(os.path.dirname(__file__), "../public/data.json")
    with open(output_path, "w") as f:
        json.dump(dataset, f, indent=2)

    print(f"\nDone. {len(dataset['movies'])} unique films written to public/data.json")
