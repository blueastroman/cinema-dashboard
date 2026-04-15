"""
generate_verdicts.py

Run after scrape in GitHub Actions to generate editorial one-liners for each film.
Uses a cache to avoid re-running API calls for films already processed.
Films released within the last 30 days get refreshed to account for score changes.

Usage:
  python generate_verdicts.py

Env:
  ANTHROPIC_API_KEY - your Anthropic API key

Files:
  data.json          - input/output, the main showtimes data
  verdicts_cache.json - persistent cache of generated verdicts (commit this to repo)
"""

import json
import os
import urllib.request
from datetime import datetime, timedelta

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATA_FILE = "public/data.json"
CACHE_FILE = "scripts/verdicts_cache.json"
BATCH_SIZE = 30
REFRESH_DAYS = 30

SYSTEM_PROMPT = """You are the editorial voice of a curated NYC cinema showtimes board. Your job: for each film, give a verdict (WATCH, DEPENDS, or SKIP) and a one-liner.

VERDICT RULES:
- Use all available scores (RT, IMDB, Metacritic, Letterboxd) plus your knowledge of the film and director.
- WATCH = genuinely worth going to a theater for. Strong scores, strong filmmaker, or a must-see experience.
- DEPENDS = decent but not essential. Mixed scores, niche appeal, or "good not great."
- SKIP = not worth the trip. Weak scores, lazy franchise, or actively bad.
- Classic repertory films screening at art house theaters (older acclaimed films) should almost always be WATCH. If it survived 30+ years and is screening at a place like Metrograph or Film Forum, it earned that.

VOICE RULES:
- Talk like you're texting a friend who asked "should I see this?"
- One to two sentences max. Period.
- Be specific to THIS film. Reference the actual plot, director, cast, or what makes it tick.
- Have a real opinion. Don't hedge.
- No adjective stacking. No "masterful," "riveting," "poignant," "tour de force," "compelling," "gripping."
- No film critic language. No "exploration of," "meditation on," "unflinching look at."
- No em dashes.
- Funny is good when it fits. Blunt is always good.
- For SKIP, be honest about why. Don't be mean for sport but don't sugarcoat it.
- If you don't know the film well, lean on the data and be upfront about it.

RESPOND IN THIS EXACT JSON FORMAT (array of objects):
[
  {"title": "Film Title", "verdict": "WATCH", "reason": "Your one-liner here."},
  ...
]

No markdown. No backticks. Just the JSON array."""


def load_json(path, default=None):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default if default is not None else {}


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def parse_release_year(movie):
    """Extract release year from movie data. Returns int or None."""
    year_str = movie.get("ratings", {}).get("year")
    if year_str:
        try:
            return int(str(year_str).strip()[:4])
        except (ValueError, TypeError):
            pass
    return None


def is_recent_release(movie, now=None):
    """Check if a film was released within the last REFRESH_DAYS days."""
    if now is None:
        now = datetime.now()

    year = parse_release_year(movie)
    if year is None:
        return True  # no year data, assume recent to be safe

    current_year = now.year
    cutoff_year = (now - timedelta(days=REFRESH_DAYS)).year

    # If the film's year is the current year or within the cutoff window, treat as recent
    if year >= cutoff_year:
        return True

    return False


def needs_verdict(movie, cache, now=None):
    """Determine if a film needs a new API call."""
    movie_id = movie.get("id")
    if not movie_id:
        return True

    if movie_id not in cache:
        return True

    # Cached, but check if it's a recent release that should be refreshed
    if is_recent_release(movie, now):
        return True

    return False


def build_film_block(movie):
    """Build the text description of a film for the API prompt."""
    r = movie.get("ratings", {})
    lines = [f"Title: {movie['title']}"]

    if r.get("year"):
        lines.append(f"Year: {r['year']}")
    if r.get("director"):
        lines.append(f"Director: {r['director']}")
    if r.get("genre"):
        lines.append(f"Genre: {r['genre']}")
    if r.get("runtime"):
        lines.append(f"Runtime: {r['runtime']}")
    if r.get("rt"):
        lines.append(f"RT Critics: {r['rt']}")
    if r.get("imdb"):
        lines.append(f"IMDB: {r['imdb']}")
    if r.get("metacritic"):
        lines.append(f"Metacritic: {r['metacritic']}")
    if r.get("letterboxd"):
        lines.append(f"Letterboxd: {r['letterboxd']}")
    if r.get("plot"):
        lines.append(f"Plot: {r['plot']}")

    return "\n".join(lines)


def call_claude(films_block):
    """Send a batch of films to the Claude API and return parsed verdicts."""
    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4000,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": f"Here are the films. Give me verdict + one-liner for each:\n\n{films_block}",
            }
        ],
    }

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    text = result["content"][0]["text"]
    clean = text.replace("```json", "").replace("```", "").strip()
    return json.loads(clean)


def main():
    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set")
        return

    # Load data
    data = load_json(DATA_FILE)
    cache = load_json(CACHE_FILE, default={})
    movies = data.get("movies", [])
    now = datetime.now()

    print(f"Total films: {len(movies)}")
    print(f"Cached verdicts: {len(cache)}")

    # Split into needs-work and already-done
    to_process = []
    cached_count = 0

    for movie in movies:
        if needs_verdict(movie, cache, now):
            to_process.append(movie)
        else:
            cached_count += 1

    print(f"Using cache: {cached_count}")
    print(f"Need API calls: {len(to_process)}")

    # Process in batches
    if to_process:
        batches = [
            to_process[i : i + BATCH_SIZE]
            for i in range(0, len(to_process), BATCH_SIZE)
        ]

        for batch_idx, batch in enumerate(batches):
            print(
                f"\nBatch {batch_idx + 1}/{len(batches)}: {len(batch)} films"
            )

            films_block = "\n---\n".join(build_film_block(m) for m in batch)

            try:
                verdicts = call_claude(films_block)

                # Map results by title for matching
                verdict_map = {v["title"]: v for v in verdicts}

                for movie in batch:
                    title = movie["title"]
                    movie_id = movie.get("id")

                    if title in verdict_map:
                        v = verdict_map[title]
                        cache_entry = {
                            "verdict": v["verdict"],
                            "reason": v["reason"],
                            "generated_at": now.isoformat(),
                        }

                        # Store in cache by IMDB ID
                        if movie_id:
                            cache[movie_id] = cache_entry

                        print(f"  {v['verdict']:7s} {title}")
                    else:
                        print(f"  MISS    {title} (no match in API response)")

            except Exception as e:
                print(f"  ERROR: {e}")
                print("  Skipping batch, will retry next run")
                continue

    # Write verdicts from cache back into data.json
    updated = 0
    for movie in movies:
        movie_id = movie.get("id")
        if movie_id and movie_id in cache:
            entry = cache[movie_id]
            movie["verdict"] = {
                "verdict": entry["verdict"],
                "reason": entry["reason"],
                "vibe": movie.get("verdict", {}).get("vibe", ""),
            }
            updated += 1

    print(f"\nUpdated {updated}/{len(movies)} films in data.json")

    # Save
    save_json(DATA_FILE, data)
    save_json(CACHE_FILE, cache)

    print(f"Cache size: {len(cache)} entries")
    print("Done.")


if __name__ == "__main__":
    main()
