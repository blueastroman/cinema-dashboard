"""
refresh_recent_ratings.py

Refreshes Rotten Tomatoes and OMDB ratings for recently-released movies in data.json.
Unlike scrape.py, this does NOT re-scrape theater showtimes — it only updates ratings
for movies that were recently released.

This is meant to run periodically (e.g., every 3 days) to keep ratings fresh for new
releases without re-fetching all theater data.

Usage:
  cd scripts
  OMDB_KEY=<key> python refresh_recent_ratings.py [--release-days N] [--dry-run]

Env:
  OMDB_KEY  - OMDb API key (required for OMDB lookups; RT scrape works without it)

Flags:
  --release-days N  - Only refresh movies released in the last N days (default: 30)
  --dry-run         - Print what would change without writing
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).parent))

from backfill_ratings import (
    fetch_omdb_rating,
    fetch_rt_rating,
    normalize_title,
)


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def get_movie_release_date(movie: dict[str, Any]) -> Optional[datetime]:
    """Extract the movie's release date."""
    ratings = movie.get("ratings") or {}
    release_date = ratings.get("releaseDate")
    if release_date:
        try:
            # Handle ISO date format (YYYY-MM-DD)
            return datetime.fromisoformat(release_date)
        except (ValueError, TypeError):
            pass
    return None


def is_recent_release(movie: dict[str, Any], days_threshold: int) -> bool:
    """Check if a movie was released within the last N days."""
    release_date = get_movie_release_date(movie)
    if not release_date:
        return False
    cutoff = datetime.now() - timedelta(days=days_threshold)
    return release_date >= cutoff


def should_refresh_ratings(movie: dict[str, Any]) -> bool:
    """Check if a movie's ratings should be refreshed."""
    ratings = movie.get("ratings") or {}
    title = movie.get("title", "")

    # Refresh if missing RT or OMDB scores
    has_rt = bool(ratings.get("rt"))
    has_omdb = bool(ratings.get("metacritic"))

    return not (has_rt and has_omdb) and bool(title.strip())


def refresh_movie_ratings(
    movie: dict[str, Any],
    omdb_key: str,
    dry_run: bool = False,
) -> tuple[bool, Optional[str]]:
    """Attempt to refresh a single movie's ratings. Returns (changed, message)."""
    title = movie.get("title", "").strip()
    if not title:
        return False, "No title"

    ratings = movie.get("ratings") or {}
    original_rt = ratings.get("rt")
    original_omdb = ratings.get("metacritic")

    # Fetch RT rating
    if not original_rt:
        rt_score = fetch_rt_rating(title, ratings.get("year"))
        if rt_score and not dry_run:
            if "ratings" not in movie:
                movie["ratings"] = {}
            movie["ratings"]["rt"] = rt_score

    # Fetch OMDB rating
    if not original_omdb and omdb_key:
        omdb_score = fetch_omdb_rating(
            normalize_title(title),
            omdb_key,
            ratings.get("year"),
        )
        if omdb_score and not dry_run:
            if "ratings" not in movie:
                movie["ratings"] = {}
            movie["ratings"]["metacritic"] = omdb_score

    changed = movie.get("ratings", {}).get("rt") != original_rt or \
              movie.get("ratings", {}).get("metacritic") != original_omdb

    if changed:
        rt_status = f"RT: {movie.get('ratings', {}).get('rt', 'N/A')}"
        omdb_status = f"OMDB: {movie.get('ratings', {}).get('metacritic', 'N/A')}"
        return True, f"{rt_status}, {omdb_status}"

    return False, None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-days", type=int, default=30, help="Days threshold for recently-released movies (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    args = parser.parse_args()

    data_path = Path(__file__).parent.parent / "public" / "data.json"
    if not data_path.exists():
        print(f"ERROR: {data_path} not found")
        sys.exit(1)

    omdb_key = __import__("os").environ.get("OMDB_KEY", "").strip()
    if not omdb_key:
        print("WARNING: OMDB_KEY not set; OMDB ratings will be skipped")

    print(f"Loading {data_path}...")
    data = load_json(data_path)
    movies = data.get("movies", [])

    print(f"Total movies: {len(movies)}")

    # Filter to recently-released movies
    recent_movies = [m for m in movies if is_recent_release(m, args.release_days)]
    print(f"Recently-released movies (released in last {args.release_days} days): {len(recent_movies)}")

    # Filter to those needing refresh
    to_refresh = [m for m in recent_movies if should_refresh_ratings(m)]
    print(f"Movies needing rating refresh: {len(to_refresh)}")

    if not to_refresh:
        print("No movies to refresh.")
        return

    # Refresh each movie
    refreshed_count = 0
    for idx, movie in enumerate(to_refresh, 1):
        title = movie.get("title", "")
        print(f"  [{idx}/{len(to_refresh)}] {title}...", end=" ", flush=True)

        changed, message = refresh_movie_ratings(movie, omdb_key, args.dry_run)
        if changed:
            print(f"✓ {message}")
            refreshed_count += 1
        else:
            print("(already complete)")

    print(f"\nRefreshed {refreshed_count}/{len(to_refresh)} movies")

    if not args.dry_run:
        print(f"Saving {data_path}...")
        save_json(data_path, data)
        print("Done.")
    else:
        print("(dry-run mode; no changes written)")


if __name__ == "__main__":
    main()
