"""Refresh MoMA showtimes in data.json from a locally exported HTML file.

To update MoMA data:
  1. Visit https://www.moma.org/calendar/?happening_filter=Films&location=both
  2. Save the full page as HTML to scripts/moma_export.html
  3. Commit the file and trigger this action (or it runs automatically on push).
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from cinema_backend.common import (  # noqa: E402
    THEATER_CONFIG,
    build_theater_meta,
    exact_title_identity_key,
    extract_year_int,
    get_source_ticket_url,
    make_movie_id,
    ny_now,
    sort_time_labels,
    split_trailing_title_year,
)
from scrape import fetch_moma_showtimes  # noqa: E402

OUTPUT_DATA_PATH = ROOT / "public" / "data.json"
THEATER_NAME = "Museum of Modern Art"

NULL_RATINGS: dict[str, Any] = {
    "imdbID": None, "rt": None, "imdb": None, "metacritic": None,
    "letterboxd": None, "cinemaScore": None, "poster": None,
    "genre": None, "runtime": None, "plot": None, "year": None, "director": None,
}


def load_dataset(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_dataset(path: Path, dataset: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(dataset, handle, indent=2, ensure_ascii=False)


def movie_lookup_keys(movie: dict[str, Any]) -> list[str]:
    title = str(movie.get("title") or "").strip()
    ratings = movie.get("ratings") or {}
    year = extract_year_int(ratings.get("year"))
    base_title, title_year = split_trailing_title_year(title)
    keys = [
        exact_title_identity_key(title, year),
        exact_title_identity_key(title, title_year),
        exact_title_identity_key(title),
        exact_title_identity_key(base_title, year),
        exact_title_identity_key(base_title, title_year),
        exact_title_identity_key(base_title),
    ]
    seen: set[str] = set()
    return [k for k in keys if k and not (k in seen or seen.add(k))]  # type: ignore[func-returns-value]


def build_movie_index(movies: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for movie in movies:
        for key in movie_lookup_keys(movie):
            index.setdefault(key, movie)
    return index


def unique_movie_id(title: str, ratings: dict[str, Any], used_ids: set[str]) -> str:
    base_id = make_movie_id(title, ratings) or "moma-movie"
    movie_id = base_id
    suffix = 2
    while movie_id in used_ids:
        movie_id = f"{base_id}-{suffix}"
        suffix += 1
    used_ids.add(movie_id)
    return movie_id


def ensure_movie(
    title: str,
    movies: list[dict[str, Any]],
    movie_index: dict[str, dict[str, Any]],
    used_ids: set[str],
) -> dict[str, Any]:
    for key in movie_lookup_keys({"title": title, "ratings": {}}):
        movie = movie_index.get(key)
        if movie:
            return movie

    ratings = dict(NULL_RATINGS)
    movie: dict[str, Any] = {
        "id": unique_movie_id(title, ratings, used_ids),
        "title": title,
        "ratings": ratings,
        "theaters": [],
        "special_formats": [],
    }
    movies.append(movie)
    for key in movie_lookup_keys(movie):
        movie_index.setdefault(key, movie)
    return movie


def merge_moma_entries(dataset: dict[str, Any], entries: list[dict[str, Any]]) -> None:
    movies = [m for m in dataset.get("movies", []) if isinstance(m, dict)]

    for movie in movies:
        movie["theaters"] = [t for t in (movie.get("theaters") or []) if t.get("name") != THEATER_NAME]
        movie["special_formats"] = sorted({
            str(fmt).strip()
            for t in movie.get("theaters", [])
            for fmt in (t.get("special_formats") or [])
            if str(fmt).strip()
        })

    movie_index = build_movie_index(movies)
    used_ids = {str(m.get("id") or "").strip() for m in movies if str(m.get("id") or "").strip()}
    theater_meta = dict(dataset.get("theater_meta") or {})
    theater_cfg = THEATER_CONFIG.get(THEATER_NAME, {})
    theater_meta[THEATER_NAME] = build_theater_meta(THEATER_NAME)

    schedules: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "slots": {},
        "ticket_url": "",
        "special_formats": set(),
    })

    for entry in entries:
        title = str(entry.get("title") or "").strip()
        day = str(entry.get("day") or "").strip()
        if not title or not day:
            continue

        movie = ensure_movie(title, movies, movie_index, used_ids)
        movie_key = str(movie.get("id") or title)
        bucket = schedules[movie_key]
        bucket["movie"] = movie
        bucket["ticket_url"] = bucket["ticket_url"] or str(entry.get("ticket_url") or "").strip()
        bucket["special_formats"].update(str(f).strip() for f in (entry.get("special_formats") or []) if str(f).strip())

        slot = bucket["slots"].setdefault(day, {
            "day": day,
            "date": str(entry.get("date") or "").strip(),
            "times": set(),
            "ticket_urls": {},
        })
        slot["times"].update(str(t).strip() for t in (entry.get("times") or []) if str(t).strip())
        for time, url in (entry.get("ticket_urls") or {}).items():
            if str(time).strip() and str(url).strip():
                slot["ticket_urls"][str(time).strip()] = str(url).strip()

    theater = {"name": THEATER_NAME, **theater_cfg}
    for payload in schedules.values():
        movie = payload["movie"]
        clean_schedule = []
        for slot in payload["slots"].values():
            times = sort_time_labels(sorted(slot["times"]))
            clean_slot: dict[str, Any] = {"day": slot["day"], "times": times}
            if slot.get("date"):
                clean_slot["date"] = slot["date"]
            ticket_urls = {t: url for t, url in slot.get("ticket_urls", {}).items() if t in times and url}
            if ticket_urls:
                clean_slot["ticket_urls"] = ticket_urls
            clean_schedule.append(clean_slot)
        clean_schedule.sort(key=lambda s: (s.get("date") or "", s.get("day") or ""))
        formats = sorted(payload["special_formats"])
        movie.setdefault("theaters", []).append({
            "name": THEATER_NAME,
            "ticket_url": payload["ticket_url"] or get_source_ticket_url(theater),
            "schedule": clean_schedule,
            "special_formats": formats,
        })
        movie["special_formats"] = sorted(set(movie.get("special_formats") or []).union(formats))

    dataset["generated_at"] = ny_now().isoformat()
    dataset["week_of"] = (ny_now() + timedelta(days=(4 - ny_now().weekday()) % 7)).strftime("%B %d, %Y")
    dataset["theater_meta"] = theater_meta
    dataset["movies"] = sorted(
        [m for m in movies if m.get("theaters")],
        key=lambda m: str(m.get("title") or "").lower(),
    )
    dataset["theaters"] = sorted({
        str(t.get("name") or "").strip()
        for m in dataset["movies"]
        for t in (m.get("theaters") or [])
        if str(t.get("name") or "").strip()
    })


def main() -> int:
    export_path = SCRIPT_DIR / "moma_export.html"
    if not export_path.exists():
        print(f"[ERROR] {export_path} not found.")
        print("Export https://www.moma.org/calendar/?happening_filter=Films&location=both")
        print("and save it as scripts/moma_export.html before running this script.")
        return 1

    print(f"Refreshing {THEATER_NAME} showtimes from {export_path.name}...")
    theater = {"name": THEATER_NAME, **THEATER_CONFIG[THEATER_NAME]}
    entries = fetch_moma_showtimes(theater)
    print(f"  Parsed {len(entries)} showtime entries")

    dataset = load_dataset(OUTPUT_DATA_PATH)
    merge_moma_entries(dataset, entries)
    save_dataset(OUTPUT_DATA_PATH, dataset)

    moma_movies = [m for m in dataset["movies"] if any(t["name"] == THEATER_NAME for t in m.get("theaters", []))]
    print(f"  {len(moma_movies)} films at {THEATER_NAME} written to data.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
