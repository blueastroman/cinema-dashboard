"""Refresh the rolling six-month coming-soon movie feed.

The release calendar is discovered from Box Office Mojo. Wide releases and
dated specialty/independent theatrical releases are retained. Metadata is
enriched through OMDb when ``OMDB_KEY`` is available, with TMDB's public movie
pages as a best-effort fallback.

Usage:
    python scripts/refresh_coming_soon.py
    python scripts/refresh_coming_soon.py --dry-run
    python scripts/refresh_coming_soon.py --today 2026-07-19
"""

from __future__ import annotations

import argparse
import calendar
import json
import os
import re
import time
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "public" / "coming-soon.json"
BOX_OFFICE_MOJO_CALENDAR = "https://www.boxofficemojo.com/calendar/{year}-{month:02d}-01/"
TMDB_SEARCH = "https://www.themoviedb.org/search/movie?query={query}"
TMDB_ROOT = "https://www.themoviedb.org"
LETTERBOXD_FILM = "https://letterboxd.com/film/{slug}/"

SPECIALTY_DISTRIBUTORS = {
    "a24",
    "black bear",
    "briarcliff entertainment",
    "focus features",
    "gkids",
    "greenwich entertainment",
    "ifc films",
    "independent film company",
    "kino lorber",
    "mubi",
    "neon",
    "orion pictures",
    "oscilloscope",
    "roadside attractions",
    "row k entertainment",
    "searchlight pictures",
    "sony pictures classics",
    "strand releasing",
    "variance films",
    "vertical entertainment",
    "well go usa entertainment",
}

SKIP_TITLE_MARKERS = (
    "anniversary",
    "re-release",
    "remastered",
    "untitled",
    "unknown title",
)


def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; ShowtimesNYC/1.0; +https://github.com/blueastroman/cinema-dashboard)",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def normalize_title(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", ascii_value.lower())


def title_slug(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    return re.sub(r"^-+|-+$", "", re.sub(r"[^a-z0-9]+", "-", ascii_value.lower()))


def add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def iter_months(start: date, end: date) -> Iterable[tuple[int, int]]:
    cursor = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while cursor <= last:
        yield cursor.year, cursor.month
        cursor = add_months(cursor, 1)


def parse_calendar_page(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    releases: list[dict[str, Any]] = []
    release_date: date | None = None

    for row in soup.select("table tr"):
        cells = row.find_all(["th", "td"], recursive=False)
        if len(cells) == 1:
            try:
                release_date = datetime.strptime(cells[0].get_text(" ", strip=True), "%B %d, %Y").date()
            except ValueError:
                release_date = None
            continue
        if len(cells) < 3 or not release_date:
            continue

        title_node = cells[0].find("h3")
        if not title_node:
            continue
        title = title_node.get_text(" ", strip=True)
        studio = cells[1].get_text(" ", strip=True)
        scale = cells[2].get_text(" ", strip=True)
        detail_text = cells[0].get_text(" ", strip=True).lower()
        genre_node = cells[0].select_one(".mojo-schedule-genres")
        genres = []
        if genre_node:
            genres = [part.strip() for part in genre_node.get_text("\n").splitlines() if part.strip()]
        poster_node = cells[0].find("img")
        poster = ""
        if poster_node:
            poster = poster_node.get("data-a-hires") or poster_node.get("src") or ""

        releases.append(
            {
                "title": title,
                "release_date": release_date.isoformat(),
                "poster": poster,
                "synopsis": "",
                "director": "",
                "genres": genres,
                "studio": studio if studio not in {"-", "N/A"} else "",
                "release_scale": scale,
                "_detail_text": detail_text,
            }
        )
    return releases


def should_include(movie: dict[str, Any], start: date, end: date) -> bool:
    try:
        release = date.fromisoformat(movie.get("release_date", ""))
    except ValueError:
        return False
    if not start <= release <= end:
        return False
    title = movie.get("title", "")
    detail = f"{title} {movie.get('_detail_text', '')}".lower()
    if any(marker in detail for marker in SKIP_TITLE_MARKERS):
        return False
    if movie.get("release_scale", "").lower() == "wide":
        return True
    return movie.get("studio", "").strip().lower() in SPECIALTY_DISTRIBUTORS


def fetch_release_calendar(session: requests.Session, start: date, end: date) -> list[dict[str, Any]]:
    discovered: dict[tuple[str, str], dict[str, Any]] = {}
    for year, month in iter_months(start, end):
        url = BOX_OFFICE_MOJO_CALENDAR.format(year=year, month=month)
        response = session.get(url, timeout=30)
        response.raise_for_status()
        for movie in parse_calendar_page(response.text):
            if should_include(movie, start, end):
                key = (normalize_title(movie["title"]), movie["release_date"])
                discovered[key] = movie
    return list(discovered.values())


def fetch_omdb_metadata(session: requests.Session, title: str, year: int, api_key: str) -> dict[str, Any]:
    if not api_key:
        return {}
    response = session.get(
        "https://www.omdbapi.com/",
        params={"apikey": api_key, "t": title, "y": year, "plot": "short"},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("Response") != "True":
        return {}
    return {
        "director": "" if payload.get("Director") == "N/A" else payload.get("Director", ""),
        "genres": [] if payload.get("Genre") == "N/A" else [x.strip() for x in payload.get("Genre", "").split(",") if x.strip()],
        "studio": "" if payload.get("Production") == "N/A" else payload.get("Production", ""),
        "synopsis": "" if payload.get("Plot") == "N/A" else payload.get("Plot", ""),
        "poster": "" if payload.get("Poster") == "N/A" else payload.get("Poster", ""),
    }


def parse_tmdb_search(html: str, title: str, year: int) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    matches: list[dict[str, Any]] = []
    for image in soup.find_all("img", alt=True):
        wrapper = image.find_parent("div", class_="flex flex-nowrap")
        if not wrapper:
            continue
        heading = wrapper.find("h2")
        date_node = wrapper.select_one(".release_date")
        link = wrapper.select_one('a[href^="/movie/"]')
        if not heading or not date_node or not link:
            continue
        candidate_title = heading.get_text(" ", strip=True)
        try:
            candidate_date = datetime.strptime(date_node.get_text(" ", strip=True), "%B %d, %Y").date()
        except ValueError:
            continue
        if normalize_title(candidate_title) != normalize_title(title) or candidate_date.year != year:
            continue
        poster = image.get("src", "").replace("/w94_and_h141_face/", "/w500/")
        overview = wrapper.find("p")
        matches.append(
            {
                "href": link.get("href", ""),
                "poster": poster,
                "synopsis": overview.get_text(" ", strip=True) if overview else "",
            }
        )
    return matches[0] if matches else {}


def parse_tmdb_details(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    directors = []
    for profile in soup.select("ol.people.no_image li.profile"):
        role = profile.select_one("p.character")
        name = profile.select_one("p a")
        if role and name and "Director" in role.get_text(" ", strip=True):
            directors.append(name.get_text(" ", strip=True))
    genre_node = soup.select_one("span.genres")
    genres = []
    if genre_node:
        genres = [a.get_text(" ", strip=True) for a in genre_node.find_all("a")]
    return {"director": ", ".join(dict.fromkeys(directors)), "genres": genres}


def fetch_tmdb_metadata(session: requests.Session, title: str, year: int) -> dict[str, Any]:
    query = quote(f"{title} y:{year}")
    response = session.get(TMDB_SEARCH.format(query=query), timeout=25)
    response.raise_for_status()
    metadata = parse_tmdb_search(response.text, title, year)
    if not metadata:
        return {}
    href = metadata.pop("href", "")
    if href:
        time.sleep(0.15)
        detail_response = session.get(f"{TMDB_ROOT}{href}", timeout=25)
        detail_response.raise_for_status()
        metadata.update({k: v for k, v in parse_tmdb_details(detail_response.text).items() if v})
    return metadata


def parse_letterboxd_match(html: str, title: str, year: int) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    title_node = soup.find("meta", property="og:title")
    if not title_node:
        return False
    display_title = title_node.get("content", "").strip()
    match = re.match(r"^(.*?)\s*\((\d{4})\)$", display_title)
    if not match:
        return False
    return normalize_title(match.group(1)) == normalize_title(title) and int(match.group(2)) == year


def fetch_letterboxd_url(session: requests.Session, title: str, year: int) -> str:
    slug = title_slug(title)
    if not slug:
        return ""
    for candidate in (slug, f"{slug}-{year}"):
        url = LETTERBOXD_FILM.format(slug=candidate)
        response = session.get(url, timeout=20)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        if parse_letterboxd_match(response.text, title, year):
            return url
    return ""


def merge_movies(existing: list[dict[str, Any]], discovered: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = {normalize_title(movie.get("title", "")): dict(movie) for movie in existing if movie.get("title")}
    for movie in discovered:
        key = normalize_title(movie["title"])
        current = merged.get(key, {})
        combined = {**movie, **current}
        # The release calendar is authoritative for dates and distribution metadata.
        combined["release_date"] = movie["release_date"]
        combined["studio"] = movie.get("studio") or current.get("studio", "")
        combined["release_scale"] = movie.get("release_scale", current.get("release_scale", ""))
        if not current.get("genres"):
            combined["genres"] = movie.get("genres", [])
        merged[key] = combined
    return list(merged.values())


def enrich_movies(session: requests.Session, movies: list[dict[str, Any]], omdb_key: str) -> None:
    for index, movie in enumerate(movies, 1):
        missing = [key for key in ("director", "genres", "studio", "synopsis", "poster", "letterboxd_url") if not movie.get(key)]
        if not missing:
            continue
        year = date.fromisoformat(movie["release_date"]).year
        print(f"  Enriching {index}/{len(movies)}: {movie['title']}")
        metadata: dict[str, Any] = {}
        try:
            metadata.update(fetch_omdb_metadata(session, movie["title"], year, omdb_key))
        except (requests.RequestException, ValueError) as exc:
            print(f"    OMDb unavailable: {exc}")
        if not metadata.get("director") or not metadata.get("genres") or not metadata.get("synopsis"):
            try:
                time.sleep(0.2)
                fallback = fetch_tmdb_metadata(session, movie["title"], year)
                metadata = {**fallback, **{k: v for k, v in metadata.items() if v}}
            except (requests.RequestException, ValueError) as exc:
                print(f"    TMDB unavailable: {exc}")
        for key, value in metadata.items():
            if value and not movie.get(key):
                movie[key] = value
        if not movie.get("letterboxd_url"):
            try:
                time.sleep(0.1)
                movie["letterboxd_url"] = fetch_letterboxd_url(session, movie["title"], year)
            except requests.RequestException as exc:
                print(f"    Letterboxd unavailable: {exc}")


def clean_movie(movie: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": movie.get("title", ""),
        "release_date": movie.get("release_date", ""),
        "poster": movie.get("poster", ""),
        "synopsis": movie.get("synopsis", ""),
        "director": movie.get("director", ""),
        "genres": movie.get("genres", []),
        "studio": movie.get("studio", ""),
        "release_scale": movie.get("release_scale", ""),
        "letterboxd_url": movie.get("letterboxd_url", ""),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--today", help="Override today's date (YYYY-MM-DD)")
    args = parser.parse_args()

    today = date.fromisoformat(args.today) if args.today else date.today()
    end = add_months(today, 6)
    session = build_session()
    existing_payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8")) if OUTPUT_PATH.exists() else {"movies": []}
    existing = [
        movie for movie in existing_payload.get("movies", [])
        if today <= date.fromisoformat(movie["release_date"]) <= end
    ]

    print(f"Discovering theatrical releases from {today} through {end}...")
    discovered = fetch_release_calendar(session, today, end)
    print(f"Found {len(discovered)} qualifying calendar releases; merging with {len(existing)} existing films.")
    movies = merge_movies(existing, discovered)
    enrich_movies(session, movies, os.environ.get("OMDB_KEY", "").strip())
    movies = sorted((clean_movie(movie) for movie in movies), key=lambda movie: (movie["release_date"], movie["title"]))

    payload = {"updated_at": today.isoformat(), "window_end": end.isoformat(), "movies": movies}
    print(f"Prepared {len(movies)} coming-soon films.")
    if args.dry_run:
        return
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Updated {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
