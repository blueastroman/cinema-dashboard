from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from cinema_backend.common import (  # noqa: E402
    AMC_ALLOWED_CITIES_BY_STATE,
    AMC_EXCLUDED_THEATRES,
    THEATER_CONFIG,
    build_theater_meta,
    clean_title,
    date_iso,
    exact_title_identity_key,
    extract_special_formats,
    extract_year_int,
    format_day_label,
    format_time_label,
    get_source_ticket_url,
    make_movie_id,
    ny_now,
    sort_time_labels,
    split_trailing_title_year,
)


OUTPUT_DATA_PATH = ROOT / "public" / "data.json"
AMC_VENDOR_KEY = os.environ.get("AMC_VENDOR_KEY", "")
AMC_API_BASE = os.environ.get("AMC_API_BASE", "https://api.amctheatres.com").rstrip("/")
AMC_THEATRE_IDS = [token.strip() for token in os.environ.get("AMC_THEATRE_IDS", "").split(",") if token.strip()]
AMC_THEATRE_PAGE_SIZE = 100
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


NULL_RATINGS = {
    "imdbID": None,
    "rt": None,
    "imdb": None,
    "metacritic": None,
    "letterboxd": None,
    "poster": None,
    "genre": None,
    "runtime": None,
    "plot": None,
    "year": None,
    "director": None,
    "cinemaScore": None,
}


def load_dataset(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_dataset(path: Path, dataset: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(dataset, handle, indent=2, ensure_ascii=False)


def amc_request(path: str, params: Optional[dict] = None) -> Optional[dict]:
    request_params = params or {}
    try:
        response = requests.get(
            f"{AMC_API_BASE}{path}",
            params=request_params,
            headers={
                **DEFAULT_HEADERS,
                "X-AMC-Vendor-Key": AMC_VENDOR_KEY,
                "Accept": "application/json",
            },
            timeout=20,
        )
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        url = getattr(getattr(exc, "response", None), "url", None) or f"{AMC_API_BASE}{path}"
        print(f"  [ERROR] AMC API request failed for {path}: {exc} ({url})")
        return None


def is_supported_amc_theatre(theatre: dict[str, Any]) -> bool:
    location = theatre.get("location") or {}
    city = str(location.get("city") or "").strip().upper()
    state = str(location.get("state") or "").strip().upper()
    return city in AMC_ALLOWED_CITIES_BY_STATE.get(state, set())


def fetch_amc_theatres() -> list[dict[str, Any]]:
    theatres_by_id: dict[str, dict[str, Any]] = {}

    if AMC_THEATRE_IDS:
        data = amc_request("/v2/theatres", {"ids": ",".join(AMC_THEATRE_IDS), "page-size": AMC_THEATRE_PAGE_SIZE})
        for theatre in ((data or {}).get("_embedded", {}) or {}).get("theatres", []):
            theatre_id = str(theatre.get("id") or "").strip()
            if theatre_id and not theatre.get("isClosed"):
                theatres_by_id[theatre_id] = theatre
    else:
        page = 1
        while True:
            data = amc_request(
                "/v2/theatres",
                {
                    "page-size": AMC_THEATRE_PAGE_SIZE,
                    "page-number": page,
                },
            )
            if not data:
                break

            theatres = ((data.get("_embedded", {}) or {}).get("theatres", []))
            for theatre in theatres:
                theatre_id = str(theatre.get("id") or "").strip()
                if theatre_id and not theatre.get("isClosed") and is_supported_amc_theatre(theatre):
                    theatres_by_id[theatre_id] = theatre

            page_size = int(data.get("pageSize") or 0)
            page_number = int(data.get("pageNumber") or page)
            count = int(data.get("count") or 0)
            if page_size <= 0 or page_number * page_size >= count:
                break
            page += 1

    results = []
    for theatre in theatres_by_id.values():
        theatre_id = str(theatre.get("id") or "").strip()
        name = (theatre.get("longName") or theatre.get("name") or "").strip()
        if not theatre_id or not name:
            continue
        if name.strip().upper() in AMC_EXCLUDED_THEATRES:
            continue
        config = THEATER_CONFIG.get(name, {})
        results.append({
            "id": theatre_id,
            "name": name,
            "source": "amc",
            "official_url": str(
                theatre.get("websiteUrl")
                or theatre.get("websiteURL")
                or theatre.get("mobileUrl")
                or theatre.get("mobileURL")
                or config.get("official_url")
                or "https://www.amctheatres.com/"
            ).strip(),
        })

    return sorted(results, key=lambda theater: theater["name"])


def fetch_amc_showtimes(theater: dict[str, Any]) -> list[dict[str, Any]]:
    theatre_id = theater.get("id")
    if not theatre_id:
        return []

    grouped: dict[str, dict[str, dict[str, Any]]] = defaultdict(
        lambda: defaultdict(lambda: {"times": [], "ticket_urls": {}})
    )
    start = ny_now().replace(tzinfo=None)

    for offset in range(7):
        target = start + timedelta(days=offset)
        api_date = target.strftime("%m-%d-%Y")
        page = 1

        while True:
            data = amc_request(
                f"/v2/theatres/{theatre_id}/showtimes/{api_date}",
                {"page-size": 100, "page-number": page},
            )
            if not data:
                break

            showtimes = ((data.get("_embedded", {}) or {}).get("showtimes", []))
            for showtime in showtimes:
                if showtime.get("isCanceled"):
                    continue

                raw_title = (
                    showtime.get("sortableMovieName")
                    or showtime.get("movieName")
                    or showtime.get("sortableTitleName")
                    or showtime.get("title")
                    or ""
                )
                title = clean_title(raw_title)
                local_dt_raw = showtime.get("showDateTimeLocal")
                if not title or not local_dt_raw:
                    continue
                title_formats = extract_special_formats(
                    raw_title,
                    showtime.get("premiumOfferingName"),
                    showtime.get("format"),
                    showtime.get("experienceName"),
                    showtime.get("amenity"),
                )

                try:
                    local_dt = datetime.fromisoformat(str(local_dt_raw))
                except Exception:
                    continue

                day_label = format_day_label(local_dt)
                time_label = format_time_label(local_dt)
                ticket_url = str(
                    showtime.get("purchaseUrl")
                    or showtime.get("purchaseURL")
                    or showtime.get("ticketUrl")
                    or showtime.get("ticketURL")
                    or showtime.get("webSalesUrl")
                    or showtime.get("webSalesURL")
                    or get_source_ticket_url(theater)
                ).strip()
                day_bucket = grouped[title][day_label]
                day_bucket["date"] = date_iso(local_dt)
                day_bucket["times"].append(time_label)
                if ticket_url:
                    day_bucket["ticket_urls"].setdefault(time_label, ticket_url)
                if title_formats:
                    day_bucket.setdefault("special_formats", set()).update(title_formats)

            page_size = int(data.get("pageSize") or 0)
            page_number = int(data.get("pageNumber") or page)
            count = int(data.get("count") or 0)
            if page_size <= 0 or page_number * page_size >= count:
                break
            page += 1

    flattened = []
    for title, days in grouped.items():
        for day_label, payload in days.items():
            unique_times = sort_time_labels(sorted(set(payload.get("times", []))))
            ticket_urls = {
                time_label: str(url).strip()
                for time_label, url in (payload.get("ticket_urls") or {}).items()
                if time_label in unique_times and str(url).strip()
            }
            ticket_url = next(iter(ticket_urls.values()), get_source_ticket_url(theater))
            flattened.append({
                "title": title,
                "theater": theater["name"],
                "day": day_label,
                "date": payload.get("date"),
                "times": unique_times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
                "special_formats": sorted(payload.get("special_formats") or []),
            })

    return flattened


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
    seen = set()
    return [key for key in keys if key and not (key in seen or seen.add(key))]


def build_movie_index(movies: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index = {}
    for movie in movies:
        for key in movie_lookup_keys(movie):
            index.setdefault(key, movie)
    return index


def unique_movie_id(title: str, ratings: dict[str, Any], used_ids: set[str]) -> str:
    base_id = make_movie_id(title, ratings) or "amc-movie"
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
    movie = {
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


def is_amc_theater(theater: dict[str, Any]) -> bool:
    return str(theater.get("name") or "").strip().upper().startswith("AMC ")


def merge_amc_entries(dataset: dict[str, Any], amc_theaters: list[dict[str, Any]], entries: list[dict[str, Any]]) -> None:
    movies = [movie for movie in dataset.get("movies", []) if isinstance(movie, dict)]
    for movie in movies:
        movie["theaters"] = [
            theater
            for theater in (movie.get("theaters") or [])
            if not is_amc_theater(theater)
        ]
        movie["special_formats"] = sorted({
            str(fmt).strip()
            for theater in movie.get("theaters", [])
            for fmt in (theater.get("special_formats") or [])
            if str(fmt).strip()
        })

    movie_index = build_movie_index(movies)
    used_ids = {str(movie.get("id") or "").strip() for movie in movies if str(movie.get("id") or "").strip()}
    theater_meta = dict(dataset.get("theater_meta") or {})
    amc_by_name = {str(theater.get("name") or "").strip(): theater for theater in amc_theaters}
    schedules: dict[str, dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(lambda: {
        "slots": {},
        "ticket_url": "",
        "special_formats": set(),
    }))

    for theater in amc_theaters:
        theater_name = str(theater.get("name") or "").strip()
        if theater_name:
            theater_meta[theater_name] = build_theater_meta(
                theater_name,
                {
                    "source_type": "amc",
                    "official_url": theater.get("official_url") or get_source_ticket_url(theater),
                },
            )

    for entry in entries:
        title = str(entry.get("title") or "").strip()
        theater_name = str(entry.get("theater") or "").strip()
        day = str(entry.get("day") or "").strip()
        if not title or not theater_name or not day:
            continue

        movie = ensure_movie(title, movies, movie_index, used_ids)
        movie_key = str(movie.get("id") or title)
        bucket = schedules[theater_name][movie_key]
        bucket["movie"] = movie
        bucket["ticket_url"] = bucket["ticket_url"] or str(entry.get("ticket_url") or "").strip()
        bucket["special_formats"].update(str(fmt).strip() for fmt in (entry.get("special_formats") or []) if str(fmt).strip())

        slot = bucket["slots"].setdefault(day, {
            "day": day,
            "date": str(entry.get("date") or "").strip(),
            "times": set(),
            "ticket_urls": {},
        })
        slot["times"].update(str(time).strip() for time in (entry.get("times") or []) if str(time).strip())
        for time, url in (entry.get("ticket_urls") or {}).items():
            time_label = str(time).strip()
            ticket_url = str(url).strip()
            if time_label and ticket_url:
                slot["ticket_urls"][time_label] = ticket_url

    for theater_name, by_movie in schedules.items():
        theater = amc_by_name.get(theater_name, {"name": theater_name})
        for payload in by_movie.values():
            movie = payload["movie"]
            clean_schedule = []
            for slot in payload["slots"].values():
                times = sort_time_labels(sorted(slot["times"]))
                clean_slot = {"day": slot["day"], "times": times}
                if slot.get("date"):
                    clean_slot["date"] = slot["date"]
                ticket_urls = {
                    time: url
                    for time, url in slot.get("ticket_urls", {}).items()
                    if time in times and url
                }
                if ticket_urls:
                    clean_slot["ticket_urls"] = ticket_urls
                clean_schedule.append(clean_slot)

            clean_schedule.sort(key=lambda slot: (slot.get("date") or "", slot.get("day") or ""))
            formats = sorted(payload["special_formats"])
            movie.setdefault("theaters", []).append({
                "name": theater_name,
                "ticket_url": payload["ticket_url"] or get_source_ticket_url(theater),
                "schedule": clean_schedule,
                "special_formats": formats,
            })
            movie["special_formats"] = sorted(set(movie.get("special_formats") or []).union(formats))

    dataset["generated_at"] = ny_now().isoformat()
    dataset["week_of"] = (ny_now() + timedelta(days=(4 - ny_now().weekday()) % 7)).strftime("%B %d, %Y")
    dataset["theater_meta"] = theater_meta
    dataset["movies"] = sorted(
        [movie for movie in movies if movie.get("theaters")],
        key=lambda movie: str(movie.get("title") or "").lower(),
    )
    dataset["theaters"] = sorted({
        str(theater.get("name") or "").strip()
        for movie in dataset["movies"]
        for theater in (movie.get("theaters") or [])
        if str(theater.get("name") or "").strip()
    })


def main() -> int:
    if not AMC_VENDOR_KEY:
        raise RuntimeError("AMC_VENDOR_KEY is required to refresh AMC showtimes.")

    dataset = load_dataset(OUTPUT_DATA_PATH)
    amc_theaters = fetch_amc_theatres()
    if not amc_theaters:
        raise RuntimeError("AMC API returned no supported theaters.")

    entries = []
    for theater in amc_theaters:
        print(f"\nRefreshing AMC: {theater['name']}")
        entries.extend(fetch_amc_showtimes(theater))

    if not entries:
        raise RuntimeError("AMC API returned no showtimes.")

    merge_amc_entries(dataset, amc_theaters, entries)
    save_dataset(OUTPUT_DATA_PATH, dataset)
    print(f"\nDone. Refreshed {len(entries)} AMC schedule entries across {len(amc_theaters)} theaters.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
