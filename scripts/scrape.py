"""
NYC Cinema Dashboard - Weekly Scraper
Runs every Wednesday via GitHub Actions
Pulls showtimes via SerpAPI/AMC API, ratings via OMDb, verdicts via Claude API
"""

import os
import json
import hashlib
import requests
import anthropic
import html
import subprocess
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote
from typing import Optional

# ─── CONFIG ──────────────────────────────────────────────────────────────────

THEATER_CONFIG = {
    "Metrograph": {
        "slug": "metrograph",
        "short_name": "Metrograph",
        "sort_name": "Metrograph",
        "source_type": "metrograph",
        "source_url": "https://t.metrograph.com/Browsing/Cinemas/Details/9999",
        "official_url": "https://metrograph.com",
        "aliases": ["metro"],
    },
    "IFC Center": {
        "slug": "ifc",
        "short_name": "IFC",
        "sort_name": "IFC",
        "source_type": "ifc",
        "source_url": "https://www.ifccenter.com/now-playing/",
        "serpapi_id": "ifc center new york",
        "official_url": "https://www.ifccenter.com",
        "aliases": ["ifc"],
    },
    "Angelika Film Center": {
        "slug": "angelika",
        "short_name": "Angelika",
        "sort_name": "Angelika",
        "source_type": "serpapi",
        "serpapi_id": "angelika film center new york",
        "official_url": "https://angelikafilmcenter.com/nyc",
        "aliases": ["angelika"],
    },
    "Film Forum": {
        "slug": "film-forum",
        "short_name": "Film Forum",
        "sort_name": "Film Forum",
        "source_type": "serpapi",
        "serpapi_id": "film forum new york",
        "official_url": "https://www.filmforum.org/now-playing/",
        "aliases": ["film", "film forum"],
    },
    "Village East by Angelika": {
        "slug": "village-east",
        "short_name": "Village East",
        "sort_name": "Village East",
        "source_type": "serpapi",
        "serpapi_id": "village east cinema new york",
        "official_url": "https://angelikafilmcenter.com/villageeast",
        "aliases": ["village", "village east"],
    },
    "Film at Lincoln Center": {
        "slug": "flc",
        "short_name": "FLC",
        "sort_name": "FLC",
        "source_type": "serpapi",
        "serpapi_id": "film at lincoln center new york",
        "official_url": "https://www.filmlinc.org/now-playing/",
        "aliases": ["flc", "film linc", "lincoln center", "film at lincoln center"],
    },
    "Alamo Drafthouse Lower Manhattan": {
        "slug": "alamo",
        "short_name": "Alamo",
        "sort_name": "Alamo",
        "source_type": "alamo",
        "market_slug": "nyc",
        "cinema_id": "2103",
        "serpapi_id": "alamo drafthouse lower manhattan new york",
        "official_url": "https://drafthouse.com/nyc",
        "aliases": ["alamo"],
    },
    "Alamo Drafthouse Downtown Brooklyn": {
        "slug": "alamo-brooklyn",
        "short_name": "Alamo Brooklyn",
        "sort_name": "Alamo Brooklyn",
        "source_type": "alamo",
        "market_slug": "nyc",
        "cinema_id": "2101",
        "serpapi_id": "alamo drafthouse downtown brooklyn new york",
        "official_url": "https://drafthouse.com/theater/downtown-brooklyn",
        "aliases": ["alamo brooklyn", "downtown brooklyn", "brooklyn alamo"],
    },
    "Alamo Drafthouse Staten Island": {
        "slug": "alamo-staten-island",
        "short_name": "Alamo Staten Island",
        "sort_name": "Alamo Staten Island",
        "source_type": "alamo",
        "market_slug": "nyc",
        "cinema_id": "2102",
        "serpapi_id": "alamo drafthouse staten island new york",
        "official_url": "https://drafthouse.com/theater/staten-island",
        "aliases": ["alamo staten island", "staten island", "staten island alamo"],
    },
    "Paris Theater": {
        "slug": "paris",
        "short_name": "Paris",
        "sort_name": "Paris",
        "source_type": "serpapi",
        "serpapi_id": "paris theater new york",
        "official_url": "https://www.paristheaternyc.com/",
        "aliases": ["paris"],
    },
    "Museum of Modern Art": {
        "slug": "moma",
        "short_name": "MoMA",
        "sort_name": "MoMA",
        "source_type": "serpapi",
        "serpapi_id": "museum of modern art new york film",
        "official_url": "https://www.moma.org/calendar/film/",
        "aliases": ["moma", "museum of modern art", "moma film"],
    },
    "AMC Landmark 8": {
        "slug": "amc-landmark-8",
        "short_name": "AMC Landmark 8",
        "sort_name": "AMC Landmark 8",
        "source_type": "amc",
        "serpapi_id": "amc landmark 8 stamford ct",
        "official_url": "https://www.amctheatres.com/movie-theatres/new-york-city/amc-landmark-8",
        "aliases": [],
    },
    "AMC Majestic 6": {
        "slug": "amc-majestic-6",
        "short_name": "AMC Majestic 6",
        "sort_name": "AMC Majestic 6",
        "source_type": "amc",
        "serpapi_id": "amc majestic 6 stamford ct",
        "official_url": "https://www.amctheatres.com/movie-theatres/new-york-city/amc-majestic-6",
        "aliases": [],
    },
}

SERPAPI_THEATERS = [
    {"name": name, **config}
    for name, config in THEATER_CONFIG.items()
    if config.get("source_type") == "serpapi"
]

STATIC_THEATERS = [
    {"name": name, **config}
    for name, config in THEATER_CONFIG.items()
    if config.get("source_type") != "amc"
]

SERPAPI_KEY = os.environ.get("SERPAPI_KEY", "")
OMDB_KEY = os.environ.get("OMDB_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
AMC_VENDOR_KEY = os.environ.get("AMC_VENDOR_KEY", "")
AMC_API_BASE = os.environ.get("AMC_API_BASE", "https://api.amctheatres.com").rstrip("/")
AMC_THEATRE_IDS = [t.strip() for t in os.environ.get("AMC_THEATRE_IDS", "").split(",") if t.strip()]
AMC_ALLOWED_CITIES_BY_STATE = {
    "NY": {"NEW YORK", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"},
    "CT": {"STAMFORD"},
}
AMC_EXCLUDED_THEATRES = {
    "AMC BAY PLAZA CINEMA 13",
    "AMC MAGIC JOHNSON HARLEM 9",
    "AMC STATEN ISLAND 11",
}

# ─── TITLE CLEANING ───────────────────────────────────────────────────────────

import re

FORMAT_TAGS = re.compile(
    r'\b(70mm|35mm|imax|4k|dcp|digital|in\s+70mm|in\s+35mm|presented\s+in\s+\w+)\b'
    r'|\s*[\(\[]?(70mm|35mm|imax|4k|dcp)[\)\]]?',
    re.IGNORECASE
)
SPECIAL_FORMAT_PATTERNS = {
    "IMAX": re.compile(r"\bimax\b", re.IGNORECASE),
    "70mm": re.compile(r"\b(?:in\s+)?70\s*mm\b", re.IGNORECASE),
    "35mm": re.compile(r"\b(?:in\s+)?35\s*mm\b", re.IGNORECASE),
}

NON_ALNUM = re.compile(r"[^a-z0-9]+")
SCRIPT_DIR = Path(__file__).resolve().parent
RATING_OVERRIDES_PATH = SCRIPT_DIR / "rating_overrides.json"
CINEMASCORE_OVERRIDES_PATH = SCRIPT_DIR / "cinemascore_overrides.json"
RATING_CACHE_PATH = SCRIPT_DIR / "rating_cache.json"
OUTPUT_DATA_PATH = (SCRIPT_DIR / "../public/data.json").resolve()
LEGACY_FAKE_PLOTS = {
    "A sweeping portrait of ambition, sacrifice, and the cost of greatness.",
    "Two cousins reunite in Poland and confront the weight of their family history.",
    "A Ghanaian immigrant navigates life in 1990s London with quiet determination.",
    "An epic meditation on the immigrant experience and the American Dream.",
    "Two brothers reckon with grief, distance, and what it means to belong.",
}


def normalize_title(title: str) -> str:
    return NON_ALNUM.sub(" ", (title or "").lower()).strip()


def slugify(value: str) -> str:
    return NON_ALNUM.sub("-", (value or "").lower()).strip("-")

def clean_title(raw: str) -> str:
    """Strip projection format tags from a showtime title before lookup."""
    cleaned = FORMAT_TAGS.sub('', raw).strip(' -–—·')
    article_match = re.match(r"^(.*),\s+(The|A|An)$", cleaned, re.IGNORECASE)
    if article_match:
        cleaned = f"{article_match.group(2)} {article_match.group(1)}"
    return re.sub(r"\s+", " ", cleaned).strip()


def extract_special_formats(*values: object) -> list[str]:
    found: list[str] = []
    haystacks = [str(value or "") for value in values if value]
    for label, pattern in SPECIAL_FORMAT_PATTERNS.items():
        if any(pattern.search(haystack) for haystack in haystacks):
            found.append(label)
    return found


def extract_year_int(value: Optional[str]) -> Optional[int]:
    match = re.search(r"\b(18|19|20)\d{2}\b", str(value or ""))
    return int(match.group(0)) if match else None


def cache_key_for_title_year(title: str, year: Optional[object] = None) -> str:
    base = normalize_title(title)
    parsed_year = extract_year_int(year)
    return f"{base}|{parsed_year}" if parsed_year else base


def make_movie_id(title: str, ratings: dict) -> str:
    imdb_id = str((ratings or {}).get("imdbID") or "").strip()
    if imdb_id:
        return imdb_id
    year = extract_year_int((ratings or {}).get("year"))
    base = slugify(title)
    return f"{base}-{year}" if year else base


def build_theater_meta(name: str, overrides: Optional[dict] = None) -> dict:
    base = dict(THEATER_CONFIG.get(name, {}))
    if overrides:
        base.update(overrides)
    official_url = str(base.get("official_url") or "https://www.amctheatres.com/").strip()
    short_name = str(base.get("short_name") or name).strip()
    sort_name = str(base.get("sort_name") or short_name).strip()
    return {
        "name": name,
        "slug": str(base.get("slug") or slugify(name)).strip(),
        "short_name": short_name,
        "sort_name": sort_name,
        "source_type": str(base.get("source_type") or "serpapi").strip(),
        "official_url": official_url,
        "aliases": [a for a in base.get("aliases", []) if a],
    }


def get_source_ticket_url(theater: dict, fallback_url: Optional[str] = None) -> str:
    return str(
        theater.get("ticket_url")
        or theater.get("official_url")
        or fallback_url
        or ""
    ).strip()


def format_day_label(dt: datetime) -> str:
    return dt.strftime("%a %b %d").replace(" 0", " ")


def format_time_label(dt: datetime) -> str:
    return dt.strftime("%I:%M%p").lstrip("0").lower()


def sort_time_labels(times: list[str]) -> list[str]:
    def parse_time(value: str) -> tuple[int, int]:
        m = re.match(r"(\d{1,2}):(\d{2})(am|pm)", (value or "").strip().lower())
        if not m:
            return (99, 99)
        hour = int(m.group(1))
        minute = int(m.group(2))
        meridiem = m.group(3)
        if meridiem == "pm" and hour != 12:
            hour += 12
        if meridiem == "am" and hour == 12:
            hour = 0
        return (hour, minute)

    return sorted(times, key=parse_time)

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
                ticket_urls = {}
                for showing in movie.get("showing", []):
                    show_url = next(
                        (
                            str(showing.get(key) or "").strip()
                            for key in ("link", "ticket_link", "ticketUrl", "url")
                            if str(showing.get(key) or "").strip()
                        ),
                        "",
                    )
                    for show_time in showing.get("time", []):
                        times.append(show_time)
                        if show_url and show_time not in ticket_urls:
                            ticket_urls[show_time] = show_url
                ticket_url = next(
                    (
                        str(showing.get(key) or "").strip()
                        for showing in movie.get("showing", [])
                        for key in ("link", "ticket_link", "ticketUrl", "url")
                        if str(showing.get(key) or "").strip()
                    ),
                    get_source_ticket_url(theater),
                )
                raw_year = movie.get("year") or movie.get("Year")
                hint_year: Optional[int] = None
                if raw_year:
                    try:
                        hint_year = int(str(raw_year)[:4])
                    except (ValueError, TypeError):
                        pass
                raw_title = movie.get("name", "Unknown")
                movies.append({
                    "title": clean_title(raw_title),
                    "hint_year": hint_year,
                    "theater": theater["name"],
                    "day": f"{day.get('day', '')} {day.get('date', '')}".strip(),
                    "times": times,
                    "ticket_url": ticket_url,
                    "ticket_urls": ticket_urls,
                    "special_formats": extract_special_formats(raw_title),
                })
        return movies
    except Exception as e:
        print(f"  [ERROR] SerpAPI failed for {theater['name']}: {e}")
        return mock_showtimes(theater["name"])


def fetch_metrograph_showtimes(theater: dict) -> list[dict]:
    source_url = str(theater.get("source_url") or "").strip()
    if not source_url:
        return []

    try:
        response = requests.get(source_url, timeout=20)
        response.raise_for_status()
        content = response.text
    except Exception as e:
        print(f"  [ERROR] Metrograph fetch failed for {theater['name']}: {e}")
        return []

    blocks = re.findall(r'<div class="film-showtimes">(.*?)</div>\s*</div>\s*</div>', content, re.DOTALL | re.IGNORECASE)
    if not blocks:
        blocks = re.findall(r'<div class="film-showtimes">(.*?)<!-- end film-showtimes -->', content, re.DOTALL | re.IGNORECASE)

    grouped: dict[str, dict[str, dict[str, str]]] = defaultdict(lambda: defaultdict(dict))
    grouped_formats: dict[str, set[str]] = defaultdict(set)

    for block in blocks:
        title_match = re.search(r'<h3 class="film-title">\s*(.*?)\s*</h3>', block, re.DOTALL | re.IGNORECASE)
        if not title_match:
            continue
        raw_title = html.unescape(re.sub(r"<.*?>", "", title_match.group(1))).strip()
        title = clean_title(raw_title)
        if not title:
            continue
        title_formats = extract_special_formats(raw_title, block)
        if title_formats:
            grouped_formats[title].update(title_formats)

        session_blocks = re.findall(
            r'<div class="[^"]*\bsession\b[^"]*">(.*?)</div>\s*</div>',
            block,
            re.DOTALL | re.IGNORECASE,
        )
        if not session_blocks:
            session_blocks = re.findall(
                r'<div class="[^"]*\bsession\b[^"]*">(.*?)(?=<div class="[^"]*\bsession\b|$)',
                block,
                re.DOTALL | re.IGNORECASE,
            )

        for session in session_blocks:
            links = re.findall(
                r'<a href="([^"]*visSelectTickets[^"]*)"[^>]*class="session-time[^"]*"[^>]*>.*?<time[^>]*datetime="([^"]+)">[^<]+</time>',
                session,
                re.DOTALL | re.IGNORECASE,
            )
            for href, iso_dt in links:
                try:
                    local_dt = datetime.fromisoformat(str(iso_dt).strip())
                except Exception:
                    continue

                time_label = format_time_label(local_dt)
                day_label = format_day_label(local_dt)
                ticket_url = html.unescape(str(href).strip())
                if ticket_url.startswith("//"):
                    ticket_url = f"https:{ticket_url}"
                elif ticket_url.startswith("/"):
                    ticket_url = f"https://t.metrograph.com{ticket_url}"
                grouped[title][day_label][time_label] = ticket_url or get_source_ticket_url(theater)

    flattened = []
    for title, days in grouped.items():
        for day_label, time_map in days.items():
            unique_times = sort_time_labels(list(time_map.keys()))
            ticket_urls = {time_label: time_map[time_label] for time_label in unique_times if time_map.get(time_label)}
            ticket_url = next(iter(ticket_urls.values()), get_source_ticket_url(theater))
            flattened.append({
                "title": title,
                "theater": theater["name"],
                "day": day_label,
                "times": unique_times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
                "special_formats": sorted(grouped_formats.get(title, [])),
            })

    return flattened


def fetch_source_html(source_url: str, theater_name: str) -> str:
    url = str(source_url or "").strip()
    if not url:
        return ""

    try:
        result = subprocess.run(
            ["curl", "-L", "--max-time", "20", url],
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stdout:
            return result.stdout
    except Exception:
        pass

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"  [ERROR] Source fetch failed for {theater_name}: {e}")
        return ""


def fetch_ifc_showtimes(theater: dict) -> list[dict]:
    source_url = str(theater.get("source_url") or "").strip()
    if not source_url:
        return []

    content = fetch_source_html(source_url, theater.get("name", "IFC Center"))
    if not content:
        return []

    grouped: dict[str, dict[str, dict[str, str]]] = defaultdict(lambda: defaultdict(dict))
    grouped_formats: dict[str, set[str]] = defaultdict(set)

    day_blocks = re.findall(
        r'(<div class="daily-schedule\s+[^"]*">.*?)(?=<div class="daily-schedule\s+[^"]*"|$)',
        content,
        re.DOTALL | re.IGNORECASE,
    )

    for full_block in day_blocks:
        day_match = re.search(r"<h3>([^<]+)</h3>", full_block, re.IGNORECASE)
        if not day_match:
            continue
        clean_day = html.unescape(re.sub(r"\s+", " ", day_match.group(1)).strip())
        movie_blocks = re.findall(
            r'<div class="details">\s*<h3><a href="[^"]+">([^<]+)</a></h3>\s*<ul class="times">(.*?)</ul>',
            full_block,
            re.DOTALL | re.IGNORECASE,
        )

        for raw_title, times_html in movie_blocks:
            clean_raw_title = html.unescape(raw_title).strip()
            title = clean_title(clean_raw_title)
            if not title:
                continue
            title_formats = extract_special_formats(clean_raw_title, times_html)
            if title_formats:
                grouped_formats[title].update(title_formats)

            links = re.findall(
                r'<a href="([^"]*ticketsearchcriteria[^"]*)"[^>]*>([^<]+)</a>',
                times_html,
                re.DOTALL | re.IGNORECASE,
            )
            for href, raw_time in links:
                time_label = html.unescape(raw_time).strip().upper()
                if not time_label:
                    continue
                ticket_url = html.unescape(href).replace("&#038;", "&").strip()
                grouped[title][clean_day][time_label] = ticket_url or get_source_ticket_url(theater)

    flattened = []
    for title, days in grouped.items():
        for day_label, time_map in days.items():
            unique_times = sort_time_labels(list(time_map.keys()))
            ticket_urls = {time_label: time_map[time_label] for time_label in unique_times if time_map.get(time_label)}
            ticket_url = next(iter(ticket_urls.values()), get_source_ticket_url(theater))
            flattened.append({
                "title": title,
                "theater": theater["name"],
                "day": day_label,
                "times": unique_times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
                "special_formats": sorted(grouped_formats.get(title, [])),
            })

    return flattened


def fetch_alamo_showtimes(theater: dict) -> list[dict]:
    market_slug = str(theater.get("market_slug") or "").strip()
    cinema_id = str(theater.get("cinema_id") or "").strip()
    if not market_slug or not cinema_id:
        return []

    algolia_headers = {
        "X-Algolia-Application-Id": "J21VYKWY3K",
        "X-Algolia-API-Key": "b475e661e58e2a407860db2f4f8f7cff",
        "Content-Type": "application/json",
    }

    presentation_hits = []
    page = 0
    while True:
        payload = {
            "params": f"query=&hitsPerPage=200&page={page}&filters=marketSlug:{market_slug}"
        }
        try:
            response = requests.post(
                "https://J21VYKWY3K-dsn.algolia.net/1/indexes/prod_on-sale-presentation/query",
                headers=algolia_headers,
                json=payload,
                timeout=20,
            )
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"  [ERROR] Alamo Algolia fetch failed for {theater['name']}: {e}")
            return []

        hits = data.get("hits") or []
        if not hits:
            break
        presentation_hits.extend(hits)
        page += 1
        if page >= int(data.get("nbPages") or 0):
            break

    unique_slugs = sorted({str(hit.get("slug") or "").strip() for hit in presentation_hits if str(hit.get("slug") or "").strip()})
    grouped: dict[str, dict[str, dict[str, str]]] = defaultdict(lambda: defaultdict(dict))
    grouped_formats: dict[str, set[str]] = defaultdict(set)

    for slug in unique_slugs:
        try:
            response = requests.get(
                f"https://drafthouse.com/s/mother/v2/schedule/presentation/{market_slug}/{slug}",
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json().get("data") or {}
        except Exception:
            continue

        presentation_lookup = {}
        for item in payload.get("presentations") or []:
            item_slug = str(item.get("slug") or "").strip()
            if item_slug:
                presentation_lookup[item_slug] = item
        primary = payload.get("presentation") or {}
        primary_slug = str(primary.get("slug") or "").strip()
        if primary_slug and primary_slug not in presentation_lookup:
            presentation_lookup[primary_slug] = primary

        for session in payload.get("sessions") or []:
            if str(session.get("cinemaId") or "").strip() != cinema_id:
                continue
            if str(session.get("status") or "").upper() in {"PAST", "CANCELLED", "CANCELED"}:
                continue

            session_dt_raw = str(session.get("showTimeClt") or "").strip()
            business_date = str(session.get("businessDateClt") or "").strip()
            session_id = str(session.get("sessionId") or "").strip()
            presentation_slug = str(session.get("presentationSlug") or "").strip() or slug
            if not session_dt_raw or not business_date or not session_id:
                continue

            try:
                session_dt = datetime.fromisoformat(session_dt_raw)
            except Exception:
                continue

            item = presentation_lookup.get(presentation_slug) or primary
            show_data = item.get("show") or {}
            event_data = item.get("event") or {}
            is_event = bool(event_data)
            raw_title = html.unescape(re.sub(r"<.*?>", "", event_data.get("title") or show_data.get("title") or "")).strip()
            title = clean_title(raw_title)
            if not title:
                continue
            title_formats = extract_special_formats(
                raw_title,
                session.get("experienceName"),
                session.get("presentationName"),
                session.get("format"),
                show_data.get("title"),
                event_data.get("title"),
            )

            route = "event" if is_event else "show"
            route_slug = presentation_slug
            day_label = format_day_label(datetime.fromisoformat(business_date))
            time_label = format_time_label(session_dt)
            ticket_url = (
                f"https://drafthouse.com/{market_slug}/{route}/{route_slug}"
                f"?cinemaId={cinema_id}&sessionId={session_id}"
            )
            bucket = grouped[title][day_label]
            bucket[time_label] = ticket_url

        if title_formats:
            grouped_formats[title].update(title_formats)

    flattened = []
    for title, days in grouped.items():
        for day_label, time_map in days.items():
            unique_times = sort_time_labels(list(time_map.keys()))
            ticket_urls = {time_label: time_map[time_label] for time_label in unique_times if time_map.get(time_label)}
            ticket_url = next(iter(ticket_urls.values()), get_source_ticket_url(theater))
            flattened.append({
                "title": title,
                "theater": theater["name"],
                "day": day_label,
                "times": unique_times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
                "special_formats": sorted(grouped_formats.get(title, [])),
            })

    return flattened


def amc_request(path: str, params: Optional[dict] = None) -> Optional[dict]:
    if not AMC_VENDOR_KEY:
        return None

    try:
        r = requests.get(
            f"{AMC_API_BASE}{path}",
            params=params or {},
            headers={
                "X-AMC-Vendor-Key": AMC_VENDOR_KEY,
                "Accept": "application/json",
            },
            timeout=20,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  [ERROR] AMC API request failed for {path}: {e}")
        return None


def is_supported_amc_theatre(theatre: dict) -> bool:
    location = theatre.get("location") or {}
    city = str(location.get("city") or "").strip().upper()
    state = str(location.get("state") or "").strip().upper()
    return city in AMC_ALLOWED_CITIES_BY_STATE.get(state, set())


def fetch_amc_theatres() -> list[dict]:
    if not AMC_VENDOR_KEY:
        return []

    theatres_by_id: dict[str, dict] = {}

    if AMC_THEATRE_IDS:
        data = amc_request("/v2/theatres", {"ids": ",".join(AMC_THEATRE_IDS), "page-size": 100})
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
                    "brand": "AMC",
                    "page-size": 500,
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
        results.append({
            "id": theatre_id,
            "name": name,
            "source": "amc",
            "official_url": str(
                theatre.get("websiteUrl")
                or theatre.get("websiteURL")
                or theatre.get("mobileUrl")
                or theatre.get("mobileURL")
                or "https://www.amctheatres.com/"
            ).strip(),
        })

    return sorted(results, key=lambda t: t["name"])


def fetch_amc_showtimes(theater: dict) -> list[dict]:
    theatre_id = theater.get("id")
    if not theatre_id:
        return []

    grouped: dict[str, dict[str, dict[str, object]]] = defaultdict(
        lambda: defaultdict(lambda: {"times": [], "ticket_urls": {}})
    )
    start = datetime.now()

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
                "times": unique_times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
                "special_formats": sorted(payload.get("special_formats") or []),
            })

    return flattened


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
CINEMASCORE_OVERRIDES = load_json_file(CINEMASCORE_OVERRIDES_PATH)
RATING_CACHE = load_json_file(RATING_CACHE_PATH)


def load_existing_movie_metadata(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  [WARN] Failed to load existing dashboard data: {e}")
        return {}

    movies = data.get("movies", [])
    existing = {}
    for movie in movies:
        title = str(movie.get("title") or "").strip()
        ratings = movie.get("ratings") or {}
        if title and isinstance(ratings, dict):
            existing[normalize_title(title)] = ratings
    return existing


EXISTING_MOVIE_METADATA = load_existing_movie_metadata(OUTPUT_DATA_PATH)


def get_existing_metadata(title: str) -> dict:
    return EXISTING_MOVIE_METADATA.get(normalize_title(title)) or {}


def get_best_cached_match(title: str, query_year: Optional[int] = None) -> dict:
    normalized = normalize_title(title)
    candidate_keys = []
    if query_year is not None:
        candidate_keys.extend([
            cache_key_for_title_year(title, query_year),
            cache_key_for_title_year(title, query_year - 1),
            cache_key_for_title_year(title, query_year + 1),
        ])
    candidate_keys.append(normalized)

    for key in candidate_keys:
        cached = RATING_CACHE.get(key)
        if isinstance(cached, dict) and cached.get("imdbID"):
            return cached
    return {}


def set_cached_match(title: str, data: dict, source: str) -> None:
    entry = {
        "imdbID": data.get("imdbID"),
        "title": data.get("Title"),
        "year": data.get("Year"),
        "source": source,
    }
    result_year = extract_year_int(data.get("Year"))
    if result_year is not None:
        RATING_CACHE[cache_key_for_title_year(title, result_year)] = entry
    else:
        RATING_CACHE[normalize_title(title)] = entry


def omdb_request(params: dict) -> Optional[dict]:
    try:
        r = requests.get("http://www.omdbapi.com/", params={"apikey": OMDB_KEY, **params}, timeout=10)
        data = r.json()
        if data.get("Response") == "False":
            return None
        return data
    except Exception:
        return None


def rt_slug(title: str) -> str:
    return normalize_title(title).replace(" ", "_")


def fetch_rt_fallback(title: str) -> Optional[str]:
    slug = rt_slug(title)
    candidates = [
        f"https://www.rottentomatoes.com/m/{slug}",
        f"https://www.rottentomatoes.com/m/{slug}_{datetime.now().year}",
        f"https://www.rottentomatoes.com/m/{slug}_{datetime.now().year + 1}",
        f"https://www.rottentomatoes.com/m/{slug}_{datetime.now().year - 1}",
    ]

    patterns = [
        r'tomatometerscore="(\d{1,3})"',
        r'"tomatometerScoreAll"\s*:\s*\{"score"\s*:\s*(\d{1,3})',
        r'"criticsScore"\s*:\s*(\d{1,3})',
        r'"criticsScore"\s*:\s*\{[^{}]{0,240}"score"\s*:\s*"(\d{1,3})"',
        r'"scorePercent"\s*:\s*"(\d{1,3})%"',
        r'"Tomatometer","ratingCount":\d+,"ratingValue":"(\d{1,3})"',
    ]

    for url in candidates:
        try:
            page = requests.get(
                url,
                timeout=12,
                headers={"User-Agent": "Mozilla/5.0"},
            ).text
        except Exception:
            continue
        for pat in patterns:
            m = re.search(pat, page)
            if m:
                pct = int(m.group(1))
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
        "imdbID": data.get("imdbID"),
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


def empty_ratings() -> dict:
    return {
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


def is_placeholder_metadata(ratings: Optional[dict]) -> bool:
    if not ratings:
        return False
    director = str(ratings.get("director") or "").strip()
    year = str(ratings.get("year") or "").strip()
    plot = str(ratings.get("plot") or "").strip()
    return (
        (director == "Various" and year == "2024")
        or plot in LEGACY_FAKE_PLOTS
    )


def merge_existing_metadata(title: str, ratings: dict) -> dict:
    existing = get_existing_metadata(title)
    if not existing:
        return ratings

    existing_year = extract_year_int(existing.get("year"))
    current_year = extract_year_int(ratings.get("year"))

    def completeness(meta: dict) -> int:
        return sum(1 for key in ("imdbID", "rt", "imdb", "metacritic", "letterboxd", "poster", "genre", "runtime", "plot", "year", "director", "cinemaScore") if meta.get(key) not in (None, "", "N/A"))

    preserve_existing_identity = (
        existing_year is not None
        and current_year is not None
        and abs(current_year - existing_year) >= 10
        and completeness(existing) >= completeness(ratings)
    )
    if preserve_existing_identity:
        for key in ("imdbID", "imdb", "metacritic", "letterboxd", "poster", "genre", "runtime", "plot", "year", "director", "cinemaScore", "rt"):
            prior = existing.get(key)
            if prior not in (None, "", "N/A"):
                ratings[key] = prior
        return ratings

    placeholder = is_placeholder_metadata(ratings)
    for key in ("imdbID", "imdb", "metacritic", "letterboxd", "poster", "genre", "runtime", "plot", "year", "director", "cinemaScore"):
        current = ratings.get(key)
        prior = existing.get(key)
        if prior in (None, "", "N/A"):
            continue
        if current in (None, "", "N/A") or (placeholder and key in {"genre", "runtime", "plot", "year", "director"}):
            ratings[key] = prior

    if not ratings.get("rt") and existing.get("rt") not in (None, "", "N/A"):
        ratings["rt"] = existing.get("rt")

    return ratings


def apply_rating_overrides(title: str, ratings: dict) -> dict:
    override = RATING_OVERRIDES.get(normalize_title(title), {})
    if isinstance(override, str):
        override = {"imdbID": override}
    if not isinstance(override, dict):
        return ratings

    for key, value in override.items():
        if key in {"imdbID", "year", "genre", "runtime", "plot", "director", "rt", "imdb", "metacritic", "letterboxd", "poster", "cinemaScore"}:
            ratings[key] = value

    cinema_score_override = CINEMASCORE_OVERRIDES.get(normalize_title(title))
    if cinema_score_override not in (None, "", "N/A"):
        ratings["cinemaScore"] = str(cinema_score_override).strip().upper()
    return ratings


def enrich_from_rating_cache(title: str, ratings: dict, hint_year: Optional[int] = None) -> dict:
    cached = get_best_cached_match(title, query_year=hint_year or extract_year_int(ratings.get("year")))
    cached_imdb = cached.get("imdbID")
    cached_year = cached.get("year")
    if cached_imdb and not ratings.get("imdbID"):
        ratings["imdbID"] = cached_imdb
    if cached_year not in (None, "", "N/A"):
        current_year = ratings.get("year")
        if current_year in (None, "", "N/A", "2024") or str(current_year).strip() == "2024":
            ratings["year"] = cached_year
    return ratings


def strip_placeholder_metadata(ratings: dict) -> dict:
    director = str(ratings.get("director") or "").strip()
    year = str(ratings.get("year") or "").strip()
    plot = str(ratings.get("plot") or "").strip()
    if director == "Various":
        ratings["director"] = None
    if plot in LEGACY_FAKE_PLOTS:
        ratings["plot"] = None
    if director == "Various" or plot in LEGACY_FAKE_PLOTS:
        if year == "2024" and not ratings.get("imdbID"):
            ratings["year"] = None
        if str(ratings.get("genre") or "").strip() in {"Drama", "Drama, History", "Comedy, Drama", "Documentary", "Thriller"}:
            ratings["genre"] = None
        runtime = str(ratings.get("runtime") or "").strip()
        if re.fullmatch(r"\d{2,3}\s+min", runtime):
            ratings["runtime"] = None
    return ratings


def repair_dataset_metadata(dataset: dict) -> dict:
    movies = dataset.get("movies", [])
    repaired = 0
    for movie in movies:
        title = str(movie.get("title") or "").strip()
        if not title:
            continue
        ratings = fetch_ratings(title)
        if not ratings:
            continue
        movie["ratings"] = ratings
        if not movie.get("id"):
            movie["id"] = make_movie_id(title, ratings)
        repaired += 1
    print(f"Repaired metadata for {repaired} movies without touching showtimes")
    return dataset


def is_acceptable_omdb_match(query_title: str, data: Optional[dict], query_year: Optional[int] = None, minimum_score: float = 0.85, existing_year: Optional[int] = None) -> bool:
    if not data:
        return False
    media_type = str(data.get("Type") or "").strip().lower()
    if media_type and media_type != "movie":
        return False

    result_year = extract_year_int(data.get("Year"))
    title_word_count = len(normalize_title(query_title).split())
    if (
        existing_year is not None
        and result_year is not None
        and existing_year <= 2005
        and result_year >= (_CURRENT_YEAR - 1)
        and title_word_count <= 2
    ):
        return False

    score = title_match_score(
        query_title,
        data.get("Title", ""),
        query_year=query_year,
        result_year=data.get("Year"),
    )
    return score >= minimum_score


_CURRENT_YEAR = datetime.now().year
REPERTORY_THEATERS = {
    "Metrograph",
    "IFC Center",
    "Film Forum",
    "Film at Lincoln Center",
    "Paris Theater",
    "Museum of Modern Art",
}

def normalize_current_release_year(movie: dict, dataset_year: int) -> None:
    ratings = movie.get("ratings") or {}
    current_year = extract_year_int(ratings.get("year"))
    if current_year != dataset_year - 1:
        return

    title = str(movie.get("title") or "").strip()
    if not title:
        return

    normalized = normalize_title(title)
    override = RATING_OVERRIDES.get(normalized, {})
    if isinstance(override, dict) and override.get("year") not in (None, "", "N/A"):
        return

    imdb_id = str(ratings.get("imdbID") or "").strip()
    if not imdb_id:
        return

    theaters = {str(t.get("name") or "").strip() for t in (movie.get("theaters") or []) if str(t.get("name") or "").strip()}
    if not theaters or any(theater in REPERTORY_THEATERS for theater in theaters):
        return

    ratings["year"] = str(dataset_year)


def resolve_omdb_record(title: str, hint_year: Optional[int] = None, theater_name: Optional[str] = None) -> Optional[dict]:
    """
    Look up a movie in OMDb, preferring year-specific matches to avoid
    confusing a new release with an older film that shares the same title.

    hint_year: year hint from SerpAPI or rating_overrides.json.
    """
    normalized = normalize_title(title)
    override = RATING_OVERRIDES.get(normalized, {})
    if isinstance(override, str):
        override = {"imdbID": override}
    existing = get_existing_metadata(title)
    existing_year = extract_year_int(existing.get("year"))

    # Build query_year: prefer explicit hint, then override file
    query_year: Optional[int] = hint_year
    if query_year is None:
        override_year = override.get("year")
        if isinstance(override_year, int):
            query_year = override_year
        elif isinstance(override_year, str) and override_year.isdigit():
            query_year = int(override_year)
    if query_year is None and existing_year is not None:
        query_year = existing_year

    repertory_mode = (
        query_year is None
        and str(theater_name or "").strip() in REPERTORY_THEATERS
    )

    override_imdb = override.get("imdbID")
    if override_imdb:
        data = fetch_omdb_by_imdb_id(override_imdb)
        if data:
            set_cached_match(title, data, "override")
            return data
        print(f"  [WARN] Override imdbID failed for '{title}': {override_imdb}")

    # Cache — skip if year expectation strongly mismatches cached result
    cached = get_best_cached_match(title, query_year=query_year)
    cached_imdb = cached.get("imdbID")
    if cached_imdb:
        cached_year_str = str(cached.get("year") or "")
        cached_year = int(cached_year_str[:4]) if cached_year_str[:4].isdigit() else None
        year_mismatch = (
            query_year is not None
            and cached_year is not None
            and abs(query_year - cached_year) > 2
        )
        if not year_mismatch:
            data = fetch_omdb_by_imdb_id(cached_imdb)
            if is_acceptable_omdb_match(title, data, query_year=query_year, minimum_score=0.70, existing_year=existing_year):
                return data

    # Try year-specific exact lookups before the open search.
    # This catches new releases that share a title with a classic — OMDb's
    # unqualified search returns the most popular (usually oldest) match.
    years_to_try: list[int] = []
    if query_year:
        years_to_try = [query_year, query_year - 1, query_year + 1]
    elif not repertory_mode:
        years_to_try = [_CURRENT_YEAR, _CURRENT_YEAR - 1]

    for y in years_to_try:
        data = omdb_request({"t": title, "y": y, "tomatoes": "true"})
        if not data:
            continue
        result_year_str = str(data.get("Year") or "")
        result_year = int(result_year_str[:4]) if result_year_str[:4].isdigit() else None
        if (result_year is None or abs(y - result_year) <= 2) and is_acceptable_omdb_match(title, data, query_year=query_year, minimum_score=0.70, existing_year=existing_year):
            set_cached_match(title, data, "exact_year")
            return data

    # Unqualified exact search — OMDb picks the most popular result.
    # Guard: if we expect a recent film and got something old, fall through.
    exact = omdb_request({"t": title, "tomatoes": "true"})
    if exact:
        result_year_str = str(exact.get("Year") or "")
        result_year = int(result_year_str[:4]) if result_year_str[:4].isdigit() else None
        too_old = (
            query_year is not None
            and result_year is not None
            and (query_year - result_year) > 5
        )
        if not too_old and is_acceptable_omdb_match(title, exact, query_year=query_year, minimum_score=0.90, existing_year=existing_year):
            set_cached_match(title, exact, "exact")
            return exact

    # Full-text search with year-biased ranking as last resort
    effective_year = query_year or existing_year
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
            query_year=effective_year,
            result_year=c.get("Year"),
        ),
    )
    best_score = title_match_score(
        title,
        best.get("Title", ""),
        query_year=effective_year,
        result_year=best.get("Year"),
    )
    if best_score < 0.55:
        return None

    best_data = fetch_omdb_by_imdb_id(best.get("imdbID"))
    if best_data and is_acceptable_omdb_match(title, best_data, query_year=query_year, minimum_score=0.55, existing_year=existing_year):
        set_cached_match(title, best_data, "search")
        return best_data
    return None

def fetch_ratings(title: str, hint_year: Optional[int] = None, theater_name: Optional[str] = None) -> dict:
    """Fetch RT, IMDb, and CinemaScore via OMDb; include a Letterboxd-style score."""
    if not OMDB_KEY:
        return mock_ratings(title)

    try:
        data = resolve_omdb_record(title, hint_year=hint_year, theater_name=theater_name)
        parsed = parse_omdb_ratings(data) if data else empty_ratings()

        # Fallbacks for new/edge releases where OMDb is lagging.
        if not parsed.get("rt"):
            parsed["rt"] = fetch_rt_fallback(title)
        if not parsed.get("letterboxd"):
            parsed["letterboxd"] = fetch_letterboxd_fallback(title)

        parsed = merge_existing_metadata(title, parsed)
        parsed = enrich_from_rating_cache(title, parsed, hint_year=hint_year)
        parsed = apply_rating_overrides(title, parsed)
        parsed = strip_placeholder_metadata(parsed)
        return parsed
    except Exception as e:
        print(f"  [ERROR] OMDb failed for '{title}': {e}")
        parsed = empty_ratings()
        parsed["rt"] = fetch_rt_fallback(title)
        parsed["letterboxd"] = fetch_letterboxd_fallback(title)
        parsed = merge_existing_metadata(title, parsed)
        parsed = enrich_from_rating_cache(title, parsed, hint_year=hint_year)
        parsed = apply_rating_overrides(title, parsed)
        parsed = strip_placeholder_metadata(parsed)
        return parsed


def mock_ratings(title: str) -> dict:
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
    digest = hashlib.sha256(title.encode("utf-8")).hexdigest()
    idx = int(digest[:8], 16) % len(rt_scores)
    runtime_minutes = 90 + (int(digest[8:12], 16) % 61)
    lower_title = title.lower()
    inferred_genre = None
    horror_markers = [
        "ready or not",
        "scream",
        "horror",
        "kill",
        "killer",
        "yeti",
        "monster",
        "haunt",
        "ghost",
        "blood",
    ]
    documentary_markers = ["doc", "documentary", "agnes", "beyond belief"]

    if any(marker in lower_title for marker in horror_markers):
        inferred_genre = "Horror, Thriller"
    elif any(marker in lower_title for marker in documentary_markers):
        inferred_genre = "Documentary"

    return {
        "imdbID": None,
        "rt": rt_scores[idx],
        "imdb": imdb_scores[idx],
        "metacritic": str(int(rt_scores[idx].replace("%", "")) - 5),
        "letterboxd": f"{(float(imdb_scores[idx]) / 2):.1f}",
        "poster": None,
        "genre": inferred_genre or genres[idx % len(genres)],
        "runtime": f"{runtime_minutes} min",
        "plot": plots[idx % len(plots)],
        "year": "2024",
        "director": "Various",
        "cinemaScore": None,
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
    all_movies: dict[str, dict] = {}
    theater_schedule: dict[str, dict] = defaultdict(lambda: defaultdict(list))
    theater_meta: dict[str, dict] = {
        name: build_theater_meta(name)
        for name in THEATER_CONFIG.keys()
    }
    amc_theaters = fetch_amc_theatres()
    all_theaters = [*STATIC_THEATERS, *amc_theaters]

    for theater in all_theaters:
        print(f"\nFetching: {theater['name']}")
        if theater.get("source") == "amc":
            showtimes = fetch_amc_showtimes(theater)
        elif theater.get("source_type") == "metrograph":
            showtimes = fetch_metrograph_showtimes(theater)
        elif theater.get("source_type") == "ifc":
            showtimes = fetch_ifc_showtimes(theater)
        elif theater.get("source_type") == "alamo":
            showtimes = fetch_alamo_showtimes(theater)
        else:
            showtimes = fetch_showtimes(theater)

        for entry in showtimes:
            title = entry["title"]
            theater_name = entry["theater"]
            day = entry["day"]
            times = entry["times"]
            ticket_url = str(entry.get("ticket_url") or get_source_ticket_url(theater)).strip()
            ticket_urls = {
                str(time): str(url).strip()
                for time, url in (entry.get("ticket_urls") or {}).items()
                if str(time).strip() and str(url).strip()
            }
            special_formats = [
                fmt for fmt in (entry.get("special_formats") or [])
                if str(fmt).strip()
            ]

            if theater_name not in theater_meta:
                theater_meta[theater_name] = build_theater_meta(
                    theater_name,
                    {
                        "source_type": theater.get("source", "amc"),
                        "official_url": theater.get("official_url") or "https://www.amctheatres.com/",
                    },
                )

            theater_schedule[theater_name][title].append({
                "day": day,
                "times": times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
            })

            hint_year = entry.get("hint_year")
            provisional_key = normalize_title(title)
            if provisional_key not in all_movies:
                print(f"  Fetching ratings for: {title}" + (f" (year hint: {hint_year})" if hint_year else ""))
                ratings = fetch_ratings(title, hint_year=hint_year, theater_name=theater_name)
                movie_id = make_movie_id(title, ratings)
                print(f"  Fetching verdict for: {title}")
                verdict = fetch_verdict(title, ratings)
                all_movies[provisional_key] = {
                    "id": movie_id,
                    "title": title,
                    "ratings": ratings,
                    "verdict": verdict,
                    "theaters": [],
                    "special_formats": [],
                }
            if special_formats:
                existing_formats = set(all_movies[provisional_key].get("special_formats") or [])
                all_movies[provisional_key]["special_formats"] = sorted(existing_formats.union(special_formats))

    # Attach theater + showtime info to each movie
    for theater_name, movies in theater_schedule.items():
        for title, schedule in movies.items():
            key = normalize_title(title)
            if key in all_movies:
                ticket_url = next((slot.get("ticket_url") for slot in schedule if slot.get("ticket_url")), "") or theater_meta.get(theater_name, {}).get("official_url", "")
                clean_schedule = []
                for slot in schedule:
                    clean_slot = {"day": slot["day"], "times": slot["times"]}
                    if slot.get("ticket_urls"):
                        clean_slot["ticket_urls"] = slot["ticket_urls"]
                    clean_schedule.append(clean_slot)
                all_movies[key]["theaters"].append({
                    "name": theater_name,
                    "ticket_url": ticket_url,
                    "schedule": clean_schedule,
                })

    movies_list = sorted(
        all_movies.values(),
        key=lambda m: (
            {"WATCH": 0, "SKIP": 1}.get(m["verdict"]["verdict"], 2),
            -(int((m["ratings"].get("rt") or "0%").replace("%", "")) if m["ratings"].get("rt") else 0)
        )
    )

    dataset_year = datetime.now().year
    for movie in movies_list:
        normalize_current_release_year(movie, dataset_year)

    return {
        "generated_at": datetime.now().isoformat(),
        "week_of": (datetime.now() + timedelta(days=(4 - datetime.now().weekday()) % 7)).strftime("%B %d, %Y"),
        "theaters": sorted(theater_schedule.keys()),
        "theater_meta": theater_meta,
        "movies": movies_list,
    }


if __name__ == "__main__":
    dataset = build_dataset()
    save_json_file(RATING_CACHE_PATH, RATING_CACHE)

    output_path = os.path.join(os.path.dirname(__file__), "../public/data.json")
    with open(output_path, "w") as f:
        json.dump(dataset, f, indent=2)

    print(f"\nDone. {len(dataset['movies'])} unique films written to public/data.json")
