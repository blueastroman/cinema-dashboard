"""
NYC Cinema Dashboard - Weekly Scraper
Runs every Wednesday via GitHub Actions
Pulls showtimes via SerpAPI/AMC API and ratings via OMDb.
"""

import json
import hashlib
import os
import requests
import html
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote
from typing import Any, Optional
import re

from cinema_backend.common import (
    AMC_ALLOWED_CITIES_BY_STATE,
    AMC_EXCLUDED_THEATRES,
    SERPAPI_THEATERS,
    STATIC_THEATERS,
    THEATER_CONFIG,
    build_theater_meta,
    clean_title,
    date_iso,
    extract_special_formats,
    extract_year_int,
    format_day_label,
    format_time_label,
    get_source_ticket_url,
    exact_title_identity_key,
    make_movie_id,
    normalize_title,
    runtime_minutes_from_value,
    slugify,
    sort_time_labels,
    split_trailing_title_year,
    ny_now,
    title_identity_key,
    title_explicitly_allows_short,
)
from cinema_backend.http import DEFAULT_HEADERS, fetch_source_html
from cinema_backend.prestige import build_movie_prestige_tags
from cinema_backend.providers.alamo import (
    ALAMO_ALGOLIA_HEADERS,
    ALAMO_ALGOLIA_QUERY_URL,
    alamo_presentation_url,
)
from cinema_backend.runtime import (
    ScrapeContext,
    build_scrape_context,
    save_json_dict,
)

SCRIPT_DIR = Path(__file__).resolve().parent
RATING_OVERRIDES_PATH = SCRIPT_DIR / "rating_overrides.json"
CINEMASCORE_OVERRIDES_PATH = SCRIPT_DIR / "cinemascore_overrides.json"
PRESTIGE_OVERRIDES_PATH = SCRIPT_DIR / "prestige_overrides.json"
RATING_CACHE_PATH = SCRIPT_DIR / "rating_cache.json"
OUTPUT_DATA_PATH = (SCRIPT_DIR / "../public/data.json").resolve()
AMC_THEATRE_PAGE_SIZE = 100
PARIS_SITE_ID = "2001"
PARIS_AUTH_URL = "https://auth.moviexchange.com/connect/token"
PARIS_API_BASE = "https://digital-api.paristheaternyc.com/ocapi/v1"
PARIS_TICKET_BASE = "https://tickets.paristheaternyc.com/order/showtimes"
PARIS_AUTH_DATA = {
    "grant_type": "password",
    "username": "webhost-browsing-parisnyc",
    "password": "HzaJe65EAPNto7sR5",
    "client_id": "webhost-browsing-parisnyc",
}
LEGACY_FAKE_PLOTS = {
    "A sweeping portrait of ambition, sacrifice, and the cost of greatness.",
    "Two cousins reunite in Poland and confront the weight of their family history.",
    "A Ghanaian immigrant navigates life in 1990s London with quiet determination.",
    "An epic meditation on the immigrant experience and the American Dream.",
    "Two brothers reckon with grief, distance, and what it means to belong.",
}
MONTH_INDEX = {
    month.lower(): index
    for index, month in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)
}


@dataclass
class ScrapeIssue:
    stage: str
    source: str
    theater: str
    message: str


@dataclass
class CollectedEntry:
    theater: dict[str, Any]
    entry: dict[str, Any]


def infer_date_iso_from_label(day_label: object, now: Optional[datetime] = None) -> str:
    text = str(day_label or "").strip()
    if not text:
        return ""
    current = (now or ny_now()).replace(tzinfo=None)
    lowered = text.lower()
    if lowered == "today":
        return date_iso(current)
    if lowered == "tomorrow":
        return date_iso(current + timedelta(days=1))

    iso_match = re.search(r"\b((?:18|19|20)\d{2})-(\d{2})-(\d{2})\b", text)
    if iso_match:
        return iso_match.group(0)

    md_match = re.search(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})(?:,?\s+((?:18|19|20)\d{2}))?\b", text, re.IGNORECASE)
    if md_match:
        month = MONTH_INDEX.get(md_match.group(1)[:3].lower())
        day = int(md_match.group(2))
        year = int(md_match.group(3)) if md_match.group(3) else current.year
        candidate = current.replace(year=year, month=month, day=day, hour=12, minute=0, second=0, microsecond=0)
        if not md_match.group(3) and candidate.date() < current.date() and month < current.month:
            candidate = candidate.replace(year=current.year + 1)
        return date_iso(candidate)

    weekday_match = re.match(r"^(Sun|Mon|Tue|Wed|Thu|Fri|Sat)\b", text, re.IGNORECASE)
    if weekday_match:
        weekday_index = {"sun": 6, "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5}[weekday_match.group(1)[:3].lower()]
        days_ahead = (weekday_index - current.weekday()) % 7
        return date_iso(current + timedelta(days=days_ahead))

    return ""

# ─── SHOWTIMES ────────────────────────────────────────────────────────────────

def fetch_showtimes(theater: dict, ctx: ScrapeContext) -> list[dict]:
    """Pull showtimes from Google via SerpAPI for a given theater."""
    if not ctx.config.serpapi_key:
        if ctx.config.allow_mock_data:
            print(f"  [MOCK] No SerpAPI key - using mock data for {theater['name']}")
            return mock_showtimes(theater["name"])
        raise RuntimeError(f"SERPAPI_KEY is required for {theater['name']}")

    params = {
        "engine": "google",
        "q": f"showtimes {theater['serpapi_id']}",
        "api_key": ctx.config.serpapi_key,
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
        if ctx.config.allow_mock_data:
            return mock_showtimes(theater["name"])
        raise


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
    grouped_dates: dict[str, dict[str, str]] = defaultdict(dict)

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
                grouped_dates[title][day_label] = date_iso(local_dt)
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
                "date": grouped_dates.get(title, {}).get(day_label),
            })

    return flattened

def fetch_ifc_showtimes(theater: dict) -> list[dict]:
    # IFC removed their /now-playing/ listing page; film URLs now come from the homepage
    # and schedule data lives on each individual film page.
    homepage_url = str(theater.get("source_url") or "").strip()
    if not homepage_url:
        return []

    homepage = fetch_source_html(homepage_url, theater.get("name", "IFC Center"))
    if not homepage:
        return []

    film_urls = list(dict.fromkeys(
        re.findall(r'https://www\.ifccenter\.com/films/[^"\'>\s]+', homepage)
    ))

    grouped: dict[str, dict[str, dict[str, str]]] = defaultdict(lambda: defaultdict(dict))
    grouped_formats: dict[str, set[str]] = defaultdict(set)

    for film_url in film_urls:
        page = fetch_source_html(film_url, theater.get("name", "IFC Center"))
        if not page:
            continue

        h1_match = re.search(r"<h1[^>]*>(.*?)</h1>", page, re.DOTALL | re.IGNORECASE)
        if not h1_match:
            continue
        raw_title = html.unescape(re.sub(r"<[^>]+>", "", h1_match.group(1))).strip()
        title = clean_title(raw_title)
        if not title:
            continue

        title_formats = extract_special_formats(raw_title, page[:2000])
        if title_formats:
            grouped_formats[title].update(title_formats)

        sched_start = page.find('SHOWTIMES AT IFC CENTER')
        if sched_start == -1:
            continue
        # Grab a large enough slice; the schedule section is self-contained
        sched_section = page[sched_start:sched_start + 20000]

        day_blocks = re.findall(
            r"<p[^>]*>\s*<strong>([^<]+)</strong>\s*</p>.*?<ul[^>]*class=\"times\"[^>]*>(.*?)</ul>",
            sched_section,
            re.DOTALL | re.IGNORECASE,
        )
        for raw_day, times_html in day_blocks:
            clean_day = html.unescape(raw_day).strip()
            time_spans = re.findall(r"<span>([^<]+)</span>", times_html)
            ticket_hrefs = re.findall(
                r'href="([^"]*ticketsearchcriteria[^"]*)"',
                times_html,
                re.IGNORECASE,
            )
            for i, raw_time in enumerate(time_spans):
                time_label = html.unescape(raw_time).strip()
                if not time_label:
                    continue
                ticket_url = (
                    html.unescape(ticket_hrefs[i]).replace("&#038;", "&").strip()
                    if i < len(ticket_hrefs)
                    else get_source_ticket_url(theater)
                )
                grouped[title][clean_day][time_label] = ticket_url

    flattened = []
    for title, days in grouped.items():
        for day_label, time_map in days.items():
            unique_times = sort_time_labels(list(time_map.keys()))
            ticket_urls = {t: time_map[t] for t in unique_times if time_map.get(t)}
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


FILM_FORUM_WEEKDAY_INDEX = {
    "mon": 0,
    "tue": 1,
    "wed": 2,
    "thu": 3,
    "fri": 4,
    "sat": 5,
    "sun": 6,
}


def normalize_film_forum_title(raw_title: str) -> str:
    cleaned = html.unescape(str(raw_title or "")).replace("\xa0", " ")
    cleaned = cleaned.replace("<br />", " ").replace("<br/>", " ").replace("<br>", " ")
    cleaned = re.sub(r"<.*?>", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""

    possessive_match = re.match(
        r"^(?:[A-Z][\w.\-’']+\s+){1,4}[A-Z][\w.\-’']+[’']s\s+(.+)$",
        cleaned,
    )
    if possessive_match:
        cleaned = possessive_match.group(1).strip()

    starring_match = re.match(
        r"^(?:[A-Z][\w.\-’']+\s+){1,4}[A-Z][\w.\-’']+\s+in\s+(.+)$",
        cleaned,
    )
    if starring_match:
        cleaned = starring_match.group(1).strip()

    return clean_title(cleaned)


def infer_film_forum_tab_date(day_code: str, day_number: int, today: datetime) -> Optional[datetime]:
    weekday_index = FILM_FORUM_WEEKDAY_INDEX.get(str(day_code or "").strip().lower())
    if weekday_index is None:
        return None

    candidates: list[datetime] = []
    for offset in range(-7, 21):
        candidate = today + timedelta(days=offset)
        if candidate.weekday() == weekday_index and candidate.day == day_number:
            candidates.append(candidate)

    if not candidates:
        return None

    candidates.sort(
        key=lambda candidate: (
            0 if candidate.date() >= today.date() else 1,
            abs((candidate.date() - today.date()).days),
        )
    )
    return candidates[0]


def infer_film_forum_showtimes(raw_times: list[str], target_date: datetime) -> list[datetime]:
    parsed: list[tuple[int, int]] = []
    for raw_time in raw_times:
        compact_time = html.unescape(raw_time).strip()
        time_match = re.fullmatch(r"(\d{1,2}):(\d{2})", compact_time)
        if not time_match:
            continue
        parsed.append((int(time_match.group(1)), int(time_match.group(2))))

    results: list[datetime] = []
    previous_hour = -1
    for index, (hour, minute) in enumerate(parsed):
        if hour == 12:
            candidates = [12]
        elif hour <= 9:
            candidates = [hour + 12]
        elif hour in {10, 11}:
            candidates = [hour, hour + 12]
        else:
            candidates = [hour]

        if len(candidates) == 1:
            hour_24 = candidates[0]
        elif previous_hour >= 12:
            hour_24 = candidates[-1]
        else:
            later_hours = [candidate_hour for candidate_hour, _ in parsed[index + 1:]]
            has_later_afternoon_marker = any(candidate_hour == 12 or candidate_hour < hour for candidate_hour in later_hours)
            hour_24 = candidates[0] if has_later_afternoon_marker else candidates[-1]

        previous_hour = hour_24
        results.append(target_date.replace(hour=hour_24, minute=minute, second=0, microsecond=0))

    return results


def fetch_film_forum_detail_meta(detail_url: str) -> dict:
    detail_content = fetch_source_html(detail_url, "Film Forum detail")
    if not detail_content:
        return {}

    meta_match = re.search(
        r'<div class="copy">\s*<p><strong>(.*?)</strong>',
        detail_content,
        re.DOTALL | re.IGNORECASE,
    )
    if not meta_match:
        return {}

    meta_text = html.unescape(meta_match.group(1)).replace("\xa0", " ")
    meta_text = meta_text.replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
    meta_text = re.sub(r"<.*?>", "", meta_text)
    meta_text = re.sub(r"[ \t]+", " ", meta_text).strip()

    hint_year = extract_year_int(meta_text)
    special_formats = extract_special_formats(meta_text)
    return {
        "hint_year": hint_year,
        "special_formats": special_formats,
    }


def fetch_film_forum_showtimes(theater: dict) -> list[dict]:
    source_url = str(theater.get("source_url") or theater.get("official_url") or "").strip()
    if not source_url:
        return []

    content = fetch_source_html(source_url, theater.get("name", "Film Forum"))
    if not content:
        return []

    tab_pairs = re.findall(
        r'<li class=([a-z]{3})><a href="#(tabs-\d+)">([A-Z]{3})</a></li>',
        content,
        re.IGNORECASE,
    )
    tab_sections = re.findall(
        r'<div id="(tabs-\d+)">(.*?)(?=<div id="tabs-\d+">|</div>\s*</div>)',
        content,
        re.DOTALL | re.IGNORECASE,
    )
    if not tab_pairs or not tab_sections:
        return []

    today = ny_now().replace(tzinfo=None)
    section_lookup = {tab_id: body for tab_id, body in tab_sections}
    detail_meta_cache: dict[str, dict] = {}
    grouped: dict[str, dict[str, dict[str, str]]] = defaultdict(lambda: defaultdict(dict))
    grouped_formats: dict[str, set[str]] = defaultdict(set)
    grouped_dates: dict[str, dict[str, str]] = defaultdict(dict)
    hint_years: dict[str, Optional[int]] = {}

    for day_code, tab_id, _label in tab_pairs:
        body = section_lookup.get(tab_id, "")
        if not body:
            continue

        day_number_match = re.search(r"<!--\s*(\d{1,2})\s*-->", body)
        if not day_number_match:
            continue
        target_date = infer_film_forum_tab_date(day_code, int(day_number_match.group(1)), today)
        if not target_date:
            continue
        day_label = format_day_label(target_date)

        for paragraph in re.findall(r"<p>(.*?)</p>", body, re.DOTALL | re.IGNORECASE):
            title_match = re.search(
                r'<strong><a href="([^"]+)">(.*?)</a></strong>',
                paragraph,
                re.DOTALL | re.IGNORECASE,
            )
            if not title_match:
                continue

            detail_href = html.unescape(title_match.group(1)).strip()
            if detail_href.startswith("//"):
                detail_url = f"https:{detail_href}"
            elif detail_href.startswith("/"):
                detail_url = f"https://filmforum.org{detail_href}"
            else:
                detail_url = detail_href

            title = normalize_film_forum_title(title_match.group(2))
            if not title:
                continue

            detail_meta = detail_meta_cache.get(detail_url)
            if detail_meta is None:
                detail_meta = fetch_film_forum_detail_meta(detail_url)
                detail_meta_cache[detail_url] = detail_meta

            hint_year = detail_meta.get("hint_year")
            if hint_year is not None and title not in hint_years:
                hint_years[title] = hint_year

            combined_formats = set(extract_special_formats(title_match.group(2), paragraph))
            combined_formats.update(detail_meta.get("special_formats") or [])
            if combined_formats:
                grouped_formats[title].update(combined_formats)

            raw_times = re.findall(r"<span>([^<]+)</span>", paragraph, re.DOTALL | re.IGNORECASE)
            for local_dt in infer_film_forum_showtimes(raw_times, target_date):
                time_label = format_time_label(local_dt)
                grouped_dates[title][day_label] = date_iso(local_dt)
                grouped[title][day_label][time_label] = detail_url or get_source_ticket_url(theater)

    flattened = []
    for title, days in grouped.items():
        for day_label, time_map in days.items():
            unique_times = sort_time_labels(list(time_map.keys()))
            ticket_urls = {time_label: time_map[time_label] for time_label in unique_times if time_map.get(time_label)}
            ticket_url = next(iter(ticket_urls.values()), get_source_ticket_url(theater))
            flattened.append({
                "title": title,
                "hint_year": hint_years.get(title),
                "theater": theater["name"],
                "day": day_label,
                "date": grouped_dates.get(title, {}).get(day_label),
                "times": unique_times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
                "special_formats": sorted(grouped_formats.get(title, [])),
            })

    return flattened




def fetch_moma_showtimes(theater: dict) -> list[dict]:
    # MoMA blocks all automated requests (HTTP 403). Instead, read a locally saved
    # HTML export of https://www.moma.org/calendar/?happening_filter=Films&location=both
    # Refresh by re-exporting the page from a browser and saving to scripts/moma_export.html.
    source_file = str(theater.get("source_file") or "moma_export.html").strip()
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(scripts_dir, source_file)

    if not os.path.exists(file_path):
        print(f"  [WARN] MoMA export not found at {file_path}; skipping")
        return []

    with open(file_path, encoding="utf-8") as f:
        content = f.read()

    now = ny_now().replace(tzinfo=None)
    grouped: dict[str, dict[str, str]] = defaultdict(dict)
    grouped_dates: dict[str, str] = {}

    day_blocks = re.findall(
        r'<h2[^>]*>\s*([^<]+(?:&nbsp;[^<]+)?)\s*</h2>(.*?)(?=<h2[^>]*>|<div\s+data-pagination-mount=|</section>)',
        content,
        re.DOTALL | re.IGNORECASE,
    )

    for raw_day, day_html in day_blocks:
        clean_day = html.unescape(raw_day).replace("\xa0", " ").replace(",", "").strip()
        clean_day = re.sub(r"\s+", " ", clean_day).strip()
        try:
            parsed_day = datetime.strptime(f"{clean_day} {now.year}", "%a %b %d %Y")
        except ValueError:
            continue
        if parsed_day.date() < now.date():
            parsed_day = parsed_day.replace(year=now.year + 1)

        day_label = format_day_label(parsed_day)

        for m in re.finditer(
            r'<a\s+class="\s*link/disable.*?"href="/calendar/events/\d+".*?</a>',
            day_html,
            re.DOTALL | re.IGNORECASE,
        ):
            item_html = m.group(0)
            href_match = re.search(r'href="(/calendar/events/\d+)"', item_html)
            title_match = re.search(
                r"<span class='layout/block balance-text'>(.*?)</span>",
                item_html,
                re.DOTALL | re.IGNORECASE,
            )
            time_match = re.search(
                r"<span class='layout/block '>(\d{1,2}:\d{2}&nbsp;[ap]\.m\.)</span>",
                item_html,
                re.DOTALL | re.IGNORECASE,
            )
            venue_spans = re.findall(
                r"<span class='layout/block '>([^<]+)</span>",
                item_html,
                re.IGNORECASE,
            )
            if not href_match or not title_match or not time_match:
                continue

            venue = next(
                (
                    html.unescape(v).replace("\xa0", " ").strip()
                    for v in venue_spans
                    if "moma" in v.lower() or "floor" in v.lower() or "walter reade" in v.lower()
                ),
                "",
            )
            if not venue:
                continue

            title_raw = html.unescape(re.sub(r"<.*?>", "", title_match.group(1))).replace("\xa0", " ").strip()
            title_raw = re.sub(r"\s+", " ", title_raw)
            # Strip ". YEAR[–YY]. Directed by ..." or ". YEAR-YY. ..." suffixes
            title_raw = re.sub(r"\.\s*(18|19|20)\d{2}[–\-]\d{2,4}\..*$", "", title_raw).strip(". ")
            title_raw = re.sub(r"\.\s*(18|19|20)\d{2}\..*$", "", title_raw).strip(". ")
            title = clean_title(title_raw)
            if not title:
                continue

            raw_time = html.unescape(time_match.group(1)).replace("\xa0", " ").strip().lower()
            normalized = (
                raw_time.replace("a.m.", "am").replace("p.m.", "pm").replace(" ", "")
            )
            tc = re.match(r"(\d{1,2}):(\d{2})(am|pm)", normalized)
            if not tc:
                continue
            hour, minute, meridiem = int(tc.group(1)), int(tc.group(2)), tc.group(3)
            if meridiem == "pm" and hour != 12:
                hour += 12
            if meridiem == "am" and hour == 12:
                hour = 0
            time_label = format_time_label(parsed_day.replace(hour=hour, minute=minute))
            ticket_url = f"https://www.moma.org{href_match.group(1)}"
            grouped[f"{title}|{day_label}"][time_label] = ticket_url
            grouped_dates[f"{title}|{day_label}"] = date_iso(parsed_day)

    flattened = []
    for key, time_map in grouped.items():
        title, day_label = key.rsplit("|", 1)
        unique_times = sort_time_labels(list(time_map.keys()))
        ticket_urls = {t: time_map[t] for t in unique_times if time_map.get(t)}
        ticket_url = next(iter(ticket_urls.values()), get_source_ticket_url(theater))
        flattened.append({
            "title": title,
            "theater": theater["name"],
            "day": day_label,
            "date": grouped_dates.get(key),
            "times": unique_times,
            "ticket_url": ticket_url,
            "ticket_urls": ticket_urls,
            "special_formats": [],
        })

    return flattened


def html_to_plain_text(value: object) -> str:
    text = html.unescape(re.sub(r"<[^>]+>", " ", str(value or "")))
    return re.sub(r"\s+", " ", text).strip()


def extract_name_list(values: object) -> str:
    names = []
    if not isinstance(values, list):
        return ""
    for value in values:
        if isinstance(value, dict):
            name = value.get("name") or value.get("fullName") or value.get("displayName")
        else:
            name = value
        name = str(name or "").strip()
        if name:
            names.append(name)
    return ", ".join(names)


def extract_alamo_metadata(show_data: dict, event_data: dict) -> dict:
    sources = [source for source in (show_data, event_data) if isinstance(source, dict)]
    if not sources:
        return {}

    def first_value(*keys: str) -> object:
        for source in sources:
            for key in keys:
                value = source.get(key)
                if value not in (None, "", [], {}):
                    return value
        return None

    metadata = {}
    imdb_id = str(first_value("imdbId", "imdbID") or "").strip()
    if imdb_id:
        metadata["imdbID"] = imdb_id

    runtime = first_value("runtimeMinutes", "runtime")
    try:
        runtime_minutes = int(runtime)
    except (TypeError, ValueError):
        runtime_minutes = None
    if runtime_minutes:
        metadata["runtime"] = f"{runtime_minutes} min"

    release_date = str(first_value("nationalReleaseDateUtc", "releaseDate", "openingDateClt") or "").strip()
    release_year = extract_year_int(release_date)
    if release_year:
        metadata["year"] = str(release_year)

    description = html_to_plain_text(first_value("description", "shortDescription", "headline"))
    if description:
        metadata["plot"] = description

    director = extract_name_list(first_value("directors"))
    if director:
        metadata["director"] = director

    genre = extract_name_list(first_value("genres"))
    if genre:
        metadata["genre"] = genre

    return metadata


def fetch_alamo_showtimes(theater: dict) -> list[dict]:
    market_slug = str(theater.get("market_slug") or "").strip()
    cinema_id = str(theater.get("cinema_id") or "").strip()
    if not market_slug or not cinema_id:
        return []

    presentation_hits = []
    page = 0
    while True:
        payload = {
            "params": f"query=&hitsPerPage=200&page={page}&filters=marketSlug:{market_slug}"
        }
        try:
            response = requests.post(
                ALAMO_ALGOLIA_QUERY_URL,
                headers=ALAMO_ALGOLIA_HEADERS,
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
    grouped_metadata: dict[str, dict[str, str]] = {}
    grouped_dates: dict[str, dict[str, str]] = defaultdict(dict)

    for slug in unique_slugs:
        try:
            response = requests.get(
                alamo_presentation_url(market_slug, slug),
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
            if title not in grouped_metadata:
                grouped_metadata[title] = extract_alamo_metadata(show_data, event_data)

            route = "event" if is_event else "show"
            route_slug = presentation_slug
            day_label = format_day_label(datetime.fromisoformat(business_date))
            time_label = format_time_label(session_dt)
            grouped_dates[title][day_label] = date_iso(datetime.fromisoformat(business_date))
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
                "date": grouped_dates.get(title, {}).get(day_label),
                "times": unique_times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
                "special_formats": sorted(grouped_formats.get(title, [])),
                "source_metadata": grouped_metadata.get(title, {}),
            })

    return flattened


def amc_request(ctx: ScrapeContext, path: str, params: Optional[dict] = None) -> Optional[dict]:
    if not ctx.config.amc_vendor_key:
        return None

    request_params = params or {}
    try:
        r = requests.get(
            f"{ctx.config.amc_api_base}{path}",
            params=request_params,
            headers={
                "X-AMC-Vendor-Key": ctx.config.amc_vendor_key,
                "Accept": "application/json",
            },
            timeout=20,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        url = getattr(getattr(e, "response", None), "url", None) or f"{ctx.config.amc_api_base}{path}"
        print(f"  [ERROR] AMC API request failed for {path}: {e} ({url})")
        return None


def is_supported_amc_theatre(theatre: dict) -> bool:
    location = theatre.get("location") or {}
    city = str(location.get("city") or "").strip().upper()
    state = str(location.get("state") or "").strip().upper()
    return city in AMC_ALLOWED_CITIES_BY_STATE.get(state, set())


def fetch_amc_theatres(ctx: ScrapeContext) -> list[dict]:
    def serpapi_fallback() -> list[dict]:
        fallback = []
        for name, config in THEATER_CONFIG.items():
            if config.get("source_type") != "amc":
                continue
            fallback.append({
                "name": name,
                "source_type": "serpapi",
                "serpapi_id": config.get("serpapi_id") or f"{name} showtimes",
                "official_url": config.get("official_url") or "https://www.amctheatres.com/",
            })
        return sorted(fallback, key=lambda t: t["name"])

    if not ctx.config.amc_vendor_key:
        print("  [WARN] AMC_VENDOR_KEY missing; using SerpAPI fallback for AMC theaters.")
        return serpapi_fallback()

    theatres_by_id: dict[str, dict] = {}

    if ctx.config.amc_theatre_ids:
        data = amc_request(ctx, "/v2/theatres", {"ids": ",".join(ctx.config.amc_theatre_ids), "page-size": ctx.config.amc_theatre_page_size})
        for theatre in ((data or {}).get("_embedded", {}) or {}).get("theatres", []):
            theatre_id = str(theatre.get("id") or "").strip()
            if theatre_id and not theatre.get("isClosed"):
                theatres_by_id[theatre_id] = theatre
    else:
        page = 1
        while True:
            data = amc_request(
                ctx,
                "/v2/theatres",
                {
                    "page-size": ctx.config.amc_theatre_page_size,
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

    if not results:
        print("  [WARN] AMC API returned no supported theaters; using SerpAPI fallback for AMC theaters.")
        return serpapi_fallback()

    return sorted(results, key=lambda t: t["name"])


AMC_SHOWTIME_TICKET_URL_KEYS = (
    "purchaseUrl",
    "purchaseURL",
    "mobilePurchaseUrl",
    "mobilePurchaseURL",
    "ticketUrl",
    "ticketURL",
    "mobileTicketUrl",
    "mobileTicketURL",
    "webSalesUrl",
    "webSalesURL",
    "mobileWebSalesUrl",
    "mobileWebSalesURL",
    "showtimeUrl",
    "showtimeURL",
)


def normalize_amc_ticket_url(url: object) -> str:
    text = str(url or "").strip()
    if text.startswith("//"):
        return f"https:{text}"
    if text.startswith("/"):
        return f"https://www.amctheatres.com{text}"
    return text


def amc_link_href(value: object) -> str:
    if isinstance(value, str):
        return normalize_amc_ticket_url(value)
    if isinstance(value, dict):
        for key in (
            "href",
            "url",
            *AMC_SHOWTIME_TICKET_URL_KEYS,
        ):
            text = normalize_amc_ticket_url(value.get(key))
            if text:
                return text
    return ""


def is_amc_showtime_ticket_url(url: object) -> bool:
    text = normalize_amc_ticket_url(url).lower()
    if not text.startswith("http"):
        return False
    if "amctheatres.com" not in text:
        return False
    if "api.amctheatres.com" in text:
        return False
    if "/movie-theatres/" in text:
        return False
    return True


def amc_showtime_ticket_url_from_id(showtime: dict[str, Any]) -> str:
    for key in ("id", "showtimeId", "showtimeID", "showtimeNumber"):
        showtime_id = str(showtime.get(key) or "").strip()
        if showtime_id.isdigit():
            return f"https://www.amctheatres.com/showtimes/{showtime_id}/tickets"
    return ""


def amc_showtime_purchase_url(showtime: dict[str, Any]) -> str:
    for key in AMC_SHOWTIME_TICKET_URL_KEYS:
        url = amc_link_href(showtime.get(key))
        if is_amc_showtime_ticket_url(url):
            return url

    for links_key in ("_links", "links"):
        links = showtime.get(links_key) or {}
        if not isinstance(links, dict):
            continue
        preferred_values = []
        fallback_values = []
        for key, value in links.items():
            key_text = str(key).lower()
            if any(token in key_text for token in ("purchase", "ticket", "websales", "showtime")):
                preferred_values.append(value)
            elif "web" in key_text or "mobile" in key_text:
                fallback_values.append(value)
        for value in [*preferred_values, *fallback_values]:
            values = value if isinstance(value, list) else [value]
            for item in values:
                url = amc_link_href(item)
                if is_amc_showtime_ticket_url(url):
                    return url
    return amc_showtime_ticket_url_from_id(showtime)


def fetch_amc_showtimes(theater: dict, ctx: ScrapeContext) -> list[dict]:
    theatre_id = theater.get("id")
    if not theatre_id:
        return []

    grouped: dict[str, dict[str, dict[str, object]]] = defaultdict(
        lambda: defaultdict(lambda: {"times": [], "ticket_urls": {}})
    )
    start = ny_now().replace(tzinfo=None)

    for offset in range(7):
        target = start + timedelta(days=offset)
        api_date = target.strftime("%m-%d-%Y")
        page = 1

        while True:
            data = amc_request(
                ctx,
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
                ticket_url = amc_showtime_purchase_url(showtime)
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


def paris_text(value: object) -> str:
    if isinstance(value, dict):
        name_parts = [
            str(value.get("givenName") or "").strip(),
            str(value.get("middleName") or "").strip(),
            str(value.get("familyName") or "").strip(),
        ]
        if any(name_parts):
            return " ".join(part for part in name_parts if part)
        return str(value.get("text") or "").strip()
    return str(value or "").strip()


def paris_token() -> str:
    response = requests.post(
        PARIS_AUTH_URL,
        data=PARIS_AUTH_DATA,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=20,
    )
    response.raise_for_status()
    token = str((response.json() or {}).get("access_token") or "").strip()
    if not token:
        raise RuntimeError("Paris API did not return an access token")
    return token


def paris_api_get(token: str, path: str, params: Optional[dict[str, object]] = None) -> dict:
    response = requests.get(
        f"{PARIS_API_BASE}{path}",
        params=params or {},
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        timeout=20,
    )
    response.raise_for_status()
    return response.json() or {}


def extract_paris_metadata(film: dict, related_data: dict) -> dict:
    metadata: dict[str, str] = {}
    release_year = extract_year_int(film.get("releaseDate"))
    if release_year:
        metadata["year"] = str(release_year)

    runtime = film.get("runtimeInMinutes")
    try:
        runtime_minutes = int(runtime)
    except (TypeError, ValueError):
        runtime_minutes = 0
    if runtime_minutes:
        metadata["runtime"] = f"{runtime_minutes} min"

    plot = paris_text(film.get("synopsis")) or paris_text(film.get("shortSynopsis"))
    if plot:
        metadata["plot"] = plot

    cast_lookup = {
        str(item.get("id") or item.get("castAndCrewMemberId") or "").strip(): paris_text(item.get("name"))
        for item in related_data.get("castAndCrew") or []
        if isinstance(item, dict)
    }
    director_names = []
    for director in film.get("directors") or []:
        if not isinstance(director, dict):
            continue
        name = cast_lookup.get(str(director.get("castAndCrewMemberId") or "").strip())
        if name:
            director_names.append(name)
    if director_names:
        metadata["director"] = ", ".join(dict.fromkeys(director_names))

    genre_lookup = {
        str(item.get("id") or "").strip(): paris_text(item.get("name"))
        for item in related_data.get("genres") or []
        if isinstance(item, dict)
    }
    genres = [
        genre_lookup.get(str(genre_id or "").strip())
        for genre_id in film.get("genreIds") or []
    ]
    genres = [genre for genre in genres if genre]
    if genres:
        metadata["genre"] = ", ".join(dict.fromkeys(genres))

    return metadata


def fetch_paris_showtimes(theater: dict) -> list[dict]:
    try:
        token = paris_token()
        screening_dates = paris_api_get(token, "/film-screening-dates", {"siteIds": PARIS_SITE_ID})
    except Exception as e:
        print(f"  [ERROR] Paris API date fetch failed for {theater['name']}: {e}")
        return []

    today = ny_now().date().isoformat()
    available_dates = sorted({
        str(item.get("businessDate") or "").strip()
        for item in screening_dates.get("filmScreeningDates") or []
        if str(item.get("businessDate") or "").strip() >= today
    })
    if not available_dates:
        start = ny_now().replace(tzinfo=None)
        available_dates = [date_iso(start + timedelta(days=offset)) for offset in range(7)]

    grouped: dict[str, dict[str, dict[str, object]]] = defaultdict(
        lambda: defaultdict(lambda: {"times": [], "ticket_urls": {}, "special_formats": set()})
    )
    grouped_metadata: dict[str, dict[str, str]] = {}

    for business_date in available_dates:
        try:
            payload = paris_api_get(
                token,
                f"/showtimes/by-business-date/{business_date}",
                {"siteIds": PARIS_SITE_ID},
            )
        except Exception as e:
            print(f"  [WARN] Paris API showtime fetch failed for {business_date}: {e}")
            continue

        related_data = payload.get("relatedData") or {}
        film_lookup = {
            str(film.get("id") or "").strip(): film
            for film in related_data.get("films") or []
            if isinstance(film, dict)
        }
        attribute_lookup = {
            str(attribute.get("id") or "").strip(): attribute
            for attribute in related_data.get("attributes") or []
            if isinstance(attribute, dict)
        }

        for showtime in payload.get("showtimes") or []:
            if not isinstance(showtime, dict):
                continue
            film_id = str(showtime.get("filmId") or "").strip()
            film = film_lookup.get(film_id) or {}
            title = clean_title(paris_text(film.get("title")))
            starts_at = str((showtime.get("schedule") or {}).get("startsAt") or "").strip()
            showtime_id = str(showtime.get("id") or "").strip()
            if not title or not starts_at or not showtime_id:
                continue

            try:
                local_dt = datetime.fromisoformat(starts_at)
            except Exception:
                continue

            labels = []
            for attribute_id in showtime.get("attributeIds") or []:
                attribute = attribute_lookup.get(str(attribute_id or "").strip()) or {}
                labels.extend([paris_text(attribute.get("shortName")), paris_text(attribute.get("name"))])
            title_formats = extract_special_formats(title, *labels)
            day_label = format_day_label(local_dt)
            time_label = format_time_label(local_dt)
            ticket_url = f"{PARIS_TICKET_BASE}/{showtime_id}/seats"

            day_bucket = grouped[title][day_label]
            day_bucket["date"] = date_iso(local_dt)
            day_bucket["times"].append(time_label)
            day_bucket["ticket_urls"].setdefault(time_label, ticket_url)
            if title_formats:
                day_bucket["special_formats"].update(title_formats)
            if title not in grouped_metadata and film:
                grouped_metadata[title] = extract_paris_metadata(film, related_data)

    flattened = []
    for title, days in grouped.items():
        metadata = grouped_metadata.get(title, {})
        hint_year = extract_year_int(metadata.get("year"))
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
                "hint_year": hint_year,
                "theater": theater["name"],
                "day": day_label,
                "date": payload.get("date"),
                "times": unique_times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
                "special_formats": sorted(payload.get("special_formats") or []),
                "source_metadata": metadata,
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

def get_existing_metadata(ctx: ScrapeContext, title: str) -> dict:
    base_title, _ = split_trailing_title_year(title)
    return (
        ctx.state.existing_movie_metadata.get(exact_title_identity_key(title))
        or ctx.state.existing_movie_metadata.get(exact_title_identity_key(base_title))
        or {}
    )


def get_existing_movie_record(ctx: ScrapeContext, title: str) -> dict:
    base_title, _ = split_trailing_title_year(title)
    return (
        ctx.state.existing_movie_records.get(exact_title_identity_key(title))
        or ctx.state.existing_movie_records.get(exact_title_identity_key(base_title))
        or {}
    )


def existing_verdict(movie: Optional[dict]) -> Optional[dict]:
    if not isinstance(movie, dict):
        return None
    verdict = movie.get("verdict") or {}
    if not isinstance(verdict, dict):
        return None
    label = str(verdict.get("verdict") or "").strip().upper()
    reason = str(verdict.get("reason") or "").strip()
    if label not in {"WATCH", "DEPENDS", "SKIP"} or not reason:
        return None
    return {"verdict": label, "reason": reason}


def get_best_cached_match(ctx: ScrapeContext, title: str, query_year: Optional[int] = None) -> dict:
    lookup_title, title_year = split_trailing_title_year(title)
    query_year = query_year or title_year
    candidate_keys = []
    if query_year is not None:
        candidate_keys.extend([
            exact_title_identity_key(lookup_title, query_year),
            exact_title_identity_key(lookup_title, query_year - 1),
            exact_title_identity_key(lookup_title, query_year + 1),
        ])
    candidate_keys.append(exact_title_identity_key(lookup_title))

    for key in candidate_keys:
        cached = ctx.state.rating_cache.get(key)
        if isinstance(cached, dict) and cached.get("imdbID"):
            return cached
    return {}


def set_cached_match(ctx: ScrapeContext, title: str, data: dict, source: str) -> None:
    lookup_title, _ = split_trailing_title_year(title)
    entry = {
        "imdbID": data.get("imdbID"),
        "title": data.get("Title"),
        "year": data.get("Year"),
        "source": source,
    }
    result_year = extract_year_int(data.get("Year"))
    if result_year is not None:
        ctx.state.rating_cache[exact_title_identity_key(lookup_title, result_year)] = entry
    else:
        ctx.state.rating_cache[exact_title_identity_key(lookup_title)] = entry


def purge_cached_match(ctx: ScrapeContext, title: str, imdb_id: str) -> None:
    lookup_title, _ = split_trailing_title_year(title)
    exact = exact_title_identity_key(lookup_title)
    keys_to_delete = [
        key for key, value in ctx.state.rating_cache.items()
        if (
            isinstance(value, dict)
            and value.get("imdbID") == imdb_id
            and (key == exact or key.startswith(f"{exact}|"))
        )
    ]
    for key in keys_to_delete:
        del ctx.state.rating_cache[key]


def omdb_request(ctx: ScrapeContext, params: dict) -> Optional[dict]:
    try:
        r = requests.get("https://www.omdbapi.com/", params={"apikey": ctx.config.omdb_key, **params}, timeout=10, headers=DEFAULT_HEADERS)
        data = r.json()
        if data.get("Response") == "False":
            return None
        return data
    except Exception:
        return None


def rt_slug(title: str) -> str:
    return normalize_title(title).replace(" ", "_")


def title_lookup_aliases(title: str) -> list[str]:
    aliases = []
    seen = set()
    for candidate in (
        title,
        re.sub(r"\s*&\s*", " and ", title or ""),
        re.sub(r"\band\b", "&", title or "", flags=re.IGNORECASE),
    ):
        candidate = re.sub(r"\s+", " ", str(candidate or "")).strip()
        normalized = normalize_title(candidate)
        if candidate and normalized not in seen:
            aliases.append(candidate)
            seen.add(normalized)
    return aliases


FLC_REAL_FACILITY_IDS = {3, 24, 26, 93}  # Walter Reade, Francesca Beale, Howard Gilman, Hearst Plaza


def fetch_flc_showtimes(theater: dict) -> list[dict]:
    """Scrape Film at Lincoln Center showtimes from filmlinc.org Next.js RSC payload."""
    url = str(theater.get("official_url") or "https://www.filmlinc.org/now-playing/").strip()
    try:
        content = fetch_source_html(url, theater.get("name", "Film at Lincoln Center"))
    except Exception as exc:
        print(f"  [ERROR] FLC fetch failed: {exc}")
        return []
    if not content:
        return []

    # The page is Next.js with RSC payload: self.__next_f.push([1, "...escaped json..."])
    push_chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', content, re.DOTALL)
    all_showtimes_raw: Optional[list] = None
    for chunk_str in push_chunks:
        if "allShowtimes" not in chunk_str:
            continue
        unescaped = chunk_str.replace('\\"', '"').replace('\\\\', '\\').replace('\\n', '\n').replace('\\t', '\t')
        idx = unescaped.find('"allShowtimes":')
        if idx < 0:
            continue
        start = idx + len('"allShowtimes":')
        try:
            bracket_start = unescaped.index('[', start)
        except ValueError:
            continue
        depth = 0
        end = bracket_start
        for j, ch in enumerate(unescaped[bracket_start:], bracket_start):
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    end = j
                    break
        try:
            all_showtimes_raw = json.loads(unescaped[bracket_start:end + 1])
        except (json.JSONDecodeError, ValueError):
            continue
        break

    if not all_showtimes_raw:
        print(f"  [WARN] FLC: could not find allShowtimes in RSC payload")
        return []

    today_iso = ny_now().date().isoformat()
    # Group: title -> date_iso -> list of (time_label, ticket_url)
    grouped: dict[str, dict[str, list[tuple[str, str]]]] = defaultdict(lambda: defaultdict(list))

    for event in all_showtimes_raw:
        title = clean_title(str(event.get("title") or "").strip())
        if not title:
            continue
        for st in event.get("showtimes") or []:
            facility_id = st.get("facilityId")
            if facility_id not in FLC_REAL_FACILITY_IDS:
                continue
            raw_date = str(st.get("date") or "").strip()  # "2026-05-29"
            raw_time = str(st.get("time") or "").strip()  # "6:00 PM"
            if not raw_date or not raw_time or raw_date < today_iso:
                continue
            tickets_url = str(st.get("ticketsUrl") or "").strip()
            # Normalize time: "6:00 PM" -> "6:00pm"
            time_label = re.sub(r'\s*(AM|PM)', lambda m: m.group(1).lower(), raw_time)
            time_label = time_label.lstrip("0") or time_label
            grouped[title][raw_date].append((time_label, tickets_url))

    flattened: list[dict] = []
    for title, dates in grouped.items():
        for date_str, time_entries in sorted(dates.items()):
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            day_label = format_day_label(dt)
            unique_times = sort_time_labels(list(dict.fromkeys(t for t, _ in time_entries)))
            ticket_urls = {t: u for t, u in time_entries if u}
            ticket_url = next(iter(ticket_urls.values()), get_source_ticket_url(theater))
            special_formats = extract_special_formats(title)
            flattened.append({
                "title": title,
                "theater": theater["name"],
                "day": day_label,
                "date": date_str,
                "times": unique_times,
                "ticket_url": ticket_url,
                "ticket_urls": ticket_urls,
                "special_formats": special_formats,
            })

    print(f"  [FLC] {len(grouped)} films, {len(flattened)} (title, date) entries")
    return flattened


def fetch_rt_fallback(title: str, query_year: Optional[int] = None) -> Optional[str]:
    candidates = []
    for alias in title_lookup_aliases(title):
        slug = rt_slug(alias)
        candidates.extend([
            f"https://www.rottentomatoes.com/m/{slug}",
            f"https://www.rottentomatoes.com/m/{slug}_{_CURRENT_YEAR}",
            f"https://www.rottentomatoes.com/m/{slug}_{_CURRENT_YEAR + 1}",
            f"https://www.rottentomatoes.com/m/{slug}_{_CURRENT_YEAR - 1}",
        ])

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
                headers=DEFAULT_HEADERS,
            ).text
        except Exception:
            continue
        page_title, page_year = extract_page_title(page)
        if page_title and not title_result_is_compatible(title, page_title, query_year=query_year, result_year=page_year):
            continue
        for pat in patterns:
            m = re.search(pat, page)
            if m:
                pct = int(m.group(1))
                if 0 <= pct <= 100:
                    return f"{pct}%"
    return None


def fetch_letterboxd_fallback(title: str, query_year: Optional[int] = None) -> Optional[str]:
    film_page = ""
    try:
        for alias in title_lookup_aliases(title):
            search_url = f"https://letterboxd.com/search/{quote(alias)}/"
            search_page = requests.get(
                search_url,
                timeout=15,
                headers=DEFAULT_HEADERS,
            ).text
            m = re.search(r'href="(/film/[^"/]+/)"', search_page)
            if not m:
                continue
            film_url = f"https://letterboxd.com{m.group(1)}"
            film_page = requests.get(
                film_url,
                timeout=15,
                headers=DEFAULT_HEADERS,
            ).text
            break
    except Exception:
        return None
    if not film_page:
        return None

    page_title, page_year = extract_page_title(film_page)
    if page_title and not title_result_is_compatible(title, page_title, query_year=query_year, result_year=page_year):
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


def extract_tmdb_poster(page: str) -> Optional[str]:
    for pattern in (
        r'https://image\.tmdb\.org/t/p/(?:w\d+|original)/[^"\s<>]+',
        r'https://media\.themoviedb\.org/t/p/(?:w\d+|original)/[^"\s<>]+',
    ):
        match = re.search(pattern, page or "")
        if not match:
            continue
        poster_url = html.unescape(match.group(0)).replace("\\/", "/")
        poster_url = poster_url.replace("https://media.themoviedb.org/t/p/", "https://image.tmdb.org/t/p/")
        return poster_url
    return None


def tmdb_movie_candidates(search_page: str) -> list[str]:
    candidates = []
    seen = set()
    for match in re.finditer(r'href=["\'](/movie/\d+[^"\'#?]*)["\']', search_page or ""):
        path = html.unescape(match.group(1)).split("?")[0].rstrip("/")
        if path not in seen:
            candidates.append(path)
            seen.add(path)
    return candidates


def fetch_tmdb_poster_fallback(title: str, query_year: Optional[int] = None) -> Optional[str]:
    for alias in title_lookup_aliases(title):
        try:
            search_page = requests.get(
                f"https://www.themoviedb.org/search?query={quote(alias)}",
                timeout=15,
                headers=DEFAULT_HEADERS,
            ).text
        except Exception:
            continue

        for path in tmdb_movie_candidates(search_page)[:6]:
            try:
                movie_page = requests.get(
                    f"https://www.themoviedb.org{path}",
                    timeout=15,
                    headers=DEFAULT_HEADERS,
                ).text
            except Exception:
                continue

            page_title, page_year = extract_page_title(movie_page)
            if page_title and not title_result_is_compatible(title, page_title, query_year=query_year, result_year=page_year):
                continue

            poster = extract_tmdb_poster(movie_page)
            if poster:
                return poster

    return None


def ensure_poster_fallback(title: str, ratings: dict, query_year: Optional[int] = None) -> dict:
    if ratings.get("poster") not in (None, "", "N/A"):
        return ratings
    poster = fetch_tmdb_poster_fallback(title, query_year=query_year)
    if poster:
        ratings["poster"] = poster
    return ratings


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


TITLE_ARTICLES = {"a", "an", "the"}


def title_tokens(value: str) -> list[str]:
    return [token for token in normalize_title(value).split() if token]


def title_match_score(query_title: str, result_title: str, query_year: Optional[int] = None, result_year: Optional[str] = None) -> float:
    q_norm = normalize_title(query_title)
    r_norm = normalize_title(result_title or "")
    if not q_norm or not r_norm:
        return 0.0

    q_tokens = set(title_tokens(q_norm))
    r_tokens = set(title_tokens(r_norm))
    overlap = len(q_tokens & r_tokens)
    coverage = overlap / max(1, len(q_tokens))
    precision = overlap / max(1, len(r_tokens))
    extra_tokens = r_tokens - q_tokens - TITLE_ARTICLES

    exact_bonus = 0.45 if q_norm == r_norm else 0.0
    startswith_bonus = 0.12 if r_norm.startswith(q_norm) or q_norm.startswith(r_norm) else 0.0
    extra_penalty = min(0.35, 0.08 * len(extra_tokens))
    score = (coverage * 0.55) + (precision * 0.45) + exact_bonus + startswith_bonus - extra_penalty

    result_y = extract_year_int(result_year)
    if query_year and result_y:
        diff = abs(query_year - result_y)
        if diff == 0:
            score += 0.15
        elif diff == 1:
            score += 0.05
        elif diff > 3:
            score -= 0.25

    return score


def title_result_is_compatible(query_title: str, result_title: str, query_year: Optional[int] = None, result_year: Optional[object] = None, minimum_score: float = 0.72) -> bool:
    query_tokens = set(title_tokens(query_title))
    result_tokens = set(title_tokens(result_title))
    if not query_tokens or not result_tokens:
        return False

    overlap = len(query_tokens & result_tokens)
    coverage = overlap / max(1, len(query_tokens))
    precision = overlap / max(1, len(result_tokens))
    extra_tokens = result_tokens - query_tokens - TITLE_ARTICLES
    exactish = normalize_title(query_title) == normalize_title(result_title) or query_tokens == (result_tokens - TITLE_ARTICLES)

    if len(query_tokens) <= 2 and not exactish and extra_tokens:
        return False
    if len(query_tokens) > 2 and not exactish and (coverage < 0.75 or precision < 0.60):
        return False

    score = title_match_score(query_title, result_title, query_year=query_year, result_year=str(result_year or ""))
    return score >= minimum_score


def extract_page_title(page: str) -> tuple[str, Optional[int]]:
    for pattern in (
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']',
        r"<title[^>]*>(.*?)</title>",
    ):
        match = re.search(pattern, page or "", re.DOTALL | re.IGNORECASE)
        if not match:
            continue
        raw = html.unescape(re.sub(r"<[^>]+>", " ", match.group(1)))
        raw = re.sub(r"\s+", " ", raw).strip()
        raw = re.sub(r"\s*(?:\||-|[\u2013\u2014])\s*(?:Rotten Tomatoes|Letterboxd|The Movie Database.*|TMDB).*$", "", raw, flags=re.IGNORECASE).strip()
        year = extract_year_int(raw)
        raw = re.sub(r"\s*[\(\[]?(?:18|19|20)\d{2}[\)\]]?\s*$", "", raw).strip()
        if raw:
            return raw, year
    return "", None


def fetch_omdb_by_imdb_id(ctx: ScrapeContext, imdb_id: str) -> Optional[dict]:
    if not imdb_id:
        return None
    return omdb_request(ctx, {"i": imdb_id, "tomatoes": "true"})


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


def is_suspect_short_metadata(title: str, ratings: Optional[dict]) -> bool:
    if not ratings:
        return False
    runtime_minutes = runtime_minutes_from_value(ratings.get("runtime"))
    return (
        runtime_minutes is not None
        and runtime_minutes <= 45
        and not title_explicitly_allows_short(title)
    )


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


def merge_existing_metadata(ctx: ScrapeContext, title: str, ratings: dict) -> dict:
    existing = get_existing_metadata(ctx, title)
    if not existing:
        return ratings
    if is_suspect_short_metadata(title, existing):
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


def apply_rating_overrides(ctx: ScrapeContext, title: str, ratings: dict) -> dict:
    lookup_title, _ = split_trailing_title_year(title)
    override = ctx.state.rating_overrides.get(exact_title_identity_key(lookup_title), {})
    if not override:
        override = ctx.state.rating_overrides.get(normalize_title(lookup_title), {})
    if isinstance(override, str):
        override = {"imdbID": override}
    if not isinstance(override, dict):
        return ratings

    for key, value in override.items():
        if key in {"imdbID", "year", "genre", "runtime", "plot", "director", "rt", "imdb", "metacritic", "letterboxd", "poster", "cinemaScore"}:
            ratings[key] = value

    cinema_score_override = ctx.state.cinemascore_overrides.get(exact_title_identity_key(lookup_title))
    if cinema_score_override in (None, "", "N/A"):
        cinema_score_override = ctx.state.cinemascore_overrides.get(normalize_title(lookup_title))
    if cinema_score_override not in (None, "", "N/A"):
        ratings["cinemaScore"] = str(cinema_score_override).strip().upper()
    return ratings


def merge_source_metadata(ratings: dict, source_metadata: Optional[dict]) -> dict:
    if not isinstance(source_metadata, dict):
        return ratings
    has_source_identity = bool(str(source_metadata.get("imdbID") or "").strip())
    has_rating_identity = bool(str(ratings.get("imdbID") or "").strip())
    for key in ("imdbID", "poster", "genre", "runtime", "plot", "year", "director"):
        if key == "year" and not (has_source_identity or has_rating_identity):
            continue
        value = source_metadata.get(key)
        if value not in (None, "", "N/A") and ratings.get(key) in (None, "", "N/A"):
            ratings[key] = value
    return ratings


def movie_group_key(title: str, hint_year: Optional[int] = None, ratings: Optional[dict] = None, source_metadata: Optional[dict] = None) -> str:
    base_title, _ = split_trailing_title_year(title)
    normalized = normalize_title(base_title)
    imdb_id = str((ratings or {}).get("imdbID") or (source_metadata or {}).get("imdbID") or "").strip()
    if imdb_id and normalized:
        return f"{normalized}|imdb:{imdb_id}"

    year = (
        hint_year
        or extract_year_int((source_metadata or {}).get("year"))
        or extract_year_int((ratings or {}).get("year"))
    )
    return title_identity_key(title, year)


def ratings_request_key(title: str, hint_year: Optional[int] = None, source_metadata: Optional[dict] = None) -> str:
    base_title, title_year = split_trailing_title_year(title)
    exact = exact_title_identity_key(base_title)
    year = hint_year or title_year or extract_year_int((source_metadata or {}).get("year"))
    imdb_id = str((source_metadata or {}).get("imdbID") or "").strip()
    if imdb_id:
        return f"{exact}|imdb:{imdb_id}"
    return f"{exact}|{year}" if year else exact


def letterboxd_query_year(year: Optional[int]) -> Optional[int]:
    if year is None:
        return None
    return year if year < (_CURRENT_YEAR - 1) else None


def metadata_completeness(ratings: Optional[dict]) -> int:
    if not isinstance(ratings, dict):
        return 0
    return sum(
        1
        for key in ("imdbID", "rt", "imdb", "metacritic", "letterboxd", "poster", "genre", "runtime", "plot", "year", "director", "cinemaScore")
        if ratings.get(key) not in (None, "", "N/A")
    )


def merge_prior_ratings(ratings: dict, prior: Optional[dict]) -> dict:
    if not isinstance(prior, dict):
        return ratings
    for key, value in prior.items():
        if ratings.get(key) in (None, "", "N/A") and value not in (None, "", "N/A"):
            ratings[key] = value
    return ratings


def rt_sort_value(ratings: Optional[dict]) -> int:
    value = str((ratings or {}).get("rt") or "").strip()
    match = re.search(r"(\d{1,3})\s*%", value)
    return int(match.group(1)) if match else 0


def ensure_unique_movie_ids(movies: list[dict]) -> None:
    used: set[str] = set()
    for movie in movies:
        title = str(movie.get("title") or "").strip()
        ratings = movie.get("ratings") or {}
        movie_id = str(movie.get("id") or make_movie_id(title, ratings) or slugify(title) or "movie").strip()

        if movie_id in used:
            base_title, title_year = split_trailing_title_year(title)
            year = extract_year_int(ratings.get("year")) or title_year
            title_slug = slugify(base_title or title) or "movie"
            suffix = f"{title_slug}-{year}" if year else title_slug
            candidate = f"{movie_id}-{suffix}" if suffix and suffix != movie_id else f"{movie_id}-2"
            counter = 2
            while candidate in used:
                counter += 1
                candidate = f"{movie_id}-{counter}"
            print(f"  [WARN] Duplicate movie id '{movie_id}' for '{title}'; using '{candidate}'.")
            movie_id = candidate

        movie["id"] = movie_id
        used.add(movie_id)


def migrate_movie_key(all_movies: dict[str, dict], theater_schedule: dict, theater_formats: dict, old_key: str, new_key: str) -> Optional[dict]:
    if old_key == new_key:
        return all_movies.get(new_key)

    for schedule_by_movie in theater_schedule.values():
        if old_key in schedule_by_movie:
            schedule_by_movie[new_key].extend(schedule_by_movie.pop(old_key))
    for formats_by_movie in theater_formats.values():
        if old_key in formats_by_movie:
            formats_by_movie[new_key].update(formats_by_movie.pop(old_key))

    old_movie = all_movies.pop(old_key, None)
    target_movie = all_movies.get(new_key)
    if old_movie is None:
        return target_movie
    if target_movie is None:
        all_movies[new_key] = old_movie
        return old_movie

    target_movie["ratings"] = strip_placeholder_metadata(
        merge_prior_ratings(target_movie.get("ratings") or {}, old_movie.get("ratings") or {})
    )
    if existing_verdict(target_movie) is None:
        preserved_verdict = existing_verdict(old_movie)
        if preserved_verdict is not None:
            target_movie["verdict"] = preserved_verdict
    target_formats = set(target_movie.get("special_formats") or [])
    old_formats = set(old_movie.get("special_formats") or [])
    target_movie["special_formats"] = sorted(target_formats.union(old_formats))
    return target_movie


def year_from_movie_key(key: str) -> Optional[int]:
    parts = str(key or "").split("|", 1)
    if len(parts) != 2:
        return None
    return int(parts[1]) if parts[1].isdigit() else None


def find_compatible_existing_movie_key(
    all_movies: dict[str, dict],
    title: str,
    new_key: str,
    hint_year: Optional[int] = None,
    ratings: Optional[dict] = None,
    source_metadata: Optional[dict] = None,
) -> Optional[str]:
    base_title, title_year = split_trailing_title_year(title)
    normalized = normalize_title(base_title)
    if not normalized:
        return None

    target_year = (
        hint_year
        or title_year
        or extract_year_int((source_metadata or {}).get("year"))
        or extract_year_int((ratings or {}).get("year"))
    )
    target_imdb = str((ratings or {}).get("imdbID") or (source_metadata or {}).get("imdbID") or "").strip()

    matches: list[tuple[int, str]] = []
    for candidate_key, movie in all_movies.items():
        if candidate_key == new_key:
            continue
        candidate_title, candidate_title_year = split_trailing_title_year(str(movie.get("title") or ""))
        candidate_key_base = str(candidate_key or "").split("|", 1)[0]
        if normalize_title(candidate_title) != normalized and candidate_key_base != normalized:
            continue

        candidate_ratings = movie.get("ratings") or {}
        candidate_imdb = str(candidate_ratings.get("imdbID") or "").strip()
        if target_imdb and candidate_imdb and target_imdb != candidate_imdb:
            continue

        candidate_year = (
            extract_year_int(candidate_ratings.get("year"))
            or candidate_title_year
            or year_from_movie_key(candidate_key)
        )
        if target_year and candidate_year and abs(target_year - candidate_year) > 2:
            continue

        specificity = 1 if "|" in candidate_key else 0
        completeness = metadata_completeness(candidate_ratings)
        matches.append((specificity + completeness, candidate_key))

    if not matches:
        return None
    matches.sort(reverse=True)
    return matches[0][1]


def should_promote_movie_key(new_key: str, existing_key: str) -> bool:
    return "|" in str(new_key or "") and "|" not in str(existing_key or "")


def enrich_from_rating_cache(ctx: ScrapeContext, title: str, ratings: dict, hint_year: Optional[int] = None) -> dict:
    cached = get_best_cached_match(ctx, title, query_year=hint_year or extract_year_int(ratings.get("year")))
    cached_imdb = cached.get("imdbID")
    if not cached_imdb:
        return ratings

    query_year = hint_year or extract_year_int(ratings.get("year"))
    cached_data = fetch_omdb_by_imdb_id(ctx, cached_imdb)
    if not is_acceptable_omdb_match(title, cached_data, query_year=query_year, minimum_score=0.70):
        purge_cached_match(ctx, title, cached_imdb)
        return ratings

    cached_ratings = parse_omdb_ratings(cached_data)
    for key, value in cached_ratings.items():
        if ratings.get(key) in (None, "", "N/A") and value not in (None, "", "N/A"):
            ratings[key] = value
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


def repair_dataset_metadata(ctx: ScrapeContext, dataset: dict) -> dict:
    movies = dataset.get("movies", [])
    repaired = 0
    for movie in movies:
        title = str(movie.get("title") or "").strip()
        if not title:
            continue
        existing_ratings = movie.get("ratings") or {}
        hint_year = extract_year_int(existing_ratings.get("year"))
        theaters = movie.get("theaters") or []
        theater_name = str(theaters[0].get("name") or "").strip() if theaters else None
        ratings = fetch_ratings(ctx, title, hint_year=hint_year, theater_name=theater_name)
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
    runtime_minutes = runtime_minutes_from_value(data.get("Runtime"))
    if runtime_minutes is not None and runtime_minutes <= 45 and not title_explicitly_allows_short(query_title):
        return False

    result_title = str(data.get("Title") or "")
    result_year = extract_year_int(data.get("Year"))
    query_tokens = set(title_tokens(query_title))
    result_tokens = set(title_tokens(result_title))
    title_word_count = len(query_tokens)
    if not query_tokens or not result_tokens:
        return False
    overlap = len(query_tokens & result_tokens)
    coverage = overlap / max(1, len(query_tokens))
    precision = overlap / max(1, len(result_tokens))
    extra_tokens = result_tokens - query_tokens - TITLE_ARTICLES

    exactish = (
        normalize_title(query_title) == normalize_title(result_title)
        or query_tokens == (result_tokens - TITLE_ARTICLES)
    )
    if title_word_count <= 2 and not exactish and extra_tokens:
        return False
    if title_word_count > 2 and not exactish and (coverage < 0.75 or precision < 0.60):
        return False

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
        result_title,
        query_year=query_year,
        result_year=data.get("Year"),
    )
    return score >= minimum_score


_CURRENT_YEAR = ny_now().year
REPERTORY_THEATERS = {
    "Metrograph",
    "IFC Center",
    "Film Forum",
    "Film at Lincoln Center",
    "Paris Theater",
    "Museum of Modern Art",
}

def resolve_omdb_record(ctx: ScrapeContext, title: str, hint_year: Optional[int] = None, theater_name: Optional[str] = None) -> Optional[dict]:
    """
    Look up a movie in OMDb, preferring year-specific matches to avoid
    confusing a new release with an older film that shares the same title.

    hint_year: year hint from SerpAPI or rating_overrides.json.
    """
    lookup_title, title_year = split_trailing_title_year(title)
    normalized = normalize_title(lookup_title)
    override = ctx.state.rating_overrides.get(normalized, {})
    if isinstance(override, str):
        override = {"imdbID": override}
    existing = get_existing_metadata(ctx, title)
    existing_year = extract_year_int(existing.get("year"))

    # Build query_year: prefer explicit hint, then override file
    query_year: Optional[int] = hint_year or title_year
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
        data = fetch_omdb_by_imdb_id(ctx, override_imdb)
        if data:
            set_cached_match(ctx, title, data, "override")
            return data
        print(f"  [WARN] Override imdbID failed for '{title}': {override_imdb}")

    # Cache — skip if year expectation strongly mismatches cached result
    cached = get_best_cached_match(ctx, title, query_year=query_year)
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
            data = fetch_omdb_by_imdb_id(ctx, cached_imdb)
            if is_acceptable_omdb_match(lookup_title, data, query_year=query_year, minimum_score=0.70, existing_year=existing_year):
                return data
            purge_cached_match(ctx, title, cached_imdb)

    # Try year-specific exact lookups before the open search.
    # This catches new releases that share a title with a classic — OMDb's
    # unqualified search returns the most popular (usually oldest) match.
    years_to_try: list[int] = []
    if query_year:
        years_to_try = [query_year, query_year - 1, query_year + 1]
    elif not repertory_mode:
        years_to_try = [_CURRENT_YEAR, _CURRENT_YEAR - 1]

    for y in years_to_try:
        for alias in title_lookup_aliases(lookup_title):
            data = omdb_request(ctx, {"t": alias, "y": y, "tomatoes": "true"})
            if not data:
                continue
            result_year_str = str(data.get("Year") or "")
            result_year = int(result_year_str[:4]) if result_year_str[:4].isdigit() else None
            if (result_year is None or abs(y - result_year) <= 2) and is_acceptable_omdb_match(lookup_title, data, query_year=query_year, minimum_score=0.70, existing_year=existing_year):
                set_cached_match(ctx, title, data, "exact_year")
                return data

    # Unqualified exact search — OMDb picks the most popular result.
    # Guard: if we expect a recent film and got something old, fall through.
    for alias in title_lookup_aliases(lookup_title):
        exact = omdb_request(ctx, {"t": alias, "tomatoes": "true"})
        if not exact:
            continue
        result_year_str = str(exact.get("Year") or "")
        result_year = int(result_year_str[:4]) if result_year_str[:4].isdigit() else None
        too_old = (
            query_year is not None
            and result_year is not None
            and (query_year - result_year) > 5
        )
        if not too_old and is_acceptable_omdb_match(lookup_title, exact, query_year=query_year, minimum_score=0.90, existing_year=existing_year):
            set_cached_match(ctx, title, exact, "exact")
            return exact

    # Full-text search with year-biased ranking as last resort
    effective_year = query_year or existing_year
    search = None
    for alias in title_lookup_aliases(lookup_title):
        search = omdb_request(ctx, {"s": alias, "type": "movie"})
        if search:
            break
    if not search:
        return None

    candidates = search.get("Search", [])
    if not candidates:
        return None

    best = max(
        candidates,
        key=lambda c: title_match_score(
            lookup_title,
            c.get("Title", ""),
            query_year=effective_year,
            result_year=c.get("Year"),
        ),
    )
    best_score = title_match_score(
        lookup_title,
        best.get("Title", ""),
        query_year=effective_year,
        result_year=best.get("Year"),
    )
    if best_score < 0.70:
        return None

    best_data = fetch_omdb_by_imdb_id(ctx, best.get("imdbID"))
    if best_data and is_acceptable_omdb_match(lookup_title, best_data, query_year=query_year, minimum_score=0.70, existing_year=existing_year):
        set_cached_match(ctx, title, best_data, "search")
        return best_data
    return None

def fetch_ratings(ctx: ScrapeContext, title: str, hint_year: Optional[int] = None, theater_name: Optional[str] = None) -> dict:
    """Fetch RT, IMDb, and CinemaScore via OMDb; include a Letterboxd-style score."""
    lookup_title, title_year = split_trailing_title_year(title)
    effective_hint_year = hint_year or title_year
    lb_query_year = letterboxd_query_year(title_year)
    poster_query_year = letterboxd_query_year(effective_hint_year)
    if not ctx.config.omdb_key:
        if ctx.config.allow_mock_data:
            return mock_ratings(title)
        parsed = empty_ratings()
        parsed["rt"] = fetch_rt_fallback(lookup_title, query_year=effective_hint_year)
        parsed["letterboxd"] = fetch_letterboxd_fallback(lookup_title, query_year=lb_query_year)
        parsed = merge_existing_metadata(ctx, title, parsed)
        parsed = apply_rating_overrides(ctx, title, parsed)
        parsed = ensure_poster_fallback(lookup_title, parsed, query_year=poster_query_year)
        return strip_placeholder_metadata(parsed)

    try:
        data = resolve_omdb_record(ctx, title, hint_year=effective_hint_year, theater_name=theater_name)
        parsed = parse_omdb_ratings(data) if data else empty_ratings()

        # Fallbacks for new/edge releases where OMDb is lagging.
        if not parsed.get("rt"):
            parsed["rt"] = fetch_rt_fallback(lookup_title, query_year=effective_hint_year)
        if not parsed.get("letterboxd"):
            parsed["letterboxd"] = fetch_letterboxd_fallback(lookup_title, query_year=lb_query_year)

        parsed = merge_existing_metadata(ctx, title, parsed)
        parsed = enrich_from_rating_cache(ctx, title, parsed, hint_year=effective_hint_year)
        parsed = apply_rating_overrides(ctx, title, parsed)
        parsed = ensure_poster_fallback(lookup_title, parsed, query_year=poster_query_year)
        parsed = strip_placeholder_metadata(parsed)
        return parsed
    except Exception as e:
        print(f"  [ERROR] OMDb failed for '{title}': {e}")
        parsed = empty_ratings()
        parsed["rt"] = fetch_rt_fallback(lookup_title, query_year=effective_hint_year)
        parsed["letterboxd"] = fetch_letterboxd_fallback(lookup_title, query_year=lb_query_year)
        parsed = merge_existing_metadata(ctx, title, parsed)
        parsed = enrich_from_rating_cache(ctx, title, parsed, hint_year=effective_hint_year)
        parsed = apply_rating_overrides(ctx, title, parsed)
        parsed = ensure_poster_fallback(lookup_title, parsed, query_year=poster_query_year)
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


# ─── ASSEMBLE ─────────────────────────────────────────────────────────────────

def validate_runtime_configuration(ctx: ScrapeContext) -> None:
    if ctx.config.allow_mock_data:
        return
    missing = []
    if SERPAPI_THEATERS and not ctx.config.serpapi_key:
        missing.append("SERPAPI_KEY")
    if not ctx.config.omdb_key:
        missing.append("OMDB_KEY")
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(f"Missing required scraper environment variable(s): {joined}. Set ALLOW_MOCK_DATA=1 for local mock runs.")


def fetch_theater_showtimes(theater: dict, ctx: ScrapeContext) -> list[dict]:
    if theater.get("source") == "amc":
        return fetch_amc_showtimes(theater, ctx)
    if theater.get("source_type") == "metrograph":
        return fetch_metrograph_showtimes(theater)
    if theater.get("source_type") == "ifc":
        return fetch_ifc_showtimes(theater)
    if theater.get("source_type") == "filmforum":
        return fetch_film_forum_showtimes(theater)
    if theater.get("source_type") == "moma":
        return fetch_moma_showtimes(theater)
    if theater.get("source_type") == "alamo":
        return fetch_alamo_showtimes(theater)
    if theater.get("source_type") == "paris":
        return fetch_paris_showtimes(theater)
    if theater.get("source_type") == "flc":
        return fetch_flc_showtimes(theater)
    return fetch_showtimes(theater, ctx)


def collect_showtime_entries(ctx: ScrapeContext) -> tuple[list[CollectedEntry], dict[str, dict], list[ScrapeIssue]]:
    theater_meta: dict[str, dict] = {
        name: build_theater_meta(name)
        for name in THEATER_CONFIG.keys()
    }
    collected: list[CollectedEntry] = []
    issues: list[ScrapeIssue] = []
    all_theaters = [*STATIC_THEATERS, *fetch_amc_theatres(ctx)]

    for theater in all_theaters:
        print(f"\nFetching: {theater['name']}")
        try:
            showtimes = fetch_theater_showtimes(theater, ctx)
        except Exception as exc:
            issue = ScrapeIssue(
                stage="fetch_showtimes",
                source=str(theater.get("source_type") or theater.get("source") or "unknown"),
                theater=str(theater["name"]),
                message=str(exc),
            )
            print(f"  [ERROR] Theater scrape failed: {issue.theater}: {issue.message}")
            issues.append(issue)
            continue

        theater_name = str(theater["name"])
        if theater_name not in theater_meta:
            theater_meta[theater_name] = build_theater_meta(
                theater_name,
                {
                    "source_type": theater.get("source_type") or theater.get("source") or "amc",
                    "official_url": theater.get("official_url") or "https://www.amctheatres.com/",
                },
            )
        for entry in showtimes:
            collected.append(CollectedEntry(theater=theater, entry=entry))

    return collected, theater_meta, issues


def resolve_movie_records(
    ctx: ScrapeContext,
    collected_entries: list[CollectedEntry],
    theater_meta: dict[str, dict],
) -> tuple[dict[str, dict], dict[str, dict], dict[str, dict[str, set[str]]]]:
    all_movies: dict[str, dict] = {}
    theater_schedule: dict[str, dict] = defaultdict(lambda: defaultdict(list))
    theater_formats: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    request_cache: dict[str, dict] = {}

    for collected in collected_entries:
        theater = collected.theater
        entry = collected.entry
        title = entry["title"]
        _lookup_title, title_year = split_trailing_title_year(title)
        theater_name = entry["theater"]
        day = entry["day"]
        times = entry["times"]
        schedule_date = str(entry.get("date") or infer_date_iso_from_label(day)).strip()
        ticket_url = str(entry.get("ticket_url") or get_source_ticket_url(theater)).strip()
        ticket_urls = {
            str(time): str(url).strip()
            for time, url in (entry.get("ticket_urls") or {}).items()
            if str(time).strip() and str(url).strip()
        }
        special_formats = [fmt for fmt in (entry.get("special_formats") or []) if str(fmt).strip()]
        source_metadata = entry.get("source_metadata") or {}
        hint_year = entry.get("hint_year") or title_year or extract_year_int(source_metadata.get("year"))
        movie_key = movie_group_key(title, hint_year=hint_year, source_metadata=source_metadata)
        rating_key = ratings_request_key(title, hint_year=hint_year, source_metadata=source_metadata)

        movie = all_movies.get(movie_key)
        if movie is None:
            if rating_key in request_cache:
                ratings = dict(request_cache[rating_key])
            else:
                print(f"  Fetching ratings for: {title}" + (f" (year hint: {hint_year})" if hint_year else ""))
                ratings = fetch_ratings(ctx, title, hint_year=hint_year, theater_name=theater_name)
                request_cache[rating_key] = dict(ratings)
            ratings = merge_source_metadata(ratings, source_metadata)
            ratings = strip_placeholder_metadata(ratings)
            resolved_key = movie_group_key(title, hint_year=hint_year, ratings=ratings, source_metadata=source_metadata)
            request_cache.setdefault(resolved_key, dict(ratings))
            movie_key = resolved_key
            base_title, _ = split_trailing_title_year(title)
            candidate_years = [
                hint_year,
                extract_year_int(source_metadata.get("year")),
                extract_year_int(ratings.get("year")),
                None,
            ]
            candidate_old_keys: list[str] = []
            for candidate_year in candidate_years:
                candidate_old_keys.extend([
                    title_identity_key(title, candidate_year),
                    title_identity_key(base_title, candidate_year),
                ])
            for old_key in dict.fromkeys(candidate_old_keys):
                old_movie = all_movies.get(old_key)
                if not old_movie or old_key == movie_key:
                    continue
                old_ratings = old_movie.get("ratings") or {}
                old_year = extract_year_int(old_ratings.get("year"))
                year_compatible = (
                    hint_year is None
                    or old_year is None
                    or abs(old_year - hint_year) <= 2
                )
                if not old_ratings.get("imdbID") and year_compatible:
                    movie = migrate_movie_key(all_movies, theater_schedule, theater_formats, old_key, movie_key)
                    break
            else:
                movie = all_movies.get(movie_key)
            if movie is None:
                existing_key = find_compatible_existing_movie_key(
                    all_movies,
                    title,
                    movie_key,
                    hint_year=hint_year,
                    ratings=ratings,
                    source_metadata=source_metadata,
                )
                if existing_key:
                    if should_promote_movie_key(movie_key, existing_key):
                        movie = migrate_movie_key(all_movies, theater_schedule, theater_formats, existing_key, movie_key)
                    else:
                        movie_key = existing_key
                        movie = all_movies.get(movie_key)
            if movie is not None:
                merged = merge_prior_ratings(movie.get("ratings") or {}, ratings)
                merged = merge_source_metadata(merged, source_metadata)
                movie["ratings"] = strip_placeholder_metadata(merged)
                if special_formats:
                    existing_formats = set(movie.get("special_formats") or [])
                    movie["special_formats"] = sorted(existing_formats.union(special_formats))
            else:
                movie_id = make_movie_id(title, ratings)
                prior_movie = get_existing_movie_record(ctx, title)
                movie = {
                    "id": movie_id,
                    "title": title,
                    "ratings": ratings,
                    "theaters": [],
                    "special_formats": [],
                }
                preserved_verdict = existing_verdict(prior_movie)
                if preserved_verdict is not None:
                    movie["verdict"] = preserved_verdict
                all_movies[movie_key] = movie
        else:
            current_ratings = movie.get("ratings") or {}
            current_year = extract_year_int(current_ratings.get("year"))
            should_refetch = (
                hint_year is not None
                and (
                    not current_ratings.get("imdbID")
                    or current_year is None
                    or abs(current_year - hint_year) > 2
                )
            )
            if should_refetch:
                refetch_key = ratings_request_key(title, hint_year=hint_year, source_metadata=source_metadata)
                if refetch_key in request_cache:
                    refreshed = dict(request_cache[refetch_key])
                else:
                    print(f"  Fetching ratings for: {title}" + (f" (year hint: {hint_year})" if hint_year else ""))
                    refreshed = fetch_ratings(ctx, title, hint_year=hint_year, theater_name=theater_name)
                    request_cache[refetch_key] = dict(refreshed)
                current_ratings = merge_prior_ratings(refreshed, current_ratings)
            movie["ratings"] = strip_placeholder_metadata(merge_source_metadata(current_ratings, source_metadata))
            resolved_key = movie_group_key(title, hint_year=hint_year, ratings=movie["ratings"], source_metadata=source_metadata)
            if resolved_key != movie_key:
                movie = migrate_movie_key(all_movies, theater_schedule, theater_formats, movie_key, resolved_key) or movie
                movie_key = resolved_key

        theater_schedule[theater_name][movie_key].append({
            "day": day,
            "date": schedule_date,
            "times": times,
            "ticket_url": ticket_url,
            "ticket_urls": ticket_urls,
        })
        if special_formats:
            theater_formats[theater_name][movie_key].update(special_formats)
            existing_formats = set(movie.get("special_formats") or [])
            movie["special_formats"] = sorted(existing_formats.union(special_formats))

    # Deduplicate entries that resolved to the same IMDB ID.
    # This happens when different sources list the same film under slightly
    # different titles (e.g. "... (Live in 3D)", capitalisation differences).
    imdb_to_keys: dict[str, list[str]] = defaultdict(list)
    for key, movie in list(all_movies.items()):
        imdb_id = str((movie.get("ratings") or {}).get("imdbID") or "").strip()
        if imdb_id:
            imdb_to_keys[imdb_id].append(key)
    for imdb_id, keys in imdb_to_keys.items():
        if len(keys) <= 1:
            continue
        # Keep the key whose title most closely matches the OMDB-returned title,
        # falling back to the shortest title (fewest extra qualifiers).
        def key_score(k: str) -> int:
            return len(all_movies[k].get("title") or "")
        primary_key = min(keys, key=key_score)
        for dup_key in keys:
            if dup_key == primary_key:
                continue
            dup_title = (all_movies.get(dup_key) or {}).get("title", dup_key)
            print(f"  [INFO] Merging same-IMDB duplicate ({imdb_id}): '{dup_title}' → '{all_movies[primary_key].get('title')}'")
            migrate_movie_key(all_movies, theater_schedule, theater_formats, dup_key, primary_key)

    return all_movies, theater_schedule, theater_formats


def attach_schedules_to_movies(
    all_movies: dict[str, dict],
    theater_schedule: dict[str, dict],
    theater_formats: dict[str, dict[str, set[str]]],
    theater_meta: dict[str, dict],
) -> list[dict]:
    for theater_name, movies in theater_schedule.items():
        for key, schedule in movies.items():
            if key not in all_movies:
                continue
            ticket_url = next((slot.get("ticket_url") for slot in schedule if slot.get("ticket_url")), "") or theater_meta.get(theater_name, {}).get("official_url", "")
            clean_schedule = []
            for slot in schedule:
                clean_slot = {"day": slot["day"], "times": slot["times"]}
                if slot.get("date"):
                    clean_slot["date"] = slot["date"]
                if slot.get("ticket_urls"):
                    clean_slot["ticket_urls"] = slot["ticket_urls"]
                clean_schedule.append(clean_slot)
            all_movies[key]["theaters"].append({
                "name": theater_name,
                "ticket_url": ticket_url,
                "schedule": clean_schedule,
                "special_formats": sorted(theater_formats[theater_name].get(key, set())),
            })

    movies_list = sorted(
        all_movies.values(),
        key=lambda movie: (
            {"WATCH": 0, "SKIP": 1}.get((movie.get("verdict") or {}).get("verdict"), 2),
            -rt_sort_value(movie.get("ratings") or {}),
        ),
    )
    if not movies_list:
        raise RuntimeError("No movies scraped from any source")
    ensure_unique_movie_ids(movies_list)
    return movies_list


def finalize_dataset(
    ctx: ScrapeContext,
    movies_list: list[dict],
    theater_schedule: dict[str, dict],
    theater_meta: dict[str, dict],
    issues: list[ScrapeIssue],
) -> dict:
    for movie in movies_list:
        prestige_tags = build_movie_prestige_tags(movie, ctx.state.prestige_overrides)
        if prestige_tags:
            movie["prestige_tags"] = prestige_tags
        else:
            movie.pop("prestige_tags", None)
    dataset = {
        "generated_at": ny_now().isoformat(),
        "week_of": (ny_now() + timedelta(days=(4 - ny_now().weekday()) % 7)).strftime("%B %d, %Y"),
        "theaters": sorted(theater_schedule.keys()),
        "theater_meta": theater_meta,
        "movies": movies_list,
    }
    if issues:
        dataset["scrape_errors"] = [f"{issue.theater}: {issue.message}" for issue in issues]
        ctx.state.collected_issues.extend(
            {
                "stage": issue.stage,
                "source": issue.source,
                "theater": issue.theater,
                "message": issue.message,
            }
            for issue in issues
        )
    return dataset


def build_dataset(ctx: ScrapeContext) -> dict:
    validate_runtime_configuration(ctx)
    print("Starting weekly NYC cinema scrape...")
    collected_entries, theater_meta, issues = collect_showtime_entries(ctx)
    all_movies, theater_schedule, theater_formats = resolve_movie_records(ctx, collected_entries, theater_meta)
    movies_list = attach_schedules_to_movies(all_movies, theater_schedule, theater_formats, theater_meta)
    return finalize_dataset(ctx, movies_list, theater_schedule, theater_meta, issues)


if __name__ == "__main__":
    scrape_ctx = build_scrape_context(
        script_dir=SCRIPT_DIR,
        output_data_path=OUTPUT_DATA_PATH,
        rating_overrides_path=RATING_OVERRIDES_PATH,
        cinemascore_overrides_path=CINEMASCORE_OVERRIDES_PATH,
        prestige_overrides_path=PRESTIGE_OVERRIDES_PATH,
        rating_cache_path=RATING_CACHE_PATH,
    )
    dataset = build_dataset(scrape_ctx)
    save_json_dict(scrape_ctx.rating_cache_path, scrape_ctx.state.rating_cache, sort_keys=True)
    with OUTPUT_DATA_PATH.open("w", encoding="utf-8") as handle:
        json.dump(dataset, handle, indent=2, ensure_ascii=False)
    print(f"\nDone. {len(dataset['movies'])} unique films written to public/data.json")
