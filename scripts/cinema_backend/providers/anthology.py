from __future__ import annotations

import html
import re
from collections import defaultdict
from datetime import datetime
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag

from cinema_backend.common import (
    clean_title,
    extract_screening_attributes,
    extract_special_formats,
    format_day_label,
    format_time_label,
    get_source_ticket_url,
    sort_time_labels,
)
from cinema_backend.providers.support import (
    fetch_html_page,
    horizon_dates,
    horizon_iso_set,
    use_cached_on_failure,
    warn_if_empty,
)


ANTHOLOGY_BASE = "https://www.anthologyfilmarchives.org"
ANTHOLOGY_CALENDAR = f"{ANTHOLOGY_BASE}/film_screenings/calendar"


def _calendar_url(month: int, year: int) -> str:
    query = {"view": "list", "month": month, "year": year}
    return f"{ANTHOLOGY_CALENDAR}?{urlencode(query)}"


def _parse_day_header(text: str, year: int) -> datetime | None:
    normalized = re.sub(r"\s+", " ", text or "").strip()
    match = re.search(
        r"\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
        r"([A-Za-z]+)\s+(\d{1,2})\b",
        normalized,
        re.IGNORECASE,
    )
    if not match:
        return None
    for fmt in ("%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(f"{match.group(2)} {match.group(3)} {year}", fmt)
        except ValueError:
            continue
    return None


def _showing_url(anchor_name: str, month: int, year: int) -> str:
    return f"{_calendar_url(month, year)}#{anchor_name}"


def _parse_time(value: str, target_date: datetime) -> datetime | None:
    text = re.sub(r"\s+", " ", value or "").strip().lower().replace(".", "")
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            parsed = datetime.strptime(text.upper(), fmt)
            return target_date.replace(hour=parsed.hour, minute=parsed.minute)
        except ValueError:
            continue
    return None


def _program_attributes(title: str, block_text: str) -> list[str]:
    found = extract_screening_attributes(title, block_text)
    normalized = f"{title} {block_text}".lower()
    if re.search(r"\bdouble\s+feature\b| / ", normalized) and "Double Feature" not in found:
        found.append("Double Feature")
    if re.search(r"\bshorts?\b|\bshort\s+program\b", normalized) and "Shorts Program" not in found:
        found.append("Shorts Program")
    return found


def parse_anthology_calendar_html(content: str, theater: dict, *, month: int, year: int, horizon: set[str]) -> list[dict]:
    soup = BeautifulSoup(content or "", "html.parser")
    calendar = soup.select_one("#calendar")
    if calendar is None:
        return []

    grouped: dict[str, dict[str, dict[str, str]]] = defaultdict(lambda: defaultdict(dict))
    grouped_formats: dict[str, set[str]] = defaultdict(set)
    grouped_dates: dict[str, dict[str, str]] = defaultdict(dict)
    grouped_attributes: dict[str, dict[str, dict[str, set[str]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(set))
    )

    current_day: datetime | None = None
    for node in calendar.children:
        if not isinstance(node, Tag):
            continue
        if node.name == "h3":
            current_day = _parse_day_header(node.get_text(" "), year)
            continue
        classes = node.get("class") or []
        if current_day is None or "film-showing" not in classes:
            continue
        current_iso = current_day.date().isoformat()
        if current_iso not in horizon:
            continue

        title_el = node.select_one(".film-title")
        anchor = node.select_one('a[name^="showing-"]')
        if title_el is None or anchor is None:
            continue

        raw_title = html.unescape(title_el.get_text(" ", strip=True))
        title = clean_title(raw_title)
        if not title:
            continue

        time_dt = _parse_time(anchor.get_text(" ", strip=True), current_day)
        if time_dt is None:
            continue
        time_label = format_time_label(time_dt)
        day_label = format_day_label(time_dt)
        anchor_name = str(anchor.get("name") or "").strip()
        event_url = _showing_url(anchor_name, month, year) if anchor_name else ANTHOLOGY_CALENDAR
        veezi = next(
            (
                urljoin(ANTHOLOGY_BASE, str(link.get("href") or "").strip())
                for link in node.select('a[href*="veezi.com"]')
                if str(link.get("href") or "").strip()
            ),
            "",
        )
        ticket_url = veezi or event_url or get_source_ticket_url(theater)
        block_text = html.unescape(node.get_text(" ", strip=True))
        formats = extract_special_formats(raw_title, block_text)
        attributes = _program_attributes(raw_title, block_text)

        grouped[title][day_label][time_label] = ticket_url
        grouped_dates[title][day_label] = current_iso
        grouped_formats[title].update(formats)
        grouped_attributes[title][day_label][time_label].update(attributes)

    entries = []
    for title, days in grouped.items():
        for day_label, time_map in days.items():
            unique_times = sort_time_labels(list(time_map.keys()))
            ticket_urls = {time_label: time_map[time_label] for time_label in unique_times if time_map.get(time_label)}
            time_attributes = {
                time_label: sorted(grouped_attributes[title][day_label].get(time_label, set()))
                for time_label in unique_times
                if grouped_attributes[title][day_label].get(time_label)
            }
            entries.append(
                {
                    "title": title,
                    "theater": theater["name"],
                    "day": day_label,
                    "date": grouped_dates[title].get(day_label),
                    "times": unique_times,
                    "ticket_url": next(iter(ticket_urls.values()), get_source_ticket_url(theater)),
                    "ticket_urls": ticket_urls,
                    "special_formats": sorted(grouped_formats.get(title, set())),
                    "time_attributes": time_attributes,
                }
            )
    return entries


def fetch_anthology_showtimes(theater: dict, ctx=None) -> list[dict]:
    dates = horizon_dates(ctx)
    horizon = horizon_iso_set(ctx)
    month_keys = list(dict.fromkeys((day.month, day.year) for day in dates))
    entries: list[dict] = []
    try:
        for month, year in month_keys:
            content = fetch_html_page(_calendar_url(month, year), theater["name"])
            if not content:
                raise RuntimeError(f"empty calendar page for {month}/{year}")
            parsed = parse_anthology_calendar_html(content, theater, month=month, year=year, horizon=horizon)
            if not parsed and "film-showing" not in content:
                raise RuntimeError("calendar markup did not contain film-showing blocks")
            entries.extend(parsed)
    except Exception as exc:
        return use_cached_on_failure(theater, ctx, str(exc))

    warn_if_empty(theater, entries)
    return entries
