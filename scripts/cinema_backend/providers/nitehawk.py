from __future__ import annotations

import html
import re
from collections import defaultdict
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from cinema_backend.common import (
    clean_title,
    extract_screening_attributes,
    extract_special_formats,
    format_day_label,
    format_time_label,
    get_source_ticket_url,
    sort_time_labels,
    split_trailing_title_year,
)
from cinema_backend.providers.support import (
    fetch_html_page,
    horizon_dates,
    horizon_iso_set,
    use_cached_on_failure,
    warn_if_empty,
)


NITEHAWK_BASE = "https://nitehawkcinema.com"


def _location_slug(theater: dict) -> str:
    return str(theater.get("location_slug") or "").strip("/")


def _homepage_url(theater: dict) -> str:
    source = str(theater.get("source_url") or "").strip()
    if source:
        return source
    return f"{NITEHAWK_BASE}/{_location_slug(theater)}/"


def _parse_scheduled_dates(content: str) -> list[str]:
    match = re.search(r"nhScheduledDates\"\]\s*=\s*(\[[^\]]*\])", content or "")
    if match:
        return re.findall(r"\d{4}-\d{2}-\d{2}", match.group(1))
    return list(dict.fromkeys(re.findall(r'data-date=["\'](\d{4}-\d{2}-\d{2})["\']', content or "")))


def _date_url(theater: dict, date_iso: str) -> str:
    return f"{NITEHAWK_BASE}/{_location_slug(theater)}/{date_iso}/"


def _parse_time_label(raw: str, target_date: datetime) -> str:
    text = re.sub(r"\s+", " ", raw or "").strip().lower()
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*([ap])m\b", text)
    if not match:
        return ""
    hour = int(match.group(1))
    minute = int(match.group(2) or 0)
    if match.group(3) == "p" and hour != 12:
        hour += 12
    if match.group(3) == "a" and hour == 12:
        hour = 0
    return format_time_label(target_date.replace(hour=hour, minute=minute))


def _link_attributes(link) -> list[str]:
    values = [
        " ".join(link.get("class") or []),
        link.get_text(" ", strip=True),
        " ".join(badge.get_text(" ", strip=True) for badge in link.select(".badge")),
    ]
    found = extract_screening_attributes(*values)
    class_text = values[0].lower()
    badge_text = values[-1].lower()
    if ("open-captions" in class_text or re.search(r"\boc\b", badge_text)) and "Open Caption" not in found:
        found.append("Open Caption")
    return found


def parse_nitehawk_date_html(content: str, theater: dict, *, target_date: datetime) -> list[dict]:
    soup = BeautifulSoup(content or "", "html.parser")
    grouped: dict[str, dict[str, str]] = defaultdict(dict)
    grouped_formats: dict[str, set[str]] = defaultdict(set)
    grouped_attributes: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    source_url = _homepage_url(theater)
    day_label = format_day_label(target_date)
    date_value = target_date.date().isoformat()

    for block in soup.select(".show-container"):
        title_el = block.select_one(".show-title")
        if title_el is None:
            continue
        raw_title = html.unescape(title_el.get_text(" ", strip=True))
        title_without_year, hint_year = split_trailing_title_year(raw_title)
        title = clean_title(title_without_year)
        if not title:
            continue
        formats = extract_special_formats(raw_title)
        if formats:
            grouped_formats[title].update(formats)

        for link in block.select('a.showtime[href*="/purchase/"]'):
            time_label = _parse_time_label(link.get_text(" ", strip=True), target_date)
            if not time_label:
                continue
            ticket_url = urljoin(source_url, str(link.get("href") or "").strip())
            grouped[title][time_label] = ticket_url
            for attribute in _link_attributes(link):
                grouped_attributes[title][time_label].add(attribute)
        if hint_year and title:
            grouped_formats[title].update([])

    entries = []
    for title, time_map in grouped.items():
        unique_times = sort_time_labels(list(time_map.keys()))
        ticket_urls = {time_label: time_map[time_label] for time_label in unique_times if time_map.get(time_label)}
        time_attributes = {
            time_label: sorted(grouped_attributes[title].get(time_label, set()))
            for time_label in unique_times
            if grouped_attributes[title].get(time_label)
        }
        entries.append(
            {
                "title": title,
                "theater": theater["name"],
                "day": day_label,
                "date": date_value,
                "times": unique_times,
                "ticket_url": next(iter(ticket_urls.values()), get_source_ticket_url(theater)),
                "ticket_urls": ticket_urls,
                "special_formats": sorted(grouped_formats.get(title, set())),
                "time_attributes": time_attributes,
            }
        )
    return entries


def fetch_nitehawk_showtimes(theater: dict, ctx=None) -> list[dict]:
    try:
        homepage = fetch_html_page(_homepage_url(theater), theater["name"])
        if not homepage:
            raise RuntimeError("empty homepage")
        scheduled = set(_parse_scheduled_dates(homepage))
        if not scheduled:
            print(f"  [WARN] {theater['name']} homepage did not expose nhScheduledDates; falling back to seven direct dated pages.")
        horizon = horizon_iso_set(ctx)
        entries: list[dict] = []
        page_cache: dict[str, str] = {}
        for target_date in horizon_dates(ctx):
            date_value = target_date.date().isoformat()
            if scheduled and date_value not in scheduled:
                continue
            if date_value not in horizon:
                continue
            url = _date_url(theater, date_value)
            content = page_cache.get(url)
            if content is None:
                content = fetch_html_page(url, theater["name"])
                page_cache[url] = content
            if not content:
                raise RuntimeError(f"empty dated page for {date_value}")
            parsed = parse_nitehawk_date_html(content, theater, target_date=target_date)
            if not parsed and "show-container" not in content:
                raise RuntimeError(f"dated page markup changed for {date_value}")
            entries.extend(parsed)
    except Exception as exc:
        return use_cached_on_failure(theater, ctx, str(exc))

    warn_if_empty(theater, entries)
    return entries
