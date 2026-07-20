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
)
from cinema_backend.providers.support import (
    fetch_html_page,
    horizon_iso_set,
    use_cached_on_failure,
    warn_if_empty,
)


BAM_BASE = "https://www.bam.org"
BAM_COMMERCE = "https://commerce.bam.org"


def _plain_text(node) -> str:
    if node is None:
        return ""
    return html.unescape(re.sub(r"\s+", " ", node.get_text(" ", strip=True))).strip()


def _parse_index_date(value: str) -> str:
    text = str(value or "").strip()
    match = re.match(r"((?:18|19|20)\d{2})-(\d{2})-(\d{2})", text)
    return match.group(0) if match else ""


def _block_is_relevant(block, horizon: set[str]) -> bool:
    sort_date = _parse_index_date(block.get("data-sort-date"))
    if sort_date and sort_date in horizon:
        return True
    date_text = _plain_text(block.select_one(".bam-block-2x2-date, .bam-block-2x2-hover-date, .date"))
    if re.search(r"\bNow\s+Playing\b", date_text, re.IGNORECASE):
        return True
    return False


def discover_bam_productions(content: str, *, horizon: set[str]) -> list[dict]:
    soup = BeautifulSoup(content or "", "html.parser")
    productions = []
    seen: set[str] = set()
    for block in soup.select(".productionblock"):
        if str(block.get("data-sort-genre") or "").strip().lower() not in {"", "film"}:
            continue
        if not _block_is_relevant(block, horizon):
            continue
        buy_link = block.select_one('a.buy-button[href*="commerce.bam.org/production/"], a[href*="commerce.bam.org/production/"]')
        if buy_link is None:
            continue
        url = str(buy_link.get("href") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        title = html.unescape(str(block.get("data-sort-title") or "").strip())
        if not title:
            title = _plain_text(block.select_one(".bam-block-2x2-title, .bam-block-2x2-hover-title, h3.title"))
        description = _plain_text(block.select_one(".bam-block-2x2-hover-content-body"))
        label = _plain_text(block.select_one(".bam-btn-3, .bam-block-2x2-label"))
        productions.append({"title": title, "url": url, "context": " ".join(v for v in [description, label] if v)})
    return productions


def _parse_performance_datetime(text: str) -> datetime | None:
    normalized = re.sub(r"\s+", " ", text or "").strip()
    match = re.search(
        r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
        r"([A-Za-z]{3,9})\s+(\d{1,2})\s*,\s+((?:18|19|20)\d{2})\s+at\s+"
        r"(\d{1,2}):(\d{2})\s*([ap])m\b",
        normalized,
        re.IGNORECASE,
    )
    if not match:
        return None
    month_name, day, year, hour, minute, meridiem = match.groups()
    for fmt in ("%B %d %Y", "%b %d %Y"):
        try:
            base = datetime.strptime(f"{month_name} {day} {year}", fmt)
            break
        except ValueError:
            base = None
    if base is None:
        return None
    hour_value = int(hour)
    if meridiem.lower() == "p" and hour_value != 12:
        hour_value += 12
    if meridiem.lower() == "a" and hour_value == 12:
        hour_value = 0
    return base.replace(hour=hour_value, minute=int(minute))


def parse_bam_production_html(content: str, theater: dict, production: dict, *, horizon: set[str]) -> list[dict]:
    soup = BeautifulSoup(content or "", "html.parser")
    raw_title = _plain_text(soup.select_one(".production-list__title, h1")) or str(production.get("title") or "")
    title = clean_title(raw_title)
    if not title:
        return []

    context = " ".join(
        [
            raw_title,
            str(production.get("context") or ""),
            _plain_text(soup.select_one(".production-list__header-info")),
            _plain_text(soup.select_one(".production-list__description")),
        ]
    )
    formats = set(extract_special_formats(context))
    attributes = set(extract_screening_attributes(context))

    grouped: dict[str, dict[str, str]] = defaultdict(dict)
    grouped_attributes: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    grouped_dates: dict[str, str] = {}

    for item in soup.select(".performance-item"):
        dt = _parse_performance_datetime(_plain_text(item.select_one(".performance-item__time")))
        if dt is None or dt.date().isoformat() not in horizon:
            continue
        message = _plain_text(item.select_one(".performance-item__message, .performance-item__extra"))
        item_text = _plain_text(item)
        item_attributes = set(extract_screening_attributes(item_text, message))
        item_attributes.update(attributes)
        formats.update(extract_special_formats(item_text, message))
        link = item.select_one('a.performance-item__button[href*="/booking/production/bestavailable/"], a[href*="/booking/production/bestavailable/"]')
        if link is None:
            link = item.select_one("a.performance-item__button[href]")
        href = str(link.get("href") or "").strip() if link else ""
        ticket_url = urljoin(BAM_COMMERCE, href) if href else get_source_ticket_url(theater)
        time_label = format_time_label(dt)
        day_label = format_day_label(dt)
        grouped[day_label][time_label] = ticket_url
        grouped_dates[day_label] = dt.date().isoformat()
        grouped_attributes[day_label][time_label].update(item_attributes)

    entries = []
    for day_label, time_map in grouped.items():
        unique_times = sort_time_labels(list(time_map.keys()))
        ticket_urls = {time_label: time_map[time_label] for time_label in unique_times if time_map.get(time_label)}
        time_attributes = {
            time_label: sorted(grouped_attributes[day_label].get(time_label, set()))
            for time_label in unique_times
            if grouped_attributes[day_label].get(time_label)
        }
        entries.append(
            {
                "title": title,
                "theater": theater["name"],
                "day": day_label,
                "date": grouped_dates.get(day_label, ""),
                "times": unique_times,
                "ticket_url": next(iter(ticket_urls.values()), get_source_ticket_url(theater)),
                "ticket_urls": ticket_urls,
                "special_formats": sorted(formats),
                "time_attributes": time_attributes,
            }
        )
    return entries


def fetch_bam_showtimes(theater: dict, ctx=None) -> list[dict]:
    try:
        source_url = str(theater.get("source_url") or "https://www.bam.org/film").strip()
        index_html = fetch_html_page(source_url, theater["name"])
        if not index_html:
            raise RuntimeError("empty BAM film page")
        horizon = horizon_iso_set(ctx)
        productions = discover_bam_productions(index_html, horizon=horizon)
        if not productions and "productionblock" not in index_html:
            raise RuntimeError("BAM film page markup did not contain production blocks")
        entries: list[dict] = []
        for production in productions:
            detail_html = fetch_html_page(str(production["url"]), theater["name"])
            if not detail_html:
                raise RuntimeError(f"empty BAM production page: {production['url']}")
            entries.extend(parse_bam_production_html(detail_html, theater, production, horizon=horizon))
    except Exception as exc:
        return use_cached_on_failure(theater, ctx, str(exc))

    warn_if_empty(theater, entries)
    return entries
