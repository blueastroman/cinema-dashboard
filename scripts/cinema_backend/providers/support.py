from __future__ import annotations

import copy
from datetime import datetime, timedelta
from typing import Any, Optional

import requests

from cinema_backend.common import (
    NY_TZ,
    date_iso,
    exact_title_identity_key,
    format_day_label,
    get_source_ticket_url,
    ny_now,
    sort_time_labels,
)
from cinema_backend.http import DEFAULT_HEADERS


def ny_reference_now(ctx: Optional[Any] = None) -> datetime:
    current = getattr(ctx, "now", None) or ny_now()
    if current.tzinfo is None:
        return current.replace(tzinfo=NY_TZ)
    return current.astimezone(NY_TZ)


def horizon_dates(ctx: Optional[Any] = None, days: int = 7) -> list[datetime]:
    start = ny_reference_now(ctx).replace(hour=12, minute=0, second=0, microsecond=0)
    return [start + timedelta(days=offset) for offset in range(days)]


def horizon_iso_set(ctx: Optional[Any] = None, days: int = 7) -> set[str]:
    return {date_iso(day) for day in horizon_dates(ctx, days)}


def fetch_html_page(url: str, theater_name: str, *, timeout: int = 20) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(2):
        try:
            response = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS)
            response.raise_for_status()
            return response.text
        except Exception as exc:
            last_error = exc
            if attempt == 0:
                continue
    print(f"  [ERROR] Source page fetch failed for {theater_name}: {last_error}")
    return ""


def future_cached_entries(theater: dict, ctx: Optional[Any]) -> list[dict]:
    if ctx is None:
        return []
    today_iso = date_iso(ny_reference_now(ctx))
    theater_name = str(theater.get("name") or "").strip()
    records = getattr(getattr(ctx, "state", None), "existing_movie_records", {}) or {}
    entries: list[dict] = []
    for movie in records.values():
        if not isinstance(movie, dict):
            continue
        title = str(movie.get("title") or "").strip()
        if not title:
            continue
        for venue in movie.get("theaters") or []:
            if str(venue.get("name") or "").strip() != theater_name:
                continue
            for slot in venue.get("schedule") or []:
                slot_date = str(slot.get("date") or "").strip()
                if not slot_date or slot_date < today_iso:
                    continue
                times = sort_time_labels([str(t).strip() for t in slot.get("times") or [] if str(t).strip()])
                if not times:
                    continue
                ticket_urls = {
                    str(time): str(url).strip()
                    for time, url in (slot.get("ticket_urls") or {}).items()
                    if str(time).strip() and str(url).strip()
                }
                entries.append(
                    {
                        "title": title,
                        "theater": theater_name,
                        "day": str(slot.get("day") or "").strip() or format_day_label(datetime.fromisoformat(slot_date)),
                        "date": slot_date,
                        "times": times,
                        "ticket_url": str(venue.get("ticket_url") or get_source_ticket_url(theater)).strip(),
                        "ticket_urls": ticket_urls,
                        "special_formats": list(venue.get("special_formats") or movie.get("special_formats") or []),
                        "time_attributes": copy.deepcopy(slot.get("time_attributes") or {}),
                    }
                )
    return entries


def use_cached_on_failure(theater: dict, ctx: Optional[Any], reason: str) -> list[dict]:
    cached = future_cached_entries(theater, ctx)
    if cached:
        print(f"  [WARN] {theater['name']} scrape unavailable; reusing {len(cached)} cached future schedule entries: {reason}")
        return cached
    print(f"  [WARN] {theater['name']} scrape unavailable and no cached future schedule exists: {reason}")
    return []


def warn_if_empty(theater: dict, entries: list[dict], *, active: bool = True) -> None:
    if active and not entries:
        print(f"  [WARN] {theater['name']} returned zero screenings; source markup may have changed.")
