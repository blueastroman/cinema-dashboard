from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .common import exact_title_identity_key


def load_json_dict(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}


def save_json_dict(path: Path, data: dict[str, Any], *, sort_keys: bool = False) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False, sort_keys=sort_keys)


def _load_existing_movie_metadata(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    movies = data.get("movies", [])
    existing: dict[str, dict[str, Any]] = {}
    for movie in movies:
        title = str(movie.get("title") or "").strip()
        ratings = movie.get("ratings") or {}
        if title and isinstance(ratings, dict):
            existing[exact_title_identity_key(title)] = ratings
    return existing


def _load_existing_movie_records(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    movies = data.get("movies", [])
    existing: dict[str, dict[str, Any]] = {}
    for movie in movies:
        title = str(movie.get("title") or "").strip()
        if title and isinstance(movie, dict):
            existing[exact_title_identity_key(title)] = dict(movie)
    return existing


@dataclass
class ScrapeConfig:
    serpapi_key: str
    omdb_key: str
    amc_vendor_key: str
    amc_api_base: str
    amc_theatre_ids: list[str]
    amc_theatre_page_size: int = 100
    amc_force_serpapi_fallback: bool = False
    allow_mock_data: bool = False
    serpapi_monthly_budget: int = 200
    serpapi_refresh_weekdays: frozenset[int] = frozenset({2, 5})


@dataclass
class ScrapeState:
    rating_overrides: dict[str, Any] = field(default_factory=dict)
    cinemascore_overrides: dict[str, Any] = field(default_factory=dict)
    prestige_overrides: dict[str, Any] = field(default_factory=dict)
    rating_cache: dict[str, Any] = field(default_factory=dict)
    existing_movie_metadata: dict[str, dict[str, Any]] = field(default_factory=dict)
    existing_movie_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    collected_issues: list[dict[str, Any]] = field(default_factory=list)
    serpapi_quota_checked: bool = False
    serpapi_month_usage: int = 0
    serpapi_effective_limit: int = 0


@dataclass
class ScrapeContext:
    config: ScrapeConfig
    state: ScrapeState
    now: datetime
    output_data_path: Path
    rating_cache_path: Path


@dataclass
class ReviewConfig:
    api_key: str
    model: str
    data_file: Path
    cache_file: Path
    force_refresh: bool
    batch_size: int
    allow_incomplete: bool


@dataclass
class ReviewContext:
    config: ReviewConfig
    now: datetime


def build_scrape_context(
    *,
    script_dir: Path,
    output_data_path: Path,
    rating_overrides_path: Path,
    cinemascore_overrides_path: Path,
    prestige_overrides_path: Path,
    rating_cache_path: Path,
    now: Optional[datetime] = None,
) -> ScrapeContext:
    weekday_numbers = {
        "mon": 0,
        "tue": 1,
        "wed": 2,
        "thu": 3,
        "fri": 4,
        "sat": 5,
        "sun": 6,
    }
    raw_refresh_days = os.environ.get("SERPAPI_REFRESH_WEEKDAYS", "wed,sat")
    refresh_weekdays = frozenset(
        weekday_numbers[token.strip().lower()[:3]]
        for token in raw_refresh_days.split(",")
        if token.strip().lower()[:3] in weekday_numbers
    )
    try:
        monthly_budget = int(os.environ.get("SERPAPI_MONTHLY_BUDGET", "200"))
    except ValueError as exc:
        raise ValueError("SERPAPI_MONTHLY_BUDGET must be a positive integer.") from exc
    if monthly_budget <= 0:
        raise ValueError("SERPAPI_MONTHLY_BUDGET must be a positive integer.")
    if not refresh_weekdays:
        raise ValueError("SERPAPI_REFRESH_WEEKDAYS must contain at least one weekday.")

    config = ScrapeConfig(
        serpapi_key=os.environ.get("SERPAPI_KEY", ""),
        omdb_key=os.environ.get("OMDB_KEY", ""),
        amc_vendor_key=os.environ.get("AMC_VENDOR_KEY", ""),
        amc_api_base=os.environ.get("AMC_API_BASE", "https://api.amctheatres.com").rstrip("/"),
        amc_theatre_ids=[token.strip() for token in os.environ.get("AMC_THEATRE_IDS", "").split(",") if token.strip()],
        amc_force_serpapi_fallback=os.environ.get("AMC_FORCE_SERPAPI_FALLBACK", "").strip().lower() in {"1", "true", "yes"},
        allow_mock_data=os.environ.get("ALLOW_MOCK_DATA", "").strip().lower() in {"1", "true", "yes"},
        serpapi_monthly_budget=monthly_budget,
        serpapi_refresh_weekdays=refresh_weekdays,
    )
    state = ScrapeState(
        rating_overrides=load_json_dict(rating_overrides_path),
        cinemascore_overrides=load_json_dict(cinemascore_overrides_path),
        prestige_overrides=load_json_dict(prestige_overrides_path),
        rating_cache=load_json_dict(rating_cache_path),
        existing_movie_metadata=_load_existing_movie_metadata(output_data_path),
        existing_movie_records=_load_existing_movie_records(output_data_path),
    )
    return ScrapeContext(
        config=config,
        state=state,
        now=now or datetime.now(),
        output_data_path=output_data_path,
        rating_cache_path=rating_cache_path,
    )


def parse_positive_int_env(name: str, default: Optional[int] = None) -> Optional[int]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"", "0", "never", "none", "false", "no"}:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer, 0, blank, or 'never'.") from exc
    if parsed <= 0:
        return None
    return parsed


def build_review_context(*, data_file: Path, cache_file: Path, now: Optional[datetime] = None) -> ReviewContext:
    return ReviewContext(
        config=ReviewConfig(
            api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
            model=os.environ.get("ANTHROPIC_REVIEW_MODEL", "claude-haiku-4-5-20251001"),
            data_file=data_file,
            cache_file=cache_file,
            force_refresh=os.environ.get("VERDICT_FORCE_REFRESH", "").strip().lower() in {"1", "true", "yes"},
            batch_size=parse_positive_int_env("VERDICT_BATCH_SIZE", 12) or 12,
            allow_incomplete=os.environ.get("VERDICT_ALLOW_INCOMPLETE", "").strip().lower() in {"1", "true", "yes"},
        ),
        now=now or datetime.now(),
    )
