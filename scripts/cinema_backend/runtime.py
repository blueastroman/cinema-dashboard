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
    allow_mock_data: bool = False


@dataclass
class ScrapeState:
    rating_overrides: dict[str, Any] = field(default_factory=dict)
    cinemascore_overrides: dict[str, Any] = field(default_factory=dict)
    prestige_overrides: dict[str, Any] = field(default_factory=dict)
    rating_cache: dict[str, Any] = field(default_factory=dict)
    existing_movie_metadata: dict[str, dict[str, Any]] = field(default_factory=dict)
    existing_movie_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    collected_issues: list[dict[str, Any]] = field(default_factory=list)


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
    config = ScrapeConfig(
        serpapi_key=os.environ.get("SERPAPI_KEY", ""),
        omdb_key=os.environ.get("OMDB_KEY", ""),
        amc_vendor_key=os.environ.get("AMC_VENDOR_KEY", ""),
        amc_api_base=os.environ.get("AMC_API_BASE", "https://api.amctheatres.com").rstrip("/"),
        amc_theatre_ids=[token.strip() for token in os.environ.get("AMC_THEATRE_IDS", "").split(",") if token.strip()],
        allow_mock_data=os.environ.get("ALLOW_MOCK_DATA", "").strip().lower() in {"1", "true", "yes"},
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
            model=os.environ.get("ANTHROPIC_REVIEW_MODEL", "claude-sonnet-4-20250514"),
            data_file=data_file,
            cache_file=cache_file,
            force_refresh=os.environ.get("VERDICT_FORCE_REFRESH", "").strip().lower() in {"1", "true", "yes"},
            batch_size=parse_positive_int_env("VERDICT_BATCH_SIZE", 12) or 12,
        ),
        now=now or datetime.now(),
    )
