from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from .common import exact_title_identity_key, normalize_title, title_identity_key

PRESTIGE_TAG_PRIORITY = [
    "PALME D'OR",
    "BEST PICTURE WINNER",
    "BEST ANIMATED FEATURE WINNER",
    "BEST INTERNATIONAL FEATURE WINNER",
    "CRITERION COLLECTION",
]

PRESTIGE_PATTERNS: list[tuple[str, tuple[re.Pattern[str], ...]]] = [
    ("PALME D'OR", (re.compile(r"palme d['’]or", re.IGNORECASE),)),
    (
        "BEST PICTURE WINNER",
        (
            re.compile(r"best picture winner", re.IGNORECASE),
            re.compile(r"academy award(?:s)? for best picture", re.IGNORECASE),
            re.compile(r"won best picture", re.IGNORECASE),
            re.compile(r"oscar for best picture", re.IGNORECASE),
        ),
    ),
    (
        "BEST ANIMATED FEATURE WINNER",
        (
            re.compile(r"best animated feature(?: film)? winner", re.IGNORECASE),
            re.compile(r"academy award(?:s)? for best animated feature(?: film)?", re.IGNORECASE),
            re.compile(r"won best animated feature(?: film)?", re.IGNORECASE),
        ),
    ),
    (
        "BEST INTERNATIONAL FEATURE WINNER",
        (
            re.compile(r"best international feature(?: film)? winner", re.IGNORECASE),
            re.compile(r"academy award(?:s)? for best international feature(?: film)?", re.IGNORECASE),
            re.compile(r"won best international feature(?: film)?", re.IGNORECASE),
            re.compile(r"foreign language film", re.IGNORECASE),
        ),
    ),
    ("CRITERION COLLECTION", (re.compile(r"criterion collection", re.IGNORECASE),)),
]

SCRIPT_DIR = Path(__file__).resolve().parents[1]
CRITERION_COLLECTION_KEYS_PATH = SCRIPT_DIR / "criterion_collection_keys.json"
BEST_PICTURE_WINNER_KEYS_PATH = SCRIPT_DIR / "oscar_best_picture_winners.json"
BEST_ANIMATED_FEATURE_WINNER_KEYS_PATH = SCRIPT_DIR / "oscar_animated_feature_winners.json"
BEST_INTERNATIONAL_FEATURE_WINNER_KEYS_PATH = SCRIPT_DIR / "oscar_international_feature_winners.json"
PALME_DOR_WINNER_KEYS_PATH = SCRIPT_DIR / "palme_d_or_winners.json"


def _iter_text_values(value: Any) -> Iterable[str]:
    if value is None:
        return
    if isinstance(value, str):
        text = value.strip()
        if text:
            yield text
        return
    if isinstance(value, dict):
        for nested in value.values():
            yield from _iter_text_values(nested)
        return
    if isinstance(value, (list, tuple, set)):
        for nested in value:
            yield from _iter_text_values(nested)
        return


def normalize_prestige_tags(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    allowed = set(PRESTIGE_TAG_PRIORITY)
    normalized = []
    seen = set()
    for raw in values or []:
        tag = str(raw or "").strip().upper()
        if tag not in allowed or tag in seen:
            continue
        normalized.append(tag)
        seen.add(tag)
    normalized.sort(key=PRESTIGE_TAG_PRIORITY.index)
    return normalized


def extract_prestige_tags(*values: Any) -> list[str]:
    parts: list[str] = []
    for value in values:
        for text in _iter_text_values(value):
            parts.append(text)
    blob = " | ".join(parts)
    if not blob:
        return []
    lowered = blob.lower()
    matched = []
    for label, patterns in PRESTIGE_PATTERNS:
        if any(pattern.search(lowered) for pattern in patterns):
            matched.append(label)
    return normalize_prestige_tags(matched)


def merge_prestige_tags(*tag_sets: Any) -> list[str]:
    combined: list[str] = []
    seen = set()
    for tag_set in tag_sets:
        for tag in normalize_prestige_tags(tag_set):
            if tag in seen:
                continue
            combined.append(tag)
            seen.add(tag)
    combined.sort(key=PRESTIGE_TAG_PRIORITY.index)
    return combined


@lru_cache(maxsize=1)
def load_criterion_collection_keys() -> frozenset[str]:
    try:
        raw = json.loads(CRITERION_COLLECTION_KEYS_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return frozenset()
    if not isinstance(raw, list):
        return frozenset()
    keys = {str(value).strip() for value in raw if str(value or "").strip()}
    return frozenset(keys)


@lru_cache(maxsize=1)
def load_static_award_keys(path: Path) -> frozenset[str]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return frozenset()
    if not isinstance(raw, list):
        return frozenset()
    keys: set[str] = set()
    for value in raw:
        if not isinstance(value, dict):
            continue
        title = str(value.get("title") or "").strip()
        year = value.get("year")
        if not title:
            continue
        keys.add(title_identity_key(title, year))
    return frozenset(keys)


def load_best_picture_winner_keys() -> frozenset[str]:
    return load_static_award_keys(BEST_PICTURE_WINNER_KEYS_PATH)


def load_best_animated_feature_winner_keys() -> frozenset[str]:
    return load_static_award_keys(BEST_ANIMATED_FEATURE_WINNER_KEYS_PATH)


def load_best_international_feature_winner_keys() -> frozenset[str]:
    return load_static_award_keys(BEST_INTERNATIONAL_FEATURE_WINNER_KEYS_PATH)


def load_palme_dor_winner_keys() -> frozenset[str]:
    return load_static_award_keys(PALME_DOR_WINNER_KEYS_PATH)


def build_movie_prestige_tags(movie: dict[str, Any], overrides: Any = None) -> list[str]:
    title = str(movie.get("title") or "").strip()
    year = movie.get("year") or (movie.get("ratings") or {}).get("year")
    ratings = movie.get("ratings") or {}
    theaters = movie.get("theaters") or []
    theater_names = [theater.get("name") for theater in theaters if isinstance(theater, dict)]
    special_formats = movie.get("special_formats") or []

    override_tags: Any = []
    if isinstance(overrides, dict) and title:
        override = overrides.get(exact_title_identity_key(title), {})
        if not override:
            override = overrides.get(normalize_title(title), {})
        if isinstance(override, dict):
            override_tags = override.get("prestige_tags") or override.get("tags") or []
        elif isinstance(override, (list, tuple, set, str)):
            override_tags = override

    return merge_prestige_tags(
        movie.get("prestige_tags") or [],
        ["PALME D'OR"] if title_identity_key(title, year) in load_palme_dor_winner_keys() else [],
        ["BEST PICTURE WINNER"] if title_identity_key(title, year) in load_best_picture_winner_keys() else [],
        ["BEST ANIMATED FEATURE WINNER"] if title_identity_key(title, year) in load_best_animated_feature_winner_keys() else [],
        ["BEST INTERNATIONAL FEATURE WINNER"] if title_identity_key(title, year) in load_best_international_feature_winner_keys() else [],
        ["CRITERION COLLECTION"] if title_identity_key(title, year) in load_criterion_collection_keys() else [],
        extract_prestige_tags(
            title,
            ratings.get("plot"),
            special_formats,
            theater_names,
        ),
        normalize_prestige_tags(override_tags),
    )
