"""
backfill_ratings.py

Fetches Rotten Tomatoes (and other OMDB ratings) for movies in data.json
that currently have no RT score. Runs against data.json in-place.

Only touches movies where ratings.rt is null/empty. Safe to re-run — already-
rated movies are skipped.

Usage:
  cd scripts
  OMDB_KEY=<key> python backfill_ratings.py [--dry-run] [--limit N]

Env:
  OMDB_KEY  - OMDb API key (required for OMDB lookups; RT scrape works without it)

Flags:
  --dry-run   Print what would change without writing
  --limit N   Cap how many movies to process (useful for testing)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests

# Add scripts/ to path so we can import cinema_backend
sys.path.insert(0, str(Path(__file__).parent))

from cinema_backend.common import extract_year_int, normalize_title
from cinema_backend.http import DEFAULT_HEADERS


# ── RT slug / URL helpers ────────────────────────────────────────────────────

_CURRENT_YEAR = 2026


def rt_slug(title: str) -> str:
    return normalize_title(title).replace(" ", "_")


_SUFFIX_PATTERNS = re.compile(
    r"\s*[\(\[]OV[\)\]]"                          # [OV]
    r"|\s+3D$"
    r"|\s+HDR by Barco.*$"
    r"|\s+(Movie\s+)?Party$"
    r"|\s+with\s+(Live|Livestream)\s+Q&A$"
    r"|\s+(Fan\s+)?(Screening|Event)$"
    r"|\s+Sing[\-\s]Along$"
    r"|\s+Open\s+Captioning$"
    r"|\s+Early\s+Access.*$"
    r"|\s+\d{1,2}(st|nd|rd|th)\s+Anniversary.*$"
    r"|\s*[\(\[]\d{4}[\)\]]\s*$",                 # trailing year
    re.IGNORECASE,
)

_PREFIX_PATTERNS = re.compile(
    r"^[^:]{1,40}:\s+",          # "Theater Name: " or "Event: "
    re.IGNORECASE,
)

_CONJUNCTION_PATTERNS = re.compile(
    r"\s+preceded by\s+.+$"
    r"|\s+with\s+.+$"
    r"|\s*\+\s*.+$"
    r"|\s*&\s*.+$",
    re.IGNORECASE,
)


def title_lookup_aliases(title: str) -> list[str]:
    """Return title variants to try when building RT URLs."""
    seen: list[str] = [title]

    def add(t: str) -> str:
        t = t.strip(" -–—·")
        if t and t not in seen:
            seen.append(t)
        return t

    # 1. Strip common suffixes (HDR by Barco, Movie Party, 3D, etc.)
    suffix_stripped = _SUFFIX_PATTERNS.sub("", title).strip()
    add(suffix_stripped)

    # 2. Strip conjunction tails (preceded by X, with X, + X, & X)
    conj_stripped = _CONJUNCTION_PATTERNS.sub("", suffix_stripped or title).strip()
    add(conj_stripped)

    # 3. Strip venue/event prefix ("Alamo Crafthouse: X" → "X")
    for base in list(seen):
        prefix_stripped = _PREFIX_PATTERNS.sub("", base).strip()
        add(prefix_stripped)

    # 4. Move leading article to end for each variant
    for base in list(seen):
        m = re.match(r"^(The|A|An)\s+(.+)$", base, re.IGNORECASE)
        if m:
            add(f"{m.group(2)}, {m.group(1)}")

    return seen


def extract_page_title(html: str) -> tuple[str, Optional[int]]:
    for pat in (r"<title>([^<]{3,120})</title>", r'"og:title"\s+content="([^"]{3,120})"'):
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            raw = m.group(1).strip()
            raw = re.sub(
                r"\s*(?:\||-|[–—])\s*(?:Rotten Tomatoes|Letterboxd|The Movie Database.*|TMDB).*$",
                "", raw, flags=re.IGNORECASE,
            ).strip()
            year = extract_year_int(raw)
            raw = re.sub(r"\s*[\(\[]?(?:18|19|20)\d{2}[\)\]]?\s*$", "", raw).strip()
            if raw:
                return raw, year
    return "", None


def title_match_score(query: str, result: str, query_year: Optional[int] = None, result_year: Optional[int] = None) -> float:
    q = set(normalize_title(query).split())
    r = set(normalize_title(result or "").split())
    if not q or not r:
        return 0.0
    overlap = len(q & r)
    score = (overlap / max(1, len(q))) * 0.55 + (overlap / max(1, len(r))) * 0.45
    if normalize_title(query) == normalize_title(result or ""):
        score += 0.45
    extra = r - q - {"a", "an", "the"}
    score -= min(0.35, 0.08 * len(extra))
    if query_year and result_year and abs(query_year - result_year) == 0:
        score += 0.15
    return score


def title_result_is_compatible(query: str, result: str, query_year: Optional[int] = None, result_year: Optional[int] = None, minimum: float = 0.72) -> bool:
    return title_match_score(query, result, query_year, result_year) >= minimum


# ── RT scraper ───────────────────────────────────────────────────────────────

RT_PATTERNS = [
    r'tomatometerscore="(\d{1,3})"',
    r'"tomatometerScoreAll"\s*:\s*\{"score"\s*:\s*(\d{1,3})',
    r'"criticsScore"\s*:\s*(\d{1,3})',
    r'"criticsScore"\s*:\s*\{[^{}]{0,240}"score"\s*:\s*"(\d{1,3})"',
    r'"scorePercent"\s*:\s*"(\d{1,3})%"',
    r'"Tomatometer","ratingCount":\d+,"ratingValue":"(\d{1,3})"',
]

CONSENSUS_PATTERNS = [
    r'"consensus"\s*:\s*"([^"]{10,600})"',
    r'data-qa="critics-consensus"[^>]*>([^<]{10,600})<',
]


def fetch_rt(title: str, query_year: Optional[int] = None) -> tuple[Optional[str], Optional[str]]:
    """Return (score_str, consensus_str) or (None, None) if not found."""
    candidates: list[str] = []
    for alias in title_lookup_aliases(title):
        slug = rt_slug(alias)
        candidates += [
            f"https://www.rottentomatoes.com/m/{slug}",
            f"https://www.rottentomatoes.com/m/{slug}_{_CURRENT_YEAR}",
            f"https://www.rottentomatoes.com/m/{slug}_{_CURRENT_YEAR + 1}",
            f"https://www.rottentomatoes.com/m/{slug}_{_CURRENT_YEAR - 1}",
        ]

    for url in candidates:
        try:
            page = requests.get(url, timeout=12, headers=DEFAULT_HEADERS).text
        except Exception:
            continue
        page_title, page_year = extract_page_title(page)
        if page_title and not title_result_is_compatible(title, page_title, query_year, page_year):
            continue
        score = None
        for pat in RT_PATTERNS:
            m = re.search(pat, page)
            if m:
                pct = int(m.group(1))
                if 0 <= pct <= 100:
                    score = f"{pct}%"
                    break
        if score is None:
            continue
        consensus = None
        for pat in CONSENSUS_PATTERNS:
            m = re.search(pat, page)
            if m:
                text = m.group(1).strip()
                if len(text) >= 10:
                    consensus = text
                    break
        return score, consensus

    return None, None


# ── OMDB lookup ──────────────────────────────────────────────────────────────

def fetch_omdb(imdb_id: str, omdb_key: str) -> Optional[dict]:
    if not imdb_id or not omdb_key:
        return None
    try:
        r = requests.get(
            "https://www.omdbapi.com/",
            params={"apikey": omdb_key, "i": imdb_id, "tomatoes": "true"},
            timeout=10,
            headers=DEFAULT_HEADERS,
        )
        data = r.json()
        return data if data.get("Response") != "False" else None
    except Exception:
        return None


def parse_omdb(data: dict) -> dict:
    rt = next((r["Value"] for r in data.get("Ratings", []) if r["Source"] == "Rotten Tomatoes"), None)
    imdb_rating = data.get("imdbRating")
    lb = None
    try:
        n = float(imdb_rating) if imdb_rating not in (None, "N/A") else None
        if n is not None:
            lb = f"{n / 2:.1f}"
    except Exception:
        pass
    return {
        "rt": rt,
        "imdb": imdb_rating,
        "metacritic": data.get("Metascore"),
        "letterboxd": lb,
        "plot": data.get("Plot"),
        "poster": data.get("Poster"),
        "genre": data.get("Genre"),
        "runtime": data.get("Runtime"),
        "year": data.get("Year"),
        "director": data.get("Director"),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def is_missing_rt(movie: dict) -> bool:
    r = movie.get("ratings") or {}
    rt = str(r.get("rt") or "").strip()
    return not rt or rt.upper() == "N/A"


def clean_val(v) -> str:
    return "" if str(v or "").strip().upper() in ("", "N/A") else str(v).strip()


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill RT scores for unrated movies")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    parser.add_argument("--limit", type=int, default=0, help="Max movies to process")
    parser.add_argument("--data", default="public/data.json", help="Path to data.json")
    args = parser.parse_args()

    import os
    omdb_key = os.environ.get("OMDB_KEY", "")

    data_path = Path(args.data)
    data = json.loads(data_path.read_text(encoding="utf-8"))
    movies = data.get("movies", [])

    targets = [m for m in movies if is_missing_rt(m)]
    print(f"Total movies: {len(movies)}")
    print(f"Missing RT:   {len(targets)}")

    if args.limit:
        targets = targets[: args.limit]
        print(f"Capped to:    {args.limit}")

    if not targets:
        print("Nothing to do.")
        return

    updated = 0
    for i, movie in enumerate(targets, 1):
        title = movie.get("title", "")
        ratings = movie.setdefault("ratings", {})
        imdb_id = clean_val(ratings.get("imdbID"))
        year_raw = clean_val(ratings.get("year"))
        query_year = extract_year_int(year_raw)

        print(f"\n[{i}/{len(targets)}] {title} ({query_year or '?'})")

        rt_score: Optional[str] = None
        consensus: Optional[str] = None

        # 1. Try OMDB first if we have an IMDB ID and key
        if imdb_id and omdb_key:
            omdb = fetch_omdb(imdb_id, omdb_key)
            if omdb:
                parsed = parse_omdb(omdb)
                rt_score = clean_val(parsed.get("rt"))
                # Backfill any other missing fields from OMDB
                for field in ("imdb", "metacritic", "letterboxd", "plot", "poster", "genre", "runtime", "year", "director"):
                    if not clean_val(ratings.get(field)) and clean_val(parsed.get(field)):
                        if not args.dry_run:
                            ratings[field] = parsed[field]
                        print(f"  + {field}: {parsed[field]}")

        # 2. Fall back to RT direct scrape
        if not rt_score:
            rt_score, consensus = fetch_rt(title, query_year)
            time.sleep(0.8)  # be polite

        if rt_score:
            print(f"  RT: {rt_score}")
            if consensus:
                print(f"  Consensus: {consensus[:80]}{'…' if len(consensus) > 80 else ''}")
            if not args.dry_run:
                ratings["rt"] = rt_score
                if consensus and not clean_val(ratings.get("consensus")):
                    ratings["consensus"] = consensus
                # Clear the pending placeholder so generate_verdicts will pick it up
                if (movie.get("verdict") or {}).get("reason") == "Showtimes review pending.":
                    movie.pop("verdict", None)
            updated += 1
        else:
            print(f"  — not found")

    if not args.dry_run:
        data_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nDone. Updated {updated}/{len(targets)} movies.{' (dry run)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
