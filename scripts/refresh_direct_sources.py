"""
Refresh public/data.json without SerpAPI.

Live sources:
  - direct venue scrapers
  - AMC API when AMC_VENDOR_KEY is set

Cached carry-forward:
  - SerpAPI-backed non-AMC theaters only

This is useful when direct providers need a live refresh but SerpAPI quota should
not be touched.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import scrape
from cinema_backend.common import STATIC_THEATERS, THEATER_CONFIG, build_theater_meta
from cinema_backend.runtime import build_scrape_context, save_json_dict


SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DATA_PATH = (SCRIPT_DIR / "../public/data.json").resolve()
RATING_OVERRIDES_PATH = SCRIPT_DIR / "rating_overrides.json"
CINEMASCORE_OVERRIDES_PATH = SCRIPT_DIR / "cinemascore_overrides.json"
PRESTIGE_OVERRIDES_PATH = SCRIPT_DIR / "prestige_overrides.json"
RATING_CACHE_PATH = SCRIPT_DIR / "rating_cache.json"

DIRECT_SOURCE_TYPES = {
    "metrograph",
    "ifc",
    "filmforum",
    "alamo",
    "paris",
    "flc",
    "anthology",
    "nitehawk",
    "bam",
}
SERPAPI_CACHE_SOURCE_TYPES = {"serpapi", "regal"}


def disable_rating_network_fallbacks() -> None:
    scrape.fetch_rt_fallback = lambda *args, **kwargs: None
    scrape.fetch_letterboxd_fallback = lambda *args, **kwargs: None
    scrape.ensure_poster_fallback = lambda _title, parsed, **kwargs: parsed


def collect_direct_entries(ctx: scrape.ScrapeContext) -> tuple[list[scrape.CollectedEntry], dict[str, dict], list[scrape.ScrapeIssue]]:
    theater_meta = {name: build_theater_meta(name) for name in THEATER_CONFIG.keys()}
    collected: list[scrape.CollectedEntry] = []
    issues: list[scrape.ScrapeIssue] = []
    summary: list[tuple[str, str, int, int]] = []

    live_theaters = [
        theater
        for theater in STATIC_THEATERS
        if theater.get("source_type") in DIRECT_SOURCE_TYPES
        or theater.get("source_type") in SERPAPI_CACHE_SOURCE_TYPES
    ]
    live_theaters.extend(scrape.fetch_amc_theatres(ctx))

    for theater in live_theaters:
        name = str(theater["name"])
        source_type = str(theater.get("source_type") or theater.get("source") or "")
        is_cached_serpapi = source_type in SERPAPI_CACHE_SOURCE_TYPES
        mode = "cached" if is_cached_serpapi else "live"
        print(f"Fetching {name} ({source_type or 'amc'})...")
        try:
            if is_cached_serpapi:
                entries = scrape.existing_showtime_entries(theater, ctx)
            else:
                entries = scrape.fetch_theater_showtimes(theater, ctx)
                if not entries:
                    cached = scrape.existing_showtime_entries(theater, ctx)
                    if cached:
                        print(f"  [WARN] zero live entries; preserving {len(cached)} cached future entries.")
                        entries = cached
                        mode = "cached-after-empty-live"
        except Exception as exc:
            cached = scrape.existing_showtime_entries(theater, ctx)
            if cached:
                print(f"  [WARN] failed ({exc}); preserving {len(cached)} cached future entries.")
                entries = cached
                mode = "cached-after-error"
            else:
                print(f"  [ERROR] failed with no cache: {exc}")
                issues.append(scrape.ScrapeIssue("fetch_showtimes", source_type or "unknown", name, str(exc)))
                entries = []
                mode = "failed"

        summary.append((name, mode, len(entries), sum(len(entry.get("times") or []) for entry in entries)))
        print(f"  {mode}: {len(entries)} entries")
        for entry in entries:
            collected.append(scrape.CollectedEntry(theater=theater, entry=entry))

    print("\nDirect refresh collection summary")
    for name, mode, entry_count, showtime_count in summary:
        print(f"{mode:23s} {entry_count:4d} entries {showtime_count:4d} showtimes  {name}")
    print(f"TOTAL collected entries={len(collected)} showtimes={sum(row[3] for row in summary)}")

    return collected, theater_meta, issues


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh direct/API showtimes without using SerpAPI.")
    parser.add_argument(
        "--allow-missing-amc-key",
        action="store_true",
        help="Permit AMC to fall back to cached future showtimes when AMC_VENDOR_KEY is absent.",
    )
    parser.add_argument(
        "--skip-rating-network-fallbacks",
        action="store_true",
        help="Avoid RT/Letterboxd/poster fallback crawling when OMDB_KEY is absent.",
    )
    args = parser.parse_args()

    ctx = build_scrape_context(
        script_dir=SCRIPT_DIR,
        output_data_path=OUTPUT_DATA_PATH,
        rating_overrides_path=RATING_OVERRIDES_PATH,
        cinemascore_overrides_path=CINEMASCORE_OVERRIDES_PATH,
        prestige_overrides_path=PRESTIGE_OVERRIDES_PATH,
        rating_cache_path=RATING_CACHE_PATH,
    )
    if not ctx.config.amc_vendor_key and not args.allow_missing_amc_key:
        raise RuntimeError(
            "AMC_VENDOR_KEY is required for this direct refresh so AMC uses the API, not cached/SerpAPI fallback. "
            "Use --allow-missing-amc-key only for local dry recovery."
        )
    if not ctx.config.omdb_key and args.skip_rating_network_fallbacks:
        disable_rating_network_fallbacks()

    collected_entries, theater_meta, issues = collect_direct_entries(ctx)
    all_movies, theater_schedule, theater_formats = scrape.resolve_movie_records(ctx, collected_entries, theater_meta)
    movies_list = scrape.attach_schedules_to_movies(all_movies, theater_schedule, theater_formats, theater_meta)
    dataset = scrape.finalize_dataset(ctx, movies_list, theater_schedule, theater_meta, issues)

    save_json_dict(ctx.rating_cache_path, ctx.state.rating_cache, sort_keys=True)
    with OUTPUT_DATA_PATH.open("w", encoding="utf-8") as handle:
        json.dump(dataset, handle, indent=2, ensure_ascii=False)

    print(f"\nDone. {len(dataset['movies'])} unique films written to public/data.json")
    print(f"Theaters in dataset: {len(dataset.get('theaters') or [])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
