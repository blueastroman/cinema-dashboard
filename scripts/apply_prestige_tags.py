from __future__ import annotations

import argparse
from pathlib import Path

from cinema_backend.prestige import build_movie_prestige_tags
from cinema_backend.runtime import load_json_dict, save_json_dict


def apply_prestige_tags(data: dict, overrides: dict) -> int:
    movies = data.get("movies") or []
    updated = 0
    for movie in movies:
        if not isinstance(movie, dict):
            continue
        tags = build_movie_prestige_tags(movie, overrides)
        if tags:
            if movie.get("prestige_tags") != tags:
                movie["prestige_tags"] = tags
                updated += 1
        elif "prestige_tags" in movie:
            movie.pop("prestige_tags", None)
            updated += 1
    return updated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply prestige tags to the scraped movie dataset.")
    parser.add_argument("--data-file", type=Path, default=Path("public/data.json"), help="Path to the dataset JSON file.")
    parser.add_argument(
        "--overrides-file",
        type=Path,
        default=Path("scripts/prestige_overrides.json"),
        help="Path to the manual prestige overrides JSON file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = load_json_dict(args.data_file)
    if not data:
        raise RuntimeError(f"Could not read dataset from {args.data_file}")
    overrides = load_json_dict(args.overrides_file)
    updated = apply_prestige_tags(data, overrides)
    save_json_dict(args.data_file, data)
    print(f"Updated prestige tags for {updated} movies.")


if __name__ == "__main__":
    main()
