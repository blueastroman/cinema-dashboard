import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from cinema_backend.common import (
    extract_year_int,
    normalize_title,
    runtime_minutes_from_value,
    title_explicitly_allows_short,
)


ROOT = Path(__file__).resolve().parent.parent
DATASET_PATH = ROOT / "public" / "data.json"
CURRENT_YEAR = datetime.now().year
MAX_REASONABLE_FUTURE_YEAR = CURRENT_YEAR + 2


def load_dataset(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_dataset(dataset: dict) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    warning_samples: dict[str, list[str]] = {
        "missing_genre": [],
        "missing_director": [],
        "missing_theaters": [],
        "missing_ticket_url": [],
        "duplicate_titles": [],
    }
    warning_counts: Counter = Counter()

    required_top_level = {"generated_at", "week_of", "theaters", "theater_meta", "movies"}
    missing_top_level = sorted(required_top_level - set(dataset.keys()))
    if missing_top_level:
        errors.append(f"Missing top-level keys: {', '.join(missing_top_level)}")
        return errors, warnings

    movies = dataset.get("movies")
    theater_meta = dataset.get("theater_meta")
    theaters = dataset.get("theaters")

    if not isinstance(movies, list) or not movies:
        errors.append("Dataset has no movies.")
        return errors, warnings
    if not isinstance(theater_meta, dict) or not theater_meta:
        errors.append("Dataset has no theater_meta.")
    if not isinstance(theaters, list) or not theaters:
        errors.append("Dataset has no theaters list.")

    seen_ids = Counter()
    seen_title_keys = Counter()

    for movie in movies:
        title = str(movie.get("title") or "").strip()
        movie_id = str(movie.get("id") or "").strip()
        ratings = movie.get("ratings") or {}
        movie_theaters = movie.get("theaters") or []
        verdict = movie.get("verdict") or {}

        if not title:
            errors.append("Encountered movie without a title.")
            continue
        if not movie_id:
            errors.append(f"{title}: missing id.")
        else:
            seen_ids[movie_id] += 1
        seen_title_keys[title.lower()] += 1

        if not isinstance(ratings, dict):
            errors.append(f"{title}: ratings is not an object.")
            continue
        if not isinstance(verdict, dict):
            errors.append(f"{title}: verdict is not an object.")
        if not isinstance(movie_theaters, list):
            errors.append(f"{title}: theaters is not a list.")
            continue
        if not movie_theaters:
            warning_counts["missing_theaters"] += 1
            if len(warning_samples["missing_theaters"]) < 10:
                warning_samples["missing_theaters"].append(title)

        runtime_minutes = runtime_minutes_from_value(ratings.get("runtime"))
        if (
            runtime_minutes is not None
            and runtime_minutes <= 45
            and not title_explicitly_allows_short(title)
        ):
            errors.append(f"{title}: suspicious short runtime ({runtime_minutes} min).")

        year = extract_year_int(ratings.get("year"))
        if year is not None and year > MAX_REASONABLE_FUTURE_YEAR:
            errors.append(f"{title}: suspicious future year ({year}).")
        if year is not None and year < 1888:
            errors.append(f"{title}: impossible year ({year}).")
        has_imdb_identity = bool(str(ratings.get("imdbID") or "").strip())
        has_core_metadata = any(ratings.get(key) for key in ("genre", "runtime", "director", "plot"))
        if has_imdb_identity and year is not None and year >= CURRENT_YEAR - 1 and not has_core_metadata:
            errors.append(
                f"{title}: suspicious recent IMDb match ({ratings.get('imdbID')}, {year}) with no core metadata."
            )

        if not ratings.get("genre"):
            warning_counts["missing_genre"] += 1
            if len(warning_samples["missing_genre"]) < 10:
                warning_samples["missing_genre"].append(title)
        if not ratings.get("director"):
            warning_counts["missing_director"] += 1
            if len(warning_samples["missing_director"]) < 10:
                warning_samples["missing_director"].append(title)

        for theater in movie_theaters:
            theater_name = str(theater.get("name") or "").strip()
            schedule = theater.get("schedule") or []
            ticket_url = str(theater.get("ticket_url") or "").strip()

            if not theater_name:
                errors.append(f"{title}: theater entry missing name.")
                continue
            if theater_name not in theater_meta:
                errors.append(f"{title}: theater_meta missing {theater_name}.")
            if not isinstance(schedule, list) or not schedule:
                errors.append(f"{title}: {theater_name} has empty schedule.")
                continue
            if not ticket_url:
                warning_counts["missing_ticket_url"] += 1
                if len(warning_samples["missing_ticket_url"]) < 10:
                    warning_samples["missing_ticket_url"].append(f"{title} @ {theater_name}")

            for slot in schedule:
                day = str(slot.get("day") or "").strip()
                times = slot.get("times") or []
                if not day:
                    errors.append(f"{title}: {theater_name} schedule entry missing day.")
                if not isinstance(times, list) or not times:
                    errors.append(f"{title}: {theater_name} {day or '[missing day]'} has no times.")

    duplicate_ids = [movie_id for movie_id, count in seen_ids.items() if count > 1]
    if duplicate_ids:
        errors.append(f"Duplicate movie ids found: {', '.join(sorted(duplicate_ids)[:10])}")

    duplicate_titles = [title for title, count in seen_title_keys.items() if count > 1]
    if duplicate_titles:
        warning_counts["duplicate_titles"] += len(duplicate_titles)
        warning_samples["duplicate_titles"] = sorted(duplicate_titles)[:10]

    warning_labels = {
        "missing_genre": "Movies missing genre",
        "missing_director": "Movies missing director",
        "missing_theaters": "Movies with no theaters attached",
        "missing_ticket_url": "Theater entries missing ticket_url",
        "duplicate_titles": "Duplicate normalized titles",
    }
    for key, count in warning_counts.items():
        samples = warning_samples.get(key) or []
        sample_text = f" Sample: {', '.join(samples)}" if samples else ""
        warnings.append(f"{warning_labels.get(key, key)}: {count}.{sample_text}")

    return errors, warnings


def main() -> int:
    dataset = load_dataset(DATASET_PATH)
    errors, warnings = validate_dataset(dataset)

    if warnings:
        print("Dataset validation warnings:")
        for warning in warnings:
            print(f"  - {warning}")

    if errors:
        print("Dataset validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print(f"Dataset validation passed for {len(dataset.get('movies', []))} movies.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
