import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from cinema_backend.common import (
    clean_title,
    extract_year_int,
    normalize_title,
    ny_now,
    runtime_minutes_from_value,
    split_trailing_title_year,
    title_explicitly_allows_short,
)


ROOT = Path(__file__).resolve().parent.parent
DATASET_PATH = ROOT / "public" / "data.json"
CURRENT_YEAR = ny_now().year
MAX_REASONABLE_FUTURE_YEAR = CURRENT_YEAR + 2
ENGLISH_WEEKDAY_PREFIXES = {"sun", "mon", "tue", "wed", "thu", "fri", "sat"}
ENGLISH_MONTH_PATTERN = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?"


def is_parseable_schedule_day(day: str, schedule_date: str = "") -> bool:
    if schedule_date and re.fullmatch(r"(?:18|19|20)\d{2}-\d{2}-\d{2}", schedule_date):
        return True
    normalized = str(day or "").strip()
    if not normalized:
        return False
    if normalized.lower() in {"today", "tomorrow", "yesterday"}:
        return True
    if normalized[:3].lower() in ENGLISH_WEEKDAY_PREFIXES:
        return True
    if re.search(rf"\b{ENGLISH_MONTH_PATTERN}\s+\d{{1,2}}\b", normalized, re.IGNORECASE):
        return True
    return bool(re.search(rf"\b\d{{1,2}}\s+{ENGLISH_MONTH_PATTERN}\b", normalized, re.IGNORECASE))


def is_parseable_showtime(value: str) -> bool:
    normalized = str(value or "").strip()
    if re.fullmatch(r"(?:0?[1-9]|1[0-2])(?::[0-5]\d)?\s*(?:AM|PM)", normalized, re.IGNORECASE):
        return True
    return bool(re.fullmatch(r"(?:[01]?\d|2[0-3]):[0-5]\d", normalized))


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
        "screening_variants": [],
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

    generated_at_raw = str(dataset.get("generated_at") or "").strip()
    if not generated_at_raw:
        errors.append("Dataset missing generated_at timestamp.")
    else:
        try:
            generated_at = datetime.fromisoformat(generated_at_raw.replace("Z", "+00:00"))
            compare_now = ny_now()
            if generated_at.tzinfo is None:
                compare_now = compare_now.replace(tzinfo=None)
            if generated_at < compare_now - timedelta(days=2):
                errors.append(f"Dataset is stale: generated_at={generated_at_raw}")
        except ValueError:
            errors.append(f"Dataset has invalid generated_at timestamp: {generated_at_raw}")

    seen_ids = Counter()
    seen_title_keys = Counter()
    future_showtime_found = False
    unparseable_days: dict[str, set[str]] = defaultdict(set)
    unparseable_times: dict[str, set[str]] = defaultdict(set)

    for movie in movies:
        title = str(movie.get("title") or "").strip()
        movie_id = str(movie.get("id") or "").strip()
        ratings = movie.get("ratings") or {}
        movie_theaters = movie.get("theaters") or []
        verdict = movie.get("verdict") or {}

        if not title:
            errors.append("Encountered movie without a title.")
            continue
        canonical_title = clean_title(title)
        if canonical_title and canonical_title != title:
            warning_counts["screening_variants"] += 1
            if len(warning_samples["screening_variants"]) < 10:
                warning_samples["screening_variants"].append(f"{title} → {canonical_title}")
        if not movie_id:
            errors.append(f"{title}: missing id.")
        else:
            seen_ids[movie_id] += 1
        base_title, title_year = split_trailing_title_year(title)
        year = title_year or extract_year_int(ratings.get("year"))
        seen_title_keys[f"{normalize_title(base_title)}|{year}" if year else normalize_title(base_title)] += 1

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
        core_metadata_count = sum(1 for key in ("genre", "runtime", "director", "plot") if ratings.get(key))
        has_core_metadata = core_metadata_count >= 2
        if has_imdb_identity and year is not None and year >= CURRENT_YEAR - 1 and not has_core_metadata:
            warnings.append(
                f"{title}: weak recent IMDb match ({ratings.get('imdbID')}, {year}) with only {core_metadata_count} core metadata field(s)."
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
                schedule_date = str(slot.get("date") or "").strip()
                times = slot.get("times") or []
                time_attributes = slot.get("time_attributes") or {}
                if not day:
                    errors.append(f"{title}: {theater_name} schedule entry missing day.")
                if schedule_date and not re.fullmatch(r"(?:18|19|20)\d{2}-\d{2}-\d{2}", schedule_date):
                    errors.append(f"{title}: {theater_name} {day or '[missing day]'} has invalid date ({schedule_date}).")
                if schedule_date and schedule_date >= ny_now().date().isoformat():
                    future_showtime_found = True
                if day and not is_parseable_schedule_day(day, schedule_date):
                    unparseable_days[theater_name].add(day)
                if not isinstance(times, list) or not times:
                    errors.append(f"{title}: {theater_name} {day or '[missing day]'} has no times.")
                elif isinstance(times, list):
                    for time in times:
                        if not is_parseable_showtime(str(time or "")):
                            unparseable_times[theater_name].add(str(time or "").strip() or "[empty]")
                if not isinstance(time_attributes, dict):
                    errors.append(f"{title}: {theater_name} {day or '[missing day]'} time_attributes is not an object.")
                else:
                    for time, attributes in time_attributes.items():
                        if time not in times:
                            errors.append(f"{title}: {theater_name} {day or '[missing day]'} has attributes for unknown time {time}.")
                        if not isinstance(attributes, list) or not all(isinstance(attribute, str) and attribute.strip() for attribute in attributes):
                            errors.append(f"{title}: {theater_name} {day or '[missing day]'} has invalid attributes for {time}.")

    for theater_name, days in sorted(unparseable_days.items()):
        samples = ", ".join(sorted(days)[:5])
        errors.append(f"{theater_name}: unparseable schedule day label(s): {samples}.")
    for theater_name, times in sorted(unparseable_times.items()):
        samples = ", ".join(sorted(times)[:5])
        errors.append(f"{theater_name}: unparseable showtime value(s): {samples}.")

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
        "screening_variants": "Uncanonicalized screening-variant titles",
    }
    for key, count in warning_counts.items():
        samples = warning_samples.get(key) or []
        sample_text = f" Sample: {', '.join(samples)}" if samples else ""
        warnings.append(f"{warning_labels.get(key, key)}: {count}.{sample_text}")

    if not future_showtime_found:
        errors.append("All showtimes in the dataset are in the past.")

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
