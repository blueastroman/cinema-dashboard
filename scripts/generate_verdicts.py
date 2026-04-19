"""
generate_verdicts.py

Run after scrape in GitHub Actions to generate editorial one-liners for each film.
Uses a cache to avoid re-running API calls for films already processed.

Usage:
  python generate_verdicts.py

Env:
  ANTHROPIC_API_KEY - your Anthropic API key

Files:
  data.json          - input/output, the main showtimes data
  verdicts_cache.json - persistent cache of generated verdicts (commit this to repo)
"""

import json
import os
import re
import urllib.request
from datetime import datetime

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATA_FILE = "public/data.json"
CACHE_FILE = "scripts/verdicts_cache.json"
FORCE_REFRESH = os.environ.get("VERDICT_FORCE_REFRESH", "").strip().lower() in {
    "1",
    "true",
    "yes",
}
NEVER_REFRESH_VALUES = {"", "0", "never", "none", "false", "no"}


def parse_positive_int_env(name, default=None):
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in NEVER_REFRESH_VALUES:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer, 0, blank, or 'never'.") from exc
    if parsed <= 0:
        return None
    return parsed


BATCH_SIZE = parse_positive_int_env("VERDICT_BATCH_SIZE", 30) or 30

SYSTEM_PROMPT = """You are the editorial voice of a curated NYC cinema showtimes board. Your job: for each film, give a verdict (WATCH, DEPENDS, or SKIP) and a short recommendation blurb.

VERDICT RULES:
- WATCH = genuinely worth going to a theater for. Strong filmmaker, compelling premise, or a must-see experience.
- DEPENDS = decent but not essential. Niche appeal or "good not great."
- SKIP = not worth the trip. Lazy franchise, weak premise, or actively bad.
- Classic repertory films screening at art house theaters (older acclaimed films) should almost always be WATCH. If it survived 30+ years and is screening at a place like Metrograph or Film Forum, it earned that.

VOICE RULES:
- Talk like you're texting a friend who asked "should I see this?"
- Exactly two sentences.
- Sentence 1 must start with "A " or "An " and state the premise/setup in concrete terms.
- Sentence 2 must start with "Best for " or "For " and state the audience or mood fit.
- Be specific to THIS film. Explain what it is about and what kind of movie it is.
- Have a real opinion. Don't hedge.
- No adjective stacking. No "masterful," "riveting," "poignant," "tour de force," "compelling," "gripping."
- No film critic language. No "exploration of," "meditation on," "unflinching look at."
- No em dashes.
- Funny is good when it fits. Blunt is always good.
- For SKIP, be honest about why. Don't be mean for sport but don't sugarcoat it.
- If you don't know the film well, describe the setup and the likely audience instead of summary-score language.
- Do not mention Rotten Tomatoes, Metacritic, Letterboxd, critics, reviews, scores, reception, metrics, or percentages.
- Avoid vague review-summary language like "critically acclaimed" or "reviews are great." Say what the movie is and who the natural audience is.
- Do not use hedges like "could be," "might be," "sounds like," "seems," "depends," "wild card," or "easy yes."

RESPOND IN THIS EXACT JSON FORMAT (array of objects):
[
  {"title": "Film Title", "verdict": "WATCH", "reason": "Your recommendation blurb here."},
  ...
]

No markdown. No backticks. Just the JSON array."""


def load_json(path, default=None):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default if default is not None else {}


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def is_usable_cache_entry(entry):
    if not isinstance(entry, dict):
        return False
    verdict = str(entry.get("verdict") or "").strip().upper()
    reason = str(entry.get("reason") or "").strip()
    if verdict not in {"WATCH", "DEPENDS", "SKIP"} or not reason:
        return False
    return True


def existing_verdict_entry(movie, now=None):
    verdict = movie.get("verdict") or {}
    if not isinstance(verdict, dict):
        return None
    reason = str(verdict.get("reason") or "").strip()
    raw_verdict = str(verdict.get("verdict") or "").strip().upper()
    if raw_verdict not in {"WATCH", "DEPENDS", "SKIP"} or not reason:
        return None
    if now is None:
        now = datetime.now()
    entry = {
        "verdict": raw_verdict,
        "reason": reason,
        "generated_at": now.isoformat(),
    }
    return entry


def needs_verdict(movie, cache, now=None):
    """Determine if a film needs a new API call."""
    movie_id = movie.get("id")
    if not movie_id:
        return True

    if FORCE_REFRESH:
        return True

    entry = cache.get(movie_id)
    if not is_usable_cache_entry(entry):
        return True

    return False


def build_film_block(movie):
    """Build the text description of a film for the API prompt."""
    r = movie.get("ratings", {})
    lines = [f"Title: {movie['title']}"]

    if r.get("year"):
        lines.append(f"Year: {r['year']}")
    if r.get("director"):
        lines.append(f"Director: {r['director']}")
    if r.get("genre"):
        lines.append(f"Genre: {r['genre']}")
    if r.get("runtime"):
        lines.append(f"Runtime: {r['runtime']}")
    if r.get("plot"):
        lines.append(f"Plot: {r['plot']}")

    return "\n".join(lines)


FORBIDDEN_REASON_PATTERNS = [
    r"\brt\b",
    r"rotten tomatoes",
    r"\bmetacritic\b",
    r"\bletterboxd\b",
    r"\bcritics?\b",
    r"\bcritical\b",
    r"\breviews?\b",
    r"\bscore[s]?\b",
    r"\breception\b",
    r"\bmetrics?\b",
    r"\baudience\b",
    r"\bpraise[d]?\b",
    r"\bacclaim[ed]?\b",
    r"\bmixed\b",
    r"\bpoor\b",
    r"\bstrong\b",
    r"\bweak\b",
    r"\bperfect\b",
    r"\bpercent(ages?)?\b",
    r"\b\d{1,3}%\b",
    r"\bcould be\b",
    r"\bmight be\b",
    r"\bsounds like\b",
    r"\bseems\b",
    r"\bdepends\b",
    r"\bwild card\b",
    r"\beasy yes\b",
    r"\bfor fans\b",
]


def validate_reason(reason):
    text = str(reason or "").strip()
    if not text:
        return False, "empty reason"
    words = text.split()
    if len(words) < 6:
        return False, "too short"
    sentence_count = sum(1 for part in text.replace("!", ".").replace("?", ".").split(".") if part.strip())
    if sentence_count != 2:
        return False, "must be exactly two sentences"
    lower = text.lower()
    for pattern in FORBIDDEN_REASON_PATTERNS:
        if re.search(pattern, lower, re.IGNORECASE):
            return False, f"forbidden language matched: {pattern}"
    if not lower.startswith(("a ", "an ")):
        return False, "first sentence must start with A or An"
    if not re.search(r"\.\s*(best for |for )", text, re.IGNORECASE):
        return False, "second sentence must start with Best for or For"
    if lower.startswith(("critics ", "reviews ", "score ", "scores ", "rt ", "100% rt", "high rt", "low rt", "mixed reception", "poor critical", "moderate reception", "critical reception")):
        return False, "starts like a score summary"
    return True, ""


def validate_verdict_payload(verdicts, expected_titles):
    if not isinstance(verdicts, list):
        return False, "Claude did not return a JSON array"
    if len(verdicts) != len(expected_titles):
        return False, f"Expected {len(expected_titles)} results, got {len(verdicts)}"

    seen_titles = set()
    expected_set = set(expected_titles)
    for idx, item in enumerate(verdicts):
        if not isinstance(item, dict):
            return False, f"Item {idx + 1} is not an object"
        title = str(item.get("title") or "").strip()
        verdict = str(item.get("verdict") or "").strip().upper()
        reason = str(item.get("reason") or "").strip()
        if title not in expected_set:
            return False, f"Unexpected title: {title or '(blank)'}"
        if title in seen_titles:
            return False, f"Duplicate title: {title}"
        seen_titles.add(title)
        if verdict not in {"WATCH", "DEPENDS", "SKIP"}:
            return False, f"Invalid verdict for {title}: {verdict or '(blank)'}"
        ok, why = validate_reason(reason)
        if not ok:
            return False, f"Invalid reason for {title}: {why}"

    missing = expected_set - seen_titles
    if missing:
        return False, f"Missing titles: {', '.join(sorted(missing)[:5])}"
    return True, ""


def call_claude(films_block):
    """Send a batch of films to the Claude API and return parsed verdicts."""
    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4000,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": (
                    "For each film, return a verdict and a recommendation blurb.\n"
                    'Use this exact shape: "A [premise]. Best for [audience or mood]."\n'
                    "The blurb must be exactly 2 sentences: sentence 1 is the premise, sentence 2 is the audience or mood fit.\n"
                    "Do not mention Rotten Tomatoes, Metacritic, Letterboxd, critics, reviews, scores, reception, metrics, percentages, or hedges like could/might/seems/sounds like.\n"
                    "Return only the JSON array.\n\n"
                    f"{films_block}"
                ),
            }
        ],
    }

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    text = result["content"][0]["text"]
    clean = text.replace("```json", "").replace("```", "").strip()
    return json.loads(clean)


def call_claude_strict(films_block, titles):
    verdicts = call_claude(films_block)
    ok, message = validate_verdict_payload(verdicts, titles)
    if ok:
        return verdicts

    retry_payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4000,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": (
                    "Your previous response failed validation and is being rejected.\n"
                    f"Problem: {message}\n\n"
                    "Rewrite only the same titles. Return a JSON array with the exact titles below.\n"
                    'Each reason must follow this exact shape: "A [premise]. Best for [audience or mood]."\n'
                    "Each reason must be exactly 2 sentences, premise first and audience/mood second.\n"
                    "Do not mention Rotten Tomatoes, Metacritic, Letterboxd, critics, reviews, scores, reception, metrics, percentages, or hedges like could/might/seems/sounds like.\n"
                    "Return only the JSON array.\n\n"
                    f"Titles: {', '.join(titles)}\n\n"
                    f"Films:\n{films_block}"
                ),
            }
        ],
    }

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(retry_payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    text = result["content"][0]["text"]
    clean = text.replace("```json", "").replace("```", "").strip()
    verdicts = json.loads(clean)
    ok, message = validate_verdict_payload(verdicts, titles)
    if not ok:
        raise RuntimeError(f"Claude output rejected after retry: {message}")
    return verdicts


def main():
    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set")
        return

    # Load data
    data = load_json(DATA_FILE)
    cache = load_json(CACHE_FILE, default={})
    movies = data.get("movies", [])
    now = datetime.now()

    if FORCE_REFRESH:
        for movie in movies:
            movie.pop("verdict", None)

    print(f"Total films: {len(movies)}")
    print(f"Cached verdicts: {len(cache)}")

    # Split into needs-work and already-done
    to_process = []
    cached_count = 0

    for movie in movies:
        movie_id = movie.get("id")
        if FORCE_REFRESH and movie_id:
            cache.pop(movie_id, None)
        if movie_id and not FORCE_REFRESH and not is_usable_cache_entry(cache.get(movie_id)):
            seeded = existing_verdict_entry(movie, now)
            if seeded and is_usable_cache_entry(seeded):
                cache[movie_id] = seeded

        if needs_verdict(movie, cache, now):
            to_process.append(movie)
        else:
            cached_count += 1

    print(f"Using cache: {cached_count}")
    print(f"Need API calls: {len(to_process)}")

    # Process in batches
    if to_process:
        batches = [
            to_process[i : i + BATCH_SIZE]
            for i in range(0, len(to_process), BATCH_SIZE)
        ]

        for batch_idx, batch in enumerate(batches):
            print(
                f"\nBatch {batch_idx + 1}/{len(batches)}: {len(batch)} films"
            )

            films_block = "\n---\n".join(build_film_block(m) for m in batch)
            batch_titles = [m["title"] for m in batch]

            try:
                verdicts = call_claude_strict(films_block, batch_titles)

                # Map results by title for matching
                verdict_map = {v["title"]: v for v in verdicts}

                for movie in batch:
                    title = movie["title"]
                    movie_id = movie.get("id")

                    if title in verdict_map:
                        v = verdict_map[title]
                        cache_entry = {
                            "verdict": v["verdict"],
                            "reason": v["reason"],
                            "generated_at": now.isoformat(),
                        }
                        # Store in cache by IMDB ID
                        if movie_id:
                            cache[movie_id] = cache_entry

                        print(f"  {v['verdict']:7s} {title}")
                    else:
                        print(f"  MISS    {title} (no match in API response)")

            except Exception as e:
                print(f"  ERROR: {e}")
                print("  Skipping batch, will retry next run")
                continue

    # Write verdicts from cache back into data.json
    updated = 0
    for movie in movies:
        movie_id = movie.get("id")
        if movie_id and is_usable_cache_entry(cache.get(movie_id)):
            entry = cache[movie_id]
            movie["verdict"] = {
                "verdict": entry["verdict"],
                "reason": entry["reason"],
            }
            updated += 1

    print(f"\nUpdated {updated}/{len(movies)} films in data.json")

    # Save
    save_json(DATA_FILE, data)
    save_json(CACHE_FILE, cache)

    print(f"Cache size: {len(cache)} entries")
    print("Done.")


if __name__ == "__main__":
    main()
