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

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path

from cinema_backend.review_client import AnthropicReviewClient
from cinema_backend.runtime import ReviewContext, build_review_context, parse_positive_int_env

SYSTEM_PROMPT = """You are the editorial voice of Showtimes NYC, a curated cinema board for people deciding whether to buy a ticket tonight.

For each film, give:
VERDICT: WATCH, DEPENDS, or SKIP
BLURB: exactly two sentences

The job is not to summarize the movie. The job is to make a ticket-buying call.

VERDICT RULES

WATCH — Worth the trip. Strong film, rare screening, essential theatrical experience, major repertory title, or a filmmaker doing notable work.

DEPENDS — Has real merit but is not essential tonight. Use when the film is uneven, niche, overlong, minor, better at home, or mainly worthwhile because of venue, format, cast, director, or mood.

SKIP — Not worth the subway ride. Weak execution, thin premise, disposable franchise work, poor theatrical value, or a better version exists elsewhere.

Score anchoring:
- Critics Score is the starting bias, not the final verdict.
- 85%+ defaults toward WATCH unless the film lacks theatrical urgency.
- 40%- defaults toward SKIP unless there is a concrete counterargument.
- Repertory titles at Film Forum, Metrograph, BAM, Anthology, MoMA, Lincoln Center, or Museum of the Moving Image default toward WATCH only when reputation, rarity, restoration, print, venue, filmmaker, or historical value makes the screening worthwhile.

TWO-SENTENCE RULE

Sentence 1 — Make the call. This must be a direct recommendation — not a description of the film.
BAD: "The painter revisits an abandoned project." (plot summary — banned)
BAD: "Questions about art and truth are explored." (description — banned)
GOOD: "Worth the trip if you can commit to three hours of Rivette."
GOOD: "One of the great films about creative obsession — see it on a big screen."
GOOD: "Skip it; the theatrical cut is available streaming and the runtime punishes patience."

Sentence 2 — Give one specific reason that applies to this film alone. Use a detail from the direction, performance, image, sound, editing, structure, venue, format, restoration, print, historical place, or theatrical experience.

VOICE

You have seen everything and you have real opinions.
Write like the smartest person in the theater, not like a press release.
Short words beat long words. Specific beats vague. Funny is welcome when it fits.
Every sentence is a judgment. Nothing is merely descriptive.
Be sharp, but do not be smug.
Be concise, but do not be cryptic.

BANNED

- Plot summary
- Audience-targeting phrases: "For fans of," "For viewers who," "Best for," "Anyone who likes"
- Score language: "critics agree," "well-reviewed," "acclaimed," "strong reviews," "critics are united"
- Empty praise or criticism: masterful, riveting, poignant, haunting, compelling, gripping, stunning, breathtaking, tour de force, thought-provoking, unflinching, visceral, nuanced
- Critic-speak: "exploration of," "meditation on," "examination of," "portrait of," "study of"
- Hedges: might, could, seems, arguably, perhaps, appears to be, sounds like
- Generic genre observations that apply to any film in the category
- Em dashes
- Stacked adjectives

GROUNDING RULE

Never invent a film-specific reason. If there is not enough information, base the second sentence on a known grounded fact: director, cast, venue, format, restoration, print, runtime, franchise context, release context, or reputation.

RESPOND IN THIS EXACT JSON FORMAT (array of objects):
[
  {"title": "Film Title", "verdict": "WATCH", "reason": "Your recommendation blurb here."},
  ...
]

No markdown. No backticks. Just the JSON array."""


def load_json(path, default=None):
    file_path = Path(path)
    if file_path.exists():
        with file_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return default if default is not None else {}


def save_json(path, data):
    with Path(path).open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def has_placeholder_premise(reason):
    return "premise unavailable" in str(reason or "").lower()


def only_review_placeholder_premises() -> bool:
    return os.environ.get("VERDICT_ONLY_PREMISE_UNAVAILABLE", "").strip().lower() in {"1", "true", "yes"}


def get_movie_consensus_text(movie):
    ratings = movie.get("ratings") or {}
    return next(
        (
            str(value).strip()
            for value in (
                ratings.get("consensus"),
                ratings.get("rtConsensus"),
                ratings.get("criticConsensus"),
                ratings.get("rottenTomatoesConsensus"),
                movie.get("consensus"),
            )
            if str(value or "").strip()
        ),
        "",
    )


def get_movie_premise_text(movie):
    ratings = movie.get("ratings") or {}
    return next(
        (
            str(value).strip()
            for value in (ratings.get("plot"), movie.get("plot"))
            if str(value or "").strip()
        ),
        "",
    )


def is_usable_cache_entry(entry):
    if not isinstance(entry, dict):
        return False
    verdict = str(entry.get("verdict") or "").strip().upper()
    reason = str(entry.get("reason") or "").strip()
    if verdict not in {"WATCH", "DEPENDS", "SKIP"} or not reason:
        return False
    if has_placeholder_premise(reason):
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
    if has_placeholder_premise(reason):
        return None
    if now is None:
        now = datetime.now()
    entry = {
        "verdict": raw_verdict,
        "reason": reason,
        "generated_at": now.isoformat(),
    }
    return entry


def needs_verdict(movie, cache, force_refresh=False):
    """Determine if a film needs a new API call."""
    movie_id = movie.get("id")
    if not movie_id:
        return True

    if force_refresh:
        return True

    entry = cache.get(movie_id)
    if not is_usable_cache_entry(entry):
        return True

    return False


def should_review_movie(movie, cache, force_refresh=False, only_placeholder=False):
    movie_id = movie.get("id")
    if not movie_id:
        return False if only_placeholder else True

    # Never review movies with no critics score and no plot — Claude will hallucinate.
    if not force_refresh and not has_reviewable_content(movie):
        return False

    if only_placeholder:
        movie_verdict = movie.get("verdict") or {}
        movie_reason = movie_verdict.get("reason")
        cache_reason = (cache.get(movie_id) or {}).get("reason")
        if has_placeholder_premise(movie_reason) or has_placeholder_premise(cache_reason):
            return True
        return not get_movie_consensus_text(movie) and not get_movie_premise_text(movie)

    return needs_verdict(movie, cache, force_refresh)


def has_reviewable_content(movie):
    """Return True only if we have enough grounded information to write a real review.

    Without an RT score or a plot, Claude has nothing to anchor to and invents
    speculative text that sounds authoritative but is fabricated.
    """
    r = movie.get("ratings", {})
    has_score = bool(r.get("rt") or r.get("metacritic"))
    has_plot = bool(str(r.get("plot") or movie.get("plot") or "").strip())
    return has_score or has_plot


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
    if r.get("rt"):
        lines.append(f"Critics Score: {r['rt']}")
    if r.get("metacritic"):
        lines.append(f"Metacritic: {r['metacritic']}")
    if r.get("plot"):
        lines.append(f"Plot: {r['plot']}")

    return "\n".join(lines)


FORBIDDEN_REASON_PATTERNS = [
    # Score references
    r"\brt\b",
    r"rotten tomatoes",
    r"\bmetacritic\b",
    r"\bletterboxd\b",
    r"\bscore[s]?\b",
    r"\breception\b",
    r"\bmetrics?\b",
    r"\bpercent(ages?)?\b",
    r"\b\d{1,3}%\b",
    r"critics are united",
    r"well.reviewed",
    # Hedges (banned in system prompt — enforced here)
    r"\bcould\b",
    r"\bmight\b",
    r"\bperhaps\b",
    r"\barguably\b",
    r"\bseems to\b",
    r"\bappears to\b",
    r"\bsounds like\b",
    r"\brisks becoming\b",
    r"\bmay be\b",
    # Critic-speak (banned in system prompt — enforced here)
    r"\bexploration of\b",
    r"\bmeditation on\b",
    r"\bexamination of\b",
    r"\bportrait of\b",
    r"\bstudy of\b",
    r"\bjourney (of|through|into)\b",
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
    if lower.startswith(("critics ", "reviews ", "score ", "scores ", "rt ", "100% rt", "high rt", "low rt", "mixed reception", "poor critical", "moderate reception", "critical reception")):
        return False, "starts like a score summary"
    # Sentence 1 must be a ticket call, not a plot description.
    # Reject if it opens with a character/story narration pattern.
    first_sentence = text.replace("!", ".").replace("?", ".").split(".")[0].strip()
    first_lower = first_sentence.lower()
    if re.match(r"^(a |an |the )", first_lower) and not re.search(
        r"\b(film|movie|director|performance|screening|restoration|print|version|cut|cinematography|score|soundtrack|cast|actor|actress|runtime|sequel|remake|debut|return|career)\b",
        first_lower,
    ):
        return False, "sentence 1 reads like plot narration, not a ticket call"
    if re.search(r"\b(are explored|is explored|are examined|is examined|are depicted|is depicted|are questioned|follows a|tells the story|revolves around)\b", first_lower):
        return False, "sentence 1 is a plot description"
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


def review_prompt(films_block, message=None):
    if message:
        return f"{message}\n\n{films_block}"
    return (
        "For each film, return a verdict and a recommendation blurb.\n"
        'Use this exact shape: "[Direct ticket call]. [One specific reason why or why not]."\n'
        "The blurb must be exactly 2 sentences: sentence 1 is the direct ticket call, sentence 2 is one specific reason why or why not.\n"
        "Do not describe the plot. Assume the reader already knows the movie.\n"
        "Do not mention Rotten Tomatoes, Metacritic, Letterboxd, critics, reviews, scores, reception, metrics, percentages, or hedges like could/might/seems/sounds like.\n"
        "Return only the JSON array.\n\n"
        f"{films_block}"
    )


def call_claude_strict(client, films_block, titles):
    verdicts = client.send(system_prompt=SYSTEM_PROMPT, content=review_prompt(films_block))
    ok, message = validate_verdict_payload(verdicts, titles)
    if ok:
        return verdicts

    retry_message = (
        "Your previous response failed validation and is being rejected.\n"
        f"Problem: {message}\n\n"
        "Rewrite only the same titles. Return a JSON array with the exact titles below.\n"
        'Each reason must follow this exact shape: "[Direct ticket call]. [One specific reason why or why not]."\n'
        "Each reason must be exactly 2 sentences, direct ticket call first and specific reason second.\n"
        "Do not describe the plot. Assume the reader already knows the movie.\n"
        "Do not mention Rotten Tomatoes, Metacritic, Letterboxd, critics, reviews, scores, reception, metrics, percentages, or hedges like could/might/seems/sounds like.\n"
        "Return only the JSON array.\n\n"
        f"Titles: {', '.join(titles)}"
    )
    verdicts = client.send(system_prompt=SYSTEM_PROMPT, content=review_prompt(f"Films:\n{films_block}", retry_message))
    ok, message = validate_verdict_payload(verdicts, titles)
    if not ok:
        raise RuntimeError(f"Claude output rejected after retry: {message}")
    return verdicts


def main(context: ReviewContext | None = None):
    context = context or build_review_context(
        data_file=Path("public/data.json"),
        cache_file=Path("scripts/verdicts_cache.json"),
    )
    config = context.config
    data = load_json(config.data_file)
    cache = load_json(config.cache_file, default={})
    movies = data.get("movies", [])
    now = context.now
    only_placeholder = only_review_placeholder_premises()
    client = None
    if config.api_key:
        client = AnthropicReviewClient(api_key=config.api_key, model=config.model)

    if config.force_refresh:
        for movie in movies:
            movie.pop("verdict", None)

    print(f"Total films: {len(movies)}")
    print(f"Cached verdicts: {len(cache)}")

    # Split into needs-work and already-done
    to_process = []
    cached_count = 0

    for movie in movies:
        movie_id = movie.get("id")
        if config.force_refresh and movie_id:
            cache.pop(movie_id, None)
        if not only_placeholder and movie_id and not config.force_refresh and not is_usable_cache_entry(cache.get(movie_id)):
            seeded = existing_verdict_entry(movie, now)
            if seeded and is_usable_cache_entry(seeded):
                cache[movie_id] = seeded

        if should_review_movie(movie, cache, config.force_refresh, only_placeholder):
            to_process.append(movie)
        else:
            cached_count += 1

    print(f"Using cache: {cached_count}")
    print(f"Need API calls: {len(to_process)}")

    verdict_limit = parse_positive_int_env("VERDICT_LIMIT")
    if verdict_limit and len(to_process) > verdict_limit:
        print(f"VERDICT_LIMIT={verdict_limit}: capping to first {verdict_limit} film(s)")
        to_process = to_process[:verdict_limit]

    if to_process and client is None:
        print("ERROR: ANTHROPIC_API_KEY not set")
        print("Applying cached verdicts only; unresolved films will remain without reviews.")
        to_process = []

    # Process in batches
    if to_process:
        batches = [
            to_process[i : i + config.batch_size]
            for i in range(0, len(to_process), config.batch_size)
        ]

        for batch_idx, batch in enumerate(batches):
            print(
                f"\nBatch {batch_idx + 1}/{len(batches)}: {len(batch)} films"
            )

            films_block = "\n---\n".join(build_film_block(m) for m in batch)
            batch_titles = [m["title"] for m in batch]

            try:
                verdicts = call_claude_strict(client, films_block, batch_titles)

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
                        print(f"           {v['reason']}")
                    else:
                        print(f"  MISS    {title} (no match in API response)")

            except RuntimeError as e:
                # Validation failed after retry — try to salvage individual items
                print(f"  WARN: batch validation failed ({e}), attempting per-film fallback")
                for movie in batch:
                    title = movie["title"]
                    movie_id = movie.get("id")
                    try:
                        single_block = build_film_block(movie)
                        single_verdicts = call_claude_strict(client, single_block, [title])
                        v = single_verdicts[0]
                        if movie_id:
                            cache[movie_id] = {
                                "verdict": v["verdict"],
                                "reason": v["reason"],
                                "generated_at": now.isoformat(),
                            }
                        print(f"  {v['verdict']:7s} {title} (fallback)")
                    except Exception as inner_e:
                        print(f"  SKIP    {title} ({inner_e})")
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
    save_json(config.data_file, data)
    save_json(config.cache_file, cache)

    print(f"Cache size: {len(cache)} entries")
    print("Done.")


if __name__ == "__main__":
    main()
