import re
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo


THEATER_CONFIG = {
    "Metrograph": {
        "slug": "metrograph",
        "short_name": "Metrograph",
        "sort_name": "Metrograph",
        "source_type": "metrograph",
        "source_url": "https://t.metrograph.com/Browsing/Cinemas/Details/9999",
        "official_url": "https://metrograph.com",
        "aliases": ["metro"],
    },
    "IFC Center": {
        "slug": "ifc",
        "short_name": "IFC",
        "sort_name": "IFC",
        "source_type": "ifc",
        "source_url": "https://www.ifccenter.com/now-playing/",
        "serpapi_id": "ifc center new york",
        "official_url": "https://www.ifccenter.com",
        "aliases": ["ifc"],
    },
    "Angelika Film Center": {
        "slug": "angelika",
        "short_name": "Angelika",
        "sort_name": "Angelika",
        "source_type": "serpapi",
        "serpapi_id": "angelika film center new york",
        "official_url": "https://angelikafilmcenter.com/nyc",
        "aliases": ["angelika"],
    },
    "Film Forum": {
        "slug": "film-forum",
        "short_name": "Film Forum",
        "sort_name": "Film Forum",
        "source_type": "filmforum",
        "serpapi_id": "film forum new york",
        "official_url": "https://www.filmforum.org/now-playing/",
        "aliases": ["film", "film forum"],
    },
    "Village East by Angelika": {
        "slug": "village-east",
        "short_name": "Village East",
        "sort_name": "Village East",
        "source_type": "serpapi",
        "serpapi_id": "village east cinema new york",
        "official_url": "https://angelikafilmcenter.com/villageeast",
        "aliases": ["village", "village east"],
    },
    "Film at Lincoln Center": {
        "slug": "flc",
        "short_name": "FLC",
        "sort_name": "FLC",
        "source_type": "serpapi",
        "serpapi_id": "film at lincoln center new york",
        "official_url": "https://www.filmlinc.org/now-playing/",
        "aliases": ["flc", "film linc", "lincoln center", "film at lincoln center"],
    },
    "Alamo Drafthouse Lower Manhattan": {
        "slug": "alamo",
        "short_name": "Alamo",
        "sort_name": "Alamo",
        "source_type": "alamo",
        "market_slug": "nyc",
        "cinema_id": "2103",
        "serpapi_id": "alamo drafthouse lower manhattan new york",
        "official_url": "https://drafthouse.com/nyc",
        "aliases": ["alamo"],
    },
    "Alamo Drafthouse Downtown Brooklyn": {
        "slug": "alamo-brooklyn",
        "short_name": "Alamo Brooklyn",
        "sort_name": "Alamo Brooklyn",
        "source_type": "alamo",
        "market_slug": "nyc",
        "cinema_id": "2101",
        "serpapi_id": "alamo drafthouse downtown brooklyn new york",
        "official_url": "https://drafthouse.com/theater/downtown-brooklyn",
        "aliases": ["alamo brooklyn", "downtown brooklyn", "brooklyn alamo"],
    },
    "Alamo Drafthouse Staten Island": {
        "slug": "alamo-staten-island",
        "short_name": "Alamo Staten Island",
        "sort_name": "Alamo Staten Island",
        "source_type": "alamo",
        "market_slug": "nyc",
        "cinema_id": "2102",
        "serpapi_id": "alamo drafthouse staten island new york",
        "official_url": "https://drafthouse.com/theater/staten-island",
        "aliases": ["alamo staten island", "staten island", "staten island alamo"],
    },
    "Paris Theater": {
        "slug": "paris",
        "short_name": "Paris",
        "sort_name": "Paris",
        "source_type": "serpapi",
        "serpapi_id": "paris theater new york",
        "official_url": "https://www.paristheaternyc.com/",
        "aliases": ["paris"],
    },
    "Museum of Modern Art": {
        "slug": "moma",
        "short_name": "MoMA",
        "sort_name": "MoMA",
        "source_type": "moma",
        "serpapi_id": "museum of modern art new york film",
        "source_url": "https://www.moma.org/calendar/?happening_filter=Films&location=both",
        "official_url": "https://www.moma.org/calendar/?happening_filter=Films&location=both",
        "aliases": ["moma", "museum of modern art", "moma film"],
    },
    "AMC Landmark 8": {
        "slug": "amc-landmark-8",
        "short_name": "AMC Landmark 8",
        "sort_name": "AMC Landmark 8",
        "source_type": "amc",
        "serpapi_id": "amc landmark 8 stamford ct",
        "official_url": "https://www.amctheatres.com/movie-theatres/new-york-city/amc-landmark-8",
        "aliases": [],
    },
    "AMC Majestic 6": {
        "slug": "amc-majestic-6",
        "short_name": "AMC Majestic 6",
        "sort_name": "AMC Majestic 6",
        "source_type": "amc",
        "serpapi_id": "amc majestic 6 stamford ct",
        "official_url": "https://www.amctheatres.com/movie-theatres/new-york-city/amc-majestic-6",
        "aliases": [],
    },
}

SERPAPI_THEATERS = [
    {"name": name, **config}
    for name, config in THEATER_CONFIG.items()
    if config.get("source_type") == "serpapi"
]

STATIC_THEATERS = [
    {"name": name, **config}
    for name, config in THEATER_CONFIG.items()
    if config.get("source_type") != "amc"
]

AMC_ALLOWED_CITIES_BY_STATE = {
    "NY": {"NEW YORK", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"},
    "CT": {"STAMFORD"},
}

AMC_EXCLUDED_THEATRES = {
    "AMC BAY PLAZA CINEMA 13",
    "AMC MAGIC JOHNSON HARLEM 9",
    "AMC STATEN ISLAND 11",
}

FORMAT_TAGS = re.compile(
    r'\b(70mm|35mm|imax|4k|dcp|digital|in\s+70mm|in\s+35mm|presented\s+in\s+\w+)\b'
    r'|\s*[\(\[]?(70mm|35mm|imax|4k|dcp)[\)\]]?',
    re.IGNORECASE
)

SPECIAL_FORMAT_PATTERNS = {
    "IMAX": re.compile(r"\bimax\b", re.IGNORECASE),
    "70mm": re.compile(r"\b(?:in\s+)?70\s*mm\b", re.IGNORECASE),
    "35mm": re.compile(r"\b(?:in\s+)?35\s*mm\b", re.IGNORECASE),
}

NON_ALNUM = re.compile(r"[^a-z0-9]+")
NY_TZ = ZoneInfo("America/New_York")

SHORT_PROGRAM_HINTS = (
    "short",
    "shorts",
    "program",
    "anthology",
    "compilation",
    "collection",
    "selections",
)


def normalize_title(title: str) -> str:
    return NON_ALNUM.sub(" ", (title or "").lower()).strip()


def slugify(value: str) -> str:
    return NON_ALNUM.sub("-", (value or "").lower()).strip("-")


def clean_title(raw: str) -> str:
    cleaned = FORMAT_TAGS.sub("", raw).strip(" -–—·")
    article_match = re.match(r"^(.*),\s+(The|A|An)$", cleaned, re.IGNORECASE)
    if article_match:
        cleaned = f"{article_match.group(2)} {article_match.group(1)}"
    return re.sub(r"\s+", " ", cleaned).strip()


def split_trailing_title_year(title: str) -> tuple[str, Optional[int]]:
    cleaned = str(title or "").strip()
    match = re.search(r"\s*[\(\[]((?:18|19|20)\d{2})[\)\]]\s*$", cleaned)
    if not match:
        return cleaned, None
    base = cleaned[:match.start()].strip(" -–—·")
    return re.sub(r"\s+", " ", base).strip() or cleaned, int(match.group(1))


def ny_now() -> datetime:
    return datetime.now(NY_TZ)


def date_iso(dt: datetime) -> str:
    return dt.date().isoformat()


def title_identity_key(title: str, year: Optional[object] = None) -> str:
    base_title, title_year = split_trailing_title_year(title)
    parsed_year = extract_year_int(year) or title_year
    normalized = normalize_title(base_title)
    return f"{normalized}|{parsed_year}" if parsed_year else normalized


def extract_special_formats(*values: object) -> list[str]:
    found: list[str] = []
    haystacks = [str(value or "") for value in values if value]
    for label, pattern in SPECIAL_FORMAT_PATTERNS.items():
        if any(pattern.search(haystack) for haystack in haystacks):
            found.append(label)
    return found


def extract_year_int(value: Optional[str]) -> Optional[int]:
    match = re.search(r"\b(18|19|20)\d{2}\b", str(value or ""))
    return int(match.group(0)) if match else None


def cache_key_for_title_year(title: str, year: Optional[object] = None) -> str:
    base = normalize_title(title)
    parsed_year = extract_year_int(year)
    return f"{base}|{parsed_year}" if parsed_year else base


def make_movie_id(title: str, ratings: dict) -> str:
    imdb_id = str((ratings or {}).get("imdbID") or "").strip()
    if imdb_id:
        return imdb_id
    year = extract_year_int((ratings or {}).get("year"))
    base = slugify(title)
    return f"{base}-{year}" if year else base


def build_theater_meta(name: str, overrides: Optional[dict] = None) -> dict:
    base = dict(THEATER_CONFIG.get(name, {}))
    if overrides:
        base.update(overrides)
    official_url = str(base.get("official_url") or "https://www.amctheatres.com/").strip()
    short_name = str(base.get("short_name") or name).strip()
    sort_name = str(base.get("sort_name") or short_name).strip()
    return {
        "name": name,
        "slug": str(base.get("slug") or slugify(name)).strip(),
        "short_name": short_name,
        "sort_name": sort_name,
        "source_type": str(base.get("source_type") or "serpapi").strip(),
        "official_url": official_url,
        "aliases": [a for a in base.get("aliases", []) if a],
    }


def get_source_ticket_url(theater: dict, fallback_url: Optional[str] = None) -> str:
    return str(
        theater.get("ticket_url")
        or theater.get("official_url")
        or fallback_url
        or ""
    ).strip()


def format_day_label(dt: datetime) -> str:
    return dt.strftime("%a %b %d").replace(" 0", " ")


def format_time_label(dt: datetime) -> str:
    return dt.strftime("%I:%M%p").lstrip("0").lower()


def sort_time_labels(times: list[str]) -> list[str]:
    def parse_time(value: str) -> tuple[int, int]:
        m = re.match(r"(\d{1,2}):(\d{2})\s*(am|pm)", (value or "").strip().lower())
        if not m:
            return (99, 99)
        hour = int(m.group(1))
        minute = int(m.group(2))
        meridiem = m.group(3)
        if meridiem == "pm" and hour != 12:
            hour += 12
        if meridiem == "am" and hour == 12:
            hour = 0
        return (hour, minute)

    return sorted(times, key=parse_time)


def runtime_minutes_from_value(value: Optional[object]) -> Optional[int]:
    match = re.search(r"\b(\d{1,3})\s*min\b", str(value or ""), re.IGNORECASE)
    return int(match.group(1)) if match else None


def title_explicitly_allows_short(query_title: str) -> bool:
    normalized = normalize_title(query_title)
    return any(hint in normalized for hint in SHORT_PROGRAM_HINTS)
