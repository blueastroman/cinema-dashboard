import requests


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ShowtimesNYC/1.0; +https://showtimes.nyc)",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_source_html(source_url: str, theater_name: str) -> str:
    url = str(source_url or "").strip()
    if not url:
        return ""

    try:
        response = requests.get(url, timeout=20, headers=DEFAULT_HEADERS)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"  [ERROR] Source fetch failed for {theater_name}: {e}")
        return ""
