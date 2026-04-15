import requests


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
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
