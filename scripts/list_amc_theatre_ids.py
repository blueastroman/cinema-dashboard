"""
List AMC theater IDs for the configured NYC/Stamford market.

Usage:
  AMC_VENDOR_KEY=... python scripts/list_amc_theatre_ids.py

The final line is ready to paste into the AMC_THEATRE_IDS secret.
"""

import os
import sys

import requests

from cinema_backend.common import AMC_ALLOWED_CITIES_BY_STATE, AMC_EXCLUDED_THEATRES


AMC_API_BASE = os.environ.get("AMC_API_BASE", "https://api.amctheatres.com").rstrip("/")
AMC_VENDOR_KEY = os.environ.get("AMC_VENDOR_KEY", "")
PAGE_SIZE = 100


def amc_get(path, params):
    response = requests.get(
        f"{AMC_API_BASE}{path}",
        params=params,
        headers={
            "X-AMC-Vendor-Key": AMC_VENDOR_KEY,
            "Accept": "application/json",
        },
        timeout=30,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body = response.text[:800].replace("\n", " ")
        raise RuntimeError(f"{exc} for {response.url}: {body}") from exc
    return response.json()


def is_target_theatre(theatre):
    name = str(theatre.get("longName") or theatre.get("name") or "").strip()
    if name.upper() in AMC_EXCLUDED_THEATRES:
        return False

    location = theatre.get("location") or {}
    state = str(location.get("state") or "").strip().upper()
    city = str(location.get("city") or "").strip().upper()
    return city in AMC_ALLOWED_CITIES_BY_STATE.get(state, set())


def main():
    if not AMC_VENDOR_KEY:
        print("AMC_VENDOR_KEY is required.", file=sys.stderr)
        return 2

    matches = []
    page = 1
    while True:
        data = amc_get(
            "/v2/theatres",
            {
                "page-size": PAGE_SIZE,
                "page-number": page,
            },
        )
        theatres = ((data.get("_embedded", {}) or {}).get("theatres", []))
        for theatre in theatres:
            if theatre.get("isClosed") or not is_target_theatre(theatre):
                continue
            location = theatre.get("location") or {}
            matches.append(
                {
                    "id": str(theatre.get("id") or "").strip(),
                    "name": str(theatre.get("longName") or theatre.get("name") or "").strip(),
                    "city": str(location.get("city") or "").strip(),
                    "state": str(location.get("state") or "").strip(),
                }
            )

        page_size = int(data.get("pageSize") or 0)
        page_number = int(data.get("pageNumber") or page)
        count = int(data.get("count") or 0)
        if page_size <= 0 or page_number * page_size >= count:
            break
        page += 1

    matches = sorted({item["id"]: item for item in matches if item["id"]}.values(), key=lambda item: item["name"])
    for item in matches:
        print(f'{item["id"]:>8}  {item["name"]}  ({item["city"]}, {item["state"]})')

    print()
    print("AMC_THEATRE_IDS=" + ",".join(item["id"] for item in matches))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
