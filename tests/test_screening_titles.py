import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import scrape  # noqa: E402
from cinema_backend.common import clean_title, extract_screening_attributes  # noqa: E402
from cinema_backend.runtime import ScrapeConfig, ScrapeContext, ScrapeState  # noqa: E402


class ScreeningTitleTests(unittest.TestCase):
    def make_context(self):
        return ScrapeContext(
            config=ScrapeConfig(
                serpapi_key="test-key",
                omdb_key="test-key",
                amc_vendor_key="",
                amc_api_base="https://api.amctheatres.com",
                amc_theatre_ids=[],
                allow_mock_data=False,
            ),
            state=ScrapeState(),
            now=datetime(2026, 7, 17, 12, 0, 0),
            output_data_path=ROOT / "public" / "data.json",
            rating_cache_path=ROOT / "scripts" / "rating_cache.json",
        )

    def test_screening_suffixes_are_removed_from_movie_identity(self):
        self.assertEqual(clean_title("The Odyssey Premium"), "The Odyssey")
        self.assertEqual(clean_title("The Odyssey HDR by Barco"), "The Odyssey")
        self.assertEqual(clean_title("The Substance Movie Party"), "The Substance")
        self.assertEqual(clean_title("Tinsman Road with Live Q&A"), "Tinsman Road")
        self.assertEqual(clean_title("The Fast and the Furious: 25th Anniversary"), "The Fast and the Furious")

    def test_real_title_with_non_suffix_premium_is_preserved(self):
        self.assertEqual(clean_title("Premium Rush"), "Premium Rush")
        self.assertEqual(extract_screening_attributes("Premium Rush"), [])

    def test_screening_attributes_are_extracted(self):
        self.assertEqual(extract_screening_attributes("The Odyssey Premium"), ["Premium"])
        self.assertEqual(
            extract_screening_attributes("The Odyssey HDR by Barco", "IMAX"),
            ["IMAX", "HDR by Barco"],
        )
        self.assertEqual(
            extract_screening_attributes("The Fast and the Furious: 25th Anniversary Movie Party"),
            ["Movie Party", "Anniversary"],
        )

    def test_serpapi_variant_is_canonicalized_and_tagged_per_time(self):
        response = mock.Mock()
        response.json.return_value = {
            "showtimes": [
                {
                    "day": "Fri",
                    "date": "Jul 17",
                    "movies": [
                        {
                            "name": "The Odyssey Premium",
                            "showing": [{"time": ["10:00pm"], "link": "https://tickets.example/1"}],
                        }
                    ],
                }
            ]
        }

        with mock.patch.object(scrape.requests, "get", return_value=response):
            entries = scrape.fetch_showtimes(
                {"name": "AMC Lincoln Square 13", "serpapi_id": "amc lincoln square"},
                self.make_context(),
            )

        self.assertEqual(entries[0]["title"], "The Odyssey")
        self.assertEqual(entries[0]["time_attributes"], {"10:00pm": ["Premium"]})

    def test_serpapi_showtime_request_forces_english_new_york_locale(self):
        response = mock.Mock()
        response.json.return_value = {"showtimes": []}

        with mock.patch.object(scrape.requests, "get", return_value=response) as request:
            scrape.fetch_showtimes(
                {"name": "Village East by Angelika", "serpapi_id": "village east cinema new york"},
                self.make_context(),
            )

        params = request.call_args.kwargs["params"]
        self.assertEqual(params["location"], "New York, New York, United States")
        self.assertEqual(params["gl"], "us")
        self.assertEqual(params["hl"], "en")

    def test_canonical_variants_merge_before_rating_lookup(self):
        theater = {
            "name": "AMC Lincoln Square 13",
            "official_url": "https://www.amctheatres.com/movie-theatres/new-york-city/amc-lincoln-square-13",
        }
        entries = [
            scrape.CollectedEntry(
                theater=theater,
                entry={
                    "title": clean_title("The Odyssey"),
                    "theater": theater["name"],
                    "day": "Fri Jul 17",
                    "date": "2026-07-17",
                    "times": ["7:00pm"],
                    "ticket_urls": {},
                    "special_formats": [],
                    "time_attributes": {},
                },
            ),
            scrape.CollectedEntry(
                theater=theater,
                entry={
                    "title": clean_title("The Odyssey Premium"),
                    "theater": theater["name"],
                    "day": "Fri Jul 17",
                    "date": "2026-07-17",
                    "times": ["10:00pm"],
                    "ticket_urls": {},
                    "special_formats": [],
                    "time_attributes": {"10:00pm": ["Premium"]},
                },
            ),
        ]
        ratings = {
            "imdbID": "tt33764258",
            "rt": "96%",
            "year": "2026",
            "plot": "Odysseus returns home.",
        }

        with mock.patch.object(scrape, "fetch_ratings", return_value=ratings) as fetch_ratings:
            movies, schedules, formats = scrape.resolve_movie_records(
                self.make_context(),
                entries,
                {theater["name"]: {"official_url": theater["official_url"]}},
            )
            result = scrape.attach_schedules_to_movies(
                movies,
                schedules,
                formats,
                {theater["name"]: {"official_url": theater["official_url"]}},
            )

        self.assertEqual(fetch_ratings.call_count, 1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "The Odyssey")
        schedule = result[0]["theaters"][0]["schedule"]
        premium_slot = next(slot for slot in schedule if "10:00pm" in slot["times"])
        self.assertEqual(premium_slot["time_attributes"], {"10:00pm": ["Premium"]})


if __name__ == "__main__":
    unittest.main()
