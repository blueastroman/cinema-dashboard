import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import scrape  # noqa: E402
from cinema_backend.runtime import ScrapeConfig, ScrapeContext, ScrapeState  # noqa: E402


class AlamoMetadataTests(unittest.TestCase):
    def make_context(self, existing_metadata=None):
        return ScrapeContext(
            config=ScrapeConfig(
                serpapi_key="",
                omdb_key="",
                amc_vendor_key="",
                amc_api_base="https://api.amctheatres.com",
                amc_theatre_ids=[],
                allow_mock_data=False,
            ),
            state=ScrapeState(existing_movie_metadata=existing_metadata or {}),
            now=scrape.ny_now().replace(tzinfo=None),
            output_data_path=ROOT / "public" / "data.json",
            rating_cache_path=ROOT / "scripts" / "rating_cache.json",
        )

    def test_extract_alamo_metadata_merges_show_and_event_payloads(self):
        show_data = {
            "runtimeMinutes": 100,
            "directors": [{"name": "Morgan Neville"}],
        }
        event_data = {
            "imdbId": "tt39847629",
            "nationalReleaseDateUtc": "2026-04-17T00:00:00Z",
            "description": "<p>Lorne Michaels documentary.</p>",
            "genres": [{"name": "Documentary"}, {"name": "Biography"}],
        }

        metadata = scrape.extract_alamo_metadata(show_data, event_data)

        self.assertEqual(metadata["imdbID"], "tt39847629")
        self.assertEqual(metadata["runtime"], "100 min")
        self.assertEqual(metadata["director"], "Morgan Neville")
        self.assertEqual(metadata["genre"], "Documentary, Biography")
        self.assertEqual(metadata["year"], "2026")

    def test_expected_year_prevents_stale_existing_metadata_from_overriding(self):
        ctx = self.make_context(
            {
                "scary movie|2026": {
                    "imdbID": "tt0175142",
                    "year": "2000",
                    "director": "Keenen Ivory Wayans",
                    "runtime": "88 min",
                    "plot": "A year after disposing of the body...",
                    "genre": "Comedy",
                    "poster": "old-poster.jpg",
                    "rt": "52%",
                    "imdb": "6.3",
                    "metacritic": "48",
                    "letterboxd": "3.1",
                }
            }
        )
        fresh = {
            "imdbID": "tt32093575",
            "year": "2026",
            "director": "Michael Tiddes",
            "runtime": "95 min",
            "plot": "Two friends find themselves caught up in mayhem.",
            "genre": "Comedy, Horror",
            "poster": "new-poster.jpg",
            "rt": "52%",
            "imdb": "N/A",
            "metacritic": "N/A",
            "letterboxd": None,
            "cinemaScore": None,
        }

        merged = scrape.merge_existing_metadata(
            ctx,
            "Scary Movie (2026)",
            dict(fresh),
            expected_year=2026,
        )

        self.assertEqual(merged["imdbID"], "tt32093575")
        self.assertEqual(merged["year"], "2026")
        self.assertEqual(merged["director"], "Michael Tiddes")
        self.assertEqual(merged["runtime"], "95 min")


if __name__ == "__main__":
    unittest.main()
