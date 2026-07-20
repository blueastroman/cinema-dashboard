import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures" / "providers"
sys.path.insert(0, str(ROOT / "scripts"))

from cinema_backend.providers.anthology import parse_anthology_calendar_html  # noqa: E402
from cinema_backend.providers.bam import discover_bam_productions, fetch_bam_showtimes, parse_bam_production_html  # noqa: E402
from cinema_backend.providers.nitehawk import fetch_nitehawk_showtimes, parse_nitehawk_date_html  # noqa: E402
from cinema_backend.runtime import ScrapeConfig, ScrapeContext, ScrapeState  # noqa: E402


class RepertoryProviderTests(unittest.TestCase):
    def make_context(self, *, existing=None):
        return ScrapeContext(
            config=ScrapeConfig(
                serpapi_key="",
                omdb_key="",
                amc_vendor_key="",
                amc_api_base="https://api.amctheatres.com",
                amc_theatre_ids=[],
                allow_mock_data=False,
            ),
            state=ScrapeState(existing_movie_records=existing or {}),
            now=datetime(2026, 7, 30, 12, 0, 0),
            output_data_path=ROOT / "public" / "data.json",
            rating_cache_path=ROOT / "scripts" / "rating_cache.json",
        )

    def test_anthology_parses_formats_programs_urls_and_month_boundary(self):
        theater = {"name": "Anthology Film Archives", "official_url": "https://www.anthologyfilmarchives.org"}
        horizon = {"2026-07-30", "2026-08-01"}
        entries = []
        entries.extend(
            parse_anthology_calendar_html(
                (FIXTURES / "anthology_july.html").read_text(encoding="utf-8"),
                theater,
                month=7,
                year=2026,
                horizon=horizon,
            )
        )
        entries.extend(
            parse_anthology_calendar_html(
                (FIXTURES / "anthology_august.html").read_text(encoding="utf-8"),
                theater,
                month=8,
                year=2026,
                horizon=horizon,
            )
        )

        by_title = {entry["title"]: entry for entry in entries}
        self.assertIn("SHORTS PROGRAM: CINEMA FANTASTIQUE", by_title)
        self.assertEqual(by_title["SHORTS PROGRAM: CINEMA FANTASTIQUE"]["ticket_urls"]["6:45pm"], "https://ticketing.us.veezi.com/sessions/?siteToken=anthology")
        self.assertEqual(by_title["SHORTS PROGRAM: CINEMA FANTASTIQUE"]["date"], "2026-07-30")
        self.assertIn("16mm", by_title["SHORTS PROGRAM: CINEMA FANTASTIQUE"]["special_formats"])
        self.assertIn("DCP", by_title["SHORTS PROGRAM: CINEMA FANTASTIQUE"]["special_formats"])
        self.assertIn("Restoration", by_title["SHORTS PROGRAM: CINEMA FANTASTIQUE"]["special_formats"])
        self.assertIn("Shorts Program", by_title["SHORTS PROGRAM: CINEMA FANTASTIQUE"]["time_attributes"]["6:45pm"])
        self.assertIn("Double Feature", by_title["DOUBLE FEATURE: CÉLINE AND JULIE / DAISIES"]["time_attributes"]["9:00pm"])
        self.assertEqual(by_title["LA JETÉE"]["date"], "2026-08-01")

    def test_nitehawk_parses_purchase_links_dcp_oc_and_unicode_title(self):
        theater = {
            "name": "Nitehawk Cinema Williamsburg",
            "source_url": "https://nitehawkcinema.com/williamsburg/",
            "location_slug": "williamsburg",
        }
        entries = parse_nitehawk_date_html(
            (FIXTURES / "nitehawk_date.html").read_text(encoding="utf-8"),
            theater,
            target_date=datetime(2026, 7, 21, 12, 0, 0),
        )
        by_title = {entry["title"]: entry for entry in entries}

        self.assertEqual(by_title["The Odyssey"]["date"], "2026-07-21")
        self.assertEqual(by_title["The Odyssey"]["ticket_urls"]["3:00pm"], "https://nitehawkcinema.com/williamsburg/purchase/111/")
        self.assertIn("DCP", by_title["The Odyssey"]["special_formats"])
        self.assertEqual(by_title["The Odyssey"]["time_attributes"]["3:00pm"], ["Open Caption"])
        self.assertIn("El Espíritu de la Colmena", by_title)

    def test_bam_discovers_relevant_productions_and_parses_performance_links(self):
        theater = {"name": "BAM Rose Cinemas", "official_url": "https://www.bam.org/film"}
        index = (FIXTURES / "bam_index.html").read_text(encoding="utf-8")
        productions = discover_bam_productions(index, horizon={"2026-07-20"})
        self.assertEqual([item["url"] for item in productions], ["https://commerce.bam.org/production/55721"])

        entries = parse_bam_production_html(
            (FIXTURES / "bam_production.html").read_text(encoding="utf-8"),
            theater,
            productions[0],
            horizon={"2026-07-20"},
        )

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["title"], "The Odyssey")
        self.assertEqual(entries[0]["date"], "2026-07-20")
        self.assertEqual(entries[0]["times"], ["4:00pm", "7:30pm"])
        self.assertEqual(entries[0]["ticket_urls"]["7:30pm"], "https://commerce.bam.org/booking/production/bestavailable/55935")
        self.assertIn("70mm", entries[0]["special_formats"])
        self.assertIn("Restoration", entries[0]["special_formats"])

    def test_provider_failure_reuses_cached_future_schedule(self):
        theater = {
            "name": "Nitehawk Cinema Prospect Park",
            "source_url": "https://nitehawkcinema.com/prospectpark/",
            "location_slug": "prospectpark",
            "official_url": "https://nitehawkcinema.com/prospectpark/",
        }
        ctx = self.make_context(
            existing={
                "cached movie": {
                    "title": "Cached Movie",
                    "theaters": [
                        {
                            "name": "Nitehawk Cinema Prospect Park",
                            "ticket_url": "https://nitehawkcinema.com/prospectpark/purchase/1/",
                            "schedule": [
                                {
                                    "day": "Fri Jul 31",
                                    "date": "2026-07-31",
                                    "times": ["7:00pm"],
                                    "ticket_urls": {"7:00pm": "https://nitehawkcinema.com/prospectpark/purchase/1/"},
                                }
                            ],
                        }
                    ],
                }
            }
        )

        with mock.patch("cinema_backend.providers.nitehawk.fetch_html_page", return_value=""):
            entries = fetch_nitehawk_showtimes(theater, ctx)

        self.assertEqual(entries[0]["title"], "Cached Movie")
        self.assertEqual(entries[0]["date"], "2026-07-31")

    def test_changed_bam_markup_without_cache_returns_empty_not_exception(self):
        theater = {"name": "BAM Rose Cinemas", "source_url": "https://www.bam.org/film"}
        ctx = self.make_context()
        with mock.patch("cinema_backend.providers.bam.fetch_html_page", return_value="<main>No productions</main>"):
            entries = fetch_bam_showtimes(theater, ctx)
        self.assertEqual(entries, [])


if __name__ == "__main__":
    unittest.main()
