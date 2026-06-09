import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import refresh_amc  # noqa: E402
import scrape  # noqa: E402
from cinema_backend.runtime import ScrapeConfig, ScrapeContext, ScrapeState  # noqa: E402


class AmcRequestTests(unittest.TestCase):
    def make_context(self):
        return ScrapeContext(
            config=ScrapeConfig(
                serpapi_key="",
                omdb_key="",
                amc_vendor_key="test-vendor-key",
                amc_api_base="https://api.amctheatres.com",
                amc_theatre_ids=[],
                allow_mock_data=False,
            ),
            state=ScrapeState(),
            now=datetime(2026, 6, 9, 12, 0, 0),
            output_data_path=ROOT / "public" / "data.json",
            rating_cache_path=ROOT / "scripts" / "rating_cache.json",
        )

    def test_weekly_scrape_amc_request_uses_browser_headers(self):
        response = mock.Mock()
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None

        with mock.patch.object(scrape.requests, "get", return_value=response) as requests_get:
            result = scrape.amc_request(self.make_context(), "/v2/theatres", {"page-size": 100})

        self.assertEqual(result, {"ok": True})
        _, kwargs = requests_get.call_args
        self.assertEqual(kwargs["headers"]["X-AMC-Vendor-Key"], "test-vendor-key")
        self.assertIn("Mozilla/5.0", kwargs["headers"]["User-Agent"])
        self.assertEqual(kwargs["headers"]["Accept"], "application/json")

    def test_refresh_amc_request_uses_browser_headers(self):
        response = mock.Mock()
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None

        with (
            mock.patch.object(refresh_amc, "AMC_VENDOR_KEY", "test-vendor-key"),
            mock.patch.object(refresh_amc.requests, "get", return_value=response) as requests_get,
        ):
            result = refresh_amc.amc_request("/v2/theatres", {"page-size": 100})

        self.assertEqual(result, {"ok": True})
        _, kwargs = requests_get.call_args
        self.assertEqual(kwargs["headers"]["X-AMC-Vendor-Key"], "test-vendor-key")
        self.assertIn("Mozilla/5.0", kwargs["headers"]["User-Agent"])
        self.assertEqual(kwargs["headers"]["Accept"], "application/json")

    def test_refresh_amc_theatres_falls_back_to_serpapi_when_api_fails(self):
        with (
            mock.patch.object(refresh_amc, "AMC_VENDOR_KEY", "test-vendor-key"),
            mock.patch.object(refresh_amc, "amc_request", return_value=None),
        ):
            theaters = refresh_amc.fetch_amc_theatres()

        self.assertTrue(theaters)
        self.assertTrue(all(theater.get("source_type") == "serpapi" for theater in theaters))
        self.assertIn("AMC 34th Street 14", {theater["name"] for theater in theaters})


if __name__ == "__main__":
    unittest.main()
