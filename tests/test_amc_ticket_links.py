import importlib
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))


class AmcTicketLinkTests(unittest.TestCase):
    def amc_modules(self):
        return [importlib.import_module(name) for name in ("scrape", "refresh_amc")]

    def test_extracts_mobile_purchase_url(self):
        expected = "https://www.amctheatres.com/showtimes/12345/tickets"
        for module in self.amc_modules():
            with self.subTest(module=module.__name__):
                self.assertEqual(
                    module.amc_showtime_purchase_url({
                        "purchaseUrl": "",
                        "mobilePurchaseUrl": expected,
                    }),
                    expected,
                )

    def test_extracts_nested_relative_purchase_link(self):
        expected = "https://www.amctheatres.com/showtimes/12345/tickets"
        for module in self.amc_modules():
            with self.subTest(module=module.__name__):
                self.assertEqual(
                    module.amc_showtime_purchase_url({
                        "_links": {
                            "https://api.amctheatres.com/rels/v2/purchase": {
                                "href": "/showtimes/12345/tickets"
                            }
                        }
                    }),
                    expected,
                )

    def test_builds_purchase_url_from_showtime_id(self):
        expected = "https://www.amctheatres.com/showtimes/141534750/tickets"
        for module in self.amc_modules():
            with self.subTest(module=module.__name__):
                self.assertEqual(
                    module.amc_showtime_purchase_url({"id": 141534750}),
                    expected,
                )

    def test_rejects_theater_and_api_links_as_showtime_buy_links(self):
        for module in self.amc_modules():
            with self.subTest(module=module.__name__):
                self.assertEqual(
                    module.amc_showtime_purchase_url({
                        "purchaseUrl": "https://www.amctheatres.com/movie-theatres/new-york-city/amc-34th-street-14",
                        "_links": {
                            "self": {
                                "href": "https://api.amctheatres.com/v2/showtimes/12345"
                            }
                        },
                    }),
                    "",
                )


if __name__ == "__main__":
    unittest.main()
