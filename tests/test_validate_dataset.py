import sys
import unittest
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from cinema_backend.common import ny_now  # noqa: E402
import validate_dataset  # noqa: E402


class ValidateDatasetTests(unittest.TestCase):
    def test_stale_dataset_fails_validation(self):
        now = ny_now()
        stale_generated_at = (now - timedelta(days=5)).isoformat()
        future_date = (now + timedelta(days=1)).date().isoformat()
        dataset = {
            "generated_at": stale_generated_at,
            "week_of": "July 10, 2026",
            "theaters": ["Metrograph"],
            "theater_meta": {"Metrograph": {}},
            "movies": [
                {
                    "id": "movie-1",
                    "title": "Example",
                    "ratings": {"year": "2026", "runtime": "100 min", "genre": "Drama", "director": "Jane Doe"},
                    "verdict": {"verdict": "WATCH", "reason": "Go tonight. The big screen earns it."},
                    "theaters": [
                        {
                            "name": "Metrograph",
                            "ticket_url": "https://example.com",
                            "schedule": [{"day": "Tomorrow", "date": future_date, "times": ["7:00 PM"]}],
                        }
                    ],
                }
            ],
        }
        errors, _warnings = validate_dataset.validate_dataset(dataset)
        self.assertTrue(any("Dataset is stale" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
