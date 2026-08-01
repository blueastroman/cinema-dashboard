from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import backfill_ratings  # noqa: E402
import refresh_recent_ratings  # noqa: E402


class RefreshRecentRatingsTests(unittest.TestCase):
    def test_refresh_recent_ratings_imports_backfill_helpers(self):
        self.assertIs(refresh_recent_ratings.fetch_rt_rating, backfill_ratings.fetch_rt_rating)
        self.assertIs(refresh_recent_ratings.fetch_omdb_rating, backfill_ratings.fetch_omdb_rating)

    def test_fetch_rt_rating_returns_only_score(self):
        with mock.patch.object(backfill_ratings, "fetch_rt", return_value=("91%", "Consensus")) as fetch_rt:
            self.assertEqual(backfill_ratings.fetch_rt_rating("Recent Movie", "2026"), "91%")

        fetch_rt.assert_called_once_with("Recent Movie", 2026)

    def test_fetch_omdb_rating_returns_metascore_by_title_and_year(self):
        response = mock.Mock()
        response.json.return_value = {
            "Title": "Recent Movie",
            "Year": "2026",
            "Metascore": "74",
            "Response": "True",
        }

        with mock.patch.object(backfill_ratings.requests, "get", return_value=response) as request:
            self.assertEqual(backfill_ratings.fetch_omdb_rating("recent movie", "test-key", "2026"), "74")

        params = request.call_args.kwargs["params"]
        self.assertEqual(params["apikey"], "test-key")
        self.assertEqual(params["t"], "recent movie")
        self.assertEqual(params["y"], "2026")


if __name__ == "__main__":
    unittest.main()
