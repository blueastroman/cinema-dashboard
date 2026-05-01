import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import scrape  # noqa: E402


class AlamoMetadataTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
