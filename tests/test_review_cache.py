import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import generate_verdicts  # noqa: E402
from cinema_backend.runtime import ReviewConfig, ReviewContext  # noqa: E402


class ReviewCacheTests(unittest.TestCase):
    def test_main_applies_cached_verdicts_without_api_key(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_file = tmp / "data.json"
            cache_file = tmp / "verdicts_cache.json"
            data_file.write_text(
                json.dumps(
                    {
                        "movies": [
                            {
                                "id": "tt123",
                                "title": "Lorne",
                                "ratings": {"year": "2026"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            cache_file.write_text(
                json.dumps(
                    {
                        "tt123": {
                            "verdict": "WATCH",
                            "reason": "A documentary about a TV titan. Best for comedy nerds.",
                            "generated_at": "2026-01-01T00:00:00",
                        }
                    }
                ),
                encoding="utf-8",
            )

            context = ReviewContext(
                config=ReviewConfig(
                    api_key="",
                    model="test-model",
                    data_file=data_file,
                    cache_file=cache_file,
                    force_refresh=False,
                    batch_size=30,
                ),
                now=datetime(2026, 4, 23, 12, 0, 0),
            )

            generate_verdicts.main(context)

            updated = json.loads(data_file.read_text(encoding="utf-8"))
            self.assertEqual(updated["movies"][0]["verdict"]["verdict"], "WATCH")
            self.assertIn("Best for comedy nerds.", updated["movies"][0]["verdict"]["reason"])


if __name__ == "__main__":
    unittest.main()
