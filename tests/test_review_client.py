import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from cinema_backend.review_client import AnthropicReviewClient  # noqa: E402


class ReviewClientTests(unittest.TestCase):
    def test_parse_json_array_strips_code_fences(self):
        payload = """```json
        [{"title": "Lorne", "verdict": "WATCH", "reason": "A documentary about Lorne Michaels. Best for comedy obsessives."}]
        ```"""
        parsed = AnthropicReviewClient.parse_json_array(payload)
        self.assertEqual(parsed[0]["title"], "Lorne")
        self.assertEqual(parsed[0]["verdict"], "WATCH")


if __name__ == "__main__":
    unittest.main()
