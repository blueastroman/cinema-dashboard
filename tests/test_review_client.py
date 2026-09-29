import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from cinema_backend.review_client import AnthropicReviewClient  # noqa: E402
from generate_verdicts import validate_reason  # noqa: E402


class ReviewClientTests(unittest.TestCase):
    def test_parse_json_array_strips_code_fences(self):
        payload = """```json
        [{"title": "Lorne", "verdict": "WATCH", "reason": "A documentary about Lorne Michaels. Best for comedy obsessives."}]
        ```"""
        parsed = AnthropicReviewClient.parse_json_array(payload)
        self.assertEqual(parsed[0]["title"], "Lorne")
        self.assertEqual(parsed[0]["verdict"], "WATCH")

    def test_rejects_unsupported_dismissal_of_highly_rated_film(self):
        payload = {
            "critics_score": "95%",
            "premise": "Two friends race to save their town.",
            "consensus": "",
        }
        ok, reason = validate_reason(
            "Skip it; this is a generic adventure. The runtime is 109 minutes.",
            payload,
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "unsupported dismissal of a highly rated film")


if __name__ == "__main__":
    unittest.main()
