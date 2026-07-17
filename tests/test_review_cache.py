import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

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
                    allow_incomplete=True,
                ),
                now=datetime(2026, 4, 23, 12, 0, 0),
            )

            generate_verdicts.main(context)

            updated = json.loads(data_file.read_text(encoding="utf-8"))
            self.assertEqual(updated["movies"][0]["verdict"]["verdict"], "WATCH")
            self.assertIn("Best for comedy nerds.", updated["movies"][0]["verdict"]["reason"])

    def test_needs_verdict_when_model_or_source_hash_changes(self):
        movie = {
            "id": "tt123",
            "title": "Lorne",
            "ratings": {"year": "2026", "plot": "A TV builder remakes late night."},
            "theaters": [{"name": "Metrograph", "special_formats": []}],
        }
        cache = {
            "tt123": {
                "verdict": "WATCH",
                "reason": "Go for it tonight. The venue does half the work.",
                "generated_at": "2026-01-01T00:00:00",
                "model": "old-model",
                "prompt_version": generate_verdicts.PROMPT_VERSION,
                "cache_schema_version": generate_verdicts.CACHE_SCHEMA_VERSION,
                "source_hash": generate_verdicts.build_movie_source_hash(movie),
            }
        }
        self.assertTrue(generate_verdicts.needs_verdict(movie, cache, "test-model"))

        cache["tt123"]["model"] = "test-model"
        cache["tt123"]["source_hash"] = "outdated"
        self.assertTrue(generate_verdicts.needs_verdict(movie, cache, "test-model"))

    def test_main_migrates_legacy_cache_without_an_api_call(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_file = tmp / "data.json"
            cache_file = tmp / "verdicts_cache.json"
            movie = {
                "id": "tt123",
                "title": "Lorne",
                "ratings": {"year": "2026", "plot": "A TV builder remakes late night."},
                "verdict": {
                    "verdict": "WATCH",
                    "reason": "Buy the ticket tonight. The archival footage earns the big screen.",
                },
            }
            data_file.write_text(json.dumps({"movies": [movie]}), encoding="utf-8")
            cache_file.write_text(
                json.dumps(
                    {
                        "tt123": {
                            "verdict": "WATCH",
                            "reason": "Buy the ticket tonight. The archival footage earns the big screen.",
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
                    allow_incomplete=False,
                ),
                now=datetime(2026, 4, 23, 12, 0, 0),
            )

            self.assertEqual(generate_verdicts.main(context), 0)
            migrated = json.loads(cache_file.read_text(encoding="utf-8"))["tt123"]
            self.assertEqual(migrated["model"], "test-model")
            self.assertEqual(migrated["cache_schema_version"], generate_verdicts.CACHE_SCHEMA_VERSION)
            self.assertEqual(migrated["source_hash"], generate_verdicts.build_movie_source_hash(movie))

    def test_movie_without_grounding_does_not_require_an_api_call(self):
        movie = {"id": "tt999", "title": "Unknown Event", "ratings": {"year": "2026"}}

        self.assertFalse(generate_verdicts.should_review_movie(movie, {}, "test-model"))
        self.assertTrue(
            generate_verdicts.should_review_movie(movie, {}, "test-model", force_refresh=True)
        )

    def test_per_film_fallback_uses_structured_payload_and_versioned_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_file = tmp / "data.json"
            cache_file = tmp / "verdicts_cache.json"
            movie = {
                "id": "tt456",
                "title": "Fallback Film",
                "ratings": {"year": "2026", "plot": "A grounded premise."},
            }
            data_file.write_text(json.dumps({"movies": [movie]}), encoding="utf-8")
            cache_file.write_text("{}", encoding="utf-8")
            context = ReviewContext(
                config=ReviewConfig(
                    api_key="test-key",
                    model="test-model",
                    data_file=data_file,
                    cache_file=cache_file,
                    force_refresh=False,
                    batch_size=30,
                    allow_incomplete=False,
                ),
                now=datetime(2026, 4, 23, 12, 0, 0),
            )
            fallback_result = [
                {
                    "title": "Fallback Film",
                    "verdict": "WATCH",
                    "reason": "Buy the ticket tonight. The grounded premise earns attention.",
                }
            ]

            with (
                mock.patch.object(generate_verdicts, "AnthropicReviewClient", return_value=object()),
                mock.patch.object(
                    generate_verdicts,
                    "call_claude_strict",
                    side_effect=[RuntimeError("batch rejected"), fallback_result],
                ) as strict_call,
            ):
                self.assertEqual(generate_verdicts.main(context), 0)

            fallback_payload = strict_call.call_args_list[1].args[1]
            self.assertEqual(fallback_payload, [generate_verdicts.build_movie_prompt_payload(movie)])
            cached = json.loads(cache_file.read_text(encoding="utf-8"))["tt456"]
            self.assertEqual(cached["model"], "test-model")
            self.assertEqual(cached["cache_schema_version"], generate_verdicts.CACHE_SCHEMA_VERSION)
            self.assertTrue(cached["source_hash"])

    def test_main_fails_when_missing_api_key_and_verdicts_are_needed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_file = tmp / "data.json"
            cache_file = tmp / "verdicts_cache.json"
            data_file.write_text(
                json.dumps(
                    {
                        "movies": [
                            {
                                "id": "tt999",
                                "title": "New Film",
                                "ratings": {"year": "2026", "plot": "A new film needs a verdict."},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            cache_file.write_text("{}", encoding="utf-8")
            context = ReviewContext(
                config=ReviewConfig(
                    api_key="",
                    model="test-model",
                    data_file=data_file,
                    cache_file=cache_file,
                    force_refresh=False,
                    batch_size=30,
                    allow_incomplete=False,
                ),
                now=datetime(2026, 4, 23, 12, 0, 0),
            )
            self.assertEqual(generate_verdicts.main(context), 1)


if __name__ == "__main__":
    unittest.main()
