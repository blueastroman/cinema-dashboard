from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from refresh_coming_soon import (  # noqa: E402
    add_months,
    merge_movies,
    parse_calendar_page,
    parse_letterboxd_match,
    parse_tmdb_details,
    parse_tmdb_search,
    should_include,
    title_slug,
)


class ComingSoonRefreshTests(unittest.TestCase):
    def test_add_months_clamps_end_of_month(self):
        self.assertEqual(add_months(date(2026, 8, 31), 6), date(2027, 2, 28))

    def test_calendar_parser_extracts_release_metadata(self):
        html = """
        <table>
          <tr><td>August 21, 2026</td></tr>
          <tr>
            <td>
              <img data-a-hires="https://example.com/poster.jpg">
              <div class="mojo-schedule-release-details">
                <h3>Festival Favorite</h3>
                <div class="mojo-schedule-genres">Drama\nThriller</div>
              </div>
            </td>
            <td>A24</td>
            <td>Limited</td>
          </tr>
        </table>
        """
        movies = parse_calendar_page(html)
        self.assertEqual(len(movies), 1)
        self.assertEqual(movies[0]["title"], "Festival Favorite")
        self.assertEqual(movies[0]["release_date"], "2026-08-21")
        self.assertEqual(movies[0]["genres"], ["Drama", "Thriller"])
        self.assertTrue(should_include(movies[0], date(2026, 7, 19), date(2027, 1, 19)))

    def test_non_specialty_limited_release_is_excluded(self):
        movie = {
            "title": "Small Event",
            "release_date": "2026-08-21",
            "studio": "Fathom Events",
            "release_scale": "Limited",
            "_detail_text": "",
        }
        self.assertFalse(should_include(movie, date(2026, 7, 19), date(2027, 1, 19)))

    def test_merge_preserves_copy_but_updates_calendar_fields(self):
        existing = [{
            "title": "Example Film",
            "release_date": "2026-09-01",
            "synopsis": "Existing synopsis",
            "poster": "existing.jpg",
        }]
        discovered = [{
            "title": "Example Film",
            "release_date": "2026-09-08",
            "studio": "Neon",
            "genres": ["Drama"],
            "release_scale": "Limited",
        }]
        movie = merge_movies(existing, discovered)[0]
        self.assertEqual(movie["release_date"], "2026-09-08")
        self.assertEqual(movie["synopsis"], "Existing synopsis")
        self.assertEqual(movie["studio"], "Neon")

    def test_tmdb_parsers_extract_matching_year_and_director(self):
        search_html = """
        <div class="flex flex-nowrap">
          <a href="/movie/123-example"><img alt="Example" src="https://media.themoviedb.org/t/p/w94_and_h141_face/a.jpg"></a>
          <h2>Example</h2><span class="release_date">August 21, 2026</span><p>Overview.</p>
        </div>
        """
        details_html = """
        <span class="genres"><a>Drama</a><a>Thriller</a></span>
        <ol class="people no_image"><li class="profile"><p><a>Jane Director</a></p><p class="character">Director</p></li></ol>
        """
        search = parse_tmdb_search(search_html, "Example", 2026)
        details = parse_tmdb_details(details_html)
        self.assertEqual(search["href"], "/movie/123-example")
        self.assertEqual(search["synopsis"], "Overview.")
        self.assertEqual(details["director"], "Jane Director")
        self.assertEqual(details["genres"], ["Drama", "Thriller"])

    def test_letterboxd_slug_and_match_are_verified(self):
        self.assertEqual(title_slug("Coyote vs. Acme"), "coyote-vs-acme")
        html = '<meta property="og:title" content="Coyote vs. Acme (2026)">'
        self.assertTrue(parse_letterboxd_match(html, "Coyote vs. Acme", 2026))
        self.assertFalse(parse_letterboxd_match(html, "Coyote vs. Acme", 1996))


if __name__ == "__main__":
    unittest.main()
