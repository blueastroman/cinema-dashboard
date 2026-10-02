from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / "public" / "index.html").read_text(encoding="utf-8")


def test_movie_blurb_cache_is_identity_bound():
    assert "cinema_movie_blurbs_identity_v2" in INDEX
    assert "function hasVerifiedCloudOverrideIdentity(movie, override)" in INDEX
    assert "if (direct.cloud && !cloudBlurbMatchesMovie(movie, direct)) return null;" in INDEX
    assert "if (legacy.cloud && !cloudBlurbMatchesMovie(movie, legacy)) return null;" in INDEX
    assert "const ov = cloudScoreOverrides[key] || cloudScoreOverrides[getLegacyMovieBlurbKey(movie)];" in INDEX
