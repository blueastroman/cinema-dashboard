from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_coming_soon_override_contract_is_shared_by_admin_public_and_sql():
    admin = (ROOT / "public" / "admin" / "index.html").read_text(encoding="utf-8")
    public = (ROOT / "public" / "index.html").read_text(encoding="utf-8")
    sql = (ROOT / "supabase" / "admin-dashboard-policies.sql").read_text(encoding="utf-8")

    fields = {
        "title_override",
        "release_date_override",
        "poster_override",
        "synopsis_override",
        "director_override",
        "genres_override",
        "studio_override",
        "release_scale_override",
        "letterboxd_url_override",
    }
    for field in fields:
        assert field in admin
        assert field in public
        assert field in sql

    assert "coming_soon_overrides" in admin
    assert "coming_soon_overrides" in public
    assert "coming_soon_overrides" in sql
    assert ".filter(movie => !movie.disabled)" in public

