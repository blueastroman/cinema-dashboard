import re
from pathlib import Path


INDEX = (Path(__file__).resolve().parents[1] / "public" / "index.html").read_text(encoding="utf-8")


def function_body(name: str) -> str:
    match = re.search(rf"function {name}\([^)]*\) \{{(.*?)(?=\nfunction |\n// ──)", INDEX, re.S)
    assert match, f"missing function {name}"
    return match.group(1)


def test_save_and_hide_actions_do_not_rerender_the_whole_dashboard():
    handlers = (
        "setComingSoonPreference",
        "onWatched",
        "onHide",
        "onTheaterWatched",
        "onTheaterHide",
        "onRankingWatched",
        "onRankingHide",
    )
    for handler in handlers:
        body = function_body(handler)
        assert "render();" not in body
        assert "renderComingSoonView();" not in body


def test_movie_action_buttons_are_non_submit_buttons():
    assert 'class="theater-action-btn star${watched' in INDEX
    assert 'class="theater-action-btn star${watched ? \' active\' : \'\'}" type="button"' in INDEX
    assert 'class="action-btn${watched ? \' active-watched\' : \'\'}" type="button"' in INDEX
    assert 'class="theater-action-btn star coming-soon-action${saved ? \' active\' : \'\'}" type="button"' in INDEX


def test_coming_soon_reuses_the_standard_star_and_hide_icons():
    assert 'class="theater-action-btn star coming-soon-action${saved' in INDEX
    assert '>★</button>' in INDEX
    assert 'class="theater-action-btn hide coming-soon-action${hidden' in INDEX
    assert '>×</button>' in INDEX
    assert "trigger.textContent = isSaved ? '★ Saved' : 'Save'" not in INDEX


def test_hidden_movies_receive_red_card_state_in_every_view():
    assert "--hidden-card-bg:" in INDEX
    assert ".movie-card.is-hidden-state" in INDEX
    assert ".theater-film-row.is-hidden-state" in INDEX
    assert ".ranking-row.is-hidden-state" in INDEX
    assert INDEX.count("isHiddenRow ? ' is-hidden-state' : ''") == 2
    assert "hidden ? ' is-hidden-state' : ''" in INDEX


def test_coming_soon_uses_movie_title_for_letterboxd_link():
    assert 'class="coming-soon-title-link"' in INDEX
    assert 'Letterboxd ↗' not in INDEX
