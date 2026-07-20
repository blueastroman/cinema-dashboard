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
    assert 'class="action-btn coming-soon-action${saved ? \' active-save\' : \'\'}" type="button"' in INDEX

