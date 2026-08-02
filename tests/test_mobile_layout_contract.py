from pathlib import Path


INDEX = (Path(__file__).parents[1] / "public" / "index.html").read_text()


def test_mobile_filter_sheet_does_not_restore_stale_state():
    assert "localStorage.removeItem('mobile_filters_open')" in INDEX
    assert "localStorage.getItem('mobile_filters_open')" not in INDEX
    assert "localStorage.setItem('mobile_filters_open'" not in INDEX


def test_mobile_filter_sheet_has_accessible_controls():
    assert 'aria-controls="dashboard-filters"' in INDEX
    assert 'id="dashboard-filters"' in INDEX
    assert 'class="mobile-sheet-done"' in INDEX
    assert 'aria-label="Close open panel"' in INDEX


def test_mobile_sheets_are_bounded_and_lock_background_scroll():
    assert "max-height: min(82dvh" in INDEX
    assert "body.mobile-showtimes-open { overflow: hidden; }" in INDEX
    assert "body.filters-open .mobile-sheet-backdrop" in INDEX


def test_mobile_showtimes_use_a_dedicated_sheet():
    assert 'class="mobile-showtimes-header"' in INDEX
    assert "panel.classList.add('mobile-active')" in INDEX
    assert "document.body.appendChild(panel)" in INDEX
    assert "row.appendChild(panel)" in INDEX
    assert ".ranking-times-panel.mobile-active" in INDEX
    assert "function closeMobileShowtimes(" in INDEX


def test_mobile_showtime_sheet_slides_smoothly_from_the_bottom():
    assert "@keyframes mobileShowtimesSheetIn" in INDEX
    assert "transform: translate3d(0, 100%, 0);" in INDEX
    assert "animation: mobileShowtimesSheetIn 360ms cubic-bezier(0.22, 1, 0.36, 1) both;" in INDEX
    assert ".ranking-times-panel.mobile-active.mobile-closing" in INDEX
    assert "animation: mobileShowtimesSheetOut 240ms cubic-bezier(0.4, 0, 1, 1) both;" in INDEX
    assert "body.mobile-showtimes-closing .mobile-sheet-backdrop" in INDEX
    assert "mobileShowtimesCloseTimer = window.setTimeout(finishClose, 260);" in INDEX
    assert "@media (prefers-reduced-motion: reduce)" in INDEX


def test_mobile_primary_targets_are_at_least_44_pixels():
    assert "min-height: 44px" in INDEX
    assert "width: 44px;\n        height: 44px;" in INDEX


def test_coming_soon_mobile_controls_are_simplified():
    assert "body.coming-soon-mode .mobile-filter-bar { display: none; }" in INDEX
    assert 'class="coming-soon-more"' in INDEX
    assert "function toggleComingSoonSynopsis(button)" in INDEX


def test_mobile_sort_control_has_an_aligned_visible_label():
    assert 'class="control-group sort-control"' in INDEX
    assert '<label class="control-label sort-control-label" for="sort-select">Sort</label>' in INDEX
    assert ".stats-row .sort-control" in INDEX
    assert ".sort-control-label { display: block; }" in INDEX


def test_mobile_movie_synopses_are_not_line_clamped():
    mobile_css = INDEX.split("/* ── MOBILE EXPERIENCE", 1)[1].split("</style>", 1)[0]
    ranking_rule = mobile_css.split(".ranking-blurb {", 1)[1].split("}", 1)[0]
    theater_rule = mobile_css.split(".theater-film-review {", 1)[1].split("}", 1)[0]
    assert "display: block;" in ranking_rule
    assert "-webkit-line-clamp: unset;" in ranking_rule
    assert "overflow: visible;" in ranking_rule
    assert "-webkit-line-clamp: unset;" in theater_rule
    assert "overflow: visible;" in theater_rule


def test_mobile_showtime_sheet_covers_translucent_safari_toolbar():
    assert "inset: auto 0 -144px;" in INDEX
    assert "max-height: min(calc(82dvh + 144px), 844px);" in INDEX
    assert "calc(168px + env(safe-area-inset-bottom, 0px))" in INDEX


def test_coming_soon_actions_share_the_compact_release_row():
    mobile_css = INDEX.split("/* ── MOBILE EXPERIENCE", 1)[1].split("</style>", 1)[0]
    assert "grid-template-columns: minmax(0, 1fr) auto;" in mobile_css
    assert ".coming-soon-actions" in mobile_css
    assert "grid-row: 1 / span 2;" in mobile_css
    assert "grid-template-columns: repeat(2, 44px);" in mobile_css
    assert "width: 44px;" in mobile_css


def test_coming_soon_header_control_returns_to_rankings():
    assert 'onclick="toggleComingSoonView()"' in INDEX
    assert "function toggleComingSoonView()" in INDEX
    assert "setView(viewMode === 'coming' ? 'rankings' : 'coming')" in INDEX
    assert "comingSoonButton.textContent = isComingSoon ? '← Movies' : 'Coming Soon'" in INDEX
    assert "Back to movie rankings" in INDEX
