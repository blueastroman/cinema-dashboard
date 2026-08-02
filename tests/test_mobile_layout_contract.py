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
    assert "document.body.classList.toggle('mobile-showtimes-open', open)" in INDEX
    assert "function closeMobileShowtimes()" in INDEX


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
