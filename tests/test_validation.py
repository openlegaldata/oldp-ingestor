"""Tests for oldp_ingestor.validation module."""

from datetime import date, timedelta


from oldp_ingestor.validation import MIN_CONTENT_LENGTH, MIN_DATE, validate_case


def _make_case(**overrides):
    """Build a valid case dict, then apply overrides."""
    base = {
        "content": "x" * (MIN_CONTENT_LENGTH + 10),
        "date": "2024-06-15",
        "file_number": "1 ZR 123/24",
        "court_name": "Bundesgerichtshof",
    }
    base.update(overrides)
    return base


# --- Valid case ---


def test_validate_case_valid():
    """A fully valid case returns None."""
    assert validate_case(_make_case()) is None


def test_validate_case_valid_boundary_content_length():
    """Content exactly at the minimum length is valid."""
    case = _make_case(content="x" * MIN_CONTENT_LENGTH)
    assert validate_case(case) is None


# --- Short / missing content ---


def test_validate_case_short_content():
    """Content shorter than MIN_CONTENT_LENGTH is rejected."""
    case = _make_case(content="too short")
    result = validate_case(case)
    assert result is not None
    assert "content too short" in result


def test_validate_case_empty_content():
    """Empty content string is rejected."""
    case = _make_case(content="")
    result = validate_case(case)
    assert result is not None
    assert "content too short" in result


def test_validate_case_missing_content_key():
    """Missing content key is rejected."""
    case = _make_case()
    del case["content"]
    result = validate_case(case)
    assert result is not None
    assert "content too short" in result


# --- Missing date ---


def test_validate_case_missing_date():
    """Empty date is rejected."""
    case = _make_case(date="")
    result = validate_case(case)
    assert result is not None
    assert "missing date" in result


def test_validate_case_missing_date_key():
    """Missing date key is rejected."""
    case = _make_case()
    del case["date"]
    result = validate_case(case)
    assert result is not None
    assert "missing date" in result


# --- Future date ---


def test_validate_case_future_date():
    """Date more than 30 days in the future is rejected."""
    future = date.today() + timedelta(days=60)
    case = _make_case(date=future.isoformat())
    result = validate_case(case)
    assert result is not None
    assert "date in future" in result


# --- Old date ---


def test_validate_case_old_date():
    """Date before MIN_DATE is rejected."""
    case = _make_case(date="1900-01-01")
    result = validate_case(case)
    assert result is not None
    assert "date too old" in result


def test_validate_case_boundary_min_date():
    """Date exactly at MIN_DATE is valid."""
    case = _make_case(date=MIN_DATE)
    assert validate_case(case) is None


# --- Unparseable date ---


def test_validate_case_unparseable_date():
    """Non-ISO date string is rejected."""
    case = _make_case(date="not-a-date")
    result = validate_case(case)
    assert result is not None
    assert "unparseable date" in result


def test_validate_case_german_format_date():
    """DD.MM.YYYY format (not ISO) is treated as unparseable."""
    case = _make_case(date="15.06.2024")
    result = validate_case(case)
    assert result is not None
    assert "unparseable date" in result


# --- Missing file_number ---


def test_validate_case_missing_file_number():
    """Empty file_number is rejected."""
    case = _make_case(file_number="")
    result = validate_case(case)
    assert result is not None
    assert "missing file_number" in result


def test_validate_case_whitespace_file_number():
    """Whitespace-only file_number is rejected."""
    case = _make_case(file_number="   ")
    result = validate_case(case)
    assert result is not None
    assert "missing file_number" in result


# --- Missing court_name ---


def test_validate_case_missing_court_name():
    """Empty court_name is rejected."""
    case = _make_case(court_name="")
    result = validate_case(case)
    assert result is not None
    assert "missing court_name" in result


def test_validate_case_whitespace_court_name():
    """Whitespace-only court_name is rejected."""
    case = _make_case(court_name="  \t ")
    result = validate_case(case)
    assert result is not None
    assert "missing court_name" in result


# --- Injection patterns ---


def test_validate_case_script_injection():
    """Content with <script> tag is rejected."""
    case = _make_case(content="x" * 200 + '<script>alert("xss")</script>')
    result = validate_case(case)
    assert result is not None
    assert "injection" in result


def test_validate_case_iframe_injection():
    """Content with <iframe> is rejected."""
    case = _make_case(content="x" * 200 + "<iframe src='evil.com'></iframe>")
    result = validate_case(case)
    assert result is not None
    assert "injection" in result


def test_validate_case_onclick_injection():
    """Content with onclick= handler is rejected."""
    case = _make_case(content="x" * 200 + '<div onclick="doEvil()">text</div>')
    result = validate_case(case)
    assert result is not None
    assert "injection" in result


def test_validate_case_javascript_uri_injection():
    """Content with javascript: URI is rejected."""
    case = _make_case(content="x" * 200 + '<a href="javascript:void(0)">link</a>')
    result = validate_case(case)
    assert result is not None
    assert "injection" in result


def test_validate_case_clean_html_allowed():
    """Normal HTML tags (p, div, h2) should pass validation."""
    case = _make_case(content="<h2>Tenor</h2><p>" + "x" * 200 + "</p><div>more</div>")
    assert validate_case(case) is None
