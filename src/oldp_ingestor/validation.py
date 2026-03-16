"""Pre-POST validation for cases.

Checks data quality before sending to the OLDP API. Cases that fail
validation are skipped with a warning, not sent to the API.
"""

import logging
import re
from datetime import date, timedelta

logger = logging.getLogger(__name__)

_INJECTION_PATTERNS = re.compile(
    r"<script|<iframe|onclick\s*=|onerror\s*=|onload\s*=|javascript:", re.IGNORECASE
)

MIN_CONTENT_LENGTH = 100
MIN_DATE = "1950-01-01"


def validate_case(case: dict) -> str | None:
    """Validate a case dict before POSTing to the API.

    Returns None if valid, or an error message string if invalid.
    """
    content = case.get("content", "")
    date_str = case.get("date", "")
    file_number = case.get("file_number", "")
    court_name = case.get("court_name", "")

    # Content present and non-trivial
    if not content or len(content) < MIN_CONTENT_LENGTH:
        return f"content too short ({len(content)} chars, min {MIN_CONTENT_LENGTH})"

    # Date present and valid
    if not date_str:
        return "missing date"

    try:
        d = date.fromisoformat(date_str)
        max_date = date.today() + timedelta(days=30)
        if d < date.fromisoformat(MIN_DATE):
            return f"date too old: {date_str}"
        if d > max_date:
            return f"date in future: {date_str}"
    except ValueError:
        return f"unparseable date: {date_str}"

    # File number present
    if not file_number or not file_number.strip():
        return "missing file_number"

    # Court name present
    if not court_name or not court_name.strip():
        return "missing court_name"

    # No script injection in content
    if _INJECTION_PATTERNS.search(content):
        return "content contains script/injection patterns"

    return None
