"""Shared HTTP infrastructure for RIS (Rechtsinformationsportal) providers.

Provides the base URL, retry logic, and session management used by both
:class:`~oldp_ingestor.providers.de.ris.RISProvider` (legislation) and
:class:`~oldp_ingestor.providers.de.ris_cases.RISCaseProvider` (case law).
"""

import re

from oldp_ingestor.providers.http_client import HttpBaseClient

# Re-export for backward compatibility (tests import these)
from oldp_ingestor.providers.http_client import (  # noqa: F401
    INITIAL_BACKOFF,
    MAX_RETRIES,
    USER_AGENT,
    _RETRYABLE_STATUS_CODES,
    _retry_delay,
)

BASE_URL = "https://testphase.rechtsinformationen.bund.de"
MAX_PAGE_SIZE = 300
MIN_CONTENT_LENGTH = 10


def extract_body(html: str) -> str:
    """Extract inner body content from a full HTML page."""
    match = re.search(r"<body[^>]*>(.*)</body>", html, re.DOTALL)
    if match:
        return match.group(1).strip()
    return html


class RISBaseClient(HttpBaseClient):
    """Shared HTTP client for RIS API providers.

    Inherits retry, pacing, and session management from
    :class:`HttpBaseClient`, with RIS base URL pre-configured.
    """

    def __init__(self, request_delay: float = 0.2):
        super().__init__(base_url=BASE_URL, request_delay=request_delay)
