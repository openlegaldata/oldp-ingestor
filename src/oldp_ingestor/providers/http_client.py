"""Generic HTTP client with retry, pacing, and session management."""

import logging
import time

import requests
from requests import Response

logger = logging.getLogger(__name__)

USER_AGENT = "oldp-ingestor/0.1.2 (+https://github.com/openlegaldata)"
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds

# HTTP status codes that trigger a retry
_RETRYABLE_STATUS_CODES = (429, 503)


def _retry_delay(resp: Response, attempt: int) -> float:
    """Compute wait time from Retry-After header or exponential backoff."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(float(retry_after), 1)
        except (ValueError, TypeError):
            pass
    return INITIAL_BACKOFF * (2**attempt)


class HttpBaseClient:
    """Generic HTTP client with retry, pacing, and session management.

    Manages a requests.Session with configurable request delay
    and automatic retry with exponential backoff on 429/503 responses and
    connection errors. Respects Retry-After headers when present.
    """

    def __init__(self, base_url: str = "", request_delay: float = 0.2):
        self.base_url = base_url
        self.request_delay = request_delay
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT

    def _request_with_retry(self, method: str, url: str, **kwargs) -> Response:
        """Execute HTTP request with retry on 429/503 and connection errors.

        Retries up to MAX_RETRIES times. If response includes Retry-After
        header, its value is used as wait time; otherwise exponential backoff
        is applied (1s, 2s, 4s, ...).
        """
        for attempt in range(MAX_RETRIES + 1):
            try:
                if self.request_delay > 0:
                    time.sleep(self.request_delay)
                resp = self.session.request(method, url, timeout=30, **kwargs)
                if (
                    resp.status_code in _RETRYABLE_STATUS_CODES
                    and attempt < MAX_RETRIES
                ):
                    delay = _retry_delay(resp, attempt)
                    logger.warning(
                        "%d on %s, retrying in %ds (attempt %d/%d)",
                        resp.status_code,
                        url,
                        delay,
                        attempt + 1,
                        MAX_RETRIES,
                    )
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
                return resp
            except requests.ConnectionError:
                if attempt < MAX_RETRIES:
                    delay = INITIAL_BACKOFF * (2**attempt)
                    logger.warning(
                        "Connection error on %s, retrying in %ds (attempt %d/%d)",
                        url,
                        delay,
                        attempt + 1,
                        MAX_RETRIES,
                    )
                    time.sleep(delay)
                    continue
                raise
        # Unreachable in practice -- the last attempt either returns or raises
        raise requests.ConnectionError(f"Failed after {MAX_RETRIES} retries: {url}")

    def _get(self, url_or_path: str, **kwargs) -> Response:
        """GET request, prepending base_url if path doesn't start with http."""
        if url_or_path.startswith("http"):
            url = url_or_path
        else:
            url = f"{self.base_url}{url_or_path}"
        logger.debug("GET %s kwargs=%s", url, kwargs or "")
        return self._request_with_retry("GET", url, **kwargs)

    def _post(self, url_or_path: str, **kwargs) -> Response:
        """POST request, prepending base_url if path doesn't start with http."""
        if url_or_path.startswith("http"):
            url = url_or_path
        else:
            url = f"{self.base_url}{url_or_path}"
        logger.debug("POST %s", url)
        return self._request_with_retry("POST", url, **kwargs)

    def _get_json(self, path: str, **params) -> dict:
        """GET JSON from base_url + path."""
        resp = self._get(path, params=params)
        return resp.json()

    def _get_text(self, path: str) -> str:
        """GET text content from base_url + path."""
        return self._get(path).text

    def _get_content(self, path: str) -> bytes:
        """GET raw bytes from base_url + path."""
        return self._get(path).content
