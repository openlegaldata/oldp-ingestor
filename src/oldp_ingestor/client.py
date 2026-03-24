import logging
import time

import requests

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds

# HTTP status codes that trigger a retry
_RETRYABLE_STATUS_CODES = (429, 503)


def _retry_delay(resp: requests.Response, attempt: int) -> float:
    """Compute wait time from Retry-After header or exponential backoff."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(float(retry_after), 1)
        except (ValueError, TypeError):
            pass
    return INITIAL_BACKOFF * (2**attempt)


class OLDPClient:
    """Client for communicating with an OLDP API instance."""

    def __init__(
        self,
        api_url: str,
        api_token: str = "",
        http_auth: str = "",
        write_delay: float = 0.0,
    ):
        self.api_url = api_url.rstrip("/")
        self.write_delay = write_delay
        self.session = requests.Session()

        if api_token:
            self.session.headers["Authorization"] = f"Token {api_token}"

        if http_auth and ":" in http_auth:
            user, password = http_auth.split(":", 1)
            self.session.auth = (user, password)

        logger.debug("Initialized OLDP client for %s", self.api_url)

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Execute HTTP request with retry on 429/503 and connection errors."""
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self.session.request(method, url, **kwargs)
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

    def get(self, path: str, **kwargs) -> dict:
        url = f"{self.api_url}{path}"
        logger.debug("GET %s", url)
        resp = self._request_with_retry("GET", url, **kwargs)
        return resp.json()

    def post(self, path: str, data: dict, **kwargs) -> dict:
        url = f"{self.api_url}{path}"
        logger.debug("POST %s", url)
        if self.write_delay > 0:
            time.sleep(self.write_delay)
        resp = self._request_with_retry("POST", url, json=data, **kwargs)
        return resp.json()

    @classmethod
    def from_settings(cls, write_delay: float = 0.0) -> "OLDPClient":
        from oldp_ingestor import settings

        if not settings.OLDP_API_URL:
            raise ValueError("OLDP_API_URL is not set")

        return cls(
            api_url=settings.OLDP_API_URL,
            api_token=settings.OLDP_API_TOKEN,
            http_auth=settings.OLDP_API_HTTP_AUTH,
            write_delay=write_delay,
        )
