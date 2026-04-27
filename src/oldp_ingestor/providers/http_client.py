"""Generic HTTP client with retry, pacing, rate-limiting, and circuit breaker."""

import logging
import random
import subprocess
import threading
import time
from importlib.metadata import version
from urllib.parse import urlparse

import requests
from requests import Response

logger = logging.getLogger(__name__)


def _build_user_agent() -> str:
    """Build user agent string from package version and git commit hash."""
    try:
        ver = version("oldp-ingestor")
    except Exception:
        ver = "0.0.0"
    try:
        commit = (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            .decode()
            .strip()
        )
    except Exception:
        commit = ""
    ua = f"oldp-ingestor/{ver}"
    if commit:
        ua += f"+{commit}"
    ua += (
        " (research; non-commercial; "
        "+https://github.com/openlegaldata/oldp-ingestor; "
        "contact: https://openlegaldata.io)"
    )
    return ua


USER_AGENT = _build_user_agent()
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds
REQUEST_JITTER_FRAC = 0.2  # ±20% random jitter on request_delay

# HTTP status codes that trigger a retry
_RETRYABLE_STATUS_CODES = (429, 503)

# --- Process-wide defaults (CLI may override via configure_defaults) ---
_DEFAULT_MAX_RPM: int | None = None
_DEFAULT_CB_THRESHOLD: int = 5


def configure_defaults(
    max_rpm: int | None = None,
    circuit_breaker_threshold: int | None = None,
) -> None:
    """Set process-wide defaults read by every HttpBaseClient instance.

    Called once by the CLI before provider instantiation so the knobs reach
    subclasses that don't forward these kwargs through their constructors.
    None means 'leave unchanged'; pass 0 to disable a check.
    """
    global _DEFAULT_MAX_RPM, _DEFAULT_CB_THRESHOLD
    if max_rpm is not None:
        _DEFAULT_MAX_RPM = max_rpm if max_rpm > 0 else None
    if circuit_breaker_threshold is not None:
        _DEFAULT_CB_THRESHOLD = max(0, circuit_breaker_threshold)


class BlockedHostError(requests.RequestException):
    """Raised when a host trips the circuit breaker after repeated failures.

    Distinct from a transient ConnectionError so callers can fail the whole
    run loudly instead of retrying forever against an unreachable target.
    """


class _HostRateLimiter:
    """Minimum-interval limiter keyed by host, shared across a process.

    Thread-safe. Computes the enforced inter-request gap from ``max_rpm``
    and blocks the caller just long enough to stay under the cap.
    """

    def __init__(self) -> None:
        self._last: dict[str, float] = {}
        self._fails: dict[str, int] = {}
        self._lock = threading.Lock()

    def wait(self, host: str, max_rpm: int | None) -> None:
        if not max_rpm or max_rpm <= 0:
            return
        min_interval = 60.0 / max_rpm
        with self._lock:
            now = time.monotonic()
            last = self._last.get(host, 0.0)
            wait_for = min_interval - (now - last)
            if wait_for > 0:
                time.sleep(wait_for)
                now = time.monotonic()
            self._last[host] = now

    def record_failure(self, host: str) -> int:
        with self._lock:
            self._fails[host] = self._fails.get(host, 0) + 1
            return self._fails[host]

    def record_success(self, host: str) -> None:
        with self._lock:
            if host in self._fails:
                self._fails[host] = 0


_LIMITER = _HostRateLimiter()


def _host_of(url: str) -> str:
    try:
        return urlparse(url).hostname or url
    except Exception:
        return url


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

    Manages a requests.Session with configurable request delay,
    automatic retry with exponential backoff on 429/503 responses and
    connection errors, an optional per-host requests-per-minute ceiling,
    and a circuit breaker that fails the run after repeated host errors.

    Args:
        base_url: Base URL prepended to relative paths.
        request_delay: Baseline delay in seconds between requests; ±20%
            random jitter is applied to avoid synchronous bursts.
        proxy: Optional SOCKS5/HTTP proxy URL (e.g. ``"socks5h://localhost:1080"``).
        max_rpm: Hard ceiling of requests per minute per target host.
            None (default) falls back to the process-wide default set by
            :func:`configure_defaults`. 0 disables the cap regardless of
            the default.
        circuit_breaker_threshold: After this many consecutive fully-failed
            calls to the same host (every retry exhausted), raise
            :class:`BlockedHostError` instead of retrying forever. None
            falls back to the process-wide default; 0 disables.
    """

    def __init__(
        self,
        base_url: str = "",
        request_delay: float = 0.2,
        proxy: str | None = None,
        max_rpm: int | None = None,
        circuit_breaker_threshold: int | None = None,
    ):
        self.base_url = base_url
        self.request_delay = request_delay
        self.max_rpm = max_rpm if max_rpm is not None else _DEFAULT_MAX_RPM
        self.circuit_breaker_threshold = (
            circuit_breaker_threshold
            if circuit_breaker_threshold is not None
            else _DEFAULT_CB_THRESHOLD
        )
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    def _pace(self, host: str) -> None:
        """Apply request_delay (with jitter) and per-host RPM cap."""
        if self.request_delay > 0:
            jitter = 1.0 + random.uniform(-REQUEST_JITTER_FRAC, REQUEST_JITTER_FRAC)
            time.sleep(self.request_delay * jitter)
        _LIMITER.wait(host, self.max_rpm)

    def _trip_if_blocked(self, host: str, exc: Exception) -> None:
        """Increment host failure count and raise BlockedHostError when tripped."""
        n = _LIMITER.record_failure(host)
        if self.circuit_breaker_threshold and n >= self.circuit_breaker_threshold:
            raise BlockedHostError(
                f"Circuit breaker tripped for {host} after {n} consecutive "
                f"failures (last: {type(exc).__name__}: {exc}). Stopping to "
                f"avoid hammering an unreachable target."
            ) from exc

    def _request_with_retry(self, method: str, url: str, **kwargs) -> Response:
        """Execute HTTP request with retry on 429/503 and connection errors.

        Retries up to MAX_RETRIES times. If response includes Retry-After
        header, its value is used as wait time; otherwise exponential backoff
        is applied (1s, 2s, 4s, ...).
        """
        host = _host_of(url)
        for attempt in range(MAX_RETRIES + 1):
            try:
                self._pace(host)
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
                _LIMITER.record_success(host)
                return resp
            except requests.ConnectionError as exc:
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
                self._trip_if_blocked(host, exc)
                raise
        # Unreachable in practice -- the last attempt either returns or raises
        exc = requests.ConnectionError(f"Failed after {MAX_RETRIES} retries: {url}")
        self._trip_if_blocked(host, exc)
        raise exc

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
