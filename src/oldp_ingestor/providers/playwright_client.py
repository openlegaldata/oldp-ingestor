"""Base client for JavaScript-rendered pages using Playwright.

Provides headless browser support for sites that use JavaScript SPAs
(e.g. eJustiz Bürgerservice portals). Lazy browser initialization
and configurable pacing between page loads.
"""

import logging
import time

import lxml.html

logger = logging.getLogger(__name__)


class PlaywrightBaseClient:
    """Base client for JavaScript-rendered pages using Playwright.

    Uses sync Playwright API with lazy browser initialization.
    Browser is only started when the first page is fetched.

    Args:
        request_delay: Delay in seconds between page loads.
        headless: Whether to run browser in headless mode.
    """

    def __init__(self, request_delay: float = 0.5, headless: bool = True):
        self.request_delay = request_delay
        self.headless = headless
        self._playwright = None
        self._browser = None
        self._context = None

    def _ensure_browser(self):
        """Lazily start Playwright browser."""
        if self._browser is None:
            from playwright.sync_api import sync_playwright

            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(headless=self.headless)
            self._context = self._browser.new_context(
                user_agent="oldp-ingestor/0.1.2 (+https://github.com/openlegaldata)"
            )

    def _get_page_html(
        self, url: str, wait_selector: str | None = None, timeout: int = 30000
    ) -> str:
        """Navigate to URL, wait for content, return rendered HTML.

        Args:
            url: Full URL to navigate to.
            wait_selector: CSS selector to wait for before extracting HTML.
            timeout: Maximum wait time in milliseconds.
        """
        self._ensure_browser()

        if self.request_delay > 0:
            time.sleep(self.request_delay)

        page = self._context.new_page()
        try:
            logger.debug("Navigating to %s", url)
            page.goto(url, timeout=timeout)

            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=timeout)
                except Exception:
                    logger.warning(
                        "Timeout waiting for selector '%s' on %s",
                        wait_selector,
                        url,
                    )

            return page.content()
        finally:
            page.close()

    def _get_page_tree(
        self, url: str, wait_selector: str | None = None, timeout: int = 30000
    ) -> lxml.html.HtmlElement:
        """Navigate to URL and return parsed lxml tree."""
        html = self._get_page_html(url, wait_selector=wait_selector, timeout=timeout)
        return lxml.html.fromstring(html)

    def close(self) -> None:
        """Close browser and playwright instance."""
        if self._context is not None:
            self._context.close()
            self._context = None
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright is not None:
            self._playwright.stop()
            self._playwright = None
