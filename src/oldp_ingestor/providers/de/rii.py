"""Provider for rechtsprechung-im-internet.de (federal courts).

Fetches case law from all German federal courts:
- BVerfG (Bundesverfassungsgericht)
- BGH (Bundesgerichtshof)
- BVerwG (Bundesverwaltungsgericht)
- BFH (Bundesfinanzhof)
- BAG (Bundesarbeitsgericht)
- BSG (Bundessozialgericht)
- BPatG (Bundespatentgericht)

Data format: ZIP files containing XML documents.
"""

import logging
import re

import requests
from lxml import etree

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

RII_BASE_URL = "https://www.rechtsprechung-im-internet.de/jportal"

COURTS = ["bverfg", "bgh", "bverwg", "bfh", "bag", "bsg", "bpatg"]

# Court name normalization: RII XML uses "type location" format for some courts.
# Federal courts with a location suffix need mapping to DB names.
_COURT_NAME_MAP = {
    "BPatG München": "Bundespatentgericht",
}

# Sections in XML that contain the case content
CONTENT_TAGS = [
    ("tenor", "Tenor"),
    ("tatbestand", "Tatbestand"),
    ("entscheidungsgruende", "Entscheidungsgründe"),
    ("gruende", "Gründe"),
    ("abwmeinung", "Abw. Meinung"),
    ("sonstlt", "Sonstige Literatur"),
]


class RiiCaseProvider(ScraperBaseClient, PlaywrightBaseClient, CaseProvider):
    """Fetches case law from rechtsprechung-im-internet.de (federal courts).

    For each court, paginates through search results, downloads ZIP files
    containing XML decisions, and maps XML fields to OLDP case dicts.

    Supports server-side date filtering via Playwright extended search form.
    When date_from/date_to are set, Playwright navigates to the search page,
    clicks "Erweiterte Suche", fills ``#dateFrom``/``#dateTo`` fields
    (DD.MM.YYYY format), and submits the form. The portal returns only
    matching results. When no dates are set, falls back to the fast
    HTTP-only per-court listing (no Playwright needed).

    Args:
        court: Optional single court code (e.g. "bverfg"). If None, all courts.
        date_from: Optional start date (YYYY-MM-DD). Triggers Playwright-based
            extended search with server-side date filtering via ``#dateFrom``.
        date_to: Optional end date (YYYY-MM-DD). Triggers Playwright-based
            extended search with server-side date filtering via ``#dateTo``.
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "Rechtsprechung im Internet (RII)",
        "homepage": "https://www.rechtsprechung-im-internet.de",
    }

    SEARCH_URL = "https://www.rechtsprechung-im-internet.de/jportal/portal/t/1jph/page/bsjrsprod.psml/js_peid/Suchportlet2"
    WAIT_SELECTOR = ".resultList, #resultListDiv, .result-list-entry"
    SPA_TIMEOUT = 15000

    def __init__(
        self,
        court: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
    ):
        ScraperBaseClient.__init__(
            self, base_url=RII_BASE_URL, request_delay=request_delay
        )
        # Initialize Playwright attributes (used when date filtering)
        self.headless = True
        self._playwright = None
        self._browser = None
        self._context = None
        self.court = court
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit
        self._date_search_submitted = False

    @staticmethod
    def _iso_to_german_date(iso_date: str) -> str:
        """Convert YYYY-MM-DD to DD.MM.YYYY."""
        parts = iso_date.split("-")
        if len(parts) == 3:
            return f"{parts[2]}.{parts[1]}.{parts[0]}"
        return iso_date

    def _submit_date_search(self) -> list[str]:
        """Use Playwright to submit the extended search with date fields.

        Flow: main page -> click "Entscheidungssuche" -> click "Erweiterte Suche"
        -> fill #dateFrom/#dateTo -> submit -> extract doc IDs.
        """
        import time

        self._ensure_browser()

        logger.info(
            "Submitting RII extended search with dates: %s to %s",
            self.date_from,
            self.date_to,
        )

        if self.request_delay > 0:
            time.sleep(self.request_delay)

        page = self._context.new_page()
        try:
            # Navigate to main page
            page.goto(
                "https://www.rechtsprechung-im-internet.de/",
                timeout=self.SPA_TIMEOUT,
            )
            page.wait_for_load_state("networkidle", timeout=self.SPA_TIMEOUT)

            # Click "Entscheidungssuche"
            page.locator("a:has-text('Entscheidungssuche')").first.click()
            page.wait_for_load_state("networkidle", timeout=self.SPA_TIMEOUT)

            # Click "Erweiterte Suche" to reveal date fields
            page.locator("text=Erweiterte Suche").first.click()
            page.wait_for_selector("#dateFrom", timeout=self.SPA_TIMEOUT)

            # Fill date fields — clear first to avoid value concatenation
            if self.date_from:
                from_field = page.locator("#dateFrom")
                from_field.click()
                from_field.fill("")
                from_field.type(self._iso_to_german_date(self.date_from))

            if self.date_to:
                to_field = page.locator("#dateTo")
                to_field.click()
                to_field.fill("")
                to_field.type(self._iso_to_german_date(self.date_to))

            # Submit — value is lowercase "suchen"
            page.locator('input[type="submit"][name="standardsuche"]').click()

            try:
                page.wait_for_selector(self.WAIT_SELECTOR, timeout=self.SPA_TIMEOUT)
            except Exception:
                logger.warning(
                    "Timeout waiting for RII search results after date submit"
                )

            self._date_search_submitted = True
            html = page.content()
            return list(set(re.findall(r"doc\.id=([a-zA-Z0-9-]+?)&", html)))
        finally:
            page.close()

    def _get_date_search_page(self, page_num: int) -> list[str]:
        """Get subsequent pages after the initial date search."""
        per_page = 26
        offset = (page_num - 1) * per_page
        url = (
            f"{self.base_url}/portal/t/1jph/page/bsjrsprod.psml"
            f"/js_peid/Suchportlet2?action=portlets.jw.MainAction"
            f"&eventSubmit_doNavigate=searchInSubtree"
            f"&currentNavigationPosition={offset}"
        )
        html = self._get_page_html(
            url, wait_selector=self.WAIT_SELECTOR, timeout=self.SPA_TIMEOUT
        )
        return list(set(re.findall(r"doc\.id=([a-zA-Z0-9-]+?)&", html)))

    def _get_page_url(self, page: int, court: str, per_page: int = 26) -> str:
        """Build listing URL for a court's decisions (no date filter)."""
        offset = (page - 1) * per_page
        return (
            f"{self.base_url}/portal/t/xs6/page/bsjrsprod.psml"
            f"/js_peid/Suchportlet1?action=portlets.jw.MainAction"
            f"&eventSubmit_doNavigate=searchInSubtree&p1={court}"
            f"&currentNavigationPosition={offset}"
        )

    def _get_ids_from_page(self, url: str) -> list[str]:
        """Extract doc IDs from search results page (HTTP, no Playwright)."""
        text = self._get(url).text
        ids = list(set(re.findall(r"doc\.id=([a-zA-Z0-9-]+?)&", text)))
        return ids

    def _get_zip_url(self, doc_id: str) -> str:
        """URL to ZIP file for a document."""
        return f"{self.base_url}/docs/bsjrs/{doc_id}.zip"

    @staticmethod
    def _get_tag_text(
        tree,
        tag_name: str,
        default: str | None = None,
        join_multiple_with: str = "\n",
    ) -> str | None:
        """Get text content of an XML tag under //dokument/."""
        matches = tree.xpath(f"//dokument/{tag_name}/text()")
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            return join_multiple_with.join(matches)
        return default

    def _parse_case_from_xml(self, xml_str: str) -> dict | None:
        """Parse XML decision and return OLDP case dict.

        Returns None if access rights are not public or content is empty.
        """
        tree = etree.fromstring(xml_str.encode())

        # Skip non-public documents
        if self._get_tag_text(tree, "accessRights") != "public":
            return None

        # Build court name from gertyp + gerort
        court_type = self._get_tag_text(tree, "gertyp", default="")
        court_location = self._get_tag_text(tree, "gerort", default="")
        if court_location:
            court_name = f"{court_type} {court_location}".strip()
        else:
            court_name = court_type or ""
        court_name = _COURT_NAME_MAP.get(court_name, court_name)

        # Parse date: YYYYMMDD -> YYYY-MM-DD
        date_str = self._get_tag_text(tree, "entsch-datum", default="")
        if len(date_str) == 8:
            date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        else:
            date = date_str

        # Build content HTML from sections
        content = self._build_content_html(tree, CONTENT_TAGS)

        # Build abstract from leitsatz
        abstract = self._build_content_html(
            tree, [("leitsatz", "Leitsatz")], with_headline=False
        )

        if not content.strip():
            return None

        case: dict = {
            "court_name": court_name,
            "file_number": self._get_tag_text(tree, "aktenzeichen", default=""),
            "date": date,
            "content": content,
        }

        # Optional fields
        case_type = self._get_tag_text(tree, "doktyp")
        if case_type:
            case["type"] = case_type

        ecli = self._get_tag_text(tree, "ecli")
        if ecli:
            case["ecli"] = ecli

        if abstract.strip():
            case["abstract"] = abstract

        return case

    def _fetch_and_parse_cases(self, ids: list[str], cases: list[dict]) -> bool:
        """Download ZIP/XML for doc IDs and append parsed cases.

        Returns True if the limit was reached.
        """
        for doc_id in ids:
            zip_url = self._get_zip_url(doc_id)
            try:
                xml_str = self._get_xml_from_zip(zip_url)
            except requests.RequestException as exc:
                logger.warning("Failed to download ZIP for %s: %s", doc_id, exc)
                continue

            if xml_str is None:
                continue

            try:
                case = self._parse_case_from_xml(xml_str)
            except Exception as exc:
                logger.warning("Failed to parse XML for %s: %s", doc_id, exc)
                continue

            if case is not None:
                cases.append(case)

            if self.limit and len(cases) >= self.limit:
                return True
        return False

    def _get_cases_with_dates(self) -> list[dict]:
        """Fetch cases using Playwright extended search with date filter."""
        cases: list[dict] = []

        try:
            # Page 1: submit the date search form via Playwright
            ids = self._submit_date_search()
            if not ids:
                return cases

            logger.info("Date search page 1: found %d doc IDs", len(ids))
            if self._fetch_and_parse_cases(ids, cases):
                return cases

            # Subsequent pages via Playwright navigation
            page = 2
            empty_pages = 0
            while True:
                try:
                    ids = self._get_date_search_page(page)
                except Exception as exc:
                    logger.warning("Failed date search page %d: %s", page, exc)
                    break

                if not ids:
                    empty_pages += 1
                    if empty_pages >= 2:
                        break
                    page += 1
                    continue

                empty_pages = 0
                logger.info("Date search page %d: found %d doc IDs", page, len(ids))
                if self._fetch_and_parse_cases(ids, cases):
                    return cases
                page += 1
        finally:
            self.close()

        return cases

    def _get_cases_full_fetch(self) -> list[dict]:
        """Fetch all cases per court without date filter (original behavior)."""
        cases: list[dict] = []
        courts = [self.court] if self.court else COURTS

        for court_code in courts:
            page = 1
            empty_pages = 0

            while True:
                url = self._get_page_url(page, court_code)
                try:
                    ids = self._get_ids_from_page(url)
                except requests.RequestException as exc:
                    logger.warning(
                        "Failed to fetch page %d for %s: %s", page, court_code, exc
                    )
                    break

                if not ids:
                    empty_pages += 1
                    if empty_pages >= 2:
                        break
                    page += 1
                    continue

                empty_pages = 0
                logger.info(
                    "Court %s page %d: found %d doc IDs",
                    court_code,
                    page,
                    len(ids),
                )

                if self._fetch_and_parse_cases(ids, cases):
                    return cases
                page += 1

        return cases

    def get_cases(self) -> list[dict]:
        """Paginate listings, download ZIP/XML for each case.

        Uses Playwright extended search when date_from/date_to are set.
        Otherwise uses the fast HTTP-only per-court listing.
        """
        if self.date_from or self.date_to:
            return self._get_cases_with_dates()
        return self._get_cases_full_fetch()
