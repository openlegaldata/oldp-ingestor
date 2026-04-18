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

Supports an optional XML cache directory (``cache_dir``) so that downloaded
XMLs are persisted to disk. On subsequent runs the cached file is read
instead of re-downloading the ZIP, allowing interrupted runs to resume
without repeating expensive network requests.
"""

import logging
import os
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

    SEARCH_URL = (
        "https://www.rechtsprechung-im-internet.de/jportal/portal/t/xs6"
        "/page/bsjrsprod.psml/js_peid/Suchportlet2"
    )
    WAIT_SELECTOR = ".resultList, #resultListDiv, .result-list-entry"
    SPA_TIMEOUT = 15000

    def __init__(
        self,
        court: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
        proxy: str | None = None,
        cache_dir: str | None = None,
    ):
        ScraperBaseClient.__init__(
            self, base_url=RII_BASE_URL, request_delay=request_delay, proxy=proxy
        )
        # Initialize Playwright attributes (used when date filtering)
        self.headless = True
        self._playwright = None
        self._browser = None
        self._context = None
        self.proxy = proxy
        self.court = court
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit
        self.cache_dir = cache_dir
        self._date_search_submitted = False
        self._session_token: str = ""
        self._seen_doc_ids: set[str] = set()
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    @staticmethod
    def _iso_to_german_date(iso_date: str) -> str:
        """Convert YYYY-MM-DD to DD.MM.YYYY."""
        parts = iso_date.split("-")
        if len(parts) == 3:
            return f"{parts[2]}.{parts[1]}.{parts[0]}"
        return iso_date

    @staticmethod
    def _extract_session_token(url: str) -> str:
        """Extract jPortal session token from URL (e.g. ``/t/12f6/`` → ``12f6``)."""
        match = re.search(r"/t/([^/]+)/", url)
        return match.group(1) if match else ""

    def _submit_date_search(self) -> list[str]:
        """Use Playwright to submit the extended search with date fields.

        Flow: main page -> click "Entscheidungssuche" -> click "Erweiterte Suche"
        -> fill #dateFrom/#dateTo -> submit -> extract doc IDs.

        Captures the server-side session token from the post-submit URL so that
        subsequent pagination requests stay within the filtered session.
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

            # Set date fields via JS to bypass the portal's auto-copy behaviour
            # (filling dateFrom triggers JS that copies the value into dateTo,
            # causing concatenation when dateTo is filled afterwards).
            if self.date_from:
                val = self._iso_to_german_date(self.date_from)
                page.evaluate(f'document.querySelector("#dateFrom").value = "{val}"')
            if self.date_to:
                val = self._iso_to_german_date(self.date_to)
                page.evaluate(f'document.querySelector("#dateTo").value = "{val}"')

            # Submit — value is lowercase "suchen"
            page.locator('input[type="submit"][name="standardsuche"]').click()

            try:
                page.wait_for_selector(self.WAIT_SELECTOR, timeout=self.SPA_TIMEOUT)
            except Exception:
                logger.warning(
                    "Timeout waiting for RII search results after date submit"
                )

            # Capture session token from post-submit URL for pagination
            self._session_token = self._extract_session_token(page.url)
            if self._session_token:
                logger.info("Captured session token: %s", self._session_token)
            else:
                logger.warning("Could not extract session token from %s", page.url)

            self._date_search_submitted = True
            html = page.content()
            ids = list(set(re.findall(r"doc\.id=([a-zA-Z0-9-]+?)&", html)))
            self._seen_doc_ids.update(ids)
            return ids
        finally:
            page.close()

    def _get_date_search_page(self, page_num: int) -> list[str]:
        """Get subsequent pages using the captured session token.

        Uses the session token from the initial date search submission so that
        pagination stays within the server-side filtered result set.
        """
        per_page = 26
        offset = (page_num - 1) * per_page
        token = self._session_token or "1jph"
        url = (
            f"{self.base_url}/portal/t/{token}/page/bsjrsprod.psml"
            f"/js_peid/Suchportlet2?action=portlets.jw.MainAction"
            f"&eventSubmit_doNavigate=searchInSubtree"
            f"&currentNavigationPosition={offset}"
        )
        html = self._get_page_html(
            url, wait_selector=self.WAIT_SELECTOR, timeout=self.SPA_TIMEOUT
        )
        ids = list(set(re.findall(r"doc\.id=([a-zA-Z0-9-]+?)&", html)))
        # Deduplicate across pages
        new_ids = [i for i in ids if i not in self._seen_doc_ids]
        self._seen_doc_ids.update(new_ids)
        return new_ids

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

    @staticmethod
    def _extract_dates_from_listing(html: str) -> list[str]:
        """Extract DD.MM.YYYY dates from the result listing HTML.

        The listing shows dates in ``<span>DD.MM.YYYY</span>`` elements
        in the first column, sorted newest-first.  Returns ISO dates
        (YYYY-MM-DD) in page order.
        """
        raw = re.findall(r"<span\s*>\s*(\d{2}\.\d{2}\.\d{4})\s*</span>", html)
        iso: list[str] = []
        for d in raw:
            parts = d.split(".")
            if len(parts) == 3:
                iso.append(f"{parts[2]}-{parts[1]}-{parts[0]}")
        return iso

    def _get_ids_and_dates_from_page(self, url: str) -> tuple[list[str], list[str]]:
        """Extract doc IDs and dates from a listing page."""
        text = self._get(url).text
        ids = list(set(re.findall(r"doc\.id=([a-zA-Z0-9-]+?)&", text)))
        dates = self._extract_dates_from_listing(text)
        return ids, dates

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

    def _parse_case_from_xml(self, xml_str: str, source_url: str = "") -> dict | None:
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
            "source_url": source_url,
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

    def _get_xml_for_doc(self, doc_id: str) -> str | None:
        """Get XML for a doc ID, using cache if available."""
        if self.cache_dir:
            cache_path = os.path.join(self.cache_dir, f"{doc_id}.xml")
            if os.path.exists(cache_path):
                with open(cache_path, encoding="utf-8") as f:
                    return f.read()

        zip_url = self._get_zip_url(doc_id)
        try:
            xml_str = self._get_xml_from_zip(zip_url)
        except requests.RequestException as exc:
            logger.warning("Failed to download ZIP for %s: %s", doc_id, exc)
            return None

        if xml_str is not None and self.cache_dir:
            cache_path = os.path.join(self.cache_dir, f"{doc_id}.xml")
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(xml_str)

        return xml_str

    def _fetch_and_parse_cases(self, ids: list[str], cases: list[dict]) -> bool:
        """Download ZIP/XML for doc IDs and append parsed cases.

        Applies client-side date filtering after parsing each case to avoid
        accumulating out-of-range cases in memory. Uses the XML cache when
        ``cache_dir`` is set so that interrupted runs can resume cheaply.

        Returns True if the limit was reached.
        """
        for doc_id in ids:
            xml_str = self._get_xml_for_doc(doc_id)
            if xml_str is None:
                continue

            zip_url = self._get_zip_url(doc_id)
            try:
                case = self._parse_case_from_xml(xml_str, source_url=zip_url)
            except Exception as exc:
                logger.warning("Failed to parse XML for %s: %s", doc_id, exc)
                continue

            if case is None:
                continue

            # Client-side date filter — discard immediately to save memory
            if not self._is_within_date_range(case.get("date", "")):
                continue

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
        """Fetch cases per court with early-stop on date.

        The per-court listing is sorted newest-first.  When ``date_from``
        is set, pagination stops as soon as ALL dates on a page are older
        than ``date_from`` — no need to download ZIPs for cases that will
        be filtered out anyway.
        """
        cases: list[dict] = []
        courts = [self.court] if self.court else COURTS

        for court_code in courts:
            page = 1
            empty_pages = 0

            while True:
                url = self._get_page_url(page, court_code)
                try:
                    ids, page_dates = self._get_ids_and_dates_from_page(url)
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

                # Early-stop: if ALL dates on the page are before date_from,
                # no subsequent pages can contain in-range cases (newest-first).
                if self.date_from and page_dates:
                    newest_on_page = max(page_dates)
                    if newest_on_page < self.date_from:
                        logger.info(
                            "Court %s: early stop at page %d "
                            "(newest date %s < date_from %s)",
                            court_code,
                            page,
                            newest_on_page,
                            self.date_from,
                        )
                        break

                if self._fetch_and_parse_cases(ids, cases):
                    return cases
                page += 1

        return cases

    def _build_search_url(self, offset: int = 0) -> str:
        """Build Suchportlet2 GET URL with server-side date filtering.

        Uses ``dateFrom``/``dateTo`` GET params (DD.MM.YYYY) which are
        honoured by the jPortal backend.  Results are sorted oldest-first
        (``sortmethod=date_inc``) so that the most recent page can be
        detected as the last one.
        """
        params = (
            f"action=portlets.jw.MainAction"
            f"&eventSubmit_doSearch=suchen"
            f"&form=jurisExpertSearch"
            f"&desc=date&query=date"
            f"&sortmethod=date_inc"
            f"&standardsuche=suchen"
            f"&currentNavigationPosition={offset}"
        )
        if self.date_from:
            params += f"&dateFrom={self._iso_to_german_date(self.date_from)}"
        if self.date_to:
            params += f"&dateTo={self._iso_to_german_date(self.date_to)}"
        return f"{self.SEARCH_URL}?{params}"

    def _get_cases_with_dates(self) -> list[dict]:
        """Fetch cases using HTTP GET with server-side date filtering.

        The Suchportlet2 endpoint supports ``dateFrom``/``dateTo`` GET
        parameters.  Results are sorted oldest-first so pagination ends
        naturally when a page returns no results.
        """
        cases: list[dict] = []
        offset = 0
        per_page = 26
        empty_pages = 0

        while True:
            url = self._build_search_url(offset)
            try:
                text = self._get(url).text
            except requests.RequestException as exc:
                logger.warning("Failed date search at offset %d: %s", offset, exc)
                break

            ids = list(set(re.findall(r"doc\.id=([a-zA-Z0-9-]+?)&", text)))
            if not ids:
                empty_pages += 1
                if empty_pages >= 2:
                    break
                offset += per_page
                continue

            empty_pages = 0
            page_num = offset // per_page + 1
            logger.info(
                "Date search page %d (offset %d): found %d doc IDs",
                page_num,
                offset,
                len(ids),
            )

            if self._fetch_and_parse_cases(ids, cases):
                return cases
            offset += per_page

        return cases

    def get_cases(self) -> list[dict]:
        """Paginate listings, download ZIP/XML for each case.

        When ``date_from`` or ``date_to`` are set, uses the Suchportlet2
        HTTP GET endpoint with server-side date filtering (only matching
        cases are returned).  Otherwise falls back to the per-court
        listing which returns all cases.  When ``cache_dir`` is set,
        downloaded XMLs are persisted to disk so interrupted runs can
        resume without re-downloading.
        """
        if self.date_from or self.date_to:
            return self._get_cases_with_dates()
        return self._get_cases_full_fetch()
