"""Provider for voris.wolterskluwer-online.de (Lower Saxony / Niedersachsen).

Fetches case law from the Niedersachsen VORIS portal (Wolters Kluwer Drupal site).
Data format: server-rendered HTML pages, GET-based search with query params.
"""

import logging
import re

import lxml.html
import requests

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

NS_BASE_URL = "https://voris.wolterskluwer-online.de"
NS_SEARCH_PATH = "/search"
NS_PER_PAGE = 12  # fixed by the portal
NS_MAX_PAGE = 5000


class NsCaseProvider(ScraperBaseClient, CaseProvider):
    """Fetches case law from voris.wolterskluwer-online.de (Niedersachsen).

    Uses GET-based search with query parameters. The site is a Drupal 11
    SSR application (no JavaScript rendering needed).

    Args:
        date_from: Optional start date filter (YYYY-MM-DD, not used in search URL yet).
        date_to: Optional end date filter (YYYY-MM-DD, not used in search URL yet).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "NI-VORIS Niedersachsen",
        "homepage": "https://voris.wolterskluwer-online.de",
    }

    def __init__(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
    ):
        super().__init__(base_url=NS_BASE_URL, request_delay=request_delay)
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    def _search_page(self, page: int) -> list[str]:
        """GET search page and extract document UUIDs from results.

        Returns list of document paths like '/browse/document/<uuid>'.
        """
        params = {
            "query": "*",
            "publicationtype": "publicationform-ats-filter!ATS_Rechtsprechung",
            "page": str(page),
        }

        resp = self._get(NS_SEARCH_PATH, params=params)

        links = []
        seen = set()
        for match in re.finditer(r"/browse/document/([a-f0-9-]{36})", resp.text):
            path = match.group(0)
            if path not in seen:
                seen.add(path)
                links.append(path)

        return links

    def _get_field_value(self, dl_element, field_name: str) -> str:
        """Extract field value from a <dl> element by matching <dt> text.

        Handles the WK pattern where [keine Angabe] is wrapped in a
        <span class="wkde-empty"> — returns empty string for those.
        """
        dts = dl_element.xpath("dt")
        dds = dl_element.xpath("dd")

        for dt, dd in zip(dts, dds):
            if dt.text_content().strip() == field_name:
                # Check for [keine Angabe] placeholder
                empty_spans = dd.xpath('.//span[@class="wkde-empty"]')
                if empty_spans:
                    return ""
                return dd.text_content().strip()
        return ""

    def _parse_case_from_html(self, html_str: str, source_url: str) -> dict | None:
        """Parse WK/VORIS case detail page and return OLDP case dict."""
        tree = lxml.html.fromstring(html_str)

        # Extract content from wkde-document-body section
        body_sections = tree.xpath('//*[contains(@class, "wkde-document-body")]')
        if not body_sections:
            logger.warning("Could not find document body from: %s", source_url)
            return None

        content = self.get_inner_html(body_sections[0])
        if not content or not content.strip():
            logger.warning("Empty document body from: %s", source_url)
            return None

        # Extract metadata from bibliography <dl>
        bib_sections = tree.xpath('//*[contains(@class, "wkde-bibliography")]//dl')
        if not bib_sections:
            logger.warning("Could not find bibliography from: %s", source_url)
            return None

        dl = bib_sections[0]

        # Replace non-breaking spaces with regular spaces (WK uses \xa0)
        court_name = self._get_field_value(dl, "Gericht").replace("\xa0", " ")
        date_raw = self._get_field_value(dl, "Datum")
        date = self.parse_german_date(date_raw) if date_raw else ""
        file_number = self._get_field_value(dl, "Aktenzeichen")
        ecli = self._get_field_value(dl, "ECLI")
        case_type = self._get_field_value(dl, "Entscheidungsform")

        if not court_name or not file_number:
            logger.warning("Missing court or file_number from: %s", source_url)
            return None

        case: dict = {
            "court_name": court_name,
            "file_number": file_number,
            "date": date,
            "content": content,
        }

        if case_type:
            case["type"] = case_type
        if ecli:
            case["ecli"] = ecli

        return case

    def get_cases(self) -> list[dict]:
        """Search VORIS and fetch individual case pages."""
        cases: list[dict] = []

        page = 0  # 0-indexed pagination
        empty_pages = 0

        while page <= NS_MAX_PAGE:
            try:
                links = self._search_page(page)
            except requests.RequestException as exc:
                logger.warning("Failed to search page %d: %s", page, exc)
                break

            if not links:
                empty_pages += 1
                if empty_pages >= 2:
                    break
                page += 1
                continue

            empty_pages = 0
            logger.info("Page %d: found %d case links", page, len(links))

            for doc_path in links:
                case_url = f"{NS_BASE_URL}{doc_path}"
                try:
                    html_str = self._get(doc_path).text
                except requests.RequestException as exc:
                    logger.warning("Failed to fetch case %s: %s", case_url, exc)
                    continue

                try:
                    case = self._parse_case_from_html(html_str, case_url)
                except Exception as exc:
                    logger.warning("Failed to parse case %s: %s", case_url, exc)
                    continue

                if case is not None:
                    cases.append(case)

                if self.limit and len(cases) >= self.limit:
                    return cases

            page += 1

        return cases
