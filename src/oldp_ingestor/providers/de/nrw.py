"""Provider for nrwesuche.justiz.nrw.de (North Rhine-Westphalia).

Fetches case law from the NRW legal database.
Data format: HTML pages, POST-based search with session cookies.
"""

import logging
import re

import lxml.html
import requests
from lxml.cssselect import CSSSelector

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

NRW_BASE_URL = "https://nrwesuche.justiz.nrw.de"
NRW_SEARCH_URL = f"{NRW_BASE_URL}/index.php"
NRW_PER_PAGE = 100
NRW_MAX_PAGE = 1600


class NrwCaseProvider(ScraperBaseClient, CaseProvider):
    """Fetches case law from nrwesuche.justiz.nrw.de.

    Uses POST-based search with form data. Session-based with cookies.

    Args:
        court_type: Optional filter by court type (gerichtstyp).
        date_from: Optional start date filter (DD.MM.YYYY format for NRW).
        date_to: Optional end date filter (DD.MM.YYYY format for NRW).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "NRWE Rechtsprechungsdatenbank",
        "homepage": "https://nrwesuche.justiz.nrw.de",
    }

    def __init__(
        self,
        court_type: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
    ):
        super().__init__(base_url=NRW_BASE_URL, request_delay=request_delay)
        self.court_type = court_type or ""
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    def _search_page(self, page: int) -> list[str]:
        """POST search form and extract case paths from results."""
        page_str = str(page)
        data = {
            "gerichtstyp": self.court_type,
            "von2": "",
            "q": "*",
            "absenden": "Suchen",
            "schlagwoerter": "",
            "method": "stem",
            "von": self.date_from,
            "aktenzeichen": "",
            "bis": self.date_to,
            "bis2": "",
            "advanced_search": "false",
            "sortieren_nach": "datum_absteigend",
            "date": "",
            "qSize": NRW_PER_PAGE,
            "entscheidungsart": "",
            "gerichtsort": "",
            "validFrom": "",
            "gerichtsbarkeit": "",
            f"page{page_str}": page_str,
        }

        resp = self._post(f"{NRW_SEARCH_URL}#solrNrwe", data=data)

        tree = lxml.html.fromstring(resp.text)
        links = []
        for link in CSSSelector(".einErgebnis a")(tree):
            href = link.attrib.get("href", "")
            if href:
                links.append(href)

        return links

    def _get_field_value(self, tree, field_name: str) -> str:
        """Extract field value from NRW HTML div structure."""
        values = tree.xpath(
            f'//div[contains(@class, "feldbezeichnung") and text()='
            f'"{field_name}:"]/following-sibling::div[1]/text()'
        )
        return "\n".join(values).strip()

    def _parse_case_from_html(self, html_str: str, source_url: str) -> dict | None:
        """Parse NRW HTML case page and return OLDP case dict."""
        tree = lxml.html.fromstring(html_str)

        # Extract content from p.absatzLinks parent
        content = None
        for m in tree.xpath('//p[contains(@class, "absatzLinks")]'):
            content = self.get_inner_html(m.getparent())
            break

        if content is None:
            logger.warning("Could not find content from: %s", source_url)
            return None

        # Prepend tenor to content
        for tenor_match in tree.xpath(
            '//div[contains(@class, "feldbezeichnung") and text()="Tenor:"]'
            "/following-sibling::div[1]"
        ):
            tenor = self.get_inner_html(tenor_match).strip()
            content = (
                "<h2>Tenor</h2>\n\n" + tenor + '<br style="clear:both">\n\n' + content
            )

        # Mark section headlines
        content = re.sub(
            r'class="absatzLinks">(\s?[A-Z](\s[a-z]){4,})',
            r'class="h2 absatzLinks">\1',
            content,
        )

        court_name = self._get_field_value(tree, "Gericht")
        date_raw = self._get_field_value(tree, "Datum")
        date = self.parse_german_date(date_raw) if date_raw else ""
        file_number = self._get_field_value(tree, "Aktenzeichen")
        ecli = self._get_field_value(tree, "ECLI")
        case_type = self._get_field_value(tree, "Entscheidungsart")

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
        """Search NRW and fetch individual case pages."""
        cases: list[dict] = []

        page = 1
        empty_pages = 0

        while page <= NRW_MAX_PAGE:
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

            for case_url in links:
                try:
                    html_str = self._get(case_url).text
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
