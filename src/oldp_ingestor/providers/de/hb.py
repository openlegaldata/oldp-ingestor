"""Provider for Bremen court decisions.

Fetches case law from 5 Bremen court portals (SixCMS-based) with identical
HTML structure. Content is PDF-only; text is extracted via pymupdf.

Courts:
  - OLG Bremen (oberlandesgericht.bremen.de)
  - OVG Bremen (oberverwaltungsgericht.bremen.de)
  - VG Bremen (verwaltungsgericht.bremen.de)
  - LAG Bremen (landesarbeitsgericht.bremen.de)
  - StGH Bremen (staatsgerichtshof.bremen.de)
"""

import logging
import re

import lxml.html
import requests
from lxml.cssselect import CSSSelector

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

COURTS = {
    "olg": {
        "base_url": "https://oberlandesgericht.bremen.de",
        "path": "/entscheidungen/entscheidungsuebersicht-2335",
        "court_name": "Hanseatisches Oberlandesgericht in Bremen",
    },
    "ovg": {
        "base_url": "https://oberverwaltungsgericht.bremen.de",
        "path": "/entscheidungen/entscheidungsuebersicht-11265",
        "court_name": "Oberverwaltungsgericht der Freien Hansestadt Bremen",
    },
    "vg": {
        "base_url": "https://verwaltungsgericht.bremen.de",
        "path": "/entscheidungen/entscheidungsuebersicht-13039",
        "court_name": "Verwaltungsgericht der Freien Hansestadt Bremen",
    },
    "lag": {
        "base_url": "https://landesarbeitsgericht.bremen.de",
        "path": "/entscheidungen/entscheidungsuebersicht-11508",
        "court_name": "Landesarbeitsgericht Bremen",
    },
    "stgh": {
        "base_url": "https://staatsgerichtshof.bremen.de",
        "path": "/entscheidungen/entscheidungsuebersicht-11569",
        "court_name": "Staatsgerichtshof der Freien Hansestadt Bremen",
    },
}

HB_PAGE_SIZE = 100


class BremenCaseProvider(ScraperBaseClient, CaseProvider):
    """Fetches case law from Bremen court portals.

    All five Bremen court portals use identical SixCMS HTML structure.
    Decisions are PDF-only; text is extracted via pymupdf.

    Args:
        court: Filter to single court key (olg, ovg, vg, lag, stgh).
            If None, all courts are scraped.
        date_from: Only include decisions on or after this date (YYYY-MM-DD).
        date_to: Only include decisions on or before this date (YYYY-MM-DD).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "Justiz Bremen",
        "homepage": "https://www.justiz.bremen.de",
    }

    def __init__(
        self,
        court: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
    ):
        # base_url is set per court during scraping
        super().__init__(base_url="", request_delay=request_delay)
        self.court = court
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    def _get_courts(self) -> list[dict]:
        """Return list of court configs to scrape."""
        if self.court:
            key = self.court.lower()
            if key not in COURTS:
                logger.error("Unknown court '%s', valid: %s", key, list(COURTS))
                return []
            return [COURTS[key]]
        return list(COURTS.values())

    def _fetch_listing_page(
        self, base_url: str, path: str, skip: int
    ) -> lxml.html.HtmlElement:
        """Fetch one page of the listing and return parsed tree."""
        url = f"{base_url}{path}?max={HB_PAGE_SIZE}&skip={skip}"
        resp = self._get(url)
        return lxml.html.fromstring(resp.text.replace("\r\n", "\n"))

    def _parse_listing_rows(
        self, tree: lxml.html.HtmlElement, court_cfg: dict
    ) -> list[dict]:
        """Parse search-result rows from a listing page."""
        rows = CSSSelector("tr.search-result")(tree)
        cases = []
        for row in rows:
            try:
                case = self._parse_row(row, court_cfg)
                if case:
                    cases.append(case)
            except Exception as exc:
                logger.warning("Failed to parse row: %s", exc)
        return cases

    def _parse_row(self, row, court_cfg: dict) -> dict | None:
        """Parse a single <tr class="search-result"> row."""
        date = row.get("data-date", "")

        # Client-side date filtering
        if self.date_from and date < self.date_from:
            return None
        if self.date_to and date > self.date_to:
            return None

        tds = row.findall("td")
        if len(tds) < 2:
            return None

        # Left td: date, file_number, norms, legal_area, type
        # Fields are separated by <br> tags; text_content() loses them.
        # Serialize inner HTML and split on <br> to get fields.
        left_td = tds[0]
        from lxml import etree

        left_html = etree.tostring(left_td, encoding="unicode", method="html")
        # Split on <br>, strip tags and whitespace
        parts = re.split(r"<br\s*/?>", left_html, flags=re.IGNORECASE)
        left_lines = [self.strip_tags(p).strip() for p in parts]
        left_lines = [line for line in left_lines if line]
        # Typical: ['29.01.2026', '2 U 106/22', '§§ 823 ...', 'Zivilrecht', 'Urteil']
        file_number = left_lines[1] if len(left_lines) > 1 else ""
        case_type = left_lines[-1] if len(left_lines) > 2 else ""

        # Right td: PDF link, abstract, detail link
        right_td = tds[1]
        pdf_link = None
        title = ""
        detail_href = None

        for link in right_td.findall(".//a"):
            href = link.get("href", "")
            if "/sixcms/media.php/" in href and href.endswith(".pdf"):
                pdf_link = href
                link_text = link.text_content().strip()
                # Strip "(pdf, NNN KB)" suffix
                title = re.sub(r"\s*\(pdf,\s*[\d.,]+\s*[KMG]?B\)\s*$", "", link_text)
            elif "detail.php?gsid=" in href:
                detail_href = href

        if not file_number:
            return None

        case: dict = {
            "court_name": court_cfg["court_name"],
            "file_number": file_number,
            "date": date,
        }

        if case_type and case_type in ("Urteil", "Beschluss", "Sonstiges"):
            case["type"] = case_type
        if title:
            case["title"] = title

        # Fetch PDF content
        if pdf_link:
            pdf_url = f"{court_cfg['base_url']}{pdf_link}"
            try:
                content = self._extract_text_from_pdf(pdf_url)
                if content and len(content) >= 10:
                    case["content"] = content
            except Exception as exc:
                logger.warning("Failed to extract PDF for %s: %s", file_number, exc)

        if "content" not in case:
            logger.debug("No content for %s, skipping", file_number)
            return None

        # Optionally fetch detail page for full abstract
        if detail_href:
            try:
                abstract = self._fetch_abstract(court_cfg["base_url"], detail_href)
                if abstract:
                    case["abstract"] = abstract
            except Exception as exc:
                logger.debug("Failed to fetch detail for %s: %s", file_number, exc)

        return case

    def _fetch_abstract(self, base_url: str, detail_href: str) -> str | None:
        """Fetch detail page and extract Leitsatz."""
        url = f"{base_url}/entscheidungen/{detail_href}"
        tree = self._get_html_tree(url)

        for info_div in CSSSelector("div.project_info")(tree):
            left = info_div.find('.//div[@class="project_info_left"]')
            right = info_div.find('.//div[@class="project_info_right"]')
            if left is not None and right is not None:
                label = left.text_content().strip()
                if label == "Leitsatz":
                    return right.text_content().strip() or None
        return None

    def get_cases(self) -> list[dict]:
        """Scrape all configured courts and return cases."""
        cases: list[dict] = []
        courts = self._get_courts()

        for court_cfg in courts:
            base_url = court_cfg["base_url"]
            path = court_cfg["path"]
            self.base_url = base_url

            logger.info("Scraping %s ...", court_cfg["court_name"])

            skip = 0
            while True:
                try:
                    tree = self._fetch_listing_page(base_url, path, skip)
                except requests.RequestException as exc:
                    logger.warning("Failed to fetch listing at skip=%d: %s", skip, exc)
                    break

                page_cases = self._parse_listing_rows(tree, court_cfg)

                if not page_cases and skip > 0:
                    break

                for case in page_cases:
                    cases.append(case)
                    if self.limit and len(cases) >= self.limit:
                        return cases

                # Check if there are more pages
                rows = CSSSelector("tr.search-result")(tree)
                if len(rows) < HB_PAGE_SIZE:
                    break

                skip += HB_PAGE_SIZE

        return cases
