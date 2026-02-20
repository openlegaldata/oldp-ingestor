"""Provider for Verfassungsgerichtshof des Freistaates Sachsen.

Fetches case law from justiz.sachsen.de/esaver/.
AJAX-based search returning HTML fragments. Content is PDF-only.
No Playwright needed — plain HTTP POST is sufficient.
"""

import logging
import re

import lxml.html
import requests

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

SN_VERFGH_BASE_URL = "https://www.justiz.sachsen.de/esaver"
SEARCH_ENDPOINT = f"{SN_VERFGH_BASE_URL}/answers.php"

GERMAN_MONTHS = {
    "Januar": "01",
    "Februar": "02",
    "März": "03",
    "April": "04",
    "Mai": "05",
    "Juni": "06",
    "Juli": "07",
    "August": "08",
    "September": "09",
    "Oktober": "10",
    "November": "11",
    "Dezember": "12",
}

# Patterns for h4 text.  Multiple legacy formats exist:
#   "SächsVerfGH, Beschluss vom 15. Januar 2026 - Vf. 18-IV-25"
#   "SächsVerfGH - Beschluss vom 8. Dezember 2011 - Vf. 85-IV-11"
#   "Beschluss des SächsVerfGH vom 25. Oktober 2007 - Vf. 90-IV-06"
#   "Beschluss vom 29. November 2018 - Vf. 60-IV-18"
#   "SächsVerfGH, Beschluss vom 28.Juni 2006 - Vf. 26-IV-06"
_H4_PATTERNS = [
    # Primary: "SächsVerfGH, TYPE vom DD. MONTH YYYY - FILE"
    re.compile(
        r"S.chsVerfGH[,\s-]+\s*(Beschluss|Urteil)\s+vom\s+"
        r"(\d{1,2})\.\s*(\w+)\s+(\d{4})\s*-\s*(.+)"
    ),
    # Inverted: "TYPE des SächsVerfGH vom DD. MONTH YYYY - FILE"
    re.compile(
        r"(Beschluss|Urteil)\s+des\s+S.chsVerfGH\s+vom\s+"
        r"(\d{1,2})\.\s*(\w+)\s+(\d{4})\s*-\s*(.+)"
    ),
    # No court prefix: "TYPE vom DD. MONTH YYYY - FILE"
    re.compile(
        r"(Beschluss|Urteil)\s+vom\s+"
        r"(\d{1,2})\.\s*(\w+)\s+(\d{4})\s*-\s*(.+)"
    ),
]


def _parse_verfgh_date(day: str, month_name: str, year: str) -> str:
    """Convert German date parts to YYYY-MM-DD."""
    month = GERMAN_MONTHS.get(month_name, "01")
    return f"{year}-{month}-{int(day):02d}"


class SnVerfghCaseProvider(ScraperBaseClient, CaseProvider):
    """Fetches case law from the Sachsen VerfGH decision database.

    Uses AJAX POST to /esaver/answers.php. All results are returned
    in a single response (no pagination). Content is PDF-only.

    Args:
        date_from: Only include decisions on or after this date (YYYY-MM-DD).
        date_to: Only include decisions on or before this date (YYYY-MM-DD).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "Verfassungsgerichtshof Sachsen",
        "homepage": "https://www.justiz.sachsen.de/esaver/",
    }

    def __init__(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
    ):
        super().__init__(base_url=SN_VERFGH_BASE_URL, request_delay=request_delay)
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    def _search(self) -> str:
        """POST search and return HTML fragment with results."""
        data = {
            "funkt": "search",
            "verfart": "",
            "feldnorm": "",
            "feldgrund": "",
            "akz": "",
            "stichwort": "",
            "entschart": "",
            "ecli": "",
            "datumvon": self.date_from or "1990-01-01",
            "datumbis": self.date_to or "2099-12-31",
        }
        resp = self._post(SEARCH_ENDPOINT, data=data)
        return resp.text

    def _parse_results(self, html: str) -> list[dict]:
        """Parse search result HTML fragment and return case metadata."""
        tree = lxml.html.fromstring(html)
        entries = []

        rows = tree.xpath("//table[@id='tEntschList']//tr")
        if not rows:
            # Fallback: try all <h4> tags
            rows = tree.xpath("//tr")

        i = 0
        while i < len(rows):
            row = rows[i]
            h4_elements = row.xpath(".//h4")
            if not h4_elements:
                i += 1
                continue

            h4_text = h4_elements[0].text_content().strip()
            match = None
            for pattern in _H4_PATTERNS:
                match = pattern.match(h4_text)
                if match:
                    break
            if not match:
                logger.debug("Could not parse h4: %s", h4_text)
                i += 1
                continue

            case_type, day, month_name, year, file_number = match.groups()
            date = _parse_verfgh_date(day, month_name, year)
            file_number = file_number.strip()

            # Extract PDF link
            pdf_link = None
            for link in row.xpath('.//a[contains(@href, ".pdf")]'):
                href = link.get("href", "")
                if href:
                    # Normalize relative URL
                    if href.startswith("./"):
                        href = href[2:]
                    pdf_link = href
                    break

            # Extract abstract: <p> text between h4 and PDF link,
            # or <p id="lst_N"> for Leitsatz
            abstract = None
            p_elements = row.xpath(".//td/p")
            for p in p_elements:
                p_id = p.get("id", "")
                # Leitsatz paragraph (hidden by default but contains full text)
                if p_id.startswith("lst_"):
                    text = p.text_content().strip()
                    if text:
                        abstract = text
                        break
                # Description paragraph (between h4 and PDF link)
                if p.find("a") is None and p.find("b") is None:
                    p_text = p.text_content().strip()
                    p_id_val = p.get("id", "")
                    if p_text and not p_id_val.startswith("ls_"):
                        # This might be a short description
                        if abstract is None:
                            abstract = p_text

            entry = {
                "file_number": file_number,
                "date": date,
                "type": case_type,
                "court_name": "Verfassungsgerichtshof des Freistaates Sachsen",
                "pdf_link": pdf_link,
            }
            if abstract:
                entry["abstract"] = abstract

            entries.append(entry)
            i += 1

        return entries

    def get_cases(self) -> list[dict]:
        """Search VerfGH and fetch PDFs for content."""
        cases: list[dict] = []

        logger.info("Searching Sachsen VerfGH...")
        try:
            html = self._search()
        except requests.RequestException as exc:
            logger.error("Search failed: %s", exc)
            return cases

        entries = self._parse_results(html)
        logger.info("Found %d decision(s)", len(entries))

        if self.limit and len(entries) > self.limit:
            entries = entries[: self.limit]

        for entry in entries:
            pdf_link = entry.pop("pdf_link", None)
            if not pdf_link:
                logger.debug("No PDF link for %s, skipping", entry["file_number"])
                continue

            pdf_url = f"{SN_VERFGH_BASE_URL}/{pdf_link}"
            try:
                content = self._extract_text_from_pdf(pdf_url)
            except Exception as exc:
                logger.warning(
                    "Failed to extract PDF for %s: %s", entry["file_number"], exc
                )
                continue

            if not content or len(content) < 10:
                logger.debug("No content for %s, skipping", entry["file_number"])
                continue

            entry["content"] = content
            cases.append(entry)

            if self.limit and len(cases) >= self.limit:
                break

        return cases
