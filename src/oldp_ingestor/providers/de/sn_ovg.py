"""Provider for Sächsisches Oberverwaltungsgericht Bautzen.

Fetches case law from justiz.sachsen.de/ovgentschweb/.
Simple PHP app: POST search returns all results on one page,
document detail pages have structured metadata, content is PDF.
"""

import logging
import re

import lxml.html
import requests

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

SN_OVG_BASE_URL = "https://www.justiz.sachsen.de/ovgentschweb"


class SnOvgCaseProvider(ScraperBaseClient, CaseProvider):
    """Fetches case law from the Sachsen OVG decision database.

    Uses POST-based search. All results are returned on a single page
    (no server-side pagination). Document details and PDF links are
    fetched per document.

    Args:
        date_from: Only include decisions on or after this date (YYYY-MM-DD).
        date_to: Only include decisions on or before this date (YYYY-MM-DD).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "Sächsisches Oberverwaltungsgericht",
        "homepage": "https://www.justiz.sachsen.de/ovgentschweb/",
    }

    def __init__(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
    ):
        super().__init__(base_url=SN_OVG_BASE_URL, request_delay=request_delay)
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    def _build_datum_param(self) -> str:
        """Build the datum form parameter for date range filtering.

        The OVG search accepts dates in formats: DD.MM.YYYY, MM.YYYY, YYYY,
        and ranges as 'FROM-TO' (e.g. '1.1.2025-31.12.2025').
        """
        if self.date_from and self.date_to:
            d_from = self._iso_to_german(self.date_from)
            d_to = self._iso_to_german(self.date_to)
            return f"{d_from}-{d_to}"
        if self.date_from:
            d_from = self._iso_to_german(self.date_from)
            return f"{d_from}-31.12.2099"
        if self.date_to:
            d_to = self._iso_to_german(self.date_to)
            return f"1.1.1990-{d_to}"
        return ""

    @staticmethod
    def _iso_to_german(iso_date: str) -> str:
        """Convert YYYY-MM-DD to D.M.YYYY."""
        parts = iso_date.split("-")
        if len(parts) == 3:
            return f"{int(parts[2])}.{int(parts[1])}.{parts[0]}"
        return iso_date

    def _search(self) -> list[str]:
        """POST search and extract document IDs from results.

        Returns list of numeric document IDs.
        """
        data = {"aktenzeichen": "*"}
        datum = self._build_datum_param()
        if datum:
            data["datum"] = datum

        resp = self._post("/searchlist.phtml", data=data)
        text = resp.text

        # Extract IDs from popupDocument('ID') calls
        ids = re.findall(r"popupDocument\('(\d+)'\)", text)
        return ids

    def _fetch_document(self, doc_id: str) -> dict | None:
        """Fetch document detail page and extract case data."""
        url = f"/document.phtml?id={doc_id}"
        try:
            resp = self._get(url)
        except requests.RequestException as exc:
            logger.warning("Failed to fetch document %s: %s", doc_id, exc)
            return None

        tree = lxml.html.fromstring(resp.text)

        # Header: bold div with court, type, file_number on separate lines
        header_div = tree.xpath('//td[@class="schattiert gross"]//div[@style]')
        if not header_div:
            logger.warning("No header found for document %s", doc_id)
            return None

        from lxml import etree

        header_html = etree.tostring(header_div[0], encoding="unicode", method="html")
        parts = re.split(r"<br\s*/?>", header_html, flags=re.IGNORECASE)
        header_lines = [self.strip_tags(p).strip() for p in parts]
        header_lines = [line for line in header_lines if line]

        if len(header_lines) < 3:
            logger.warning(
                "Incomplete header for document %s: %s", doc_id, header_lines
            )
            return None

        court_name = header_lines[0]
        case_type = header_lines[1]
        file_number = header_lines[2]

        # Date: right-aligned TD in the header table
        date_cells = tree.xpath(
            '//td[@class="schattiert gross"]//td[@align="right"]/text()'
        )
        date_raw = date_cells[0].strip() if date_cells else ""
        date = self.parse_german_date(date_raw) if date_raw else ""

        # Client-side date filtering
        if self.date_from and date and date < self.date_from:
            return None
        if self.date_to and date and date > self.date_to:
            return None

        # Leitsatz: text after "Leitsatz:" in the dedicated table row
        abstract = None
        leitsatz_cells = tree.xpath("//table/td")
        for cell in leitsatz_cells:
            cell_text = cell.text_content().strip()
            if cell_text.startswith("Leitsatz:"):
                abstract_text = cell_text[len("Leitsatz:") :].strip()
                if abstract_text:
                    abstract = abstract_text

        # PDF link: <a href="documents/XXX.pdf">
        pdf_link = None
        for link in tree.xpath('//a[contains(@href, "documents/")]'):
            href = link.get("href", "")
            if href.endswith(".pdf"):
                pdf_link = href
                break

        # Fetch PDF content
        content = ""
        if pdf_link:
            pdf_url = f"{SN_OVG_BASE_URL}/{pdf_link}"
            try:
                content = self._extract_text_from_pdf(pdf_url)
            except Exception as exc:
                logger.warning("Failed to extract PDF for %s: %s", file_number, exc)

        if not content or len(content) < 10:
            logger.debug("No content for document %s, skipping", doc_id)
            return None

        case: dict = {
            "court_name": court_name,
            "file_number": file_number,
            "date": date,
            "content": content,
        }

        if case_type:
            case["type"] = case_type
        if abstract:
            case["abstract"] = abstract

        return case

    def get_cases(self) -> list[dict]:
        """Search OVG and fetch individual document pages."""
        cases: list[dict] = []

        logger.info("Searching OVG Bautzen...")
        try:
            doc_ids = self._search()
        except requests.RequestException as exc:
            logger.error("Search failed: %s", exc)
            return cases

        logger.info("Found %d document(s)", len(doc_ids))

        if self.limit and len(doc_ids) > self.limit:
            doc_ids = doc_ids[: self.limit]

        for doc_id in doc_ids:
            try:
                case = self._fetch_document(doc_id)
            except Exception as exc:
                logger.warning("Failed to process document %s: %s", doc_id, exc)
                continue

            if case is not None:
                cases.append(case)

            if self.limit and len(cases) >= self.limit:
                break

        return cases
