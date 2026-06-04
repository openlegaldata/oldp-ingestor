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

    Supports server-side date filtering via the ``datum`` POST parameter
    in year-only format (YYYY or YYYY-YYYY range). The portal does not
    support day-level server-side filtering, so day-level precision is
    applied client-side via ``_is_within_date_range``.

    Args:
        date_from: Only include decisions on or after this date (YYYY-MM-DD).
            Year is extracted and sent as ``datum`` POST parameter for
            server-side filtering. Day-level filtering applied client-side.
        date_to: Only include decisions on or before this date (YYYY-MM-DD).
            Year is extracted and sent as ``datum`` POST parameter for
            server-side filtering. Day-level filtering applied client-side.
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
        proxy: str | None = None,
    ):
        super().__init__(
            base_url=SN_OVG_BASE_URL, request_delay=request_delay, proxy=proxy
        )
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    def _build_datum_param(self) -> str:
        """Build the datum form parameter for date range filtering.

        The OVG search supports YYYY and YYYY-YYYY year ranges.
        DD.MM.YYYY-DD.MM.YYYY ranges return 0 results (not supported).
        """
        from_year = self.date_from[:4] if self.date_from else ""
        to_year = self.date_to[:4] if self.date_to else ""

        if from_year and to_year:
            if from_year == to_year:
                return from_year
            return f"{from_year}-{to_year}"
        if from_year:
            # Just the year — the search interprets this as "from this year onwards"
            return from_year
        if to_year:
            return f"1990-{to_year}"
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

        # Extract IDs from popupDocument('ID') calls — deduplicate preserving order
        seen: set[str] = set()
        ids: list[str] = []
        for m in re.finditer(r"popupDocument\('(\d+)'\)", text):
            doc_id = m.group(1)
            if doc_id not in seen:
                seen.add(doc_id)
                ids.append(doc_id)
        return ids

    def _fetch_document(self, doc_id: str) -> tuple[dict | None, bool, bool]:
        """Fetch document detail page and extract case data.

        Returns ``(case_or_None, permanent_failure, out_of_window)``:
          * ``case_or_None``: parsed case dict or None
          * ``permanent_failure``: True when the URL is structurally broken
            and should be fed to the FailureTracker (vs a transient
            network/PDF error that should be retried next run).
          * ``out_of_window``: True when the document parsed fine but its
            decision date sits outside the configured ``date_from`` /
            ``date_to`` window — the caller surfaces this in an end-of-run
            summary so "Found 0 case(s)" no longer hides what happened.
        """
        url = f"/document.phtml?id={doc_id}"
        try:
            resp = self._get(url)
        except requests.RequestException as exc:
            logger.warning("Failed to fetch document %s: %s", doc_id, exc)
            return None, False, False  # network → transient

        tree = lxml.html.fromstring(resp.text)

        # Header: bold div with court, type, file_number on separate lines
        header_div = tree.xpath('//td[@class="schattiert gross"]//div[@style]')
        if not header_div:
            logger.warning("No header found for document %s", doc_id)
            return None, True, False

        from lxml import etree

        header_html = etree.tostring(header_div[0], encoding="unicode", method="html")
        parts = re.split(r"<br\s*/?>", header_html, flags=re.IGNORECASE)
        header_lines = [self.strip_tags(p).strip() for p in parts]
        header_lines = [line for line in header_lines if line]

        if len(header_lines) < 3:
            logger.warning(
                "Incomplete header for document %s: %s", doc_id, header_lines
            )
            return None, True, False

        court_name = header_lines[0]
        case_type = header_lines[1]
        file_number = header_lines[2]

        # Date: right-aligned TD in the header table
        date_cells = tree.xpath(
            '//td[@class="schattiert gross"]//td[@align="right"]/text()'
        )
        date_raw = date_cells[0].strip() if date_cells else ""
        date = self.parse_german_date(date_raw) if date_raw else ""

        # Client-side date filtering — out-of-window is not a failure
        if self.date_from and date and date < self.date_from:
            return None, False, True
        if self.date_to and date and date > self.date_to:
            return None, False, True

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
        pdf_failed = False
        if pdf_link:
            pdf_url = f"{SN_OVG_BASE_URL}/{pdf_link}"
            try:
                content = self._extract_text_from_pdf(pdf_url)
            except requests.RequestException as exc:
                # Upstream 5xx / network — transient, retry next run.
                logger.warning("Failed to extract PDF for %s: %s", file_number, exc)
                pdf_failed = True
            except Exception as exc:
                logger.warning("Failed to extract PDF for %s: %s", file_number, exc)

        if not content or len(content) < 10:
            logger.debug("No content for document %s, skipping", doc_id)
            # If the PDF was a network failure leave it transient; otherwise
            # the document page genuinely has no/short content (permanent).
            return None, not pdf_failed, False

        case: dict = {
            "court_name": court_name,
            "file_number": file_number,
            "date": date,
            "content": content,
            # PDF is where content came from if present, otherwise the doc page
            "source_url": pdf_url if pdf_link else f"{SN_OVG_BASE_URL}{url}",
        }

        if case_type:
            case["type"] = case_type
        if abstract:
            case["abstract"] = abstract

        return case, False, False

    def get_cases(self) -> list[dict]:
        """Search OVG and fetch individual document pages."""
        cases: list[dict] = []
        date_filtered = 0

        logger.info("Searching OVG Bautzen...")
        try:
            doc_ids = self._search()
        except requests.RequestException as exc:
            logger.error("Search failed: %s", exc)
            return cases

        logger.info("Found %d document(s)", len(doc_ids))

        for doc_id in doc_ids:
            if self.failure_tracker.should_skip(doc_id):
                continue

            try:
                case, permanent_failure, out_of_window = self._fetch_document(doc_id)
            except Exception as exc:
                logger.warning("Failed to process document %s: %s", doc_id, exc)
                self.failure_tracker.record_failure(doc_id, exc)
                continue

            if case is not None:
                self.failure_tracker.record_success(doc_id)
                cases.append(case)
            elif out_of_window:
                date_filtered += 1
            elif permanent_failure:
                self.failure_tracker.record_failure(
                    doc_id, "structural failure parsing document"
                )

            if self.limit and len(cases) >= self.limit:
                break

        # Surface the date-filter outcome — without this, "Found 0 case(s)"
        # at the end of the run looks indistinguishable from a parsing
        # failure even though the OVG search only supports year-level
        # ``datum`` filtering and day-level pruning happens here.
        if date_filtered:
            logger.info(
                "Excluded %d/%d document(s) by date range (date_from=%s date_to=%s)",
                date_filtered,
                len(doc_ids),
                self.date_from or "-",
                self.date_to or "-",
            )

        return cases
