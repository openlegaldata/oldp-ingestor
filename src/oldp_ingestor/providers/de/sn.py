"""Provider for Sachsen ESAMOSplus (Scopeland/ASP.NET).

Fetches case law from justiz.sachsen.de/esamosplus/.
Covers OLG Dresden + 3 Landgerichte + 11 Amtsgerichte.

Uses Playwright for browser automation due to ASP.NET ViewState/PostBack
complexity. Content is PDF-only.
"""

import logging
import re
import time

import lxml.html

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.playwright_client import PlaywrightBaseClient

logger = logging.getLogger(__name__)

SN_BASE_URL = "https://www.justiz.sachsen.de/esamosplus"
SEARCH_URL = f"{SN_BASE_URL}/pages/suchen.aspx"

# Court dropdown values (DV1_C39)
COURTS = {
    "Oberlandesgericht Dresden": "1012",
    "Amtsgericht Stollberg": "1015",
    "Amtsgericht Döbeln": "1017",
    "Amtsgericht Bautzen": "1018",
    "Amtsgericht Dresden": "1019",
    "Landgericht Dresden": "1020",
    "Amtsgericht Dippoldiswalde": "1021",
    "Amtsgericht Meißen": "1022",
    "Amtsgericht Pirna": "1023",
    "Amtsgericht Riesa": "1024",
    "Amtsgericht Leipzig": "1025",
    "Landgericht Leipzig": "1026",
    "Amtsgericht Eilenburg": "1027",
    "Amtsgericht Torgau": "1028",
    "Landgericht Zwickau": "1029",
}

# Reverse map: value -> court name
_COURT_BY_VALUE = {v: k for k, v in COURTS.items()}


class SnCaseProvider(PlaywrightBaseClient, CaseProvider):
    """Fetches case law from ESAMOSplus (Saxon ordinary courts).

    Uses Playwright to handle ASP.NET WebForms ViewState/PostBack.
    Content is PDF-only; text is extracted via pymupdf.

    Args:
        court: Filter to single court name (e.g. "Oberlandesgericht Dresden").
        date_from: Only include decisions on or after this date (YYYY-MM-DD).
        date_to: Only include decisions on or before this date (YYYY-MM-DD).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between page loads.
    """

    SOURCE = {
        "name": "ESAMOSplus Sachsen",
        "homepage": "https://www.justiz.sachsen.de/esamosplus/",
    }

    def __init__(
        self,
        court: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.5,
    ):
        super().__init__(request_delay=request_delay)
        self.court = court
        self.date_from = date_from
        self.date_to = date_to
        self.limit = limit

    @staticmethod
    def _iso_to_german(iso_date: str) -> str:
        """Convert YYYY-MM-DD to DD.MM.YYYY."""
        parts = iso_date.split("-")
        if len(parts) == 3:
            return f"{parts[2]}.{parts[1]}.{parts[0]}"
        return iso_date

    @staticmethod
    def _german_to_iso(german_date: str) -> str:
        """Convert DD.MM.YYYY to YYYY-MM-DD."""
        parts = german_date.strip().split(".")
        if len(parts) == 3:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return german_date

    def _extract_pdf_from_response(self, page) -> bytes | None:
        """Handle PDF download triggered by clicking a document button.

        The server responds with raw PDF bytes when a document button
        is clicked. We intercept the response.
        """
        # Not used in current approach — see get_cases for download handling
        return None

    def _parse_results_table(self, html: str, table_id: str) -> list[dict]:
        """Parse results table (DV16_Table or DV13_Table) into case metadata.

        Columns: 0=selector, 1=date (Col0), 2=file_number (Col1),
        3=court (Col2), 4=document (Col3).
        Data elements are either <span> (DV16) or <input type="submit"> (DV13).
        """
        tree = lxml.html.fromstring(html)
        rows = []

        # Find the table
        table = tree.xpath(f'//*[@id="{table_id}"]')
        if not table:
            logger.debug("Table %s not found in HTML", table_id)
            return rows

        for tr in table[0].xpath(".//tr"):
            cells = tr.xpath("td")
            if len(cells) < 5:
                continue

            # Extract date (Col0, cells[1])
            date_el = cells[1].xpath(
                ".//span[contains(@id,'_Col0_')] | "
                ".//input[@type='submit'][contains(@id,'_Col0_')]"
            )
            if not date_el:
                continue
            date_raw = (
                date_el[0].get("value", "") or date_el[0].text_content()
            ).strip()
            if not re.match(r"\d{2}\.\d{2}\.\d{4}", date_raw):
                continue

            date = self._german_to_iso(date_raw)

            # Extract file number (Col1, cells[2])
            az_el = cells[2].xpath(
                ".//span[contains(@id,'_Col1_')] | "
                ".//input[@type='submit'][contains(@id,'_Col1_')]"
            )
            file_number = ""
            abstract = None
            if az_el:
                file_number = (
                    az_el[0].get("value", "") or az_el[0].text_content()
                ).strip()
                title_attr = az_el[0].get("title", "")
                if title_attr:
                    # Strip "Leitsatz:" prefix if present
                    abstract = re.sub(r"^Leitsatz:\s*", "", title_attr.strip())

            # Extract court name (Col2, cells[3])
            court_el = cells[3].xpath(
                ".//span[contains(@id,'_Col2_')] | "
                ".//input[@type='submit'][contains(@id,'_Col2_')]"
            )
            court_name = ""
            if court_el:
                court_name = (
                    court_el[0].get("value", "") or court_el[0].text_content()
                ).strip()

            # Extract document button name (Col3, cells[4])
            doc_btn = cells[4].xpath(".//input[@type='submit'][contains(@id,'_Col3_')]")
            doc_btn_name = doc_btn[0].get("name", "") if doc_btn else ""

            if not file_number or not court_name:
                continue

            entry = {
                "file_number": file_number,
                "date": date,
                "court_name": court_name,
                "doc_btn_name": doc_btn_name,
            }
            if abstract:
                entry["abstract"] = abstract

            rows.append(entry)

        return rows

    def get_cases(self) -> list[dict]:
        """Navigate ESAMOSplus, search, and extract cases."""
        import pymupdf

        cases: list[dict] = []
        self._ensure_browser()
        page = self._context.new_page()

        try:
            # Navigate to search page
            logger.info("Loading ESAMOSplus search page...")
            page.goto(SEARCH_URL, timeout=30000)
            page.wait_for_selector("#DV1_C24", timeout=15000)

            # Set court filter if specified
            if self.court:
                court_value = COURTS.get(self.court)
                if court_value:
                    page.select_option("#DV1_C39", court_value)
                    time.sleep(1)  # Wait for auto-postback

            # Set date range
            if self.date_from:
                page.fill("#DV1_C34", self._iso_to_german(self.date_from))
            if self.date_to:
                page.fill("#DV1_C35", self._iso_to_german(self.date_to))

            # Click search
            logger.info("Submitting search...")
            page.click("#DV1_C24")
            page.wait_for_load_state("networkidle", timeout=30000)

            # After search, results may appear in DV13_Table (search results)
            # or remain in DV16_Table (latest decisions). Check for DV13 first.
            if page.query_selector("#DV13_Table"):
                table_id = "DV13_Table"
            else:
                table_id = "DV16_Table"

            while True:
                if self.request_delay > 0:
                    time.sleep(self.request_delay)

                html = page.content()
                entries = self._parse_results_table(html, table_id)
                logger.info("Parsed %d entries from %s", len(entries), table_id)

                if not entries:
                    break

                for entry in entries:
                    doc_btn_name = entry.pop("doc_btn_name", "")
                    if not doc_btn_name:
                        continue

                    # Download PDF by clicking document button.
                    # The server responds with Content-Disposition: attachment,
                    # so the page stays on the results table (no navigation).
                    try:
                        with page.expect_download(timeout=30000) as download_info:
                            page.click(f'input[name="{doc_btn_name}"]')

                        download = download_info.value
                        pdf_path = download.path()
                        if pdf_path:
                            doc = pymupdf.open(str(pdf_path))
                            paragraphs = []
                            for pg in doc:
                                text = pg.get_text()
                                if text.strip():
                                    paragraphs.append(text)
                            doc.close()
                            if paragraphs:
                                content = "\n".join(f"<p>{t}</p>" for t in paragraphs)
                                if len(content) >= 10:
                                    entry["content"] = content
                    except Exception as exc:
                        logger.warning(
                            "Failed to download PDF for %s: %s",
                            entry["file_number"],
                            exc,
                        )
                        continue

                    if "content" not in entry:
                        continue

                    cases.append(entry)
                    if self.limit and len(cases) >= self.limit:
                        return cases

                # Try next page
                next_btn = page.query_selector(
                    'input[type="submit"][value="Vorwärts"]:not([disabled])'
                )
                if not next_btn:
                    break

                next_btn.click()
                page.wait_for_load_state("networkidle", timeout=30000)

        except Exception as exc:
            logger.error("ESAMOSplus scraping failed: %s", exc)
        finally:
            page.close()
            self.close()

        return cases
