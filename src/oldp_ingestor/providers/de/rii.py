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
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

RII_BASE_URL = "https://www.rechtsprechung-im-internet.de/jportal"

COURTS = ["bverfg", "bgh", "bverwg", "bfh", "bag", "bsg", "bpatg"]

# Sections in XML that contain the case content
CONTENT_TAGS = [
    ("tenor", "Tenor"),
    ("tatbestand", "Tatbestand"),
    ("entscheidungsgruende", "Entscheidungsgründe"),
    ("gruende", "Gründe"),
    ("abwmeinung", "Abw. Meinung"),
    ("sonstlt", "Sonstige Literatur"),
]


class RiiCaseProvider(ScraperBaseClient, CaseProvider):
    """Fetches case law from rechtsprechung-im-internet.de (federal courts).

    For each court, paginates through search results, downloads ZIP files
    containing XML decisions, and maps XML fields to OLDP case dicts.

    Args:
        court: Optional single court code (e.g. "bverfg"). If None, all courts.
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "Rechtsprechung im Internet (RII)",
        "homepage": "https://www.rechtsprechung-im-internet.de",
    }

    def __init__(
        self,
        court: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
    ):
        super().__init__(base_url=RII_BASE_URL, request_delay=request_delay)
        self.court = court
        self.limit = limit

    def _get_page_url(self, page: int, court: str, per_page: int = 26) -> str:
        """Build listing URL for a court's decisions."""
        offset = (page - 1) * per_page
        return (
            f"{self.base_url}/portal/t/xs6/page/bsjrsprod.psml"
            f"/js_peid/Suchportlet1?action=portlets.jw.MainAction"
            f"&eventSubmit_doNavigate=searchInSubtree&p1={court}"
            f"&currentNavigationPosition={offset}"
        )

    def _get_ids_from_page(self, url: str) -> list[str]:
        """Extract doc IDs from search results page."""
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

    def get_cases(self) -> list[dict]:
        """Paginate listings, download ZIP/XML for each case."""
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
                        return cases

                page += 1

        return cases
