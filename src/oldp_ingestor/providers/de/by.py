"""Provider for gesetze-bayern.de (Bavaria).

Fetches case law from the Bavarian legal database.
Data format: ZIP files containing XML documents (ISO-8859-1 encoding).
"""

import logging
import re

import requests
from lxml import etree

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

BY_BASE_URL = "https://www.gesetze-bayern.de"

# Court name normalization: Bavaria XML uses short names like "VGH München"
# that the OLDP court resolver can't match. Map to full names used in the DB.
_COURT_NAME_MAP = {
    "VGH München": "Bayerischer Verwaltungsgerichtshof",
    "BayObLG München": "Bayerisches Oberstes Landesgericht",
    "VerfGH München": "Bayerischer Verfassungsgerichtshof",
}

# Case type abbreviation expansion
_TYPE_MAP = {
    "Bes": "Beschluss",
    "Urt": "Urteil",
    "Ent": "Entscheidung",
}

CONTENT_TAGS = [
    ("tenor", "Tenor"),
    ("tatbestand", "Tatbestand"),
    ("entschgruende", "Entscheidungsgründe"),
    ("gruende", "Gründe"),
    ("abwmeinung", "Abw. Meinung"),
    ("sonstlt", "Sonstige Literatur"),
]


def _expand_case_type(type_str: str) -> str:
    """Expand abbreviation: Bes->Beschluss, Urt->Urteil, etc."""
    result = type_str
    for abbr, full in _TYPE_MAP.items():
        result = result.replace(abbr, full)
    return result


class ByCaseProvider(ScraperBaseClient, CaseProvider):
    """Fetches case law from gesetze-bayern.de.

    Uses session-based navigation with cookie state. Pages through
    search results, downloads ZIP/XML for each case.

    Supports server-side date filtering via POST to ``/Search`` with
    ``SearchFields.DatumVon`` / ``SearchFields.DatumBis`` form fields
    (DD.MM.YYYY format). The POST also requires a
    ``__RequestVerificationToken`` (ASP.NET CSRF token) extracted from
    the search form HTML.

    Args:
        date_from: Optional start date (YYYY-MM-DD). Sent as
            ``SearchFields.DatumVon`` (DD.MM.YYYY) for server-side filtering.
        date_to: Optional end date (YYYY-MM-DD). Sent as
            ``SearchFields.DatumBis`` (DD.MM.YYYY) for server-side filtering.
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {"name": "Gesetze Bayern", "homepage": "https://www.gesetze-bayern.de"}

    def __init__(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
        proxy: str | None = None,
    ):
        super().__init__(base_url=BY_BASE_URL, request_delay=request_delay, proxy=proxy)
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    @staticmethod
    def _to_german_date(iso_date: str) -> str:
        """Convert YYYY-MM-DD to DD.MM.YYYY."""
        parts = iso_date.split("-")
        if len(parts) == 3:
            return f"{parts[2]}.{parts[1]}.{parts[0]}"
        return iso_date

    def _init_search_session(self) -> None:
        """Initialize session and submit search with optional date filter.

        Sets the DOKTYP=rspr filter, then if dates are set, fetches the
        search form to extract the CSRF token and POSTs with date fields.
        """
        self._get("/Search/Filter/DOKTYP/rspr")

        if not self.date_from and not self.date_to:
            return

        # Fetch search form to get CSRF token
        import lxml.html

        resp = self._get("/Search/Hitlist")
        tree = lxml.html.fromstring(resp.text)
        tokens = tree.xpath('//input[@name="__RequestVerificationToken"]/@value')
        token = tokens[0] if tokens else ""

        # Submit search with date fields
        logger.info(
            "Submitting BY search with dates: %s to %s",
            self.date_from,
            self.date_to,
        )
        self._post(
            "/Search",
            data={
                "SearchFields.Content": "",
                "SearchFields.DatumVon": (
                    self._to_german_date(self.date_from) if self.date_from else ""
                ),
                "SearchFields.DatumBis": (
                    self._to_german_date(self.date_to) if self.date_to else ""
                ),
                "SearchFields.Aktenzeichen": "",
                "SearchFields.Norm": "",
                "__RequestVerificationToken": token,
            },
        )

    def _get_ids_from_page(self, page: int) -> list[str]:
        """Fetch page and extract document IDs via regex."""
        url = f"{self.base_url}/Search/Page/{page}"
        resp = self._get(url)

        if "/Search/Hitlist" not in resp.url:
            logger.warning("Unexpected redirect to %s", resp.url)
            return []

        return re.findall(r"/Content/Document/(.*?)\?hl=true", resp.text)

    def _get_zip_url(self, doc_id: str) -> str:
        """ZIP download URL for a document."""
        return f"{self.base_url}/Content/Zip/{doc_id}"

    def _parse_case_from_xml(self, xml_str: str, source_url: str) -> dict | None:
        """Parse Bavaria XML (ISO-8859-1) and return OLDP case dict."""
        # XML comes from ZIP decoded as iso-8859-1, so it's now a Unicode string.
        # lxml needs bytes with matching encoding declaration, or we can re-encode
        # as UTF-8 and strip the encoding declaration so lxml parses correctly.
        xml_bytes = xml_str.encode("utf-8")
        # Remove encoding declaration that says iso-8859-1 since we re-encoded to utf-8
        xml_bytes = xml_bytes.replace(b'encoding="iso-8859-1"', b'encoding="utf-8"')
        tree = etree.fromstring(xml_bytes)

        court_type = self._xpath_text(tree, "//metadaten/gericht/gertyp", default="")
        court_location = self._xpath_text(
            tree, "//metadaten/gericht/gerort", default=""
        )
        court_name = f"{court_type} {court_location}".strip()
        court_name = _COURT_NAME_MAP.get(court_name, court_name)

        case_type_raw = self._xpath_text(tree, "//metadaten/doktyp", default="")
        case_type = _expand_case_type(case_type_raw)

        # Date format is already YYYY-MM-DD in Bavaria XML
        date = self._xpath_text(tree, "//metadaten/entsch-datum", default="")

        file_number = self._xpath_text(tree, "//metadaten/aktenzeichen", default="")

        # Build content from textdaten sections
        content = self._build_content_html(
            tree, CONTENT_TAGS, xpath_tpl="//textdaten/{tag}/body"
        )

        # Build abstract from leitsatz
        abstract = self._build_content_html(
            tree,
            [("leitsatz", "Leitsatz")],
            xpath_tpl="//textdaten/{tag}/body",
            with_headline=False,
        )

        # Build title from titelzeile
        title_html = self._build_content_html(
            tree,
            [("titelzeile", "")],
            xpath_tpl="//textdaten/{tag}/body",
            with_headline=False,
        )
        title = self.strip_tags(title_html).strip() if title_html else ""

        if not content.strip():
            return None

        case: dict = {
            "court_name": court_name,
            "file_number": file_number,
            "date": date,
            "content": content,
        }

        if case_type:
            case["type"] = case_type
        if title:
            case["title"] = title
        if abstract.strip():
            case["abstract"] = abstract

        return case

    def get_cases(self) -> list[dict]:
        """Paginate through search results and download cases."""
        cases: list[dict] = []

        self._init_search_session()

        page = 1
        empty_pages = 0

        while True:
            try:
                ids = self._get_ids_from_page(page)
            except requests.RequestException as exc:
                logger.warning("Failed to fetch page %d: %s", page, exc)
                break

            if not ids:
                empty_pages += 1
                if empty_pages >= 2:
                    break
                page += 1
                continue

            empty_pages = 0
            logger.info("Page %d: found %d doc IDs", page, len(ids))

            for doc_id in ids:
                zip_url = self._get_zip_url(doc_id)
                try:
                    xml_str = self._get_xml_from_zip(zip_url, encoding="iso-8859-1")
                except requests.RequestException as exc:
                    logger.warning("Failed to download ZIP for %s: %s", doc_id, exc)
                    continue

                if xml_str is None:
                    continue

                source_url = f"{self.base_url}/Content/Document/{doc_id}"
                try:
                    case = self._parse_case_from_xml(xml_str, source_url)
                except Exception as exc:
                    logger.warning("Failed to parse XML for %s: %s", doc_id, exc)
                    continue

                if case is not None:
                    cases.append(case)

                if self.limit and len(cases) >= self.limit:
                    return cases

            page += 1

        return cases
