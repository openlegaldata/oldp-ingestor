"""Provider for EUR-Lex (European Court of Justice case law).

Fetches case law from EUR-Lex using the CELLAR SPARQL endpoint for ECLI search
and EUR-Lex HTML/XML detail pages for metadata and content.

Three-step pipeline:
1. SPARQL query to CELLAR for ECLI identifiers (paginated)
2. XML detail page per ECLI -> metadata (date, title, file_number, type)
3. HTML full text per ECLI -> content

The SPARQL endpoint (publications.europa.eu) is public and requires no
authentication, unlike the legacy SOAP web service.
"""

import logging
import re
from urllib.parse import urljoin

import lxml.html
from lxml import etree

from oldp_ingestor.providers.base import CaseProvider
from oldp_ingestor.providers.scraper_common import ScraperBaseClient

logger = logging.getLogger(__name__)

EURLEX_BASE_URL = "https://eur-lex.europa.eu"
CELLAR_SPARQL_URL = "https://publications.europa.eu/webapi/rdf/sparql"
EURLEX_SPARQL_PAGE_SIZE = 100
EURLEX_MIN_CONTENT_LEN = 10

# CELEX sector 6 type code -> German case type name
_CELEX_TYPE_NAMES = {
    "CJ": "Urteil",
    "CO": "Beschluss",
    "CC": "Schlussantrag des Generalanwalts",
    "CS": "Pfändung",
    "CT": "Drittwiderspruch",
    "CV": "Gutachten",
    "CX": "Beschluss",
    "CD": "Entscheidung",
    "CP": "Stellungnahme",
    "CN": "Mitteilung neue Rechtssache",
    "CA": "Mitteilung Urteil",
    "CB": "Mitteilung Beschluss",
    "CU": "Mitteilung Gutachtenantrag",
    "CG": "Mitteilung: Gutachten",
    "TJ": "Urteil",
    "TO": "Beschluss",
    "TC": "Schlussantrag des Generalanwalts",
    "TT": "Drittwiderspruch",
    "TN": "Mitteilung neue Rechtssache",
    "TA": "Mitteilung Urteil",
    "TB": "Mitteilung Beschluss",
    "FJ": "Urteil",
    "FO": "Beschluss",
    "FT": "Drittwiderspruch",
    "FN": "Mitteilung neue Rechtssache",
    "FA": "Mitteilung Urteil",
    "FB": "Mitteilung Beschluss",
}

# Unicode non-breaking hyphen (U+2011) used in EUR-Lex file numbers
_UNICODE_DASH = chr(8209)


class EuCaseProvider(ScraperBaseClient, CaseProvider):
    """Fetches case law from EUR-Lex (European Court of Justice).

    Uses the CELLAR SPARQL endpoint for ECLI search and EUR-Lex HTML/XML
    endpoints for case details and content.

    Args:
        username: EUR-Lex web service username (unused, kept for CLI compat).
        password: EUR-Lex web service password (unused, kept for CLI compat).
        date_from: Optional start date filter (YYYY-MM-DD).
        date_to: Optional end date filter (YYYY-MM-DD).
        limit: Maximum number of cases to return.
        request_delay: Delay in seconds between requests.
    """

    SOURCE = {
        "name": "EUR-Lex",
        "homepage": "https://eur-lex.europa.eu/",
    }

    def __init__(
        self,
        username: str = "",
        password: str = "",
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        request_delay: float = 0.2,
    ):
        super().__init__(base_url=EURLEX_BASE_URL, request_delay=request_delay)
        self.username = username
        self.password = password
        self.date_from = date_from or ""
        self.date_to = date_to or ""
        self.limit = limit

    def _build_sparql_query(self, limit: int, offset: int) -> str:
        """Build SPARQL query for ECLI search with optional date filters."""
        filters = []
        if self.date_from:
            filters.append(f'FILTER(?date >= "{self.date_from}"^^xsd:date)')
        if self.date_to:
            filters.append(f'FILTER(?date <= "{self.date_to}"^^xsd:date)')
        filter_clause = "\n  ".join(filters)

        return f"""\
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?ecli ?date
WHERE {{
  ?work cdm:case-law_ecli ?ecli .
  ?work cdm:work_date_document ?date .
  {filter_clause}
}}
ORDER BY DESC(?date)
LIMIT {limit}
OFFSET {offset}"""

    def _search_eclis(self) -> list[str]:
        """Search CELLAR SPARQL endpoint for ECLI identifiers.

        Returns list of ECLI strings (e.g. 'ECLI:EU:C:2024:123').
        """
        eclis: list[str] = []
        offset = 0
        effective_limit = self.limit or 10000  # EUR-Lex max is 10000

        while len(eclis) < effective_limit:
            page_size = min(EURLEX_SPARQL_PAGE_SIZE, effective_limit - len(eclis))
            query = self._build_sparql_query(page_size, offset)

            resp = self._get(
                CELLAR_SPARQL_URL,
                params={"query": query, "format": "application/json"},
            )

            data = resp.json()
            bindings = data.get("results", {}).get("bindings", [])

            if not bindings:
                break

            page_eclis = [b["ecli"]["value"] for b in bindings if "ecli" in b]
            eclis.extend(page_eclis)

            logger.info(
                "SPARQL page (offset %d): found %d ECLIs (total %d)",
                offset,
                len(page_eclis),
                len(eclis),
            )

            if len(page_eclis) < page_size:
                break  # last page

            offset += page_size

        if self.limit and len(eclis) > self.limit:
            eclis = eclis[: self.limit]

        return eclis

    def _fetch_case_details(self, ecli: str) -> dict | None:
        """Fetch XML detail page for an ECLI and extract metadata.

        Returns dict with date, title, file_number, type, ecli or None on failure.
        """
        xml_url = f"{EURLEX_BASE_URL}/legal-content/DE/TXT/XML/?uri=ECLI:{ecli}"

        try:
            resp = self._get(xml_url)
        except Exception as exc:
            logger.warning("Failed to fetch XML for %s: %s", ecli, exc)
            return None

        try:
            return _parse_case_details_from_xml(resp.content, ecli)
        except Exception as exc:
            logger.warning("Failed to parse XML for %s: %s", ecli, exc)
            return None

    def _fetch_case_content(self, ecli: str) -> str | None:
        """Fetch HTML full text for an ECLI and extract body content.

        Returns HTML body string or None on failure.
        """
        html_url = f"{EURLEX_BASE_URL}/legal-content/DE/TXT/HTML/?uri=ECLI:{ecli}"

        try:
            resp = self._get(html_url)
        except Exception as exc:
            logger.warning("Failed to fetch HTML for %s: %s", ecli, exc)
            return None

        try:
            return _extract_html_content(resp.text, html_url)
        except Exception as exc:
            logger.warning("Failed to parse HTML for %s: %s", ecli, exc)
            return None

    def get_cases(self) -> list[dict]:
        """Fetch cases from EUR-Lex: SPARQL search -> XML details -> HTML content."""
        cases: list[dict] = []

        eclis = self._search_eclis()
        logger.info("Found %d ECLI(s) to process.", len(eclis))

        for ecli in eclis:
            details = self._fetch_case_details(ecli)
            if details is None:
                continue

            content = self._fetch_case_content(ecli)
            if content is None:
                continue

            if len(content) < EURLEX_MIN_CONTENT_LEN:
                logger.warning(
                    "Skipping %s: content too short (%d chars)", ecli, len(content)
                )
                continue

            case: dict = {
                "court_name": "Europäischer Gerichtshof",
                "file_number": details["file_number"],
                "date": details["date"],
                "content": content,
                "ecli": ecli,
            }

            if details.get("title"):
                case["title"] = details["title"]
            if details.get("type"):
                case["type"] = details["type"]

            cases.append(case)

            if self.limit and len(cases) >= self.limit:
                break

        return cases


def _parse_eclis_from_sparql(json_data: dict) -> list[str]:
    """Parse ECLI values from a SPARQL JSON response."""
    bindings = json_data.get("results", {}).get("bindings", [])
    return [b["ecli"]["value"] for b in bindings if "ecli" in b]


def _parse_case_details_from_xml(xml_bytes: bytes, ecli: str) -> dict:
    """Parse case metadata from EUR-Lex XML detail page.

    Returns dict with keys: date, title, file_number, type, ecli.
    Raises on missing required fields.
    """
    tree = etree.fromstring(xml_bytes)

    # Date
    date_values = tree.xpath("//WORK_DATE_DOCUMENT/VALUE/text()")
    date = date_values[0] if date_values else ""

    # Title — find EXPRESSION_TITLE matching DEU language
    title = _extract_title(tree)

    # File number — from title regex or SAMEAS fallback
    file_number = _extract_file_number(tree, title)

    # Case type from CELEX number
    case_type = ""
    celex_values = tree.xpath("//RESOURCE_LEGAL_ID_CELEX/VALUE/text()")
    if celex_values:
        case_type = _get_case_type_from_celex(celex_values[0])

    return {
        "date": date,
        "title": title,
        "file_number": file_number,
        "type": case_type,
        "ecli": ecli,
    }


def _extract_title(tree) -> str:
    """Extract German title from XML tree, taking first part before '#'."""
    for expr_title in tree.xpath("//EXPRESSION_TITLE"):
        titles = expr_title.xpath("./VALUE/text()")
        langs = expr_title.getparent().xpath(
            "./EXPRESSION_USES_LANGUAGE/IDENTIFIER/text()"
        )
        if len(titles) == 1 and len(langs) == 1 and langs[0] == "DEU":
            # Title may contain '#'-separated parts; use only first
            return titles[0].split("#")[0].strip()
    return ""


def _extract_file_number(tree, title: str) -> str:
    """Extract file number from title regex or SAMEAS fallback."""
    # Pattern: letter + dash (ASCII or Unicode U+2011) + digits/digits
    dash_group = "(" + re.escape(_UNICODE_DASH) + "|-)"
    fn_pattern = re.compile("[A-Z]" + dash_group + r"([0-9]{1,5})/([0-9]{2})")

    # Try last '#'-part of full title for file number (legacy used last part)
    fn_matches = list(fn_pattern.finditer(title))

    if len(fn_matches) == 1:
        file_number = fn_matches[0].group(0)
    elif len(fn_matches) > 1:
        file_number = ",".join(m.group(0) for m in fn_matches)
    else:
        # Fallback: extract from SAMEAS/URI with type="case"
        file_number = ""
        for t in tree.xpath('//SAMEAS/URI/TYPE[text()="case"]'):
            ids = t.getparent().xpath("./IDENTIFIER/text()")
            if ids:
                file_number = ids[0]
                break

        if not file_number:
            logger.warning("Cannot extract file number from title: %s", title)
            file_number = ""

    # Replace Unicode non-breaking hyphen with ASCII dash
    return file_number.replace(_UNICODE_DASH, "-")


def _get_case_type_from_celex(celex: str) -> str:
    """Map CELEX number to German case type name.

    CELEX format: sector(1 digit) + year(4 digits) + type(1-2 letters) + ...
    Only sector 6 (case law) is supported.
    """
    match = re.search(r"^([0-9])([0-9]{4})([A-Z]{1,2})", celex)
    if not match:
        return ""

    sector = match.group(1)
    type_code = match.group(3)

    if sector != "6":
        logger.debug("Unsupported CELEX sector: %s", sector)
        return ""

    return _CELEX_TYPE_NAMES.get(type_code, "")


def _extract_html_content(html_text: str, source_url: str) -> str | None:
    """Extract <body> content from EUR-Lex HTML page with link processing."""
    # Some EUR-Lex pages include <?xml encoding=...?> declarations that
    # lxml.html.fromstring() rejects on str input. Strip them.
    cleaned = re.sub(r"<\?xml[^?]*\?>", "", html_text)
    tree = lxml.html.fromstring(cleaned)

    for body in tree.xpath("//body"):
        # Process links: make relative absolute, remove non-http/non-anchor
        for link in body.xpath(".//a[@href]"):
            href = link.attrib["href"]
            if href.startswith("#"):
                pass  # keep anchor links
            elif href.startswith("http"):
                pass  # keep absolute links
            elif href.startswith("."):
                link.attrib["href"] = urljoin(source_url, href)
            else:
                del link.attrib["href"]

        # Serialize body children as HTML
        return "".join(
            etree.tostring(child, encoding="unicode") for child in body.iterchildren()
        )

    return None
